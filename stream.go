// Package stream provides a lightweight, concurrency-safe event stream.
package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrClosed is returned when publishing to a closed stream or creating a
	// stream in a closed EventChannel.
	ErrClosed = errors.New("stream: closed")
	// ErrSlowSubscriber is returned when a subscriber's buffer is full.
	// Publish is atomic: no subscriber receives the event when this is returned.
	ErrSlowSubscriber = errors.New("stream: subscriber buffer full")
	// ErrNilEventChannel is returned when New is called without an EventChannel.
	ErrNilEventChannel = errors.New("stream: nil event channel")
	// ErrInvalidBufferSize is returned when New receives a buffer size less than one.
	ErrInvalidBufferSize = errors.New("stream: buffer size must be greater than zero")
	// ErrDuplicateStream is returned when an EventChannel[TStream] already owns a stream
	// for the requested event type.
	ErrDuplicateStream = errors.New("stream: event type already registered in channel")
	// ErrUnsetEventData is returned when publishing an event without SetData.
	ErrUnsetEventData = errors.New("stream: event data was not set")
)

type subscriber[T any] struct {
	ch   chan Event[T]
	done chan struct{}
	once sync.Once
}

// Subscription receives events published to a Stream.
type Subscription[TStream, TEvent any] struct {
	C      <-chan Event[TEvent]
	stream *Stream[TStream, TEvent]
	id     uint64
	sub    *subscriber[TEvent]
}

// Events returns the read-only event channel.
func (s *Subscription[TStream, TEvent]) Events() <-chan Event[TEvent] {
	if s == nil {
		return nil
	}
	return s.C
}

// Close unregisters the subscription and closes its event channel.
// It is safe to call Close more than once.
func (s *Subscription[TStream, TEvent]) Close() {
	if s == nil || s.stream == nil {
		return
	}
	s.stream.unsubscribe(s.id)
}

// Stream distributes each published event to every active subscriber.
// A full subscriber buffer rejects the entire publish instead of dropping
// events or blocking publishers.
type Stream[TStream, TEvent any] struct {
	channel    *EventChannel[TStream]
	mu         sync.RWMutex
	nextID     uint64
	subs       map[uint64]*subscriber[TEvent]
	bufferSize int
	closed     bool
}

// New creates a stream with bufferSize slots per subscriber and registers it
// in channel for event type TEvent. Each event type may be registered only once
// in a given EventChannel[TStream]. Different channels own independent streams.
// bufferSize must be greater than zero.
func New[TStream, TEvent any](channel *EventChannel[TStream], bufferSize int) (*Stream[TStream, TEvent], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if bufferSize < 1 {
		return nil, ErrInvalidBufferSize
	}

	return register[TStream, TEvent](channel, bufferSize)
}

// Subscribe registers a subscriber. Its channel closes when ctx is cancelled,
// the subscription is closed, or the stream is closed. A nil context is
// treated as context.Background().
func (s *Stream[TStream, TEvent]) Subscribe(ctx context.Context) (*Subscription[TStream, TEvent], error) {
	if nilContext(ctx) {
		ctx = context.Background()
	}
	sub := &subscriber[TEvent]{
		ch:   make(chan Event[TEvent], s.bufferSize),
		done: make(chan struct{}),
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(sub.ch)
		close(sub.done)
		return &Subscription[TStream, TEvent]{C: sub.ch}, ErrClosed
	}
	s.nextID++
	id := s.nextID
	s.subs[id] = sub
	s.mu.Unlock()

	result := &Subscription[TStream, TEvent]{
		C:      sub.ch,
		stream: s,
		id:     id,
		sub:    sub,
	}
	go func() {
		select {
		case <-ctx.Done():
			result.Close()
		case <-sub.done:
		}
	}()
	return result, nil
}

// nilContext reports whether ctx is nil without changing the public API's
// treatment of a nil context.
func nilContext(ctx context.Context) bool {
	return ctx == nil
}

// Publish sends event to every active subscriber. Event timestamps are set
// during construction and are internal to Event. With no subscribers, Publish
// succeeds.
func (s *Stream[TStream, TEvent]) Publish(event Event[TEvent]) error {
	channel := s.channel
	if channel == nil {
		return ErrNilEventChannel
	}
	// Serialize with channel Close so admission and delivery cannot be split.
	channel.publishMu.Lock()
	defer channel.publishMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := event.validate(); err != nil {
		return fmt.Errorf("stream: invalid event policy: %w", err)
	}
	if !event.dataSet {
		return ErrUnsetEventData
	}
	// Preflight before queue admission: no persist-only publish on slow subscriber.
	for _, sub := range s.subs {
		if len(sub.ch) == cap(sub.ch) {
			return ErrSlowSubscriber
		}
	}
	channel.mu.RLock()
	if channel.closed {
		channel.mu.RUnlock()
		return ErrClosed
	}
	if channel.storageFailure != nil {
		failure := channel.storageFailure
		channel.mu.RUnlock()
		return fmt.Errorf("stream: asynchronous storage failed: %w", failure)
	}
	writer := channel.writer
	channel.mu.RUnlock()
	if writer == nil {
		return errors.New("stream: private storage writer unavailable")
	}
	payload, err := encodeEventPayload(event.data, event.redactionPolicy)
	if err != nil {
		return fmt.Errorf("stream: prepare persisted event payload: %w", err)
	}
	if err := writer.Submit(asyncWriterItem{
		owner: channel,
		channelIdentity: channelTypeIdentity[TStream](),
		streamIdentity: streamTypeIdentity[TStream, TEvent](),
		timestamp: event.time,
		payload: payload,
	}); err != nil {
		return fmt.Errorf("stream: enqueue persisted event: %w", err)
	}
	// Deliver original unredacted event only after bounded queue admission.
	for _, sub := range s.subs {
		sub.ch <- event
	}
	return nil
}

// Close closes the stream and all subscriber channels. It is idempotent.
func (s *Stream[TStream, TEvent]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	for id, sub := range s.subs {
		sub.once.Do(func() {
			close(sub.ch)
			close(sub.done)
		})
		delete(s.subs, id)
	}
}

func (s *Stream[TStream, TEvent]) unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sub, ok := s.subs[id]; ok {
		delete(s.subs, id)
		sub.once.Do(func() {
			close(sub.ch)
			close(sub.done)
		})
	}
}
