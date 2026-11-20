package stream

import (
	"reflect"
	"sync"
)

// EventChannel owns a collection of independently typed event streams.
// Streams registered in different EventChannels are isolated from one another.
type EventChannel struct {
	mu      sync.RWMutex
	streams map[reflect.Type]any
	closed  bool
}

// NewEventChannel creates an empty EventChannel.
func NewEventChannel() *EventChannel {
	return &EventChannel{
		streams: make(map[reflect.Type]any),
	}
}

// register creates and stores a stream for T. The channel lock makes duplicate
// registration and channel shutdown mutually exclusive.
func register[T any](channel *EventChannel, bufferSize int) (*Stream[T], error) {
	typ := reflect.TypeOf((*T)(nil)).Elem()

	channel.mu.Lock()
	defer channel.mu.Unlock()

	if channel.closed {
		return nil, ErrClosed
	}
	if _, exists := channel.streams[typ]; exists {
		return nil, ErrDuplicateStream
	}

	s := &Stream[T]{
		subs:       make(map[uint64]*subscriber[T]),
		bufferSize: bufferSize,
	}
	channel.streams[typ] = s
	return s, nil
}

// Get returns the stream for event type T registered in channel. It returns
// nil if channel is nil or no stream for T has been registered.
func Get[T any](channel *EventChannel) *Stream[T] {
	if channel == nil {
		return nil
	}

	typ := reflect.TypeOf((*T)(nil)).Elem()

	channel.mu.RLock()
	defer channel.mu.RUnlock()

	s, ok := channel.streams[typ]
	if !ok {
		return nil
	}
	return s.(*Stream[T])
}

// Close closes the channel and every stream it owns. It is safe to call
// Close more than once. No additional streams can be registered afterwards.
func (channel *EventChannel) Close() {
	if channel == nil {
		return
	}

	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return
	}
	channel.closed = true
	streams := make([]interface{ Close() }, 0, len(channel.streams))
	for _, value := range channel.streams {
		streams = append(streams, value.(interface{ Close() }))
	}
	channel.mu.Unlock()

	for _, s := range streams {
		s.Close()
	}
}
