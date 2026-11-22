package stream

import (
	"reflect"
	"sync"
)

type EventChannel[TStream any] struct {
	publishMu      sync.Mutex
	mu             sync.RWMutex
	streams        map[reflect.Type]any
	closed         bool
	writer         *asyncDiskWriter
	releaseWriter  func() error
	storageFailure error
}

// NewEventChannel creates a logical typed channel and acquires private storage.
// The caller must close it to release the writer ownership.
func NewEventChannel[TStream any]() (*EventChannel[TStream], error) {
	writer, release, err := acquireProcessWriter()
	if err != nil {
		return nil, err
	}
	return &EventChannel[TStream]{
		streams:       make(map[reflect.Type]any),
		writer:        writer,
		releaseWriter: release,
	}, nil
}

// Close closes owned streams and releases shared writer ownership. It is idempotent.
func (channel *EventChannel[TStream]) Close() error {
	if channel == nil {
		return nil
	}
	channel.publishMu.Lock()
	defer channel.publishMu.Unlock()
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return nil
	}
	channel.closed = true
	streams := make([]interface{ Close() }, 0, len(channel.streams))
	for _, value := range channel.streams {
		streams = append(streams, value.(interface{ Close() }))
	}
	clear(channel.streams)
	release := channel.releaseWriter
	channel.releaseWriter = nil
	channel.mu.Unlock()

	for _, stream := range streams {
		stream.Close()
	}
	if release != nil {
		return release()
	}
	return nil
}

// storageWriteFailed retains the terminal asynchronous writer failure for this
// channel. ErrorStream routing is added in the next integration phase.
func (channel *EventChannel[TStream]) storageWriteFailed(err error) {
	channel.mu.Lock()
	if channel.storageFailure == nil {
		channel.storageFailure = err
	}
	channel.mu.Unlock()
}

func register[TStream, TEvent any](channel *EventChannel[TStream], bufferSize int) (*Stream[TStream, TEvent], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	eventType := reflect.TypeOf((*TEvent)(nil)).Elem()

	channel.mu.Lock()
	defer channel.mu.Unlock()
	if channel.closed {
		return nil, ErrClosed
	}
	if _, exists := channel.streams[eventType]; exists {
		return nil, ErrDuplicateStream
	}
	stream := &Stream[TStream, TEvent]{
		channel:    channel,
		subs:       make(map[uint64]*subscriber[TEvent]),
		bufferSize: bufferSize,
	}
	channel.streams[eventType] = stream
	return stream, nil
}

// Get returns the registered stream for TEvent, or nil when absent.
func Get[TStream, TEvent any](channel *EventChannel[TStream]) *Stream[TStream, TEvent] {
	if channel == nil {
		return nil
	}
	eventType := reflect.TypeOf((*TEvent)(nil)).Elem()
	channel.mu.RLock()
	defer channel.mu.RUnlock()
	if channel.closed {
		return nil
	}
	value, exists := channel.streams[eventType]
	if !exists {
		return nil
	}
	stream, _ := value.(*Stream[TStream, TEvent])
	return stream
}
