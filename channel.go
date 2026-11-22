package stream

import (
	"sync"
	"time"
)

type EventChannel[TStream any] struct {
	publishMu            sync.Mutex
	mu                   sync.RWMutex
	done                 chan struct{}
	closed               bool
	writer               *asyncDiskWriter
	releaseWriter        func() error
	storageFailure       error
	storageFailureRouted bool
	errorStream          *Stream[ErrorStream, StreamWriteErrorEvent]
}

// NewEventChannel creates a logical typed channel and acquires private storage.
// The caller must close it to release the writer ownership.
func NewEventChannel[TStream any]() (*EventChannel[TStream], error) {
	writer, release, err := acquireProcessWriter()
	if err != nil {
		return nil, err
	}
	return &EventChannel[TStream]{
		done:          make(chan struct{}),
		writer:        writer,
		releaseWriter: release,
	}, nil
}

// Close stops new work, releases writer ownership while the ErrorStream is
// still available for terminal failure routing, then closes that system stream.
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
	close(channel.done)
	release := channel.releaseWriter
	channel.releaseWriter = nil
	channel.mu.Unlock()

	var closeErr error
	if release != nil {
		closeErr = release()
	}
	channel.mu.RLock()
	errorStream := channel.errorStream
	channel.mu.RUnlock()
	if errorStream != nil {
		errorStream.Close()
	}
	return closeErr
}

// ErrorStream is the type identity for a channel's internal write-error stream.
type ErrorStream struct{}

// StreamWriteErrorEvent reports the terminal cause of asynchronous persistence.
type StreamWriteErrorEvent struct {
	Cause error
}

// Errors lazily returns the one channel-owned system-error stream. Its
// subscribers have one reserved slot for the terminal notification.
func (channel *EventChannel[TStream]) Errors(bufferSize int) (*Stream[ErrorStream, StreamWriteErrorEvent], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if bufferSize < 1 {
		return nil, ErrInvalidBufferSize
	}
	channel.mu.Lock()
	if channel.closed {
		channel.mu.Unlock()
		return nil, ErrClosed
	}
	if channel.errorStream == nil {
		channel.errorStream = &Stream[ErrorStream, StreamWriteErrorEvent]{
			subs:          make(map[uint64]*subscriber[StreamWriteErrorEvent]),
			bufferSize:    bufferSize,
			reservedSlots: 1,
			systemOnly:    true,
		}
	}
	errorStream := channel.errorStream
	failure := channel.storageFailure
	route := failure != nil && !channel.storageFailureRouted
	if route {
		channel.storageFailureRouted = true
	}
	channel.mu.Unlock()
	if route {
		errorStream.publishSystem(Event[StreamWriteErrorEvent]{
			time: time.Now(), data: StreamWriteErrorEvent{Cause: failure},
			dataSet: true, redactionPolicy: NewRedactionPolicy(),
		})
	}
	return errorStream, nil
}

// storageWriteFailed records the first terminal cause and routes exactly one
// notification when the channel has requested its ErrorStream. This callback
// deliberately does not take publishMu: it may run on the writer goroutine
// while a publisher is waiting for queue admission.
func (channel *EventChannel[TStream]) storageWriteFailed(err error) {
	channel.mu.Lock()
	if channel.storageFailure != nil {
		channel.mu.Unlock()
		return
	}
	channel.storageFailure = err
	errorStream := channel.errorStream
	route := errorStream != nil && !channel.storageFailureRouted
	if route {
		channel.storageFailureRouted = true
	}
	channel.mu.Unlock()
	if route {
		errorStream.publishSystem(Event[StreamWriteErrorEvent]{
			time: time.Now(), data: StreamWriteErrorEvent{Cause: err},
			dataSet: true, redactionPolicy: NewRedactionPolicy(),
		})
	}
}

func register[TStream, TEvent any](channel *EventChannel[TStream], bufferSize int) (*Stream[TStream, TEvent], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	channel.mu.Lock()
	defer channel.mu.Unlock()
	if channel.closed {
		return nil, ErrClosed
	}
	stream := &Stream[TStream, TEvent]{
		channel:            channel,
		subs:               make(map[uint64]*subscriber[TEvent]),
		bufferSize:         bufferSize,
		closeOnChannelDone: true,
	}
	return stream, nil
}
