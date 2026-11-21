package stream

import (
	"fmt"
	"reflect"
	"sync"

)

// EventChannel is a logical grouping identity for a family of related streams.
// Every stream registered with a channel shares its TStream type context.
type streamSchemaIdentityProvider interface {
	defaultSchemaIdentity() (schemaIdentity, error)
}

type EventChannel[TStream any] struct {

	mu      sync.RWMutex
	streams map[reflect.Type]any
	closed  bool
}

// NewEventChannel creates a logical event channel identified by its TStream type.
func NewEventChannel[TStream any]() *EventChannel[TStream] {
	return &EventChannel[TStream]{
		streams: make(map[reflect.Type]any),
	}
}

// Close closes every stream owned by this channel. It is idempotent.
func (channel *EventChannel[TStream]) Close() {
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
	clear(channel.streams)
	channel.mu.Unlock()

	for _, stream := range streams {
		stream.Close()
	}
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

// Get returns the stream for TEvent registered in channel, or nil if absent.
func Get[TStream, TEvent any](channel *EventChannel[TStream]) *Stream[TStream, TEvent] {
	if channel == nil || channel.id == uuid.Nil {
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

func (channel *EventChannel[TStream]) schemaIdentityForStream(activeEventType reflect.Type, activeStream schemaIdentity) (schemaIdentity, error) {
	if channel == nil {
		return schemaIdentity{}, ErrNilEventChannel
	}

	channel.mu.RLock()
	defer channel.mu.RUnlock()

	dependencies := make([]schemaIdentity, 0, len(channel.streams))
	activeFound := false
	for eventType, value := range channel.streams {
		provider, ok := value.(streamSchemaIdentityProvider)
		if !ok {
			return schemaIdentity{}, fmt.Errorf("stream: registered value does not provide schema identity")
		}
		if eventType == activeEventType {
			dependencies = append(dependencies, activeStream)
			activeFound = true
			continue
		}
		identity, err := provider.defaultSchemaIdentity()
		if err != nil {
			return schemaIdentity{}, fmt.Errorf("stream: derive registered stream schema identity: %w", err)
		}
		dependencies = append(dependencies, identity)
	}
	if !activeFound {
		return schemaIdentity{}, fmt.Errorf("stream: active event type is not registered in channel")
	}
	return channelSchemaIdentity[TStream](dependencies), nil
}
