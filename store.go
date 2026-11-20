package stream

import (
	"reflect"
	"sync"

	"github.com/google/uuid"
)

// streamKey identifies one typed stream within a logical event channel.
// It is stable across processes when the same channel UUID is reused.
type streamKey struct {
	ChannelID uuid.UUID
	EventType reflect.Type
}

// streamStore is the in-memory registry implementation. Keeping it separate
// from EventChannel means channel identity is not tied to this storage mechanism.
type streamStore struct {
	mu      sync.RWMutex
	streams map[streamKey]any
}

var defaultStore = &streamStore{
	streams: make(map[streamKey]any),
}

// EventChannel is a logical grouping identity for related typed streams.
// Its UUID is independent of the in-memory registry and can be persisted by
// applications that need to reconstruct the same logical channel later.
type EventChannel struct {
	ID uuid.UUID

	mu     sync.RWMutex
	closed bool
}

// NewEventChannel creates a new logical event channel with a random UUID.
func NewEventChannel() *EventChannel {
	return &EventChannel{ID: uuid.New()}
}

// NewEventChannelWithID reconstructs a logical event channel using an existing
// UUID. This is useful when restoring a previously persisted channel identity.
// uuid.Nil is rejected because it does not identify a valid channel.
func NewEventChannelWithID(id uuid.UUID) (*EventChannel, error) {
	if id == uuid.Nil {
		return nil, ErrNilEventChannelID
	}
	return &EventChannel{ID: id}, nil
}

// register creates and stores a stream for T under the channel's stable UUID.
func register[T any](channel *EventChannel, bufferSize int) (*Stream[T], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if channel.ID == uuid.Nil {
		return nil, ErrNilEventChannelID
	}

	channel.mu.RLock()
	defer channel.mu.RUnlock()
	if channel.closed {
		return nil, ErrClosed
	}

	key := streamKey{
		ChannelID: channel.ID,
		EventType: reflect.TypeOf((*T)(nil)).Elem(),
	}

	defaultStore.mu.Lock()
	defer defaultStore.mu.Unlock()

	if _, exists := defaultStore.streams[key]; exists {
		return nil, ErrDuplicateStream
	}

	s := &Stream[T]{
		subs:       make(map[uint64]*subscriber[T]),
		bufferSize: bufferSize,
	}
	defaultStore.streams[key] = s
	return s, nil
}

// Get returns the stream for event type T registered under channel's UUID.
// It returns nil if the channel is nil, closed, or has no such stream.
func Get[T any](channel *EventChannel) *Stream[T] {
	if channel == nil || channel.ID == uuid.Nil {
		return nil
	}

	channel.mu.RLock()
	defer channel.mu.RUnlock()
	if channel.closed {
		return nil
	}

	key := streamKey{
		ChannelID: channel.ID,
		EventType: reflect.TypeOf((*T)(nil)).Elem(),
	}

	defaultStore.mu.RLock()
	defer defaultStore.mu.RUnlock()

	s, ok := defaultStore.streams[key]
	if !ok {
		return nil
	}
	return s.(*Stream[T])
}

// Close closes every stream registered under this channel UUID. The channel
// identity remains valid for reconstruction with NewEventChannelWithID.
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
	channel.mu.Unlock()

	defaultStore.removeChannel(channel.ID)
}

func (store *streamStore) removeChannel(id uuid.UUID) {
	store.mu.Lock()
	removed := make([]interface{ Close() }, 0)
	for key, value := range store.streams {
		if key.ChannelID == id {
			removed = append(removed, value.(interface{ Close() }))
			delete(store.streams, key)
		}
	}
	store.mu.Unlock()

	for _, s := range removed {
		s.Close()
	}
}
