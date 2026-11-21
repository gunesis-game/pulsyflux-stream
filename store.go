package stream

import (
	"reflect"
	"sync"

	"github.com/google/uuid"
)

// streamKey identifies one typed stream within a logical event channel.
// It is stable for the lifetime of the channel handle.
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

// register creates and stores a stream for T under the channel's private UUID.
func register[T any](channel *EventChannel, bufferSize int) (*Stream[T], error) {
	if channel == nil {
		return nil, ErrNilEventChannel
	}
	if channel.id == uuid.Nil {
		return nil, ErrNilEventChannelID
	}

	channel.mu.RLock()
	defer channel.mu.RUnlock()
	if channel.closed {
		return nil, ErrClosed
	}

	key := streamKey{
		ChannelID: channel.id,
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

// removeChannel removes all streams associated with a channel UUID.
// It releases the registry lock before closing streams to avoid holding storage
// locks while stream shutdown notifies subscribers.
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
