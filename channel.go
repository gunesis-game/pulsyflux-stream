package stream

import (
	"sync"

	"github.com/google/uuid"
)

// EventChannel is a logical grouping identity for related typed streams.
// Its private UUID scopes the channel's in-memory typed stream registrations.
type EventChannel struct {
	id uuid.UUID

	mu     sync.RWMutex
	closed bool
}

// NewEventChannel creates a new logical event channel with a random UUID.
func NewEventChannel() *EventChannel {
	return &EventChannel{id: uuid.New()}
}

// Close closes every stream registered under this channel UUID. The channel
// private identity remains unchanged for the lifetime of this handle.
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

	defaultStore.removeChannel(channel.id)
}
