package stream

import (
	"sync"

	"github.com/google/uuid"
)

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
