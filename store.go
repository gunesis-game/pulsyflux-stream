package stream

import (
	"reflect"
	"sync"
)

// streamStore keeps the most recently constructed stream for each event type.
// It is intentionally unexported; consumers access it through Get.
type streamStore struct {
	mu      sync.RWMutex
	streams map[reflect.Type]any
}

var defaultStore = streamStore{
	streams: make(map[reflect.Type]any),
}

// register stores s as the shared stream for T.
func register[T any](s *Stream[T]) {
	typ := reflect.TypeOf((*T)(nil)).Elem()

	defaultStore.mu.Lock()
	defaultStore.streams[typ] = s
	defaultStore.mu.Unlock()
}

// Get returns the stream most recently constructed with New for event type T.
// It returns nil if New has not been called for T.
func Get[T any]() *Stream[T] {
	typ := reflect.TypeOf((*T)(nil)).Elem()

	defaultStore.mu.RLock()
	defer defaultStore.mu.RUnlock()

	s, ok := defaultStore.streams[typ]
	if !ok {
		return nil
	}

	return s.(*Stream[T])
}
