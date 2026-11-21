package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestEvent[T any](t *testing.T, name string, data T) Event[T] {
	t.Helper()
	event, err := NewEvent[T](NewPolicy())
	if err != nil {
		t.Fatal(err)
	}
	event.Name = name
	if err := event.SetData(data); err != nil {
		t.Fatal(err)
	}
	return event
}

func newTestStream[T any](t *testing.T, bufferSize int) *Stream[T] {
	t.Helper()
	channel := NewEventChannel()
	s, err := New[T](channel, bufferSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(channel.Close)
	return s
}

func TestEventChannelRegistersTypedStreams(t *testing.T) {
	type firstEvent struct{ Value string }
	type secondEvent struct{ Value int }

	channel := NewEventChannel()
	defer channel.Close()

	first, err := New[firstEvent](channel, 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New[secondEvent](channel, 13)
	if err != nil {
		t.Fatal(err)
	}

	if got := Get[firstEvent](channel); got != first {
		t.Fatal("Get did not return the first registered stream")
	}
	if got := Get[secondEvent](channel); got != second {
		t.Fatal("Get did not return the second registered stream")
	}
	if first.bufferSize != 7 || second.bufferSize != 13 {
		t.Fatalf("unexpected buffer sizes: first=%d second=%d", first.bufferSize, second.bufferSize)
	}
}

func TestEventChannelsAreIsolated(t *testing.T) {
	type isolatedEvent struct{ Value string }

	firstChannel := NewEventChannel()
	secondChannel := NewEventChannel()
	defer firstChannel.Close()
	defer secondChannel.Close()

	first, err := New[isolatedEvent](firstChannel, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New[isolatedEvent](secondChannel, 4)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("different channels shared the same stream")
	}
	if Get[isolatedEvent](firstChannel) != first || Get[isolatedEvent](secondChannel) != second {
		t.Fatal("Get returned a stream from the wrong channel")
	}

	firstSub, err := first.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer firstSub.Close()
	secondSub, err := second.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer secondSub.Close()

	if err := first.Publish(newTestEvent(t, "", isolatedEvent{Value: "first"})); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-secondSub.Events():
		t.Fatalf("event leaked into second channel: %#v", got)
	default:
	}
	if got := <-firstSub.Events(); got.Data().Value != "first" {
		t.Fatalf("unexpected event: %#v", got)
	}
}

func TestDuplicateStreamRegistrationReturnsError(t *testing.T) {
	type duplicateEvent struct{}
	channel := NewEventChannel()
	defer channel.Close()

	if _, err := New[duplicateEvent](channel, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := New[duplicateEvent](channel, 2); !errors.Is(err, ErrDuplicateStream) {
		t.Fatalf("got %v, want ErrDuplicateStream", err)
	}
}

func TestNewValidatesChannelAndBuffer(t *testing.T) {
	if _, err := New[int](nil, 1); !errors.Is(err, ErrNilEventChannel) {
		t.Fatalf("got %v, want ErrNilEventChannel", err)
	}
	channel := NewEventChannel()
	defer channel.Close()
	if _, err := New[int](channel, 0); !errors.Is(err, ErrInvalidBufferSize) {
		t.Fatalf("got %v, want ErrInvalidBufferSize", err)
	}
}

func TestGetReturnsNilBeforeNew(t *testing.T) {
	type neverCreatedEvent struct{}
	channel := NewEventChannel()
	defer channel.Close()

	if got := Get[neverCreatedEvent](channel); got != nil {
		t.Fatalf("Get returned %#v, want nil", got)
	}
	if got := Get[int](nil); got != nil {
		t.Fatalf("Get with nil channel returned %#v, want nil", got)
	}
}

func TestEventChannelCloseClosesOwnedStreams(t *testing.T) {
	channel := NewEventChannel()
	first, err := New[int](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New[string](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstSub, err := first.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondSub, err := second.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	channel.Close()
	channel.Close()

	if err := first.Publish(Event[int]{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("first stream publish: got %v, want ErrClosed", err)
	}
	if err := second.Publish(Event[string]{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("second stream publish: got %v, want ErrClosed", err)
	}
	if _, ok := <-firstSub.Events(); ok {
		t.Fatal("first subscription channel remained open")
	}
	if _, ok := <-secondSub.Events(); ok {
		t.Fatal("second subscription channel remained open")
	}
	if _, err := New[bool](channel, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("New after channel close: got %v, want ErrClosed", err)
	}
}

func TestPublishToMultipleSubscribers(t *testing.T) {
	s := newTestStream[string](t, 2)
	a, err := s.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := s.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	want := newTestEvent(t, "created", "item-1")
	if err := s.Publish(want); err != nil {
		t.Fatal(err)
	}

	for _, sub := range []*Subscription[string]{a, b} {
		select {
		case got := <-sub.Events():
			if got.Name != want.Name || got.Data() != want.Data() {
				t.Fatalf("got %#v, want %#v", got, want)
			}
			if got.Time.IsZero() {
				t.Fatal("Publish did not set event timestamp")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestPublishIsAtomicWhenSubscriberIsFull(t *testing.T) {
	s := newTestStream[int](t, 1)
	fast, _ := s.Subscribe(context.Background())
	defer fast.Close()
	slow, _ := s.Subscribe(context.Background())
	defer slow.Close()

	if err := s.Publish(newTestEvent(t, "", 1)); err != nil {
		t.Fatal(err)
	}
	<-fast.Events()
	if err := s.Publish(newTestEvent(t, "", 2)); !errors.Is(err, ErrSlowSubscriber) {
		t.Fatalf("got %v, want ErrSlowSubscriber", err)
	}
	select {
	case got := <-fast.Events():
		t.Fatalf("partial delivery occurred: fast subscriber unexpectedly got %d", got.Data())
	default:
	}
	if got := <-slow.Events(); got.Data() != 1 {
		t.Fatalf("slow subscriber got unexpected event: %d", got.Data())
	}
}

func TestContextCancellationUnsubscribes(t *testing.T) {
	s := newTestStream[int](t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sub, err := s.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("expected subscription channel to close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription was not closed after context cancellation")
	}
	if err := s.Publish(newTestEvent(t, "", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	s := newTestStream[int](t, 1)
	sub, err := s.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s.Close()
	if err := s.Publish(Event[int]{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("expected closed channel")
	}
}

func TestSubscribeAfterClose(t *testing.T) {
	s := newTestStream[int](t, 1)
	s.Close()
	sub, err := s.Subscribe(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("expected closed channel")
	}
}

func TestConcurrentPublishAndSubscribe(t *testing.T) {
	s := newTestStream[int](t, 64)
	sub, err := s.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				err := s.Publish(newTestEvent(t, "", base*100 + n))
				if err != nil && !errors.Is(err, ErrSlowSubscriber) {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestEventChannelUsesStableUUID(t *testing.T) {
	type persistedEvent struct{ Value string }

	id := uuid.MustParse("3a85f5f2-6e15-4c0d-8b3e-7f4d52f1b123")
	firstChannel, err := NewEventChannelWithID(id)
	if err != nil {
		t.Fatal(err)
	}
	if firstChannel.ID != id {
		t.Fatalf("channel ID = %s, want %s", firstChannel.ID, id)
	}

	first, err := New[persistedEvent](firstChannel, 4)
	if err != nil {
		t.Fatal(err)
	}

	// A reconstructed handle with the same UUID addresses the same logical
	// stream identity and cannot register a duplicate event type.
	restoredChannel, err := NewEventChannelWithID(id)
	if err != nil {
		t.Fatal(err)
	}
	if got := Get[persistedEvent](restoredChannel); got != first {
		t.Fatal("restored channel did not resolve the stream by UUID")
	}
	if _, err := New[persistedEvent](restoredChannel, 4); !errors.Is(err, ErrDuplicateStream) {
		t.Fatalf("got %v, want ErrDuplicateStream for same UUID", err)
	}

	firstChannel.Close()

	// Closing removes the in-memory streams, but does not invalidate the
	// persisted logical UUID. A new handle can register streams for replay.
	replayedChannel, err := NewEventChannelWithID(id)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := New[persistedEvent](replayedChannel, 4)
	if err != nil {
		t.Fatal(err)
	}
	if replayed == first {
		t.Fatal("reconstructed channel reused the closed in-memory stream")
	}
	replayedChannel.Close()
}

func TestNewEventChannelWithIDRejectsNilUUID(t *testing.T) {
	if _, err := NewEventChannelWithID(uuid.Nil); !errors.Is(err, ErrNilEventChannelID) {
		t.Fatalf("got %v, want ErrNilEventChannelID", err)
	}
}
