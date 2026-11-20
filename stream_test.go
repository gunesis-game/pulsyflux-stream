package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPublishToMultipleSubscribers(t *testing.T) {
	s := New[string](2)
	defer s.Close()

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

	want := Event[string]{Name: "created", Data: "item-1"}
	if err := s.Publish(want); err != nil {
		t.Fatal(err)
	}

	for _, sub := range []*Subscription[string]{a, b} {
		select {
		case got := <-sub.Events():
			if got.Name != want.Name || got.Data != want.Data {
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
	s := New[int](1)
	defer s.Close()

	fast, _ := s.Subscribe(context.Background())
	defer fast.Close()

	slow, _ := s.Subscribe(context.Background())
	defer slow.Close()

	if err := s.Publish(Event[int]{Data: 1}); err != nil {
		t.Fatal(err)
	}
	// Both buffers are now full; consume only from fast.
	<-fast.Events()

	if err := s.Publish(Event[int]{Data: 2}); !errors.Is(err, ErrSlowSubscriber) {
		t.Fatalf("got %v, want ErrSlowSubscriber", err)
	}
	select {
	case got := <-fast.Events():
		t.Fatalf("partial delivery occurred: fast subscriber unexpectedly got %d", got.Data)
	default:
	}

	if got := <-slow.Events(); got.Data != 1 {
		t.Fatalf("slow subscriber got unexpected event: %d", got.Data)
	}
}

func TestContextCancellationUnsubscribes(t *testing.T) {
	s := New[int](1)
	defer s.Close()

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

	if err := s.Publish(Event[int]{Data: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	s := New[int](1)
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
	s := New[int](1)
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
	s := New[int](64)
	defer s.Close()

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
				err := s.Publish(Event[int]{Data: base*100 + n})
				if err != nil && !errors.Is(err, ErrSlowSubscriber) {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
