package stream

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
)

type persistenceTestChannel struct{}
type persistenceTestEvent struct {
	Visible string `json:"visible"`
	Secret  string `json:"secret"`
}

func TestPublishPersistsRedactedPayloadWithoutSubscribers(t *testing.T) {
	channel, err := NewEventChannel[persistenceTestChannel]()
	if err != nil {
		t.Fatalf("NewEventChannel: %v", err)
	}
	stream, err := New[persistenceTestChannel, persistenceTestEvent](channel, 2)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEvent[persistenceTestEvent](NewRedactionPolicy("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(persistenceTestEvent{Visible: "keep", Secret: "hide"}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Publish(event); err != nil {
		t.Fatalf("Publish without subscribers: %v", err)
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("Close to flush: %v", err)
	}

	dir, err := privateStorageRoot()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open persisted reader: %v", err)
	}
	defer reader.Close()
	found := false
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		if record.ChannelIdentity != channelTypeIdentity[persistenceTestChannel]() ||
			record.StreamIdentity != streamTypeIdentity[persistenceTestChannel, persistenceTestEvent]() {
			continue
		}
		var persisted persistenceTestEvent
		if err := json.Unmarshal(record.Payload, &persisted); err != nil {
			t.Fatalf("decode persisted payload: %v", err)
		}
		if persisted.Visible != "keep" || persisted.Secret != "" {
			t.Fatalf("unexpected persisted payload: %#v", persisted)
		}
		found = true
	}
	if !found {
		t.Fatal("no matching event record was persisted")
	}
}

func TestPublishSlowSubscriberRejectionDoesNotPersist(t *testing.T) {
	type channelIdentity struct{}
	type eventPayload struct {
		Value int `json:"value"`
	}
	channel, err := NewEventChannel[channelIdentity]()
	if err != nil {
		t.Fatalf("NewEventChannel: %v", err)
	}
	stream, err := New[channelIdentity, eventPayload](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := stream.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	first, err := NewEvent[eventPayload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetData(eventPayload{Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Publish(first); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	second, err := NewEvent[eventPayload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.SetData(eventPayload{Value: 2}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Publish(second); err != ErrSlowSubscriber {
		t.Fatalf("second Publish = %v, want ErrSlowSubscriber", err)
	}
	<-sub.Events()
	if err := channel.Close(); err != nil {
		t.Fatalf("Close to flush: %v", err)
	}

	dir, err := privateStorageRoot()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open persisted reader: %v", err)
	}
	defer reader.Close()
	count := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		if record.ChannelIdentity == channelTypeIdentity[channelIdentity]() &&
			record.StreamIdentity == streamTypeIdentity[channelIdentity, eventPayload]() {
			count++
			var got eventPayload
			if err := json.Unmarshal(record.Payload, &got); err != nil {
				t.Fatalf("decode record: %v", err)
			}
			if got.Value != 1 {
				t.Fatalf("unexpected record value %d", got.Value)
			}
		}
	}
	if count != 1 {
		t.Fatalf("persisted record count = %d, want 1", count)
	}
}

type concurrentPublishChannel struct{}
type concurrentPublishEvent struct {
	Value int `json:"value"`
}

func TestConcurrentPublishPersistsAllAcceptedEvents(t *testing.T) {
	const publishCount = 64
	channel, err := NewEventChannel[concurrentPublishChannel]()
	if err != nil {
		t.Fatalf("NewEventChannel: %v", err)
	}
	stream, err := New[concurrentPublishChannel, concurrentPublishEvent](channel, 1)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, publishCount)
	for i := 0; i < publishCount; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			event, err := NewEvent[concurrentPublishEvent](NewRedactionPolicy())
			if err != nil {
				errs <- err
				return
			}
			if err := event.SetData(concurrentPublishEvent{Value: value}); err != nil {
				errs <- err
				return
			}
			errs <- stream.Publish(event)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Publish: %v", err)
		}
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("Close to flush: %v", err)
	}

	dir, err := privateStorageRoot()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open persisted reader: %v", err)
	}
	defer reader.Close()
	seen := make(map[int]bool, publishCount)
	count := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		if record.ChannelIdentity != channelTypeIdentity[concurrentPublishChannel]() ||
			record.StreamIdentity != streamTypeIdentity[concurrentPublishChannel, concurrentPublishEvent]() {
			continue
		}
		var got concurrentPublishEvent
		if err := json.Unmarshal(record.Payload, &got); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if got.Value < 0 || got.Value >= publishCount {
			t.Errorf("unexpected event value %d", got.Value)
			continue
		}
		if seen[got.Value] {
			t.Errorf("duplicate event value %d", got.Value)
		}
		seen[got.Value] = true
		count++
	}
	if count != publishCount {
		t.Errorf("persisted %d events, want %d", count, publishCount)
	}
}
