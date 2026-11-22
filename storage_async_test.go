package stream

import (
	"encoding/json"
	"errors"
	"sync"
	"io"
	"testing"
	"time"
)

func TestAsyncDiskWriterDrainsAcceptedItemsInOrder(t *testing.T) {
	dir := t.TempDir()
	disk, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open disk writer: %v", err)
	}
	writer, err := newAsyncDiskWriter(disk, 3, 4, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("new async writer: %v", err)
	}

	const records = 12
	for i := 0; i < records; i++ {
		payload, err := json.Marshal(map[string]int{"index": i})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		item := asyncWriterItem{
			channelIdentity: "test-channel",
			streamIdentity:  "test-stream",
			timestamp:       time.Now(),
			payload:         payload,
		}
		if err := writer.Submit(item); err != nil {
			t.Fatalf("submit record %d: %v", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close async writer: %v", err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	for i := 0; i < records; i++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read record %d: %v", i, err)
		}
		if record.Sequence != uint64(i+1) {
			t.Errorf("record %d sequence = %d, want %d", i, record.Sequence, i+1)
		}
		var payload struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			t.Fatalf("decode record %d: %v", i, err)
		}
		if payload.Index != i {
			t.Errorf("record %d payload index = %d, want %d", i, payload.Index, i)
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Errorf("reader after %d records returned %v, want EOF", records, err)
	}
}

func TestAsyncDiskWriterCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	disk, err := openDiskWriter(dir, diskWriterOptions{})
	if err != nil {
		t.Fatalf("open disk writer: %v", err)
	}
	writer, err := newAsyncDiskWriter(disk, 2, 2, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new async writer: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := writer.Submit(asyncWriterItem{
		channelIdentity: "channel",
		streamIdentity:  "stream",
		timestamp:       time.Now(),
		payload:         []byte("1"),
	}); err != ErrClosed {
		t.Errorf("Submit after Close error = %v, want ErrClosed", err)
	}
}

type asyncWriterTestOwner struct {
	mu sync.Mutex
	failures []error
}

func (owner *asyncWriterTestOwner) storageWriteFailed(err error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.failures = append(owner.failures, err)
}

func (owner *asyncWriterTestOwner) failureCount() int {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return len(owner.failures)
}

func TestAsyncDiskWriterCopiesPayloadAtAdmission(t *testing.T) {
	dir := t.TempDir()
	disk, err := openDiskWriter(dir, diskWriterOptions{})
	if err != nil {
		t.Fatalf("open disk writer: %v", err)
	}
	writer, err := newAsyncDiskWriter(disk, 2, 5, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("new async writer: %v", err)
	}

	payload := []byte(`{"value":"before"}`)
	if err := writer.Submit(asyncWriterItem{
		channelIdentity: "channel",
		streamIdentity:  "stream",
		timestamp:       time.Now(),
		payload:          payload,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	copy(payload, []byte(`{"value":"after"}`))
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()
	record, err := reader.Next()
	if err != nil {
		t.Fatalf("read persisted record: %v", err)
	}
	if string(record.Payload) != `{"value":"before"}` {
		t.Fatalf("payload = %s, want original admitted bytes", record.Payload)
	}
}

func TestAsyncDiskWriterFailureNotifiesEachOwnerOnce(t *testing.T) {
	cause := errors.New("disk failure")
	ownerA := &asyncWriterTestOwner{}
	ownerB := &asyncWriterTestOwner{}
	w := &asyncDiskWriter{
		queue: []asyncWriterItem{
			{owner: ownerA},
			{owner: ownerA},
			{owner: ownerB},
		},
		cond: sync.NewCond(&sync.Mutex{}),
	}
	w.fail(cause, make(map[asyncWriterOwner]struct{}))
	if ownerA.failureCount() != 1 {
		t.Errorf("owner A notifications = %d, want 1", ownerA.failureCount())
	}
	if ownerB.failureCount() != 1 {
		t.Errorf("owner B notifications = %d, want 1", ownerB.failureCount())
	}
	if !errors.Is(w.failure, cause) {
		t.Errorf("writer failure = %v, want original cause", w.failure)
	}
	if len(w.queue) != 0 {
		t.Errorf("failure left %d queued records, want queue drained", len(w.queue))
	}
}
