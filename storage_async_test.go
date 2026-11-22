package stream

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type asyncDiskTestDouble struct {
	appendStarted chan struct{}
	appendGate    chan struct{}
	appendErr     error
	flushErr      error
	startedOnce   sync.Once
	mu            sync.Mutex
	appendCount   int
}

func (disk *asyncDiskTestDouble) Append(_, _ string, _ time.Time, _ []byte) (uint64, uuid.UUID, error) {
	if disk.appendStarted != nil {
		disk.startedOnce.Do(func() { close(disk.appendStarted) })
	}
	if disk.appendGate != nil {
		<-disk.appendGate
	}
	disk.mu.Lock()
	disk.appendCount++
	disk.mu.Unlock()
	return 0, uuid.Nil, disk.appendErr
}

func (disk *asyncDiskTestDouble) Flush() error {
	return disk.flushErr
}

func (disk *asyncDiskTestDouble) Close() error {
	return nil
}

func (disk *asyncDiskTestDouble) count() int {
	disk.mu.Lock()
	defer disk.mu.Unlock()
	return disk.appendCount
}

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
	mu       sync.Mutex
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
		payload:         payload,
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

func TestAsyncDiskWriterBackpressureWaitsForQueueCapacity(t *testing.T) {
	disk := &asyncDiskTestDouble{
		appendStarted: make(chan struct{}),
		appendGate:    make(chan struct{}),
	}
	writer, err := newAsyncDiskWriter(disk, 1, 32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Submit(asyncWriterItem{timestamp: time.Now(), payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disk.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin first append")
	}
	if err := writer.Submit(asyncWriterItem{timestamp: time.Now(), payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	third := make(chan error, 1)
	go func() { third <- writer.Submit(asyncWriterItem{timestamp: time.Now(), payload: []byte("third")}) }()
	select {
	case err := <-third:
		t.Fatalf("Submit returned while queue was full: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(disk.appendGate)
	select {
	case err := <-third:
		if err != nil {
			t.Fatalf("blocked Submit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Submit did not resume after queue capacity became available")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := disk.count(); got != 3 {
		t.Fatalf("append count = %d, want 3", got)
	}
}

func TestAsyncDiskWriterAppendFailureWakesBlockedSubmittersAndNotifiesOwners(t *testing.T) {
	cause := errors.New("injected append failure")
	disk := &asyncDiskTestDouble{
		appendStarted: make(chan struct{}),
		appendGate:    make(chan struct{}),
		appendErr:     cause,
	}
	writer, err := newAsyncDiskWriter(disk, 1, 32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ownerA, ownerB, ownerC := &asyncWriterTestOwner{}, &asyncWriterTestOwner{}, &asyncWriterTestOwner{}
	if err := writer.Submit(asyncWriterItem{owner: ownerA, timestamp: time.Now(), payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disk.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin append")
	}
	if err := writer.Submit(asyncWriterItem{owner: ownerB, timestamp: time.Now(), payload: []byte("queued")}); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() {
		blocked <- writer.Submit(asyncWriterItem{owner: ownerC, timestamp: time.Now(), payload: []byte("blocked")})
	}()
	select {
	case err := <-blocked:
		t.Fatalf("blocked producer returned before failure: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(disk.appendGate)
	select {
	case err := <-blocked:
		if !errors.Is(err, cause) {
			t.Fatalf("blocked Submit error = %v, want injected cause", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked producer was not woken by terminal failure")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if ownerA.failureCount() != 1 || ownerB.failureCount() != 1 {
		t.Fatalf("accepted owner notifications = A:%d B:%d, want 1 each", ownerA.failureCount(), ownerB.failureCount())
	}
	if ownerC.failureCount() != 0 {
		t.Fatalf("unadmitted owner received %d notifications, want 0", ownerC.failureCount())
	}
}

func TestAsyncDiskWriterFlushFailureNotifiesBatchAndQueuedOwners(t *testing.T) {
	cause := errors.New("injected flush failure")
	disk := &asyncDiskTestDouble{
		appendStarted: make(chan struct{}),
		appendGate:    make(chan struct{}),
		flushErr:      cause,
	}
	writer, err := newAsyncDiskWriter(disk, 2, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ownerA, ownerB := &asyncWriterTestOwner{}, &asyncWriterTestOwner{}
	if err := writer.Submit(asyncWriterItem{owner: ownerA, timestamp: time.Now(), payload: []byte("batch")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disk.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin append")
	}
	if err := writer.Submit(asyncWriterItem{owner: ownerB, timestamp: time.Now(), payload: []byte("queued")}); err != nil {
		t.Fatal(err)
	}
	close(disk.appendGate)
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !errors.Is(writer.failure, cause) {
		t.Fatalf("writer failure = %v, want flush cause", writer.failure)
	}
	if ownerA.failureCount() != 1 || ownerB.failureCount() != 1 {
		t.Fatalf("owner notifications = A:%d B:%d, want 1 each", ownerA.failureCount(), ownerB.failureCount())
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
