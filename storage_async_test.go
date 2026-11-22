package stream

import (
	"encoding/json"
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
			timestamp: time.Now(),
			payload: payload,
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
		streamIdentity: "stream",
		timestamp: time.Now(),
		payload: []byte("1"),
	}); err != ErrClosed {
		t.Errorf("Submit after Close error = %v, want ErrClosed", err)
	}
}
