package stream

import (
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

func TestSharedWriterManagerKeepsWriterUntilFinalOwner(t *testing.T) {
	manager := &sharedWriterManager{}
	dir := t.TempDir()

	firstWriter, releaseFirst, err := manager.acquire(dir)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	secondWriter, releaseSecond, err := manager.acquire(dir)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if firstWriter != secondWriter {
		t.Fatal("owners received different shared writers")
	}
	if manager.owners != 2 {
		t.Fatalf("owners = %d, want 2", manager.owners)
	}

	payload, err := json.Marshal(map[string]string{"value": "kept"})
	if err != nil {
		t.Fatal(err)
	}
	if err := firstWriter.Submit(asyncWriterItem{
		channelIdentity: "channel",
		streamIdentity:  "stream",
		timestamp:       time.Now(),
		payload:         payload,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	if err := releaseFirst(); err != nil {
		t.Fatalf("release first owner: %v", err)
	}
	if err := releaseFirst(); err != nil {
		t.Fatalf("repeat first release: %v", err)
	}
	if manager.writer == nil || manager.owners != 1 {
		t.Fatal("writer was stopped before final owner release")
	}
	if err := secondWriter.Submit(asyncWriterItem{
		channelIdentity: "channel",
		streamIdentity:  "stream",
		timestamp:       time.Now(),
		payload:         []byte(`{"value":"second"}`),
	}); err != nil {
		t.Fatalf("submit with remaining owner: %v", err)
	}

	if err := releaseSecond(); err != nil {
		t.Fatalf("release final owner: %v", err)
	}
	if manager.writer != nil || manager.owners != 0 || manager.dir != "" {
		t.Fatal("manager did not reset after final owner closed")
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open persisted reader: %v", err)
	}
	defer reader.Close()
	for i := 1; i <= 2; i++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read record %d: %v", i, err)
		}
		if record.Sequence != uint64(i) {
			t.Fatalf("record sequence = %d, want %d", record.Sequence, i)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("reader after records = %v, want EOF", err)
	}
}

func TestSharedWriterManagerRejectsDifferentRootWhileActive(t *testing.T) {
	manager := &sharedWriterManager{}
	_, release, err := manager.acquire(t.TempDir())
	if err != nil {
		t.Fatalf("acquire first root: %v", err)
	}
	defer release()

	if _, _, err := manager.acquire(t.TempDir()); err == nil {
		t.Fatal("expected different storage root to be rejected while writer active")
	}
	if manager.owners != 1 {
		t.Fatalf("rejected acquire changed owner count to %d", manager.owners)
	}
}

func TestSharedWriterManagerReinitializesAfterFinalClose(t *testing.T) {
	manager := &sharedWriterManager{}
	dir := t.TempDir()

	first, releaseFirst, err := manager.acquire(dir)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := releaseFirst(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	second, releaseSecond, err := manager.acquire(dir)
	if err != nil {
		t.Fatalf("reacquire after close: %v", err)
	}
	if first == second {
		t.Fatal("reinitialized manager reused the closed writer")
	}
	if err := releaseSecond(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
