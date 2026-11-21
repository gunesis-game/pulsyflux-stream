package stream

import (
	"bytes"
	"encoding/json"
	"io""
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testDiskRecord(seq uint64) diskRecord {
	return diskRecord{Version: recordVersion, EventID: uuid.New(), StreamID: uuid.New(), ChannelID: uuid.New(), Sequence: seq,
		Timestamp: time.Unix(1700000000, int64(seq)).UnixNano(), SchemaFingerprint: "schema", Type: "event", Payload: json.RawMessage(`{"x":1}`)}
}

func TestDiskRecordRoundTripChecksum(t *testing.T) {
	r := testDiskRecord(1)
	frame, err := encodeDiskRecord(r)
	if err != nil { t.Fatal(err) }
	got, err := decodeDiskRecord(frame)
	if err != nil { t.Fatal(err) }
	if got.EventID != r.EventID || got.Sequence != r.Sequence || !bytes.Equal(got.Payload, r.Payload) { t.Fatalf("round trip mismatch: %#v", got) }
	frame[len(frame)-1] ^= 0xff
	if _, err := decodeDiskRecord(frame); !errors.Is(err, errRecordCorrupt) { t.Fatalf("checksum not detected: %v", err) }
}

func TestScanIncompleteTailAndRejectCorruption(t *testing.T) {
	frame, err := encodeDiskRecord(testDiskRecord(1))
	if err != nil { t.Fatal(err) }
	records, offset, tail, err := scanDiskRecords(bytes.NewReader(append(frame, []byte("PFS")...)))
	if err != nil || len(records) != 1 || offset != int64(len(frame)) || !tail { t.Fatalf("scan: count=%d offset=%d tail=%v err=%v", len(records), offset, tail, err) }
	bad := append([]byte(nil), frame...); bad[0] = 'X'
	if _, _, _, err := scanDiskRecords(bytes.NewReader(bad)); !errors.Is(err, errRecordCorrupt) { t.Fatalf("corruption accepted: %v", err) }
}

func TestDiskWriterSequenceTailRecoveryAndRollover(t *testing.T) {
	dir := t.TempDir()
	w, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 220})
	if err != nil { t.Fatal(err) }
	ch, st := uuid.New(), uuid.New()
	for i := 1; i <= 4; i++ {
		seq, _, err := w.Append(ch, st, "schema", "event", time.Now(), []byte(`{"payload":"large enough to cause a segment rollover"}`))
		if err != nil { t.Fatal(err) }
		if seq != uint64(i) { t.Fatalf("sequence=%d want=%d", seq, i) }
	}
	if err := w.Close(); err != nil { t.Fatal(err) }
	files, err := diskSegments(dir)
	if err != nil { t.Fatal(err) }
	if len(files) < 2 { t.Fatalf("expected rollover, found %d segment(s)", len(files)) }

	last := files[len(files)-1]
	f, err := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil { t.Fatal(err) }
	if _, err := f.Write([]byte("PFS")); err != nil { t.Fatal(err) }
	if err := f.Close(); err != nil { t.Fatal(err) }

	w, err = openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil { t.Fatal(err) }
	next, _, err := w.Append(ch, st, "schema", "event", time.Now(), []byte(`{"payload":2}`))
	if err != nil { t.Fatal(err) }
	if next != 5 { t.Fatalf("recovered sequence=%d want=5", next) }
	if err := w.Close(); err != nil { t.Fatal(err) }
	_ = filepath.Base(last)
}

func TestDiskWriterRecoversSequenceAcrossEmptySegment(t *testing.T) {
	dir := t.TempDir()
	frame, err := encodeDiskRecord(testDiskRecord(7))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), frame, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000002.pfs"), nil, 0o640); err != nil {
		t.Fatal(err)
	}

	w, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	seq, _, err := w.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 8 {
		t.Fatalf("recovered sequence=%d want=8", seq)
	}
}

func TestDiskReaderSequentialAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 220})
	if err != nil { t.Fatal(err) }
	ch, st := uuid.New(), uuid.New()
	for i := 1; i <= 4; i++ {
		seq, _, err := w.Append(ch, st, "schema", "event", time.Now(), []byte(`{"payload":"large enough to cause a segment rollover"}`))
		if err != nil { t.Fatal(err) }
		if seq != uint64(i) { t.Fatalf("write sequence=%d want=%d", seq, i) }
	}
	if err := w.Close(); err != nil { t.Fatal(err) }

	r, err := openDiskReader(dir)
	if err != nil { t.Fatal(err) }
	defer r.Close()
	for expected := uint64(1); expected <= 4; expected++ {
		record, err := r.Next()
		if err != nil { t.Fatalf("read %d: %v", expected, err) }
		if record.Sequence != expected { t.Fatalf("sequence=%d want=%d", record.Sequence, expected) }
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) { t.Fatalf("expected EOF, got %v", err) }
}
