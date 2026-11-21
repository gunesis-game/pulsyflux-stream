package stream

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	lastSequence, offset, tail, err := scanDiskRecords(bytes.NewReader(append(frame, []byte("PFS")...)))
	if err != nil || lastSequence != 1 || offset != int64(len(frame)) || !tail { t.Fatalf("scan: lastSequence=%d offset=%d tail=%v err=%v", lastSequence, offset, tail, err) }
	bad := append([]byte(nil), frame...); bad[0] = 'X'
	if _, _, _, err := scanDiskRecords(bytes.NewReader(bad)); !errors.Is(err, errRecordCorrupt) { t.Fatalf("corruption accepted: %v", err) }
}

func TestScanDiskRecordsTracksSequenceWithBoundedState(t *testing.T) {
	var data bytes.Buffer
	const count = 4096
	for sequence := uint64(1); sequence <= count; sequence++ {
		frame, err := encodeDiskRecord(testDiskRecord(sequence))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := data.Write(frame); err != nil {
			t.Fatal(err)
		}
	}

	wantOffset := int64(data.Len())
	lastSequence, offset, tail, err := scanDiskRecords(&data)
	if err != nil {
		t.Fatal(err)
	}
	if lastSequence != count || offset != wantOffset || tail {
		t.Fatalf("scan: lastSequence=%d offset=%d tail=%v; want sequence=%d offset=%d tail=false",
			lastSequence, offset, tail, count, wantOffset)
	}
}

func TestScanDiskRecordsRejectsSequenceRegression(t *testing.T) {
	first, err := encodeDiskRecord(testDiskRecord(2))
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeDiskRecord(testDiskRecord(1))
	if err != nil {
		t.Fatal(err)
	}

	_, _, _, err = scanDiskRecords(bytes.NewReader(append(first, second...)))
	if !errors.Is(err, errRecordCorrupt) {
		t.Fatalf("sequence regression accepted: %v", err)
	}
}

type failingDiskOutput struct {
	err error
}

func (w failingDiskOutput) Write([]byte) (int, error) {
	return 0, w.err
}

func TestDiskWriterAppendFailureIsSticky(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected disk write failure")
	writer.buf = bufio.NewWriterSize(failingDiskOutput{err: injected}, 1)

	sequence, _, firstErr := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":1}`))
	if sequence != 0 || !errors.Is(firstErr, injected) {
		t.Fatalf("first append: sequence=%d err=%v", sequence, firstErr)
	}
	sequence, _, secondErr := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":2}`))
	if sequence != 0 || secondErr != firstErr {
		t.Fatalf("second append did not return sticky failure: sequence=%d first=%v second=%v", sequence, firstErr, secondErr)
	}
	if writer.nextSequence != 0 {
		t.Fatalf("failed append advanced sequence to %d", writer.nextSequence)
	}
	if err := writer.Close(); !errors.Is(err, injected) {
		t.Fatalf("close did not preserve write failure: %v", err)
	}
}

func TestDiskWriterFlushAndSyncFailuresAreSticky(t *testing.T) {
	operations := map[string]func(*diskWriter) error{
		"flush": (*diskWriter).Flush,
		"sync":  (*diskWriter).Sync,
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			writer, err := openDiskWriter(t.TempDir(), diskWriterOptions{MaxSegmentBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected buffered I/O failure")
			writer.buf = bufio.NewWriterSize(failingDiskOutput{err: injected}, 64)
			if _, err := writer.buf.Write([]byte("pending")); err != nil {
				t.Fatal(err)
			}

			firstErr := operation(writer)
			if !errors.Is(firstErr, injected) {
				t.Fatalf("%s error = %v, want injected failure", name, firstErr)
			}
			_, _, appendErr := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":1}`))
			if appendErr != firstErr {
				t.Fatalf("append did not return sticky error: first=%v append=%v", firstErr, appendErr)
			}
			if closeErr := writer.Close(); !errors.Is(closeErr, injected) {
				t.Fatalf("close did not preserve sticky error: %v", closeErr)
			}
		})
	}
}

func TestDiskWriterRotationFlushFailureIsSticky(t *testing.T) {
	writer, err := openDiskWriter(t.TempDir(), diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected rotation flush failure")
	writer.buf = bufio.NewWriterSize(failingDiskOutput{err: injected}, 64)
	if _, err := writer.buf.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	writer.size = writer.maxBytes

	sequence, _, firstErr := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":1}`))
	if sequence != 0 || !errors.Is(firstErr, injected) {
		t.Fatalf("rotation append: sequence=%d err=%v", sequence, firstErr)
	}
	_, _, secondErr := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":2}`))
	if secondErr != firstErr {
		t.Fatalf("append did not return sticky rotation error: first=%v second=%v", firstErr, secondErr)
	}
	if closeErr := writer.Close(); !errors.Is(closeErr, injected) {
		t.Fatalf("close did not preserve rotation error: %v", closeErr)
	}
}


func TestDiskWriterCloseFailureIsStable(t *testing.T) {
	writer, err := openDiskWriter(t.TempDir(), diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected close flush failure")
	writer.buf = bufio.NewWriterSize(failingDiskOutput{err: injected}, 64)
	if _, err := writer.buf.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}

	firstErr := writer.Close()
	if !errors.Is(firstErr, injected) {
		t.Fatalf("close error = %v, want injected failure", firstErr)
	}
	if secondErr := writer.Close(); secondErr != firstErr {
		t.Fatalf("repeated Close changed result: first=%v second=%v", firstErr, secondErr)
	}
}


func TestDiskWriterRecoversAfterRotationSegmentCreationFailure(t *testing.T) {
	dir := t.TempDir()
	opts := diskWriterOptions{MaxSegmentBytes: 4096}
	writer, err := openDiskWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	channelID, streamID := uuid.New(), uuid.New()
	firstSequence, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(), []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if firstSequence != 1 {
		t.Fatalf("first sequence=%d want=1", firstSequence)
	}

	// A directory at the next segment path makes exclusive file creation fail
	// after rotation has flushed and closed the previous segment.
	blockedPath := filepath.Join(dir, "segment-000002.pfs")
	if err := os.Mkdir(blockedPath, 0o750); err != nil {
		t.Fatal(err)
	}
	writer.size = writer.maxBytes
	if _, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(), []byte(`{"v":2}`)); err == nil {
		t.Fatal("append succeeded despite next-segment creation failure")
	}
	if writer.file != nil || writer.buf != nil {
		t.Fatal("writer retained an active file after rotation creation failed")
	}
	if err := writer.Close(); err == nil {
		t.Fatal("Close did not retain rotation creation failure")
	}

	if err := os.Remove(blockedPath); err != nil {
		t.Fatal(err)
	}
	writer, err = openDiskWriter(dir, opts)
	if err != nil {
		t.Fatalf("reopen after clearing segment path: %v", err)
	}
	secondSequence, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(), []byte(`{"v":2}`))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if secondSequence != 2 {
		t.Fatalf("recovered sequence=%d want=2", secondSequence)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for expected := uint64(1); expected <= 2; expected++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read sequence %d: %v", expected, err)
		}
		if record.Sequence != expected {
			t.Fatalf("read sequence=%d want=%d", record.Sequence, expected)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after recovered append, got %v", err)
	}
}

func TestDiskWriterSyncEachAppendFlushesAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	writer, err := openDiskWriter(dir, diskWriterOptions{
		MaxSegmentBytes: 220,
		SyncEachAppend:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	channelID, streamID := uuid.New(), uuid.New()
	for expected := uint64(1); expected <= 4; expected++ {
		sequence, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(),
			[]byte(`{"payload":"large enough to force segment rotation"}`))
		if err != nil {
			t.Fatalf("append %d: %v", expected, err)
		}
		if sequence != expected {
			t.Fatalf("sequence=%d want=%d", sequence, expected)
		}

		activePath := filepath.Join(dir, fmt.Sprintf("segment-%06d.pfs", writer.segment))
		info, err := os.Stat(activePath)
		if err != nil {
			t.Fatalf("stat active segment after append %d: %v", expected, err)
		}
		if info.Size() == 0 {
			t.Fatalf("active segment %q was not flushed after append %d", activePath, expected)
		}
	}

	segments, err := diskSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("expected rotation with SyncEachAppend, found %d segment(s)", len(segments))
	}

	// The reader must see every acknowledged append before the writer closes.
	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for expected := uint64(1); expected <= 4; expected++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read sequence %d before writer close: %v", expected, err)
		}
		if record.Sequence != expected {
			t.Fatalf("read sequence=%d want=%d", record.Sequence, expected)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF before writer close, got %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
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


func TestDiskReaderSkipsEmptySegmentsAndRejectsSequenceRegression(t *testing.T) {
	dir := t.TempDir()
	first, err := encodeDiskRecord(testDiskRecord(4))
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeDiskRecord(testDiskRecord(3))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), first, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000002.pfs"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000003.pfs"), second, 0o640); err != nil {
		t.Fatal(err)
	}
	r, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if record, err := r.Next(); err != nil || record.Sequence != 4 {
		t.Fatalf("first record: sequence=%d err=%v", record.Sequence, err)
	}
	if _, err := r.Next(); !errors.Is(err, errRecordCorrupt) {
		t.Fatalf("sequence regression accepted: %v", err)
	}
}

func TestDiskReaderRejectsIncompleteTailAndCompleteCorruption(t *testing.T) {
	for name, contents := range map[string][]byte{
		"incomplete": append(mustEncodeDiskRecord(t, 1), []byte("PFS")...),
		"corrupt": func() []byte {
			frame := mustEncodeDiskRecord(t, 1)
			frame[len(frame)-1] ^= 0xff
			return frame
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), contents, 0o640); err != nil {
				t.Fatal(err)
			}
			r, err := openDiskReader(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			_, firstErr := r.Next()
			if name == "incomplete" {
				if firstErr != nil {
					t.Fatalf("valid record before incomplete tail: %v", firstErr)
				}
				_, firstErr = r.Next()
			}
			if !errors.Is(firstErr, errRecordCorrupt) {
				t.Fatalf("invalid segment accepted: %v", firstErr)
			}
		})
	}
}

func TestDiskReaderStopsAtCorruptionInMiddleSegment(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeDiskRecord(t, 1)
	second := mustEncodeDiskRecord(t, 2)
	corrupt := mustEncodeDiskRecord(t, 3)
	corrupt[len(corrupt)-1] ^= 0xff
	last := mustEncodeDiskRecord(t, 4)

	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), first, 0o640); err != nil {
		t.Fatal(err)
	}
	middle := append(append([]byte(nil), second...), corrupt...)
	if err := os.WriteFile(filepath.Join(dir, "segment-000002.pfs"), middle, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000003.pfs"), last, 0o640); err != nil {
		t.Fatal(err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for expected := uint64(1); expected <= 2; expected++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read sequence %d: %v", expected, err)
		}
		if record.Sequence != expected {
			t.Fatalf("sequence=%d want=%d", record.Sequence, expected)
		}
	}
	_, firstErr := reader.Next()
	if !errors.Is(firstErr, errRecordCorrupt) {
		t.Fatalf("middle-segment corruption accepted: %v", firstErr)
	}
	_, secondErr := reader.Next()
	if secondErr != firstErr {
		t.Fatalf("terminal error changed: first=%v second=%v", firstErr, secondErr)
	}
}

func TestDiskSegmentsRejectInvalidNames(t *testing.T) {
	for _, name := range []string{"segment-000000.pfs", "segment-1.pfs", "backup.pfs"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), nil, 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := openDiskReader(dir); err == nil {
				t.Fatalf("invalid segment name %q accepted", name)
			}
		})
	}
}

func mustEncodeDiskRecord(t *testing.T, sequence uint64) []byte {
	t.Helper()
	frame, err := encodeDiskRecord(testDiskRecord(sequence))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestDiskReaderCloseClosesActiveSegment(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), mustEncodeDiskRecord(t, 1), 0o640); err != nil {
		t.Fatal(err)
	}
	r, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after Close: %v", err)
	}
}


func TestDiskSegmentsRejectNumberingGaps(t *testing.T) {
	for _, names := range [][]string{
		{"segment-000002.pfs"},
		{"segment-000001.pfs", "segment-000003.pfs"},
	} {
		t.Run(strings.Join(names, "_"), func(t *testing.T) {
			dir := t.TempDir()
			for _, name := range names {
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := openDiskReader(dir); err == nil {
				t.Fatalf("segment gap accepted: %v", names)
			}
		})
	}
}

func TestDiskWriterReopenAcrossSegmentsReaderContinuity(t *testing.T) {
	dir := t.TempDir()
	opts := diskWriterOptions{MaxSegmentBytes: 220}
	channelID, streamID := uuid.New(), uuid.New()

	writer, err := openDiskWriter(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		sequence, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(),
			[]byte(`{"payload":"large enough to force segment rotation"}`))
		if err != nil {
			t.Fatalf("initial append %d: %v", i, err)
		}
		if sequence != uint64(i) {
			t.Fatalf("initial sequence=%d want=%d", sequence, i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	writer, err = openDiskWriter(dir, opts)
	if err != nil {
		t.Fatalf("reopen writer: %v", err)
	}
	for i := 5; i <= 6; i++ {
		sequence, _, err := writer.Append(channelID, streamID, "schema", "event", time.Now(),
			[]byte(`{"payload":"after reopen"}`))
		if err != nil {
			t.Fatalf("append after reopen %d: %v", i, err)
		}
		if sequence != uint64(i) {
			t.Fatalf("reopened sequence=%d want=%d", sequence, i)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	for expected := uint64(1); expected <= 6; expected++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read sequence %d: %v", expected, err)
		}
		if record.Sequence != expected {
			t.Fatalf("read sequence=%d want=%d", record.Sequence, expected)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after reopened stream, got %v", err)
	}
}


func TestDiskWriterRejectsIncompleteTailInNonActiveSegment(t *testing.T) {
	dir := t.TempDir()
	first := append(mustEncodeDiskRecord(t, 1), []byte("PFS")...)
	second := mustEncodeDiskRecord(t, 2)
	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), first, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segment-000002.pfs"), second, 0o640); err != nil {
		t.Fatal(err)
	}

	if _, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096}); !errors.Is(err, errRecordCorrupt) {
		t.Fatalf("incomplete non-active segment accepted: %v", err)
	}
}

func TestDiskWriterRejectsCompleteCorruptActiveFrame(t *testing.T) {
	dir := t.TempDir()
	frame := mustEncodeDiskRecord(t, 1)
	frame[len(frame)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, "segment-000001.pfs"), frame, 0o640); err != nil {
		t.Fatal(err)
	}

	if _, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096}); !errors.Is(err, errRecordCorrupt) {
		t.Fatalf("complete corrupt active frame accepted as recoverable tail: %v", err)
	}
}

func TestDiskWriterTruncatesIncompleteActiveTailAtLastValidFrame(t *testing.T) {
	dir := t.TempDir()
	valid := mustEncodeDiskRecord(t, 1)
	contents := append(append([]byte(nil), valid...), []byte("PFS")...)
	path := filepath.Join(dir, "segment-000001.pfs")
	if err := os.WriteFile(path, contents, 0o640); err != nil {
		t.Fatal(err)
	}

	writer, err := openDiskWriter(dir, diskWriterOptions{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatalf("open writer with incomplete active tail: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(valid)) {
		t.Fatalf("recovered segment size=%d want=%d", info.Size(), len(valid))
	}

	sequence, _, err := writer.Append(uuid.New(), uuid.New(), "schema", "event", time.Now(), []byte(`{"v":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 2 {
		t.Fatalf("recovered append sequence=%d want=2", sequence)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := openDiskReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for expected := uint64(1); expected <= 2; expected++ {
		record, err := reader.Next()
		if err != nil {
			t.Fatalf("read sequence %d: %v", expected, err)
		}
		if record.Sequence != expected {
			t.Fatalf("read sequence=%d want=%d", record.Sequence, expected)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after recovered append, got %v", err)
	}
}
