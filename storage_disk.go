package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	recordVersion uint16 = 1
	recordHeaderBytes = 8
	recordChecksumBytes = 4
	maxRecordBytes = 64 << 20
)
var recordMagic = [4]byte{'P','F','S','R'}
var errRecordCorrupt = errors.New("stream: corrupt storage record")

type diskRecord struct {
	Version uint16
	EventID uuid.UUID
	StreamID uuid.UUID
	ChannelID uuid.UUID
	Sequence uint64
	Timestamp int64
	SchemaFingerprint string
	Type string
	Payload json.RawMessage
}

func encodeDiskRecord(r diskRecord) ([]byte, error) {
	if r.Version == 0 { r.Version = recordVersion }
	if r.Version != recordVersion { return nil, fmt.Errorf("stream: unsupported record version %d", r.Version) }
	if r.EventID == uuid.Nil || r.StreamID == uuid.Nil || r.ChannelID == uuid.Nil || r.Sequence == 0 || r.Timestamp == 0 || r.SchemaFingerprint == "" || r.Type == "" || len(r.Payload) == 0 {
		return nil, fmt.Errorf("%w: missing record field", errRecordCorrupt)
	}
	body, err := json.Marshal(r)
	if err != nil { return nil, err }
	if len(body) > maxRecordBytes { return nil, fmt.Errorf("stream: record exceeds size limit") }
	frame := make([]byte, recordHeaderBytes+len(body)+recordChecksumBytes)
	copy(frame[:4], recordMagic[:])
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(body)))
	copy(frame[8:], body)
	binary.BigEndian.PutUint32(frame[len(frame)-4:], crc32.ChecksumIEEE(body))
	return frame, nil
}

func decodeDiskRecord(frame []byte) (diskRecord, error) {
	var r diskRecord
	if len(frame) < recordHeaderBytes+recordChecksumBytes || !bytes.Equal(frame[:4], recordMagic[:]) { return r, errRecordCorrupt }
	n := int(binary.BigEndian.Uint32(frame[4:8]))
	if n > maxRecordBytes || len(frame) != recordHeaderBytes+n+recordChecksumBytes { return r, errRecordCorrupt }
	body := frame[8:8+n]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(frame[len(frame)-4:]) { return r, fmt.Errorf("%w: checksum", errRecordCorrupt) }
	if err := json.Unmarshal(body, &r); err != nil { return r, fmt.Errorf("%w: %v", errRecordCorrupt, err) }
	if r.Version != recordVersion { return r, fmt.Errorf("stream: unsupported record version %d", r.Version) }
	if r.EventID == uuid.Nil || r.StreamID == uuid.Nil || r.ChannelID == uuid.Nil || r.Sequence == 0 || r.Timestamp == 0 || r.SchemaFingerprint == "" || r.Type == "" || len(r.Payload) == 0 { return r, errRecordCorrupt }
	return r, nil
}

// scanDiskRecords only tolerates a short trailing frame; checksum or framing
// corruption in a complete frame is returned to the caller.
func scanDiskRecords(rd io.Reader) (uint64, int64, bool, error) {
	var lastSequence uint64
	var offset int64
	for {
		h := make([]byte, recordHeaderBytes)
		n, err := io.ReadFull(rd, h)
		if err == io.EOF && n == 0 { return lastSequence, offset, false, nil }
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n > 0) { return lastSequence, offset, true, nil }
		if err != nil { return lastSequence, offset, false, err }
		if !bytes.Equal(h[:4], recordMagic[:]) { return lastSequence, offset, false, fmt.Errorf("%w at byte %d: magic", errRecordCorrupt, offset) }
		size := int(binary.BigEndian.Uint32(h[4:8]))
		if size > maxRecordBytes { return lastSequence, offset, false, fmt.Errorf("%w at byte %d: oversized frame", errRecordCorrupt, offset) }
		rest := make([]byte, size+recordChecksumBytes)
		n, err = io.ReadFull(rd, rest)
		if err == io.EOF || err == io.ErrUnexpectedEOF { return lastSequence, offset, true, nil }
		if err != nil { return lastSequence, offset, false, err }
		frame := append(h, rest...)
		record, err := decodeDiskRecord(frame)
		if err != nil { return lastSequence, offset, false, fmt.Errorf("scan byte %d: %w", offset, err) }
		if record.Sequence <= lastSequence { return lastSequence, offset, false, fmt.Errorf("%w: non-increasing sequence", errRecordCorrupt) }
		lastSequence = record.Sequence
		offset += int64(len(frame))
	}
}

type diskWriterOptions struct {
	MaxSegmentBytes int64
	SyncEachAppend bool
}

// diskWriter is private append-only segment I/O. Payload must already be
// serialized and redacted by Event's private serialization boundary.
type diskWriter struct {
	mu sync.Mutex
	dir string
	maxBytes int64
	syncEachAppend bool
	file *os.File
	buf *bufio.Writer
	segment uint64
	size int64
	nextSequence uint64
	closed bool
	failed error
}

func openDiskWriter(dir string, opts diskWriterOptions) (*diskWriter, error) {
	if opts.MaxSegmentBytes <= 0 { opts.MaxSegmentBytes = 64 << 20 }
	if err := os.MkdirAll(dir, 0o750); err != nil { return nil, err }
	w := &diskWriter{dir: dir, maxBytes: opts.MaxSegmentBytes, syncEachAppend: opts.SyncEachAppend}
	paths, err := diskSegments(dir)
	if err != nil { return nil, err }
	if len(paths) == 0 {
		w.segment = 1
		if err := w.newSegment(); err != nil { return nil, err }
		return w, nil
	}
	last := paths[len(paths)-1]
	w.segment = segmentNum(last)
	if w.segment == 0 {
		return nil, fmt.Errorf("stream: invalid segment filename %q", filepath.Base(last))
	}
	// Recover sequence across every segment, not just the active segment.
	// An empty active segment can exist after a rollover interrupted an append.
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil { return nil, err }
		lastSequence, valid, tail, scanErr := scanDiskRecords(f)
		closeErr := f.Close()
		if scanErr != nil { return nil, scanErr }
		if closeErr != nil { return nil, closeErr }
		if tail && i != len(paths)-1 {
			return nil, fmt.Errorf("%w: incomplete tail in non-active segment %q", errRecordCorrupt, filepath.Base(path))
		}
		if lastSequence != 0 {
			if lastSequence <= w.nextSequence {
				return nil, fmt.Errorf("%w: non-increasing sequence across segments", errRecordCorrupt)
			}
			w.nextSequence = lastSequence
		}
		if i == len(paths)-1 {
			f, err := os.OpenFile(path, os.O_RDWR, 0o640)
			if err != nil { return nil, err }
			if tail {
				if err := f.Truncate(valid); err != nil { f.Close(); return nil, err }
			}
			if _, err := f.Seek(0, io.SeekEnd); err != nil { f.Close(); return nil, err }
			w.file, w.buf, w.size = f, bufio.NewWriter(f), valid
		}
	}
	return w, nil
}

func diskSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type segment struct {
		path string
		num  uint64
	}
	segments := make([]segment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".pfs" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		num := segmentNum(path)
		if num == 0 || fmt.Sprintf("segment-%06d.pfs", num) != entry.Name() {
			return nil, fmt.Errorf("stream: invalid segment filename %q", entry.Name())
		}
		segments = append(segments, segment{path: path, num: num})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].num < segments[j].num })
	paths := make([]string, len(segments))
	for i, segment := range segments {
		expected := uint64(i + 1)
		if segment.num != expected {
			return nil, fmt.Errorf("stream: missing segment %06d before %q", expected, filepath.Base(segment.path))
		}
		paths[i] = segment.path
	}
	return paths, nil
}

func segmentNum(path string) uint64 {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "segment-") || !strings.HasSuffix(base, ".pfs") {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(base, "segment-"), ".pfs"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
func (w *diskWriter) newSegment() error {
	path := filepath.Join(w.dir, fmt.Sprintf("segment-%06d.pfs", w.segment))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil { return err }
	w.file, w.buf, w.size = f, bufio.NewWriter(f), 0
	return nil
}

func (w *diskWriter) Append(channelID, streamID uuid.UUID, schema, typeName string, at time.Time, payload []byte) (uint64, uuid.UUID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed { return 0, uuid.Nil, ErrClosed }
	if w.failed != nil { return 0, uuid.Nil, w.failed }
	r := diskRecord{Version: recordVersion, EventID: uuid.New(), StreamID: streamID, ChannelID: channelID, Sequence: w.nextSequence+1, Timestamp: at.UnixNano(), SchemaFingerprint: schema, Type: typeName, Payload: append(json.RawMessage(nil), payload...)}
	frame, err := encodeDiskRecord(r)
	if err != nil { return 0, uuid.Nil, err }
	if w.size > 0 && w.size+int64(len(frame)) > w.maxBytes {
		if err := w.rotate(); err != nil { return 0, uuid.Nil, w.failLocked(err) }
	}
	if _, err := w.buf.Write(frame); err != nil { return 0, uuid.Nil, w.failLocked(err) }
	w.size += int64(len(frame))
	w.nextSequence = r.Sequence
	if w.syncEachAppend { if err := w.syncLocked(); err != nil { return 0, uuid.Nil, w.failLocked(err) } }
	return r.Sequence, r.EventID, nil
}
func (w *diskWriter) rotate() error {
	if err := w.flushLocked(); err != nil { return err }
	if err := w.file.Close(); err != nil {
		w.file, w.buf = nil, nil
		return err
	}
	w.file, w.buf = nil, nil
	w.segment++
	return w.newSegment()
}
func (w *diskWriter) Flush() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return ErrClosed }
	if w.failed != nil { return w.failed }
	if err := w.flushLocked(); err != nil { return w.failLocked(err) }
	return nil
}
func (w *diskWriter) flushLocked() error { return w.buf.Flush() }
func (w *diskWriter) Sync() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return ErrClosed }
	if w.failed != nil { return w.failed }
	if err := w.syncLocked(); err != nil { return w.failLocked(err) }
	return nil
}
func (w *diskWriter) syncLocked() error {
	if err := w.flushLocked(); err != nil { return err }
	return w.file.Sync()
}
func (w *diskWriter) failLocked(err error) error {
	if w.failed == nil {
		w.failed = err
	}
	return w.failed
}
func (w *diskWriter) Close() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return nil }
	var flushErr error
	if w.failed == nil && w.buf != nil {
		flushErr = w.flushLocked()
	}
	var closeErr error
	if w.file != nil {
		closeErr = w.file.Close()
	}
	w.closed = true
	return errors.Join(w.failed, flushErr, closeErr)
}

type diskReader struct {
	paths        []string
	segmentIndex int
	file         *os.File
	segmentPath  string
	offset       int64
	lastSequence uint64
	terminalErr  error
	closed       bool
}

// openDiskReader validates the segment set before opening it for sequential
// reading. Segment files are opened lazily so memory use is independent of the
// total size of a segment or the complete stream.
func openDiskReader(dir string) (*diskReader, error) {
	paths, err := diskSegments(dir)
	if err != nil {
		return nil, err
	}
	return &diskReader{paths: paths}, nil
}

// readDiskRecord reads exactly one frame. A short final header or body is
// reported as an incomplete tail; malformed complete frames are corruption.
func readDiskRecord(rd io.Reader, offset int64) (diskRecord, int64, bool, error) {
	var record diskRecord
	header := make([]byte, recordHeaderBytes)
	n, err := io.ReadFull(rd, header)
	if err == io.EOF && n == 0 {
		return record, 0, false, io.EOF
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return record, 0, true, nil
	}
	if err != nil {
		return record, 0, false, err
	}
	if !bytes.Equal(header[:4], recordMagic[:]) {
		return record, 0, false, fmt.Errorf("%w at byte %d: magic", errRecordCorrupt, offset)
	}
	size := binary.BigEndian.Uint32(header[4:8])
	if size > maxRecordBytes {
		return record, 0, false, fmt.Errorf("%w at byte %d: oversized frame", errRecordCorrupt, offset)
	}
	rest := make([]byte, int(size)+recordChecksumBytes)
	if _, err := io.ReadFull(rd, rest); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return record, 0, true, nil
		}
		return record, 0, false, err
	}
	frame := append(header, rest...)
	record, err = decodeDiskRecord(frame)
	if err != nil {
		return diskRecord{}, 0, false, fmt.Errorf("scan byte %d: %w", offset, err)
	}
	return record, int64(len(frame)), false, nil
}

// Next returns the next record in persisted sequence order, or io.EOF when
// every segment has been read. Only one frame is decoded and retained per call.
func (r *diskReader) Next() (diskRecord, error) {
	if r.closed {
		return diskRecord{}, ErrClosed
	}
	if r.terminalErr != nil {
		return diskRecord{}, r.terminalErr
	}
	for {
		if r.file == nil {
			if r.segmentIndex >= len(r.paths) {
				return diskRecord{}, io.EOF
			}
			path := r.paths[r.segmentIndex]
			r.segmentIndex++
			file, err := os.Open(path)
			if err != nil {
				r.terminalErr = err
				return diskRecord{}, err
			}
			r.file, r.segmentPath, r.offset = file, path, 0
		}

		record, frameBytes, incomplete, err := readDiskRecord(r.file, r.offset)
		if err == io.EOF {
			closeErr := r.file.Close()
			r.file, r.segmentPath = nil, ""
			if closeErr != nil {
				r.terminalErr = closeErr
				return diskRecord{}, closeErr
			}
			continue
		}
		if err != nil {
			r.terminalErr = r.failSegment(err)
			return diskRecord{}, r.terminalErr
		}
		if incomplete {
			err := fmt.Errorf("%w: incomplete tail in %q", errRecordCorrupt, filepath.Base(r.segmentPath))
			r.terminalErr = r.failSegment(err)
			return diskRecord{}, r.terminalErr
		}
		if record.Sequence <= r.lastSequence {
			err := fmt.Errorf("%w: non-increasing sequence across segments", errRecordCorrupt)
			r.terminalErr = r.failSegment(err)
			return diskRecord{}, r.terminalErr
		}
		r.offset += frameBytes
		r.lastSequence = record.Sequence
		return record, nil
	}
}

func (r *diskReader) failSegment(err error) error {
	if r.file != nil {
		err = errors.Join(err, r.file.Close())
		r.file = nil
	}
	r.segmentPath = ""
	return err
}

// Close releases the active segment handle and reader state.
func (r *diskReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.paths = nil
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}
