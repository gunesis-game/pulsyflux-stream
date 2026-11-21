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
func scanDiskRecords(rd io.Reader) ([]diskRecord, int64, bool, error) {
	var out []diskRecord
	var offset int64
	for {
		h := make([]byte, recordHeaderBytes)
		n, err := io.ReadFull(rd, h)
		if err == io.EOF && n == 0 { return out, offset, false, nil }
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n > 0) { return out, offset, true, nil }
		if err != nil { return out, offset, false, err }
		if !bytes.Equal(h[:4], recordMagic[:]) { return out, offset, false, fmt.Errorf("%w at byte %d: magic", errRecordCorrupt, offset) }
		size := int(binary.BigEndian.Uint32(h[4:8]))
		if size > maxRecordBytes { return out, offset, false, fmt.Errorf("%w at byte %d: oversized frame", errRecordCorrupt, offset) }
		rest := make([]byte, size+recordChecksumBytes)
		n, err = io.ReadFull(rd, rest)
		if err == io.EOF || err == io.ErrUnexpectedEOF { return out, offset, true, nil }
		if err != nil { return out, offset, false, err }
		frame := append(h, rest...)
		r, err := decodeDiskRecord(frame)
		if err != nil { return out, offset, false, fmt.Errorf("scan byte %d: %w", offset, err) }
		if len(out) > 0 && r.Sequence <= out[len(out)-1].Sequence { return out, offset, false, fmt.Errorf("%w: non-increasing sequence", errRecordCorrupt) }
		out = append(out, r)
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
		records, valid, tail, scanErr := scanDiskRecords(f)
		closeErr := f.Close()
		if scanErr != nil { return nil, scanErr }
		if closeErr != nil { return nil, closeErr }
		if tail && i != len(paths)-1 {
			return nil, fmt.Errorf("%w: incomplete tail in non-active segment %q", errRecordCorrupt, filepath.Base(path))
		}
		for _, record := range records {
			if record.Sequence <= w.nextSequence {
				return nil, fmt.Errorf("%w: non-increasing sequence across segments", errRecordCorrupt)
			}
			w.nextSequence = record.Sequence
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
	if err != nil { return nil, err }
	var names []string
	for _, e := range entries { if !e.IsDir() && filepath.Ext(e.Name()) == ".pfs" { names = append(names, filepath.Join(dir, e.Name())) } }
	sort.Strings(names)
	return names, nil
}
func segmentNum(path string) uint64 { var n uint64; _, _ = fmt.Sscanf(filepath.Base(path), "segment-%d.pfs", &n); return n }
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
	r := diskRecord{Version: recordVersion, EventID: uuid.New(), StreamID: streamID, ChannelID: channelID, Sequence: w.nextSequence+1, Timestamp: at.UnixNano(), SchemaFingerprint: schema, Type: typeName, Payload: append(json.RawMessage(nil), payload...)}
	frame, err := encodeDiskRecord(r)
	if err != nil { return 0, uuid.Nil, err }
	if w.size > 0 && w.size+int64(len(frame)) > w.maxBytes {
		if err := w.rotate(); err != nil { return 0, uuid.Nil, err }
	}
	if _, err := w.buf.Write(frame); err != nil { return 0, uuid.Nil, err }
	w.size += int64(len(frame))
	w.nextSequence = r.Sequence
	if w.syncEachAppend { if err := w.syncLocked(); err != nil { return 0, uuid.Nil, err } }
	return r.Sequence, r.EventID, nil
}
func (w *diskWriter) rotate() error {
	if err := w.flushLocked(); err != nil { return err }
	if err := w.file.Close(); err != nil { return err }
	w.segment++
	return w.newSegment()
}
func (w *diskWriter) Flush() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return ErrClosed }
	return w.flushLocked()
}
func (w *diskWriter) flushLocked() error { if err := w.buf.Flush(); err != nil { return err }; return nil }
func (w *diskWriter) Sync() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return ErrClosed }
	return w.syncLocked()
}
func (w *diskWriter) syncLocked() error {
	if err := w.flushLocked(); err != nil { return err }
	return w.file.Sync()
}
func (w *diskWriter) Close() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return nil }
	flushErr := w.flushLocked()
	closeErr := w.file.Close()
	w.closed = true
	return errors.Join(flushErr, closeErr)
}
