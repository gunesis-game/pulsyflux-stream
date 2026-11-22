package stream

import (
	"errors"
	"sync"
	"time"
)

// asyncWriterOwner is implemented by the EventChannel that admitted a record.
// Ownership is carried by queue entries only; the writer does not keep a registry.
type asyncWriterOwner interface {
	storageWriteFailed(error)
}

type asyncWriterItem struct {
	owner           asyncWriterOwner
	channelIdentity  string
	streamIdentity   string
	timestamp       time.Time
	payload         []byte
}

// asyncDiskWriter serializes disk I/O behind a bounded queue. It is private and
// intentionally does not expose diskWriter or storage configuration publicly.
type asyncDiskWriter struct {
	disk        *diskWriter
	queue       chan asyncWriterItem
	done        chan struct{}
	failed      chan struct{}
	mu          sync.Mutex
	failure     error
	closed      bool
	closeOnce   sync.Once
	closeErr    error
	batchSize  int
	flushEvery time.Duration
}

func newAsyncDiskWriter(disk *diskWriter, queueSize, batchSize int, flushEvery time.Duration) (*asyncDiskWriter, error) {
	if disk == nil {
		return nil, errors.New("stream: nil disk writer")
	}
	if queueSize < 1 || batchSize < 1 || flushEvery <= 0 {
		return nil, errors.New("stream: invalid async writer settings")
	}
	w := &asyncDiskWriter{
		disk: disk, queue: make(chan asyncWriterItem, queueSize),
		done: make(chan struct{}), failed: make(chan struct{}),
		batchSize: batchSize, flushEvery: flushEvery,
	}
	go w.run()
	return w, nil
}

// Submit applies bounded backpressure. Success means queue admission only.
func (w *asyncDiskWriter) Submit(item asyncWriterItem) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if w.failure != nil {
		err := w.failure
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock()

	select {
	case w.queue <- item:
		w.mu.Lock()
		err := w.failure
		w.mu.Unlock()
		return err
	case <-w.failed:
		w.mu.Lock()
		err := w.failure
		w.mu.Unlock()
		return err
	case <-w.done:
		return ErrClosed
	}
}

// Close stops admission, drains queued records, flushes and closes the disk
// writer. Repeated calls return the same shutdown result.
func (w *asyncDiskWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
	})
	<-w.done
	return w.closeErr
}

func (w *asyncDiskWriter) run() {
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	pending := 0
	for {
		select {
		case item, ok := <-w.queue:
			if !ok {
				if w.failure == nil && pending > 0 {
					if err := w.disk.Flush(); err != nil {
						w.fail(err, nil)
					}
				}
				w.closeErr = w.disk.Close()
				close(w.done)
				return
			}
			_, _, err := w.disk.Append(item.channelIdentity, item.streamIdentity, item.timestamp, item.payload)
			if err != nil {
				w.fail(err, &item)
				w.closeErr = w.disk.Close()
				close(w.done)
				return
			}
			pending++
			if pending >= w.batchSize {
				if err := w.disk.Flush(); err != nil {
					w.fail(err, nil)
					w.closeErr = w.disk.Close()
					close(w.done)
					return
				}
				pending = 0
			}
		case <-ticker.C:
			if pending == 0 {
				continue
			}
			if err := w.disk.Flush(); err != nil {
				w.fail(err, nil)
				w.closeErr = w.disk.Close()
				close(w.done)
				return
			}
			pending = 0
		}
	}
}

// fail records the first terminal cause and notifies distinct owners represented
// by the failed item and queued items. Owner references are temporary routing
// information, not a persistent or global channel registry.
func (w *asyncDiskWriter) fail(err error, failedItem *asyncWriterItem) {
	w.mu.Lock()
	if w.failure == nil {
		w.failure = err
		close(w.failed)
	}
	cause := w.failure
	w.mu.Unlock()

	owners := make(map[asyncWriterOwner]struct{})
	if failedItem != nil && failedItem.owner != nil {
		owners[failedItem.owner] = struct{}{}
	}
	for {
		select {
		case item, ok := <-w.queue:
			if !ok {
				for owner := range owners {
					owner.storageWriteFailed(cause)
				}
				return
			}
			if item.owner != nil {
				owners[item.owner] = struct{}{}
			}
		default:
			for owner := range owners {
				owner.storageWriteFailed(cause)
			}
			return
		}
	}
}
