package stream

import (
	"errors"
	"sync"
	"time"
)

// asyncWriterOwner is implemented by the EventChannel that admitted a record.
// Owner references exist only on queued records to route failures.
type asyncWriterOwner interface {
	storageWriteFailed(error)
}

type asyncWriterItem struct {
	owner           asyncWriterOwner
	channelIdentity  string
	streamIdentity  string
	timestamp       time.Time
	payload         []byte
}

// asyncDiskWriter serializes disk I/O behind a bounded private queue.
type asyncDiskWriter struct {
	disk       *diskWriter
	mu         sync.Mutex
	cond       *sync.Cond
	queue      []asyncWriterItem
	capacity   int
	closed     bool
	failure    error
	done       chan struct{}
	closeOnce  sync.Once
	closeErr   error
	batchSize  int
	flushEvery time.Duration
	flushDue  bool
}

func newAsyncDiskWriter(disk *diskWriter, queueSize, batchSize int, flushEvery time.Duration) (*asyncDiskWriter, error) {
	if disk == nil {
		return nil, errors.New("stream: nil disk writer")
	}
	if queueSize < 1 || batchSize < 1 || flushEvery <= 0 {
		return nil, errors.New("stream: invalid async writer settings")
	}
	w := &asyncDiskWriter{disk: disk, capacity: queueSize, done: make(chan struct{}), batchSize: batchSize, flushEvery: flushEvery}
	w.cond = sync.NewCond(&w.mu)
	go w.run()
	return w, nil
}

// Submit waits for bounded queue capacity. A successful return means admission.
func (w *asyncDiskWriter) Submit(item asyncWriterItem) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.queue) >= w.capacity && !w.closed && w.failure == nil {
		w.cond.Wait()
	}
	if w.failure != nil {
		return w.failure
	}
	if w.closed {
		return ErrClosed
	}
	w.queue = append(w.queue, item)
	w.cond.Signal()
	return nil
}

// Close stops admission and drains accepted items, then flushes and closes disk.
func (w *asyncDiskWriter) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	<-w.done
	return w.closeErr
}

func (w *asyncDiskWriter) run() {
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				w.mu.Lock()
				w.flushDue = true
				w.cond.Broadcast()
				w.mu.Unlock()
			case <-w.done:
				return
			}
		}
	}()
	batchCount := 0
	batchOwners := make(map[asyncWriterOwner]struct{})
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed && w.failure == nil && !w.flushDue {
			w.cond.Wait()
		}
		if w.failure != nil {
			w.mu.Unlock()
			w.closeErr = w.disk.Close()
			close(w.done)
			return
		}
		if len(w.queue) == 0 && w.flushDue && !w.closed {
			w.flushDue = false
			if batchCount == 0 {
				w.mu.Unlock()
				continue
			}
			w.mu.Unlock()
			if err := w.disk.Flush(); err != nil {
				w.fail(err, batchOwners)
				w.closeErr = w.disk.Close()
				close(w.done)
				return
			}
			batchCount = 0
			clear(batchOwners)
			continue
		}
		if len(w.queue) == 0 && w.closed {
			w.mu.Unlock()
			if batchCount > 0 {
				if err := w.disk.Flush(); err != nil {
					w.fail(err, batchOwners)
					w.closeErr = w.disk.Close()
					close(w.done)
					return
				}
			}
			w.closeErr = w.disk.Close()
			close(w.done)
			return
		}
		item := w.queue[0]
		w.queue[0] = asyncWriterItem{}
		w.queue = w.queue[1:]
		w.cond.Broadcast() // unblock producers waiting for capacity
		w.mu.Unlock()

		_, _, err := w.disk.Append(item.channelIdentity, item.streamIdentity, item.timestamp, item.payload)
		if err != nil {
			if item.owner != nil {
				batchOwners[item.owner] = struct{}{}
			}
			w.fail(err, batchOwners)
			w.closeErr = w.disk.Close()
			close(w.done)
			return
		}
		if item.owner != nil {
			batchOwners[item.owner] = struct{}{}
		}
		batchCount++
		if batchCount >= w.batchSize {
			if err := w.disk.Flush(); err != nil {
				w.fail(err, batchOwners)
				w.closeErr = w.disk.Close()
				close(w.done)
				return
			}
			batchCount = 0
			clear(batchOwners)
		}

	}
}

// fail sets the first terminal cause, drains pending queue items, and notifies
// the distinct owners whose batch or queued records could not be persisted.
func (w *asyncDiskWriter) fail(err error, owners map[asyncWriterOwner]struct{}) {
	w.mu.Lock()
	if w.failure == nil {
		w.failure = err
	}
	cause := w.failure
	for _, item := range w.queue {
		if item.owner != nil {
			owners[item.owner] = struct{}{}
		}
	}
	clear(w.queue)
	w.cond.Broadcast()
	w.mu.Unlock()
	for owner := range owners {
		owner.storageWriteFailed(cause)
	}
}
