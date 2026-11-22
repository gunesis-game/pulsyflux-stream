package stream

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	asyncQueueCapacity = 256
	asyncBatchSize    = 32
	asyncFlushInterval = 25 * time.Millisecond
	privateStorageDir = "pulsyflux-stream-data"
)

// sharedWriterManager owns one private async writer and counts active EventChannel
// owners. It deliberately keeps no owner registry.
type sharedWriterManager struct {
	mu     sync.Mutex
	writer *asyncDiskWriter
	dir    string
	owners int
}

var processWriterManager sharedWriterManager

// privateStorageRoot resolves the library-owned storage directory from cwd.
func privateStorageRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("stream: resolve private storage root: %w", err)
	}
	return filepath.Abs(filepath.Join(cwd, privateStorageDir))
}

// acquire starts or joins the process writer. The returned release function is
// idempotent; the final owner drains and closes the writer.
func (manager *sharedWriterManager) acquire(dir string) (*asyncDiskWriter, func() error, error) {
	absoluteDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("stream: resolve storage directory: %w", err)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.writer != nil {
		if manager.dir != absoluteDir {
			return nil, nil, fmt.Errorf("stream: shared writer already owns a different storage root")
		}
		manager.owners++
		return manager.writer, manager.releaseOnce(), nil
	}

	disk, err := openDiskWriter(absoluteDir, diskWriterOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("stream: initialize private disk writer: %w", err)
	}
	writer, err := newAsyncDiskWriter(disk, asyncQueueCapacity, asyncBatchSize, asyncFlushInterval)
	if err != nil {
		_ = disk.Close()
		return nil, nil, fmt.Errorf("stream: initialize async disk writer: %w", err)
	}
	manager.writer = writer
	manager.dir = absoluteDir
	manager.owners = 1
	return writer, manager.releaseOnce(), nil
}

func (manager *sharedWriterManager) releaseOnce() func() error {
	var once sync.Once
	var result error
	return func() error {
		once.Do(func() {
			manager.mu.Lock()
			defer manager.mu.Unlock()
			if manager.owners <= 0 {
				return
			}
			manager.owners--
			if manager.owners == 0 {
				writer := manager.writer
				manager.writer = nil
				manager.dir = ""
				result = writer.Close()
			}
		})
		return result
	}
}

// acquireProcessWriter initializes the process-wide private storage writer.
func acquireProcessWriter() (*asyncDiskWriter, func() error, error) {
	dir, err := privateStorageRoot()
	if err != nil {
		return nil, nil, err
	}
	return processWriterManager.acquire(dir)
}
