//go:build linux

package stream

import (
	"errors"
	"os"
)

func checkDirectorySyncSupport() error { return nil }

// syncSegmentDirectory persists the directory entry created for a new segment.
func syncSegmentDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
