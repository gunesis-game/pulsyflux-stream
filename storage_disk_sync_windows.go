//go:build windows

package stream

func checkDirectorySyncSupport() error { return errDirectorySyncUnsupported }

func syncSegmentDirectory(string) error { return errDirectorySyncUnsupported }
