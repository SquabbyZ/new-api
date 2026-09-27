//go:build !linux

package logkafka

import "errors"

// spoolFilesystemTypeSupported reports whether this platform has a filesystem
// concept the spool has to warn about.
//
// Windows and macOS have no tmpfs, so there is nothing to detect and the check
// is not applicable rather than skipped. Inventing an equivalent -- "is this
// directory really an in-memory disk?" -- would produce false positives, and a
// warning that fires on ordinary directories is worse than no warning, because
// it teaches operators to ignore the one that matters.
const spoolFilesystemTypeSupported = false

// spoolFilesystemType is never consulted on this platform; the supported flag
// above is what the caller branches on.
func spoolFilesystemType(string) (uint64, error) {
	return 0, errors.New("this platform has no tmpfs to detect")
}
