//go:build linux

package logkafka

import "golang.org/x/sys/unix"

// spoolFilesystemTypeSupported reports whether this platform has a filesystem
// concept the spool has to warn about.
const spoolFilesystemTypeSupported = true

// The two constants are the same number; this fails to compile if they ever
// drift apart, which is the only reason to keep tmpfsMagic written out in the
// portable file.
var _ = [1]struct{}{}[tmpfsMagic-unix.TMPFS_MAGIC]

// spoolFilesystemType asks the kernel what filesystem a directory lives on.
//
// statfs rather than /proc/mounts: mount points are escaped there (\040) and
// live in a mount namespace the process may not share, so parsing them gives
// both false positives and false negatives. The kernel is authoritative about
// the directory in front of it.
func spoolFilesystemType(dir string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	// Type is int32 on some architectures and int64 on others; comparing as
	// uint64 is the one form that is correct on all of them, and a negative
	// value widened naively would never match the magic.
	return uint64(stat.Type), nil
}
