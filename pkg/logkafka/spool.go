package logkafka

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
)

// The disk fallback for rows the producer could not hand to the broker.
//
// A segment file holds one row per line, each line the payload the producer
// would have sent, byte for byte, with one '\n' appended. Nothing here parses a
// row: the spool holds opaque bytes, so the package keeps its "transports, never
// interprets" boundary and a row cannot be deformed by a round trip through the
// disk.
//
// The file name is the whole of a segment's state:
//
//	active-<pid>-<unixNanoAtOpen>-<rotationSequence>.ndjson
//
// "Sealed" means "not this process's current active file", so sealing never
// renames anything. A rename would leave the writer's open descriptor pointing
// at the sealed file, the writer would keep appending to a segment the replayer
// is reading, and the torn-tail rule below would stop meaning anything. The pid
// and the open time make a name collision with another process impossible, the
// rotation sequence makes one with an earlier rotation of this process
// impossible, and the open time in the name -- rather than the file's mtime --
// is what the retention rule ages segments by, because copying or restoring a
// directory rewrites mtime.
//
// Everything that touches a file happens on this type's writer goroutine. The
// request path only ever performs a non-blocking send on a bounded channel, so
// the disk never becomes a first-class path: with a healthy broker the directory
// stays empty and not one byte is written.
const (
	segmentPrefix = "active-"
	segmentSuffix = ".ndjson"

	// tornTailStartWindow is where the backwards search for the last newline
	// starts. It doubles until it reaches the beginning of the file: a single row
	// can be large (`content` is not bounded), and a fixed window would then
	// report a complete segment as torn and eat a good line. That mistake only
	// ever loses data on the failure path, which is why the search is unbounded
	// rather than sized.
	tornTailStartWindow = 64 << 10
)

// tmpfsMagic is Linux's TMPFS_MAGIC. It is written out rather than read from
// unix.TMPFS_MAGIC because this file builds on every platform and
// golang.org/x/sys/unix does not; spool_linux.go asserts the two agree.
const tmpfsMagic = 0x01021994

// Spool is the bounded, append-only disk fallback for log rows.
type Spool struct {
	dir            string
	segmentBytes   int64
	segmentEvery   time.Duration
	maxBytes       int64
	retention      time.Duration
	replayInterval time.Duration
	retryInterval  time.Duration

	// queue is the request path's only contact with the disk. The send on it is
	// non-blocking: a full queue is a counted, reported drop, never a request
	// that waits for a file.
	queue chan []byte

	// activePath is published by the writer before the file it names is created,
	// so a replayer can never see a file that exists but is not yet known to be
	// the active one and mistake it for a sealed segment. Empty means the writer
	// holds no active file.
	activePath atomic.Value // string
	activeFile *os.File
	activeRows int64
	activeSize int64

	// rotation counts the segments this spool has opened. It is the second half
	// of a segment's identity: the clock alone repeats when two rotations land
	// inside one timer tick. Only the writer touches it.
	rotation int64

	// totalBytes is the writer's running account of the directory. It is
	// incremented on every append and recomputed by a directory scan at every
	// seal, which is what heals the drift the replayer's unlinks otherwise
	// introduce. Only the writer touches it.
	totalBytes int64

	closing   chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool

	replayCtx     context.Context
	cancelReplay  context.CancelFunc
	replayStopped chan struct{}
	replaying     atomic.Bool

	// Counters. Every degradation this type performs is counted and reported; a
	// drop that only ever reaches a log line cannot be told from a quiet period.
	// writtenRows is the one that counts successes, because it is what the report
	// prints as "row(s) written to disk": stashedRows counts the rows the request
	// path handed over, and a row handed over can still fail its write.
	stashedRows      atomic.Int64
	stashedBytes     atomic.Int64
	writtenRows      atomic.Int64
	queueFull        atomic.Int64
	rejectedRows     atomic.Int64
	writeFailed      atomic.Int64
	sealedSegments   atomic.Int64
	tornBytes        atomic.Int64
	tornRows         atomic.Int64
	emptySegments    atomic.Int64
	evictedSegments  atomic.Int64
	evictedRows      atomic.Int64
	evictedBytes     atomic.Int64
	evictFailed      atomic.Int64
	expiredSegments  atomic.Int64
	expiredRows      atomic.Int64
	droppedRows      atomic.Int64
	droppedBytes     atomic.Int64
	replayedSegments atomic.Int64
	replayedRows     atomic.Int64
	replayFailed     atomic.Int64
	scanFailed       atomic.Int64
	reported         atomic.Int64
}

// NewSpool resolves and proves the spool directory, and starts the writer
// goroutine. A directory this process cannot write to fails startup.
//
// Failing is the deliberately asymmetric half of the rule the broker follows.
// A broker that is unreachable is a transient external state and must not stop
// startup; an unwritable spool directory is a new misconfiguration, it is
// deterministic, and it would otherwise first be discovered during an outage at
// 3 a.m. -- at which point the rows it exists to keep have already been dropped
// under contract.
func NewSpool(cfg Config) (*Spool, error) {
	dir, err := filepath.Abs(cfg.SpoolDir)
	if err != nil {
		return nil, fmt.Errorf("KAFKA_LOG_SPOOL_DIR '%s' cannot be resolved to an absolute path: %w; set it to a directory this process can write to", cfg.SpoolDir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, spoolDirectoryError(dir, err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, spoolDirectoryError(dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("KAFKA_LOG_SPOOL_DIR '%s' is not a directory; point it at a directory this process can write to", dir)
	}
	// A write probe rather than a permission-bit check: the bits do not say
	// whether a read-only mount, an ACL or a full volume will accept the write.
	probe, err := os.CreateTemp(dir, ".spool-probe-*")
	if err != nil {
		return nil, spoolDirectoryError(dir, err)
	}
	probePath := probe.Name()
	_ = probe.Close()
	if err := os.Remove(probePath); err != nil {
		return nil, spoolDirectoryError(dir, err)
	}

	replayInterval := cfg.FlushInterval
	if replayInterval <= 0 {
		replayInterval = time.Second
	}
	retryInterval := cfg.DeliveryTimeout
	if retryInterval <= 0 {
		retryInterval = time.Duration(defaultDeliveryTimeoutMs) * time.Millisecond
	}
	segmentBytes := cfg.SpoolSegmentBytes
	if segmentBytes <= 0 {
		segmentBytes = defaultSpoolSegmentBytes
	}
	segmentEvery := cfg.SpoolSegmentSeconds
	if segmentEvery <= 0 {
		segmentEvery = time.Duration(defaultSpoolSegmentSeconds) * time.Second
	}
	maxBytes := cfg.SpoolMaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultSpoolMaxBytes
	}
	retentionHours := cfg.SpoolRetentionHours
	if retentionHours <= 0 {
		retentionHours = defaultSpoolRetentionHours
	}

	s := &Spool{
		dir: dir,
		// At least one segment has to fit inside the capacity, or a row that
		// fits nowhere would be evicted the moment it is written.
		segmentBytes:   min(segmentBytes, maxBytes),
		segmentEvery:   segmentEvery,
		maxBytes:       maxBytes,
		retention:      time.Duration(retentionHours) * time.Hour,
		replayInterval: replayInterval,
		retryInterval:  retryInterval,
		queue:          make(chan []byte, spoolQueueDepth),
		closing:        make(chan struct{}),
		stopped:        make(chan struct{}),
		replayStopped:  make(chan struct{}),
	}
	s.activePath.Store("")

	// The resolved absolute path is always printed: the default is relative, and
	// an operator reading back a relative value cannot tell where it landed.
	common.SysLog(fmt.Sprintf("kafka log spool: directory %s (segment %d bytes or %s, whichever comes first; capacity %d bytes; retention %s)",
		dir, s.segmentBytes, s.segmentEvery, s.maxBytes, s.retention))
	s.warnIfNotPersistent()

	go s.writeLoop()
	return s, nil
}

// spoolQueueDepth bounds how far the request path may run ahead of the writer.
// The queue exists to hold the burst between two writes, not to absorb an
// outage: past this many rows the disk itself is the bottleneck, and waiting is
// exactly what must not happen on a request.
const spoolQueueDepth = 4096

// spoolDirectoryError spells out the two things an operator can do, because the
// directory is only ever written during an outage.
func spoolDirectoryError(dir string, err error) error {
	return fmt.Errorf("KAFKA_LOG_SPOOL_DIR '%s' cannot be used for the disk fallback: %w; create that directory, or point KAFKA_LOG_SPOOL_DIR at a directory this process can write to", dir, err)
}

// warnIfNotPersistent reports a spool on a filesystem that will not survive the
// host. It warns rather than fails: an operator may deliberately put the spool
// on a tmpfs (a one-shot container whose only writable path is /tmp), and
// refusing to start over an imperfect fallback trades availability for a
// guarantee nobody asked for at that moment. Silence would be the real defect.
func (s *Spool) warnIfNotPersistent() {
	if !spoolFilesystemTypeSupported {
		// Windows and macOS have no tmpfs, so there is nothing to detect. An
		// invented equivalent would be a false positive, which is worse than a
		// missing check.
		return
	}
	fsType, err := spoolFilesystemType(s.dir)
	if err != nil {
		common.SysError(fmt.Sprintf("kafka log spool: could not read the filesystem type of %s, so whether it survives a host restart is unknown: %s", s.dir, err))
		return
	}
	if warning := spoolFilesystemWarning(fsType, s.dir); warning != "" {
		common.SysError(warning)
	}
}

// spoolFilesystemWarning is the pure half of the tmpfs check, so both branches
// are testable on a host that has no tmpfs at all.
func spoolFilesystemWarning(fsType uint64, dir string) string {
	if fsType != tmpfsMagic {
		return ""
	}
	return fmt.Sprintf("kafka log spool: %s is on a tmpfs, so rows written there are lost when the host restarts or the directory is cleaned; point KAFKA_LOG_SPOOL_DIR at a persistent path", dir)
}

// Stash offers one payload to the writer goroutine and returns immediately.
//
// It never waits for a file: a full queue and a closed spool are both counted
// and reported drops. The alternative -- blocking the caller -- would put a
// disk write on the request path, which is the one thing this design rules out.
func (s *Spool) Stash(payload []byte) {
	// A payload carrying a raw newline would split into two lines that are not
	// both rows, so it is refused rather than written. A row produced by
	// common.Marshal cannot contain one; this is the format invariant enforced
	// instead of assumed.
	if bytes.IndexByte(payload, '\n') >= 0 {
		s.rejectedRows.Add(1)
		s.report("the kafka log spool refused a row that contains a raw newline, because the spool is line delimited and such a row would not survive the round trip")
		return
	}
	if s.closed.Load() {
		s.droppedRows.Add(1)
		s.droppedBytes.Add(int64(len(payload)) + 1)
		s.report("the kafka log spool is already closed, so the row was dropped instead of written")
		return
	}
	select {
	case s.queue <- payload:
		s.stashedRows.Add(1)
		s.stashedBytes.Add(int64(len(payload)) + 1)
	default:
		s.queueFull.Add(1)
		s.droppedRows.Add(1)
		s.droppedBytes.Add(int64(len(payload)) + 1)
		s.report(fmt.Sprintf("the kafka log spool queue is full (%d rows waiting), so the row was dropped; the spool directory is the bottleneck", cap(s.queue)))
	}
}

// writeLoop is the single owner of every file in the spool directory: it creates
// the active segment, appends to it, seals it, truncates a torn tail and removes
// whole segments. The replayer only ever reads sealed segments and unlinks them.
func (s *Spool) writeLoop() {
	defer close(s.stopped)

	ticker := time.NewTicker(s.segmentEvery)
	defer ticker.Stop()

	for {
		select {
		case payload := <-s.queue:
			s.appendRow(payload)
		case <-ticker.C:
			s.sealActive()
			s.enforceRetention()
		case <-s.closing:
			s.drain()
			s.sealActive()
			s.enforceRetention()
			return
		}
	}
}

// drain writes what is already queued and returns. It is bounded by the queue
// depth, so a shutdown cannot become unbounded work -- and what it does not
// reach stays on the disk as a segment for the next start to replay.
func (s *Spool) drain() {
	for {
		select {
		case payload := <-s.queue:
			s.appendRow(payload)
		default:
			return
		}
	}
}

// appendRow appends one row to the active segment, sealing it first when the row
// would take it past the segment bound or when the row does not fit under the
// capacity at all.
func (s *Spool) appendRow(payload []byte) {
	line := int64(len(payload)) + 1
	if s.activeFile != nil && s.activeSize+line > s.segmentBytes {
		s.sealActive()
	}
	// The order is load-bearing: seal, then evict, then give up on this row.
	// Evicting before sealing could remove the oldest sealed segment while the
	// active file still holds rows that would have to be evicted next; giving up
	// before evicting would drop a row the capacity could have held.
	if !s.makeRoom(line) {
		s.droppedRows.Add(1)
		s.droppedBytes.Add(line)
		s.report(fmt.Sprintf("the kafka log spool cannot hold a %d byte row within KAFKA_LOG_SPOOL_MAX_BYTES=%d, so the row was dropped", line, s.maxBytes))
		return
	}
	if err := s.appendToActive(payload); err != nil {
		s.writeFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not append to %s, so the row was dropped: %s", s.dir, err))
	}
}

func (s *Spool) appendToActive(payload []byte) error {
	if s.activeFile == nil {
		if err := s.openActive(); err != nil {
			return err
		}
	}
	line := make([]byte, 0, len(payload)+1)
	line = append(line, payload...)
	line = append(line, '\n')
	written, err := s.activeFile.Write(line)
	s.activeSize += int64(written)
	s.totalBytes += int64(written)
	if err != nil {
		return err
	}
	s.activeRows++
	s.writtenRows.Add(1)
	return nil
}

// openActive publishes the new segment's path before creating it. The order
// matters: a replayer treats "not the active path" as sealed, so a file that
// became visible before it was published would be replayed while the writer is
// still appending to it.
//
// The name carries the rotation sequence as well as the clock, because the
// clock alone does not name a segment uniquely. time.Now().UnixNano() is only
// as fine as the platform's timer -- about 16 ms on Windows -- while a
// byte-triggered rotation runs at the speed the rows arrive, so two rotations
// inside one tick would build the same name, O_EXCL would refuse the second
// create, and the row behind it would be dropped to a name collision instead of
// to a full disk. The sequence is zero padded so that names built inside one
// tick still sort in rotation order, which is the order the replay and the
// eviction rules take segments in.
func (s *Spool) openActive() error {
	name := fmt.Sprintf("%s%d-%d-%06d%s", segmentPrefix, os.Getpid(), time.Now().UnixNano(), s.rotation, segmentSuffix)
	s.rotation++
	path := filepath.Join(s.dir, name)
	s.activePath.Store(path)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		// Do not keep claiming a file this process does not own: leaving the path
		// published would make the replayer skip a segment forever.
		s.activePath.Store("")
		return fmt.Errorf("could not create the spool segment %s: %w", path, err)
	}
	s.activeFile = file
	s.activeRows = 0
	s.activeSize = 0
	return nil
}

// sealActive closes the active segment, removes a torn tail, and never renames
// it. The path stays published for the whole operation, so the replayer cannot
// pick the file up while it is being truncated; it is unpublished -- which is
// what makes the segment sealed -- only once the file is complete.
func (s *Spool) sealActive() {
	if s.activeFile == nil {
		return
	}
	path, _ := s.activePath.Load().(string)
	file := s.activeFile
	s.activeFile, s.activeRows, s.activeSize = nil, 0, 0

	if err := file.Sync(); err != nil {
		s.writeFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not sync the segment %s: %s", path, err))
	}
	if err := file.Close(); err != nil {
		s.writeFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not close the segment %s: %s", path, err))
	}

	kept, torn, err := truncateTornTail(path)
	switch {
	case err != nil:
		s.writeFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not inspect the segment %s for a torn tail, so it was left as it is: %s", path, err))
	case kept == 0:
		// The file held no complete line at all. There is nothing to replay and
		// keeping it would make the replayer open an empty segment every tick.
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			s.writeFailed.Add(1)
			s.report(fmt.Sprintf("the kafka log spool could not remove the empty segment %s: %s", path, err))
		}
		s.emptySegments.Add(1)
		s.report(fmt.Sprintf("the kafka log spool removed the segment %s because it held no complete line (the process was killed mid-append)", filepath.Base(path)))
	case torn > 0:
		s.tornBytes.Add(torn)
		s.tornRows.Add(1)
		s.sealedSegments.Add(1)
		s.report(fmt.Sprintf("the kafka log spool truncated %d byte(s) from the end of the segment %s: the last append was interrupted, so at most one row was lost", torn, filepath.Base(path)))
	default:
		s.sealedSegments.Add(1)
	}

	s.activePath.Store("")
	s.totalBytes = s.directoryBytes()
}

// directoryBytes sums the size of every segment file, which is the authoritative
// capacity account. It is recomputed at each seal rather than maintained only
// incrementally, because the replayer removes segments without telling the
// writer and an account that only ever grows would evict for no reason.
func (s *Spool) directoryBytes() int64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		s.writeFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not read %s to account for its size: %s", s.dir, err))
		return s.totalBytes
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := parseSegmentName(entry.Name()); !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// makeRoom evicts whole sealed segments, oldest first, until the extra bytes fit
// under the capacity. It reports whether they now fit.
func (s *Spool) makeRoom(extra int64) bool {
	if s.totalBytes+extra <= s.maxBytes {
		return true
	}
	segments := s.sealedSegmentsOldestFirst()
	var evictedSegments, evictedRows, evictedBytes, failed int64
	for _, segment := range segments {
		if s.totalBytes+extra <= s.maxBytes {
			break
		}
		info, err := os.Stat(segment)
		if err != nil {
			failed++
			continue
		}
		rows, err := countSegmentRows(segment)
		if err != nil {
			failed++
			continue
		}
		if err := os.Remove(segment); err != nil {
			// Windows refuses to unlink a file another handle holds open, so an
			// eviction can legitimately fail while a segment is being replayed.
			// The capacity is then a soft bound for that interval, and counting
			// the failure is the difference between a known deviation and a
			// silent one.
			failed++
			continue
		}
		evictedSegments++
		evictedRows += rows
		evictedBytes += info.Size()
		s.totalBytes -= info.Size()
	}
	if evictedSegments > 0 || failed > 0 {
		s.evictedSegments.Add(evictedSegments)
		s.evictedRows.Add(evictedRows)
		s.evictedBytes.Add(evictedBytes)
		s.evictFailed.Add(failed)
		s.report(fmt.Sprintf("the kafka log spool reached KAFKA_LOG_SPOOL_MAX_BYTES=%d and dropped the %d oldest sealed segment(s) (%d row(s), %d byte(s)); %d segment(s) could not be removed",
			s.maxBytes, evictedSegments, evictedRows, evictedBytes, failed))
	}
	return s.totalBytes+extra <= s.maxBytes
}

// enforceRetention drops sealed segments whose name says they are older than the
// retention. Age comes from the name, not from mtime: a copied or restored
// directory rewrites mtime and would make every segment look brand new.
func (s *Spool) enforceRetention() {
	cutoff := time.Now().Add(-s.retention)
	var segments, rows, failed int64
	for _, path := range s.sealedSegmentsOldestFirst() {
		openedAt, ok := parseSegmentName(filepath.Base(path))
		if !ok || openedAt.After(cutoff) {
			continue
		}
		count, err := countSegmentRows(path)
		if err != nil {
			failed++
			continue
		}
		if err := os.Remove(path); err != nil {
			// Windows refuses to unlink a file another handle holds open, so an
			// expiry can legitimately fail while a segment is being replayed. The
			// segment then outlives its retention for that interval, and reporting
			// the refusal is what keeps it a counted deviation rather than a bound
			// that quietly does not hold.
			failed++
			continue
		}
		segments++
		rows += count
	}
	if segments > 0 || failed > 0 {
		s.expiredSegments.Add(segments)
		s.expiredRows.Add(rows)
		s.evictFailed.Add(failed)
		s.report(fmt.Sprintf("the kafka log spool dropped %d sealed segment(s) (%d row(s)) older than KAFKA_LOG_SPOOL_RETENTION_HOURS=%s; %d segment(s) could not be removed; they were never replayed",
			segments, rows, s.retention, failed))
	}
}

// sealedSegmentsOldestFirst lists the segment files that are not this process's
// active one, oldest first by the open time in the name.
func (s *Spool) sealedSegmentsOldestFirst() []string {
	active, _ := s.activePath.Load().(string)
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		// Both the writer's eviction and the replayer's choice of the next
		// segment come through here, so a listing that fails makes the spool
		// believe the directory is empty. Counting it is the difference between
		// a retry on the next tick and an appearance of "there is nothing to do".
		s.scanFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not list %s, so it cannot tell which segments are sealed: %s", s.dir, err))
		return nil
	}
	type segment struct {
		path     string
		openedAt time.Time
	}
	segments := make([]segment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		openedAt, ok := parseSegmentName(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		if path == active {
			continue
		}
		segments = append(segments, segment{path: path, openedAt: openedAt})
	}
	// Insertion sort by open time: the file count is bounded by the segment
	// interval and the retention (about 1440 a day), and this keeps the ordering
	// rule next to the reason it exists.
	for i := 1; i < len(segments); i++ {
		for j := i; j > 0 && segments[j].openedAt.Before(segments[j-1].openedAt); j-- {
			segments[j], segments[j-1] = segments[j-1], segments[j]
		}
	}
	paths := make([]string, 0, len(segments))
	for _, s := range segments {
		paths = append(paths, s.path)
	}
	return paths
}

// parseSegmentName returns the open time a segment file name carries. Anything
// the name carries after it -- the rotation sequence -- is ignored, so a
// directory written before the sequence existed is still recognised, aged and
// replayed rather than left on the disk forever.
func parseSegmentName(name string) (time.Time, bool) {
	base, ok := strings.CutSuffix(name, segmentSuffix)
	if !ok {
		return time.Time{}, false
	}
	rest, ok := strings.CutPrefix(base, segmentPrefix)
	if !ok {
		return time.Time{}, false
	}
	_, stamped, ok := strings.Cut(rest, "-")
	if !ok {
		return time.Time{}, false
	}
	nanos, _, _ := strings.Cut(stamped, "-")
	value, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, value), true
}

// countSegmentRows counts the rows a segment holds. It is only used while
// removing segments, where the row count is what an operator needs in order to
// tell "a little was dropped" from "an hour of logs was dropped".
func countSegmentRows(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return int64(bytes.Count(data, []byte{'\n'})), nil
}

// truncateTornTail removes a half-written last line and returns how many bytes
// were kept and how many were removed. Zero kept means the file held no complete
// line at all.
//
// It runs at seal time rather than at replay time. The active segment is the
// only file that can be torn (it has one writer and is append-only), and the
// moment it stops being active is the last moment it can be truncated safely;
// after that every sealed segment is complete by construction and replay needs
// no recovery logic at all.
func truncateTornTail(path string) (int64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()
	if size == 0 {
		return 0, 0, nil
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	last := make([]byte, 1)
	if _, err := file.ReadAt(last, size-1); err != nil {
		return 0, 0, err
	}
	if last[0] == '\n' {
		return size, 0, nil
	}

	cut, err := lastNewlineOffset(file, size)
	if err != nil {
		return 0, 0, err
	}
	if err := file.Truncate(cut); err != nil {
		return 0, 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, 0, err
	}
	return cut, size - cut, nil
}

// lastNewlineOffset finds the offset just past the last newline in the first
// size bytes, searching backwards in a window that doubles until it reaches the
// beginning of the file. Zero means no complete line exists.
func lastNewlineOffset(file *os.File, size int64) (int64, error) {
	window := int64(tornTailStartWindow)
	for {
		start := max(size-window, 0)
		buf := make([]byte, size-start)
		if _, err := file.ReadAt(buf, start); err != nil {
			return 0, err
		}
		if index := bytes.LastIndexByte(buf, '\n'); index >= 0 {
			return start + int64(index) + 1, nil
		}
		if start == 0 {
			return 0, nil
		}
		window *= 2
	}
}

// StartReplay runs the replay goroutine. deliver hands one payload back to the
// broker through an entry of its own and calls its callback exactly once with
// the delivery result; healthy reports whether the producer is keeping up.
//
// Replay is where this design could most easily be got wrong, and the wrong
// version is not a slow one but a live-locked one: if a failed replay put its
// rows back on the disk, a broker that stays down would replay a segment, fail,
// write the rows out again, keep the original, and end up with every row on the
// disk twice -- and then deliver both copies when the broker returns. The
// isolation is structural, not a flag: this loop holds a deliver function that
// cannot reach Stash at all.
func (s *Spool) StartReplay(ctx context.Context, deliver func([]byte, func(error)), healthy func() bool) {
	s.replayCtx, s.cancelReplay = context.WithCancel(ctx)
	go s.replayLoop(s.replayCtx, deliver, healthy)
}

// StopReplay stops the replay goroutine and waits for it. It has to run before
// the producer is closed: the replayer delivers through the same client.
func (s *Spool) StopReplay() {
	if s.cancelReplay == nil {
		return
	}
	s.cancelReplay()
	<-s.replayStopped
}

// Replaying reports whether a segment is being replayed. It is the observable
// precondition an experiment needs in place of a fixed wait before it kills the
// process mid-replay.
func (s *Spool) Replaying() bool { return s.replaying.Load() }

// Close stops the writer, writing what is already queued and sealing the active
// segment. A spool that is not empty is left on the disk on purpose: the next
// start replays it, and draining it here would make shutdown depend on the
// broker being healthy.
func (s *Spool) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.closing)
		<-s.stopped
	})
}

// report emits one rate-limited line carrying the running totals, using the same
// "first failure always, then one per reportEvery" rule as the rest of the
// transport.
func (s *Spool) report(why string) {
	total := s.queueFull.Load() + s.writeFailed.Load() + s.rejectedRows.Load() +
		s.evictedSegments.Load() + s.expiredSegments.Load() + s.droppedRows.Load() +
		s.replayFailed.Load() + s.scanFailed.Load() + s.emptySegments.Load() + s.evictFailed.Load()
	if !shouldReport(&s.reported, total) {
		return
	}
	// The counts are the ones an operator can act on: rows that reached the disk,
	// rows whose write failed, and rows that were dropped without one. Reporting
	// the rows handed over as "written to disk" would print a clean line for a
	// segment that lost a row.
	common.SysError(fmt.Sprintf("%s (spool %s, %d row(s) written to disk, %d row(s) failed to write, %d row(s) dropped, %d segment(s) evicted, %d segment(s) expired)",
		why, s.dir, s.writtenRows.Load(), s.writeFailed.Load(), s.droppedRows.Load(), s.evictedSegments.Load(), s.expiredSegments.Load()))
}
