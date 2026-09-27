package logkafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSpoolConfig is a spool sized for a test rather than for an outage: small
// segments so rotation is reachable, and a short replay tick so a test observes
// state instead of waiting a minute for it.
func testSpoolConfig(dir string, mutate func(*Config)) Config {
	cfg := Config{
		Topic:               "new-api-logs",
		GroupID:             "new-api-log-consumer-test",
		MaxBufferBytes:      defaultMaxBufferBytes,
		DeliveryTimeout:     100 * time.Millisecond,
		RequestTimeout:      200 * time.Millisecond,
		RetryBackoff:        10 * time.Millisecond,
		FlushInterval:       20 * time.Millisecond,
		SpoolDir:            dir,
		SpoolMaxBytes:       1 << 20,
		SpoolRetentionHours: 48,
		SpoolSegmentBytes:   1 << 16,
		// Rotation by time is off unless a test asks for it: a segment that
		// rotates while a test is still writing into it would make the
		// assertions below race the ticker.
		SpoolSegmentSeconds: time.Hour,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func newTestSpool(t *testing.T, cfg Config) *Spool {
	t.Helper()
	spool, err := NewSpool(cfg)
	require.NoError(t, err)
	// Registered first so it runs last: the replay has to stop before the spool
	// closes, and a cleanup that waits on the writer goroutine would otherwise
	// hang on the replay's.
	t.Cleanup(spool.Close)
	return spool
}

func startTestReplay(t *testing.T, spool *Spool, deliver func([]byte, func(error)), healthy func() bool) {
	t.Helper()
	spool.StartReplay(context.Background(), deliver, healthy)
	t.Cleanup(spool.StopReplay)
}

// segmentName is the name the writer builds, so a test can fabricate a segment
// that looks like one a previous process left behind.
func segmentName(pid int, openedAt time.Time) string {
	return fmt.Sprintf("%s%d-%d%s", segmentPrefix, pid, openedAt.UnixNano(), segmentSuffix)
}

func writeSegment(t *testing.T, dir string, openedAt time.Time, rows []string) string {
	t.Helper()
	path := filepath.Join(dir, segmentName(os.Getpid(), openedAt))
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o600))
	return path
}

func segmentNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := parseSegmentName(entry.Name()); ok {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

// spoolRowsOf reads every row in the directory in file order. File order is
// chronological here because the name carries the open time, which is also what
// the replay uses to order segments.
func spoolRowsOf(t *testing.T, dir string) []string {
	t.Helper()
	var rows []string
	for _, name := range segmentNames(t, dir) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		for _, line := range strings.Split(string(data), "\n") {
			if line != "" {
				rows = append(rows, line)
			}
		}
	}
	return rows
}

func directorySize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, name := range segmentNames(t, dir) {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		total += info.Size()
	}
	return total
}

// TestSpoolSegmentHoldsThePayloadsVerbatim is the format invariant the whole
// design rests on: one row per line, the payload exactly as the producer would
// have sent it, and a trailing newline so a torn tail is detectable at all.
func TestSpoolSegmentHoldsThePayloadsVerbatim(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, nil))

	payloads := []string{`{"id":1}`, `{"id":2,"quota":1234}`, `{"id":3}`}
	for _, payload := range payloads {
		spool.Stash([]byte(payload))
	}
	spool.Close()

	names := segmentNames(t, dir)
	require.Len(t, names, 1)
	assert.Regexp(t, `^active-\d+-\d+-\d{6}\.ndjson$`, names[0],
		"the name carries the pid, the open time and the rotation sequence: the clock alone is only as fine as the platform's timer, so a tick that holds two rotations would otherwise name both segments the same and O_EXCL would drop the second one's row")
	assert.Equal(t, payloads, spoolRowsOf(t, dir))

	data, err := os.ReadFile(filepath.Join(dir, names[0]))
	require.NoError(t, err)
	assert.Equal(t, len(payloads), bytes.Count(data, []byte{'\n'}), "one newline per row and no others")
	assert.True(t, bytes.HasSuffix(data, []byte{'\n'}), "a sealed segment always ends with a newline; that is the torn-tail judgement")
}

// TestSpoolRejectsARowThatWouldSplitALine guards the invariant above rather than
// assuming it: a payload carrying a raw newline would become two lines that are
// not both rows.
func TestSpoolRejectsARowThatWouldSplitALine(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, nil))
	output := captureSysErrors(t)

	spool.Stash([]byte("{\"id\":1}\n{\"id\":2}"))
	spool.Close()

	assert.Empty(t, spoolRowsOf(t, dir), "a row with a raw newline in it must be refused, not written")
	assert.EqualValues(t, 1, spool.rejectedRows.Load())
	assert.Contains(t, output.String(), "raw newline")
}

// TestSealRemovesOnlyTheTornTail is the failure mode that only ever loses data
// on the failure path: an off-by-one in the truncation eats the last good line
// of every segment, and nothing else in the system would notice.
func TestSealRemovesOnlyTheTornTail(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, nil))
	output := captureSysErrors(t)

	good := []string{`{"id":1}`, `{"id":2,"content":"a row that is long enough to matter"}`}
	for _, payload := range good {
		spool.Stash([]byte(payload))
	}
	require.Eventually(t, func() bool { return len(segmentNames(t, dir)) == 1 }, 5*time.Second, 10*time.Millisecond,
		"the writer creates the active segment when the first row arrives")

	path := filepath.Join(dir, segmentNames(t, dir)[0])
	require.Eventually(t, func() bool {
		info, err := os.Stat(path)
		return err == nil && info.Size() == int64(len(good[0])+len(good[1])+2)
	}, 5*time.Second, 10*time.Millisecond, "both rows have to be on the disk before the torn tail is injected")

	// The half written row a process would leave behind if it died inside the
	// append. It is written by a second handle, which is exactly the state the
	// kernel would be in.
	half := `{"id":3,"content":"this row was never finis`
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = file.WriteString(half)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	spool.Close()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, strings.Join(good, "\n")+"\n", string(data),
		"sealing has to remove exactly the half written tail and leave every complete row byte for byte")
	assert.EqualValues(t, len(half), spool.tornBytes.Load())
	assert.EqualValues(t, 1, spool.tornRows.Load())
	assert.Contains(t, output.String(), "truncated", "the truncation has to be reported, not silent")
}

// TestSealRemovesASegmentWithNoCompleteLine is the other end of the same rule:
// a file that never got a newline has nothing to replay.
func TestSealRemovesASegmentWithNoCompleteLine(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, nil))
	output := captureSysErrors(t)

	spool.Stash([]byte(`{"id":1}`))
	require.Eventually(t, func() bool { return len(segmentNames(t, dir)) == 1 }, 5*time.Second, 10*time.Millisecond)
	path := filepath.Join(dir, segmentNames(t, dir)[0])

	// Truncate the good row away and leave only a half line, which is the state
	// of a process killed during its very first append.
	require.NoError(t, os.Truncate(path, 0))
	half, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = half.WriteString(`{"id":2,"content":"never finished`)
	require.NoError(t, err)
	require.NoError(t, half.Close())

	spool.Close()

	assert.Empty(t, segmentNames(t, dir), "a segment with no complete line has nothing to replay and has to be removed")
	assert.EqualValues(t, 1, spool.emptySegments.Load())
	assert.Contains(t, output.String(), "no complete line")
}

// TestSealLeavesACompleteSegmentUntouched is the guard the truncation needs: it
// is the only assertion that catches an off-by-one that would otherwise eat one
// good row per crash, silently, forever.
func TestSealLeavesACompleteSegmentUntouched(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, nil))
	for range 3 {
		spool.Stash([]byte(`{"id":1,"content":"a complete row"}`))
	}

	// The segment is still open and already holds every row, which is the state
	// the seal path has to leave alone. Reading it before the seal is what makes
	// the assertion below about the seal: a read taken after it would only ever
	// compare the seal's own output with itself.
	require.Eventually(t, func() bool {
		names := segmentNames(t, dir)
		if len(names) != 1 {
			return false
		}
		data, err := os.ReadFile(filepath.Join(dir, names[0]))
		return err == nil && bytes.Count(data, []byte{'\n'}) == 3
	}, 5*time.Second, 10*time.Millisecond,
		"the writer creates the active segment when the first row arrives and appends every row to it")

	names := segmentNames(t, dir)
	require.Len(t, names, 1)
	path := filepath.Join(dir, names[0])
	first, err := os.ReadFile(path)
	require.NoError(t, err)

	// Closing seals the segment that is still open. That is the real path: a seal
	// called once the segment is already sealed returns at the nil guard and
	// proves nothing about it.
	spool.Close()

	second, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, first, second, "a complete segment has to survive the seal path byte for byte")
}

// TestSegmentRotatesOnBytesAndKeepsEveryRow covers the first rotation trigger.
func TestSegmentRotatesOnBytesAndKeepsEveryRow(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.SpoolSegmentBytes = 64 }))

	const rows = 20
	for i := range rows {
		spool.Stash([]byte(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, i, strings.Repeat("x", 20))))
	}
	spool.Close()

	names := segmentNames(t, dir)
	assert.Greater(t, len(names), 1,
		"a segment that reaches KAFKA_LOG_SPOOL_SEGMENT_BYTES has to be sealed and a new one started: the replay unit has to have an upper bound")
	assert.Len(t, spoolRowsOf(t, dir), rows, "rotation must not lose or duplicate a row")
	assert.EqualValues(t, rows, spool.writtenRows.Load(),
		"the report prints this counter as row(s) written to disk, so it has to be the rows on the disk and not the rows handed over: a row accepted by Stash and then lost to a failed write counts there and not here")
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.True(t, bytes.HasSuffix(data, []byte{'\n'}), "every sealed segment ends with a newline, including the ones rotation produced")
	}
}

// TestSegmentRotatesOnTime is the second trigger, and it is the one that makes
// recovery without a restart possible: only sealed segments are replayed, so a
// row that arrives during an outage would otherwise sit in the active segment
// until the process restarted.
func TestSegmentRotatesOnTime(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) {
		cfg.SpoolSegmentBytes = 1 << 20
		cfg.SpoolSegmentSeconds = 50 * time.Millisecond
	}))

	spool.Stash([]byte(`{"id":1}`))

	require.Eventually(t, func() bool {
		_, ok := spool.oldestSealedSegment()
		return ok
	}, 5*time.Second, 10*time.Millisecond,
		"the row has to become replayable on the interval; waiting for a restart is not a recovery path")

	// An idle directory must not grow one empty segment per interval: sealing is
	// skipped while there is nothing to seal.
	require.Never(t, func() bool { return len(segmentNames(t, dir)) > 1 }, 300*time.Millisecond, 10*time.Millisecond,
		"sealing an empty active segment would produce one file per interval forever")
}

// TestCapacityEvictsWholeSegmentsOldestFirst is the capacity rule, and it is
// asserted by row identity rather than by file count: "the directory is small
// enough" is also true of an implementation that dropped everything.
func TestCapacityEvictsWholeSegmentsOldestFirst(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) {
		cfg.SpoolSegmentBytes = 256
		cfg.SpoolMaxBytes = 1024
	}))
	output := captureSysErrors(t)

	row := func(i int) string { return fmt.Sprintf(`{"id":%d,"pad":"%s"}`, i, strings.Repeat("z", 40)) }
	const rows = 60
	for i := range rows {
		spool.Stash([]byte(row(i)))
	}
	spool.Close()

	stored := spoolRowsOf(t, dir)
	require.NotEmpty(t, stored)
	assert.Equal(t, row(rows-1), stored[len(stored)-1], "the newest row must never be the one evicted")
	assert.NotContains(t, stored, row(0), "eviction takes the oldest segment, so the first row is the first to go")
	assert.LessOrEqual(t, directorySize(t, dir), int64(1024), "the directory has to stay under KAFKA_LOG_SPOOL_MAX_BYTES")

	// Every segment that survived has to be complete: eviction works in whole
	// segments, so a partial file would mean rows were cut out of the middle.
	for _, name := range segmentNames(t, dir) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.True(t, bytes.HasSuffix(data, []byte{'\n'}), "eviction must never leave a half segment behind")
	}

	assert.Greater(t, spool.evictedSegments.Load(), int64(0))
	assert.Contains(t, output.String(), "KAFKA_LOG_SPOOL_MAX_BYTES")
}

// TestCapacityThatCannotHoldARowDropsItAndSaysSo is the pathological
// configuration: capacity smaller than a single row. It has to drop the row with
// a report rather than block or spin.
func TestCapacityThatCannotHoldARowDropsItAndSaysSo(t *testing.T) {
	dir := t.TempDir()
	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) {
		cfg.SpoolMaxBytes = 8
		cfg.SpoolSegmentBytes = 8
	}))
	output := captureSysErrors(t)

	spool.Stash([]byte(`{"id":1,"content":"a row that cannot fit"}`))
	spool.Close()

	assert.Empty(t, spoolRowsOf(t, dir))
	assert.EqualValues(t, 1, spool.droppedRows.Load())
	assert.Contains(t, output.String(), "KAFKA_LOG_SPOOL_MAX_BYTES")
}

// TestRetentionDropsOldSegmentsAndSparesYoungOnes ages segments by the timestamp
// in the name rather than by mtime, because copying a directory rewrites mtime
// and would make every segment look brand new.
func TestRetentionDropsOldSegmentsAndSparesYoungOnes(t *testing.T) {
	dir := t.TempDir()
	old := writeSegment(t, dir, time.Now().Add(-72*time.Hour), []string{`{"id":"old"}`})

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.SpoolRetentionHours = 48 }))
	output := captureSysErrors(t)

	spool.Stash([]byte(`{"id":"fresh"}`))
	spool.Close()

	_, err := os.Stat(old)
	assert.True(t, os.IsNotExist(err), "a segment past KAFKA_LOG_SPOOL_RETENTION_HOURS has to be dropped")
	assert.EqualValues(t, 1, spool.expiredSegments.Load())
	assert.Contains(t, output.String(), "KAFKA_LOG_SPOOL_RETENTION_HOURS", "expiry has to be reported, not a silent unlink")
	assert.Contains(t, spoolRowsOf(t, dir), `{"id":"fresh"}`, "the segment that was just sealed is younger than the retention and must survive")
}

// TestRetentionOfASegmentAnotherHandleHoldsOpenIsReported is the retention rule's
// counterpart to the eviction case further down, on the platform where the
// refusal actually happens: Windows will not unlink a file another handle holds
// open.
//
// It exists because the two paths were not the same. A refused retention unlink
// was counted and never reported, the report being gated on something having been
// removed -- which is exactly what a refusal is not. That is half of the rule
// this slice applies everywhere else ("counted and reported, never silent"), and
// it is the state an operator on Windows is most likely to be in, because the
// refusal happens precisely while a segment is being read.
func TestRetentionOfASegmentAnotherHandleHoldsOpenIsReported(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, time.Now().Add(-72*time.Hour), []string{`{"id":"old"}`})

	held, err := os.Open(path)
	require.NoError(t, err)
	defer held.Close()

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.SpoolRetentionHours = 48 }))
	output := captureSysErrors(t)

	spool.enforceRetention()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		// POSIX unlinks through the open descriptor, so this host cannot produce
		// the refusal and the removal is the whole of the behaviour.
		assert.EqualValues(t, 1, spool.expiredSegments.Load())
		assert.Zero(t, spool.evictFailed.Load())
		return
	}
	assert.EqualValues(t, 1, spool.evictFailed.Load(), "a refused retention unlink has to be counted")
	assert.Zero(t, spool.expiredSegments.Load(), "nothing was removed, so nothing expired")
	assert.Contains(t, output.String(), "could not be removed",
		"and it has to be reported: a counted degradation that is never printed cannot be told from a quiet period")
	assert.FileExists(t, path, "a refused unlink must leave the segment where the replayer can still reach it")
}

// TestReplayDeliversOldestSegmentFirstAndInRowOrder pins the ordering promise
// and its boundary: oldest segment first, rows in file order, and no global
// order across segments beyond that.
func TestReplayDeliversOldestSegmentFirstAndInRowOrder(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, time.Now().Add(-2*time.Hour), []string{"a1", "a2"})
	writeSegment(t, dir, time.Now().Add(-1*time.Hour), []string{"b1", "b2"})

	spool := newTestSpool(t, testSpoolConfig(dir, nil))

	var mu sync.Mutex
	var delivered []string
	startTestReplay(t, spool, func(payload []byte, done func(error)) {
		mu.Lock()
		delivered = append(delivered, string(payload))
		mu.Unlock()
		done(nil)
	}, func() bool { return true })

	// The window covers several retries rather than one: the removal can be
	// refused transiently on Windows while another process holds the file, and
	// the replay counts that and tries again on the next tick.
	require.Eventually(t, func() bool { return len(segmentNames(t, dir)) == 0 }, 30*time.Second, 10*time.Millisecond,
		"a segment whose every row was acknowledged has to be removed")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a1", "a2", "b1", "b2"}, delivered)
	assert.EqualValues(t, 2, spool.replayedSegments.Load())
	assert.EqualValues(t, 4, spool.replayedRows.Load())
}

// TestReplayFailureKeepsTheWholeSegment is the reverse leg of the deletion rule.
// Without it, "the segment disappeared" cannot be told from "the segment is
// never removed at all".
func TestReplayFailureKeepsTheWholeSegment(t *testing.T) {
	dir := t.TempDir()
	rows := []string{"r1", "r2", "r3"}
	path := writeSegment(t, dir, time.Now().Add(-time.Hour), rows)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.DeliveryTimeout = time.Hour }))
	output := captureSysErrors(t)

	var mu sync.Mutex
	var attempted []string
	startTestReplay(t, spool, func(payload []byte, done func(error)) {
		mu.Lock()
		attempted = append(attempted, string(payload))
		mu.Unlock()
		done(errors.New("the broker is still down"))
	}, func() bool { return true })

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempted) > 0
	}, 5*time.Second, 10*time.Millisecond)

	after, err := os.ReadFile(path)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"r1"}, attempted, "the replay stops at the first failure rather than consuming the rest")
	assert.Equal(t, before, after, "a failed replay leaves the segment exactly as it was")
	assert.Contains(t, output.String(), "will be replayed again")
	assert.Zero(t, spool.replayedRows.Load(), "nothing was acknowledged, so the segment counts as neither replayed nor consumed")
}

// TestReplayFailureWritesNothingBackToTheSpool is the live-lock guard, asserted
// where it can actually be observed: the spool's own accounting.
//
// If a failed replay reached the request path's delivery callback, a broker that
// stays down would replay a segment, fail, write every one of its rows back to
// the disk next to the original, and multiply them again on the next pass.
func TestReplayFailureWritesNothingBackToTheSpool(t *testing.T) {
	dir := t.TempDir()
	rows := []string{"r1", "r2"}
	path := writeSegment(t, dir, time.Now().Add(-time.Hour), rows)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) {
		cfg.DeliveryTimeout = 5 * time.Minute
	}))

	var mu sync.Mutex
	attempted := 0
	startTestReplay(t, spool, func(payload []byte, done func(error)) {
		mu.Lock()
		attempted++
		mu.Unlock()
		// The replay's own failure signal: a row that did not reach the broker.
		done(errors.New("the broker is still down"))
	}, func() bool { return true })

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempted >= 1
	}, 5*time.Second, 10*time.Millisecond)

	assert.Zero(t, spool.stashedRows.Load(), "a row that failed to replay must never be offered to Stash again")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Len(t, segmentNames(t, dir), 1, "the original segment is still the only copy")
}

// TestReplayDeliveryFailureNeverReachesTheSpool is the same guard at the level
// where the mistake would be made: the producer's replay entry itself.
func TestReplayDeliveryFailureNeverReachesTheSpool(t *testing.T) {
	dir := t.TempDir()
	cfg := testSpoolConfig(dir, func(cfg *Config) {
		cfg.Brokers = []string{silentBroker(t)}
		// franz-go refuses a record delivery timeout below one second, and the
		// point here is the failure, not its timing.
		cfg.DeliveryTimeout = 2 * time.Second
		cfg.RequestTimeout = time.Second
	})

	producer, err := NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	failed := make(chan error, 1)
	producer.deliverReplay([]byte(`{"id":1}`), func(err error) { failed <- err })

	select {
	case err := <-failed:
		require.Error(t, err, "a broker that never answers has to fail the delivery")
	case <-time.After(30 * time.Second):
		t.Fatal("the replay delivery never reported a result")
	}

	producer.spool.StopReplay()
	producer.spool.Close()
	assert.Zero(t, producer.spool.stashedRows.Load(),
		"the replay's delivery entry is the request path's entry's opposite: it reports, it never writes to the disk")
	assert.Empty(t, segmentNames(t, dir), "a failed replay must leave the directory exactly as it found it")
}

// TestReplayStopsMidSegmentAndRemovesNothing covers the partial-consumption
// rule: a failure on any row leaves the file whole, so the next attempt replays
// the segment from the beginning rather than from where it stopped.
func TestReplayStopsMidSegmentAndRemovesNothing(t *testing.T) {
	dir := t.TempDir()
	rows := []string{"p1", "p2", "p3"}
	path := writeSegment(t, dir, time.Now().Add(-time.Hour), rows)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.DeliveryTimeout = time.Hour }))

	var mu sync.Mutex
	var attempted []string
	startTestReplay(t, spool, func(payload []byte, done func(error)) {
		mu.Lock()
		attempted = append(attempted, string(payload))
		fail := len(attempted) == 2
		mu.Unlock()
		if fail {
			done(errors.New("this one row could not be delivered"))
			return
		}
		done(nil)
	}, func() bool { return true })

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempted) >= 2
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []string{"p1", "p2"}, attempted, "the third row is not attempted once the second failed")
	mu.Unlock()

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the whole segment has to stay, including the row that was delivered: a segment is removed only when every row in it was acknowledged")
	assert.Zero(t, spool.replayedRows.Load())
}

// TestReplayWaitsForAHealthyProducer pins the shared health criterion: replaying
// into a producer that is still behind would refill the buffer being drained.
func TestReplayWaitsForAHealthyProducer(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, time.Now().Add(-time.Hour), []string{"w1"})

	spool := newTestSpool(t, testSpoolConfig(dir, nil))
	healthy := false

	var mu sync.Mutex
	attempted := 0
	startTestReplay(t, spool, func(payload []byte, done func(error)) {
		mu.Lock()
		attempted++
		mu.Unlock()
		done(nil)
	}, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return healthy
	})

	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempted > 0
	}, 200*time.Millisecond, 10*time.Millisecond, "replay must not start while the producer is over the low water mark")

	mu.Lock()
	healthy = true
	mu.Unlock()

	require.Eventually(t, func() bool { return len(segmentNames(t, dir)) == 0 }, 30*time.Second, 10*time.Millisecond,
		"replay has to start as soon as the producer reports it has room again")
}

// TestSpoolFilesystemWarningOnlyFiresForTmpfs is the pure half of the tmpfs
// check, so both branches run on a host that has no tmpfs at all.
func TestSpoolFilesystemWarningOnlyFiresForTmpfs(t *testing.T) {
	assert.Empty(t, spoolFilesystemWarning(0xef53, "/var/spool/new-api"),
		"an ordinary filesystem must not warn: a warning that fires on every directory is one operators learn to ignore")

	warning := spoolFilesystemWarning(tmpfsMagic, "/var/spool/new-api")
	assert.Contains(t, warning, "/var/spool/new-api", "the warning has to name the path it is about")
	assert.Contains(t, warning, "KAFKA_LOG_SPOOL_DIR", "and the variable that moves it")
	assert.Contains(t, warning, "lost", "and what actually happens to the rows")
}

// TestNewSpoolRefusesADirectoryItCannotUse is the fail-fast half of the
// deliberately asymmetric rule. A broker that is unreachable must not stop
// startup; a spool directory that cannot be written is deterministic, is only
// ever discovered during an outage, and by then the rows are already gone.
func TestNewSpoolRefusesADirectoryItCannotUse(t *testing.T) {
	cases := map[string]func(t *testing.T) string{
		"parent is a file": func(t *testing.T) string {
			parent := filepath.Join(t.TempDir(), "not-a-directory")
			require.NoError(t, os.WriteFile(parent, []byte("x"), 0o600))
			return filepath.Join(parent, "spool")
		},
		"the path is a file": func(t *testing.T) string {
			file := filepath.Join(t.TempDir(), "a-file")
			require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
			return file
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			dir := build(t)
			_, err := NewSpool(testSpoolConfig(dir, nil))
			require.Error(t, err)
			assert.Contains(t, err.Error(), dir, "the message has to name the resolved absolute path, because the default is relative")
			assert.Contains(t, err.Error(), "KAFKA_LOG_SPOOL_DIR", "and the variable that changes it")
			assert.Contains(t, err.Error(), "create", "and the other remedy")
		})
	}
}

// TestEvictionOfASegmentAnotherHandleHoldsOpenIsCounted measures the one
// platform difference the capacity rule depends on, on the machine it runs on.
//
// POSIX lets an unlinked file stay readable through an open descriptor, so a
// replayer can be evicted out from under and finish reading. Windows refuses the
// unlink while any handle holds the file, so on that platform the capacity is a
// soft bound for exactly as long as a segment is being replayed -- and a refusal
// that were silent would turn the bound into a lie.
func TestEvictionOfASegmentAnotherHandleHoldsOpenIsCounted(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, time.Now().Add(-time.Hour), []string{`{"id":1}`})

	held, err := os.Open(path)
	require.NoError(t, err)
	defer held.Close()
	unlinkErr := os.Remove(path)
	t.Logf("unlinking a segment that another handle holds open returned: %v", unlinkErr)

	spool := newTestSpool(t, testSpoolConfig(dir, func(cfg *Config) { cfg.SpoolMaxBytes = 64 }))
	output := captureSysErrors(t)

	// A capacity the directory cannot meet, so the eviction path has to run, and
	// it runs while the handle is still open.
	assert.False(t, spool.makeRoom(1<<20), "there is not room, and pretending there is would let the directory grow past its bound")

	if unlinkErr == nil {
		assert.EqualValues(t, 1, spool.evictedSegments.Load())
		return
	}
	assert.EqualValues(t, 1, spool.evictFailed.Load(), "a refused eviction has to be counted; a silent failure is what turns the bound into a lie")
	assert.Contains(t, output.String(), "could not be removed")
	assert.FileExists(t, path, "a refused unlink must leave the segment where the replayer can still finish reading it")
}

func TestSpoolStartupPrintsTheResolvedAbsolutePath(t *testing.T) {
	// A relative directory, which is what the production default is, so the test
	// observes the translation rather than restating it.
	relative := fmt.Sprintf("spool-relative-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = os.RemoveAll(relative) })
	absolute, err := filepath.Abs(relative)
	require.NoError(t, err)

	output := captureSysLogs(t)
	spool, err := NewSpool(testSpoolConfig(relative, nil))
	require.NoError(t, err)
	spool.Close()

	assert.Contains(t, output.String(), "directory "+absolute,
		"the resolved absolute path is always printed: the default is relative, and a relative value does not tell an operator where the rows would land")
}
