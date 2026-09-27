package model

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// switchableBroker is a TCP proxy in front of the real broker that can be turned
// off and on again. An "unreachable broker" is not a wrong address: the client
// connects and nothing ever comes back, which is the state the disk fallback
// exists for, and it is the only way to observe the whole chain -- spool, replay,
// topic, consumer, ClickHouse -- without restarting the process in the middle of
// the experiment.
type switchableBroker struct {
	listener net.Listener
	upstream string
	open     atomic.Bool
}

func newSwitchableBroker(t *testing.T, upstream string) *switchableBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	broker := &switchableBroker{listener: listener, upstream: upstream}
	go broker.accept()
	t.Cleanup(func() { _ = listener.Close() })
	return broker
}

func (b *switchableBroker) Addr() string { return b.listener.Addr().String() }

func (b *switchableBroker) accept() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		go b.serve(conn)
	}
}

// serve holds a connection mute until the broker is switched back on, then
// splices it to the real one. Holding rather than refusing is what makes this a
// black hole: a refused connection is retried instantly and looks like a broker
// that is down, while a held one makes the client wait out its own timeout.
func (b *switchableBroker) serve(client net.Conn) {
	defer client.Close()
	for !b.open.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	upstream, err := net.Dial("tcp", b.upstream)
	if err != nil {
		return
	}
	defer upstream.Close()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
}

func spoolSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	return names
}

func spoolFileBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, name := range spoolSegmentFiles(t, dir) {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		total += info.Size()
	}
	return total
}

func spooledRowsContaining(t *testing.T, dir, needle string) int {
	t.Helper()
	count := 0
	for _, name := range spoolSegmentFiles(t, dir) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		count += strings.Count(string(data), needle)
	}
	return count
}

// TestKafkaLogSpoolCarriesAnOutageIntoClickHouse is the headline: the broker
// goes away, the rows go to the disk, the broker comes back, and the rows reach
// ClickHouse without the process restarting.
//
// It is also where the request path's cost is measured. The disk is a fallback,
// not a second write path, and the gap between the injection below and
// request.timeout.ms is wide enough that an implementation which wrote a file on
// the request goroutine could not pass it.
func TestKafkaLogSpoolCarriesAnOutageIntoClickHouse(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not configured")
	}
	topic := common.GetEnvOrDefaultString("TEST_KAFKA_LOG_TOPIC", "new-api-logs-test")

	db, dropDatabase := openIsolatedClickHouseLogDB(t)
	t.Cleanup(dropDatabase)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	broker := newSwitchableBroker(t, brokers)
	spoolDir := t.TempDir()
	requestID := fmt.Sprintf("rid-spool-%d", time.Now().UnixNano())

	t.Setenv("KAFKA_BROKERS", broker.Addr())
	t.Setenv("KAFKA_LOG_TOPIC", topic)
	t.Setenv("KAFKA_LOG_GROUP_ID", fmt.Sprintf("new-api-log-consumer-spool-%d", time.Now().UnixNano()))
	t.Setenv("KAFKA_LOG_SPOOL_DIR", spoolDir)
	t.Setenv("KAFKA_LOG_REQUEST_TIMEOUT_MS", "5000")
	// Short enough that the delivery failure callback runs inside the test, long
	// enough that the rows are still buffered when the injection finishes.
	t.Setenv("KAFKA_LOG_DELIVERY_TIMEOUT_MS", "2000")
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "200")
	// One row per segment and a short interval, so "it was sealed" and "it was
	// replayed" are states the test can observe rather than wait a minute for.
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_BYTES", "4096")
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_SECONDS", "1")

	// The billing audit invariant rides on this row: other.admin_info.
	// quota_saturation is what makes a clamped charge visible to an administrator,
	// and it has to come back byte for byte through the disk as well as the topic.
	saturated := NewLogOther()
	saturated.SetPublic("model_ratio", 1)
	saturated.SetAdmin("quota_saturation", map[string]any{"op": "mul", "kind": "overflow", "clamped": 2147483647})
	saturatedOther := saturated.JSONString()

	// Two rows deliberately share one request_id, across two log types. They are
	// both legitimate: request_id is not unique (RecordErrorLog and
	// RecordConsumeLog both take it from the request context), so an
	// implementation that used it as a deduplication key would delete one of
	// these rows instead of carrying it.
	sharedID := requestID + "-shared"

	// Registered after the temporary directories so it runs before their removal:
	// cleanups are last-in-first-out, and on Windows a directory cannot be removed
	// while the spool still holds its active segment open.
	t.Cleanup(func() { stopLogKafkaForTest(t) })
	require.NoError(t, StartLogKafka())

	const rows = 40
	started := time.Now()
	for i := range rows {
		row := sampleLogRow(1700000000+int64(i), requestID)
		if i == 0 {
			row.Other = saturatedOther
		}
		require.NoError(t, createLog(row))
	}
	shared := sampleLogRow(1700000900, sharedID)
	shared.Type = LogTypeError
	shared.Content = "upstream refused the request"
	require.NoError(t, createLog(shared))
	require.NoError(t, createLog(sampleLogRow(1700000901, sharedID)))
	elapsed := time.Since(started)
	t.Logf("%d rows through the request path with the broker unreachable took %s (request.timeout.ms is 5s)", rows+2, elapsed)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"accepting a row must not wait for the broker: 42 rows against a 5s request timeout is a 10x margin, and any implementation that wrote a file on the request goroutine would take seconds")

	// Trigger A: the rows the producer had buffered fail their delivery and go to
	// the disk. Nothing was acknowledged, so nothing may be lost.
	require.Eventually(t, func() bool {
		return spooledRowsContaining(t, spoolDir, requestID) > 0
	}, 30*time.Second, 50*time.Millisecond,
		"a delivery failure has to write the row to the spool instead of dropping it")
	assert.Zero(t, clickHouseLogRowCount(t, db, requestID), "with the broker unreachable nothing can have reached ClickHouse yet")

	// The broker comes back. The producer retries and the replay drains the
	// sealed segments back into the topic; the consumer is already running and
	// follows. No restart, no reconfiguration.
	broker.open.Store(true)

	require.Eventually(t, func() bool {
		return clickHouseLogRowCount(t, db, requestID) >= rows
	}, 90*time.Second, 200*time.Millisecond,
		"every row written to the spool has to reach ClickHouse after the broker returns")

	// The rows the spool holds must each appear at least once, and the duplicates
	// are bounded by whole segments being replayed again -- never by a partial
	// segment having been consumed.
	assert.GreaterOrEqual(t, clickHouseLogRowCount(t, db, requestID), int64(rows))
	var distinct int64
	require.NoError(t, db.Raw("SELECT uniqExact((created_at, request_id, content)) FROM logs WHERE request_id = ?", requestID).Scan(&distinct).Error)
	assert.GreaterOrEqual(t, distinct, int64(rows), "every distinguishable row has to be there at least once")

	// AC-5.2: the shared request_id keeps both of its rows.
	assert.Equal(t, int64(2), clickHouseLogRowCount(t, db, sharedID),
		"a shared request_id is legitimate and both rows have to survive: a deduplication key on request_id would delete one of them")

	// AC-14: the saturation marker arrives byte for byte, through the disk, the
	// replay, the topic and the consumer.
	var storedOther string
	require.NoError(t, db.Raw("SELECT other FROM logs WHERE request_id = ? ORDER BY created_at LIMIT 1", requestID).Scan(&storedOther).Error)
	assert.Equal(t, saturatedOther, storedOther,
		"the saturation marker has to survive the trip through the spool byte for byte; re-encoding it anywhere would make a billing anomaly disappear from the admin view")
	assert.Contains(t, storedOther, "quota_saturation")

	// AC-12.6: with the broker healthy again the spool drains and stays empty.
	// The disk is not a first-class path -- the healthy steady state is zero
	// segment files and zero bytes written.
	require.Eventually(t, func() bool {
		return len(spoolSegmentFiles(t, spoolDir)) == 0
	}, 60*time.Second, 100*time.Millisecond,
		"a replayed segment has to be removed once every row in it was acknowledged")

	before := spoolFileBytes(t, spoolDir)
	for i := range 10 {
		require.NoError(t, createLog(sampleLogRow(1700001000+int64(i), requestID+"-healthy")))
	}
	require.Never(t, func() bool { return spoolFileBytes(t, spoolDir) > before }, 2*time.Second, 100*time.Millisecond,
		"with the broker healthy not one byte may be written to the disk; the disk is a failure path, not a second write path")
}

// TestKafkaLogSpoolIsSpooledBeforeTheDeliveryTimeoutIsTheHysteresisBand is
// trigger B, the other half of the fault injection: when the producer is close to
// its buffer bound the rows go to the disk immediately, rather than failing their
// way there one delivery timeout later.
//
// The burst is sized, in the same byte unit `Send` measures, to stay inside the
// client's own bound (KAFKA_LOG_MAX_BUFFER_BYTES) while its running total crosses
// the application's 0.9 x threshold. Every record is therefore accepted by
// TryProduce and no delivery can fail before the 60 s delivery timeout, so the
// band is the only thing left that can put a row on the disk.
//
// The size is the whole test. KAFKA_LOG_MAX_BUFFER_BYTES is both the band's basis
// and the client's bound, so a burst that overflows the client bound reaches the
// same spool through ErrMaxBuffered -- and then deleting the band leaves this
// test green, which is what it did.
func TestKafkaLogSpoolIsSpooledBeforeTheDeliveryTimeoutIsTheHysteresisBand(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not configured")
	}
	topic := common.GetEnvOrDefaultString("TEST_KAFKA_LOG_TOPIC", "new-api-logs-test")

	db, dropDatabase := openIsolatedClickHouseLogDB(t)
	t.Cleanup(dropDatabase)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	// A delivery timeout far longer than the test: everything that reaches the
	// disk before it expires got there through the band, not through a failure.
	const deliveryTimeout = 60 * time.Second
	broker := newSwitchableBroker(t, brokers)
	spoolDir := t.TempDir()
	requestID := fmt.Sprintf("rid-spool-band-%d", time.Now().UnixNano())

	// The producer is handed the encoded row, so the test can size the burst in
	// exactly the bytes the band and the client bound are counted in. The bound is
	// expressed in rows rather than in a byte figure, so the arithmetic below holds
	// whatever a row happens to weigh.
	probe, err := common.Marshal(sampleLogRow(1700002000, requestID))
	require.NoError(t, err)
	rowBytes := int64(len(probe))
	const boundRows = 42
	maxBufferBytes := rowBytes * boundRows

	t.Setenv("KAFKA_BROKERS", broker.Addr())
	t.Setenv("KAFKA_LOG_TOPIC", topic)
	t.Setenv("KAFKA_LOG_GROUP_ID", fmt.Sprintf("new-api-log-consumer-band-%d", time.Now().UnixNano()))
	t.Setenv("KAFKA_LOG_SPOOL_DIR", spoolDir)
	t.Setenv("KAFKA_LOG_DELIVERY_TIMEOUT_MS", fmt.Sprintf("%d", deliveryTimeout.Milliseconds()))
	t.Setenv("KAFKA_LOG_REQUEST_TIMEOUT_MS", "5000")
	t.Setenv("KAFKA_LOG_MAX_BUFFER_BYTES", fmt.Sprintf("%d", maxBufferBytes))
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_BYTES", "4096")
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_SECONDS", "1")
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "200")

	// The band's upper edge, from the same expression NewProducer uses, and the
	// burst: enough rows to cross it and keep going, and no more, because every row
	// in the burst has to fit inside the client's own bound.
	//
	// That last part cannot be asserted as arithmetic. rows is 0.9 x boundRows + 3,
	// so it is exactly 40 for every row size and the comparison would reduce to
	// 40 < 42: true whatever the row weighs, and still true if the formula above is
	// edited. It is asserted below where it is observable instead -- no delivery
	// may report ErrMaxBuffered -- which is the property the band's absence would
	// otherwise hide behind.
	highWater := maxBufferBytes * 9 / 10
	rows := int(highWater/rowBytes) + 3

	// Registered after the temporary directories so it runs before their removal:
	// cleanups are last-in-first-out, and on Windows a directory cannot be removed
	// while the spool still holds its active segment open. The error log is
	// captured after that cleanup is registered, so the producer's reports are read
	// while this test is still running.
	t.Cleanup(func() { stopLogKafkaForTest(t) })
	output := captureSysErrors(t)
	require.NoError(t, StartLogKafka())

	started := time.Now()
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700002000+int64(i), requestID)))
	}
	elapsed := time.Since(started)

	require.Eventually(t, func() bool {
		return spooledRowsContaining(t, spoolDir, requestID) > 0
	}, 10*time.Second, 20*time.Millisecond,
		"a producer at 90% of KAFKA_LOG_MAX_BUFFER_BYTES has to send rows to the disk directly; the whole burst fits inside the client's bound and no delivery can have failed yet, so nothing else could have put them there")
	assert.Less(t, elapsed, deliveryTimeout/10,
		"the rows have to reach the disk long before the delivery timeout expires, which is what tells the band apart from a failure")

	// The premise, as the observation it is. A record the client refuses at its own
	// bound reaches this same spool through ErrMaxBuffered, so a burst that
	// overflowed KAFKA_LOG_MAX_BUFFER_BYTES would put its rows on the disk with the
	// band deleted, and the test would then pass without proving anything about the
	// band. The report is the only place that path is visible, and it always prints
	// its first occurrence, so its absence is the evidence that the band is the only
	// thing that put a row on the disk.
	require.NotContains(t, output.String(), "buffer is full (KAFKA_LOG_MAX_BUFFER_BYTES)",
		"every record in the burst has to be accepted by the client's own buffer: a refused one reaches the spool through ErrMaxBuffered, and then the band is no longer the only thing that can put a row on the disk")

	// The band releases once the producer is current again: the broker returns, the
	// buffered rows are acknowledged, the in-flight bytes fall back under the lower
	// edge (0.5 x), and new rows go through the producer instead of the disk.
	broker.open.Store(true)
	require.Eventually(t, func() bool {
		return len(spoolSegmentFiles(t, spoolDir)) == 0
	}, 90*time.Second, 100*time.Millisecond,
		"once the broker is back, the rows the band wrote have to be replayed and their segments removed")

	healthyID := requestID + "-healthy"
	before := spoolFileBytes(t, spoolDir)
	for i := range 10 {
		require.NoError(t, createLog(sampleLogRow(1700003000+int64(i), healthyID)))
	}
	require.Never(t, func() bool { return spoolFileBytes(t, spoolDir) > before }, 3*time.Second, 50*time.Millisecond,
		"with the band released the new rows have to go through the producer, so not one byte may reach the disk")
	require.Eventually(t, func() bool {
		return clickHouseLogRowCount(t, db, healthyID) == 10
	}, 60*time.Second, 100*time.Millisecond,
		"the rows sent after the band released have to reach ClickHouse, and they can only have got there through the producer: nothing of theirs was written to the disk")
}

// TestKafkaLogPayloadIsExactlyOneLine pins the invariant the torn-tail rule
// rests on. The spool separates rows by newline and decides "half written" by
// "does not end with a newline", so a payload carrying a raw newline of its own
// would produce two lines that are not both rows -- and the truncation would then
// be judging the wrong boundary.
func TestKafkaLogPayloadIsExactlyOneLine(t *testing.T) {
	row := sampleLogRow(1700000000, "rid-line-invariant")
	other := NewLogOther()
	other.SetPublic("model_ratio", 1)
	other.SetAdmin("quota_saturation", map[string]any{"op": "mul", "kind": "overflow", "clamped": 2147483647})
	row.Other = other.JSONString()
	// Every character that could turn into a line break if the encoding were
	// naive: a newline, a carriage return and a tab inside a JSON string.
	row.Content = "a line\nwith a newline, a carriage return\r and a tab\tin it"

	payload, err := common.Marshal(row)
	require.NoError(t, err)
	assert.Zero(t, bytes.Count(payload, []byte{'\n'}),
		"an encoded row must not contain a raw newline: JSON escapes one inside a string, and that is what makes 'one line per row' true")

	line := append(append([]byte{}, payload...), '\n')
	assert.Equal(t, 1, bytes.Count(line, []byte{'\n'}), "the spool's unit of storage is exactly one line")
	assert.Equal(t, byte('\n'), line[len(line)-1])
}

// TestNoSpoolDirectoryWithoutBrokers is the upgrade case, asserted on the
// filesystem rather than on a predicate: a deployment that has never heard of
// Kafka must not gain a directory, and the spool settings must not even be read.
func TestNoSpoolDirectoryWithoutBrokers(t *testing.T) {

	dir := t.TempDir()
	spoolDir := filepath.Join(dir, "new-api-log-spool")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_LOG_SPOOL_DIR", spoolDir)
	// Values that would stop startup if anything evaluated them.
	t.Setenv("KAFKA_LOG_SPOOL_MAX_BYTES", "not-a-number")
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_BYTES", "not-a-number")
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_SECONDS", "not-a-number")
	withLogDatabaseType(t, common.DatabaseTypeClickHouse)

	// Registered after the temporary directories so it runs before their removal:
	// cleanups are last-in-first-out, and on Windows a directory cannot be removed
	// while the spool still holds its active segment open.
	t.Cleanup(func() { stopLogKafkaForTest(t) })
	require.NoError(t, StartLogKafka())
	assert.NoFileExists(t, spoolDir, "KAFKA_BROKERS is empty, so nothing about the spool may be resolved, created or probed")
	assert.Nil(t, logKafkaProducer.Load())
	assert.Nil(t, logKafkaConsumer)
}

// TestKafkaLogStartupStopsDisclaimingTheSpool is the cleanup that is easiest to
// miss. The startup line used to say this process did not act on
// KAFKA_LOG_SPOOL_DIR; after this slice that is false, and a false line in a
// startup log is worse than no line at all.
func TestKafkaLogStartupStopsDisclaimingTheSpool(t *testing.T) {

	relative := fmt.Sprintf("spool-startup-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = os.RemoveAll(relative) })
	absolute, err := filepath.Abs(relative)
	require.NoError(t, err)

	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_LOG_SPOOL_DIR", relative)
	t.Setenv("KAFKA_LOG_TOPIC", "new-api-logs-startup-test")
	withLogDatabaseType(t, common.DatabaseTypeClickHouse)

	output := captureStartupLogs(t)
	// Registered after the temporary directories so it runs before their removal:
	// cleanups are last-in-first-out, and on Windows a directory cannot be removed
	// while the spool still holds its active segment open.
	t.Cleanup(func() { stopLogKafkaForTest(t) })
	require.NoError(t, StartLogKafka())

	assert.NotContains(t, output.String(), "this process does not act on them: KAFKA_LOG_SPOOL_DIR",
		"the spool is written by this process now, so the disclaimer has become a false statement")
	assert.Contains(t, output.String(), "directory "+absolute,
		"the spool prints its resolved absolute path, because the default is relative")
	assert.Contains(t, output.String(), "KAFKA_LOG_RETENTION_HOURS",
		"the topic retention is still a declaration: this application does not create topics")
}

// TestShutdownFlushFailuresStillLandOnTheDisk is the shutdown order, asserted the
// only way it can be: by making the last flush unable to place its rows and
// checking that they had somewhere to go.
//
// The producer closes the client and only then closes the spool. Swapping those
// two drops exactly the rows a shutdown could not deliver, which is the one
// moment the disk fallback exists for.
func TestShutdownFlushFailuresStillLandOnTheDisk(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not configured")
	}

	withLogDatabaseType(t, common.DatabaseTypeClickHouse)
	broker := newSwitchableBroker(t, brokers) // never switched on: the broker stays a black hole
	spoolDir := t.TempDir()
	requestID := fmt.Sprintf("rid-spool-shutdown-%d", time.Now().UnixNano())

	t.Setenv("KAFKA_BROKERS", broker.Addr())
	t.Setenv("KAFKA_LOG_TOPIC", common.GetEnvOrDefaultString("TEST_KAFKA_LOG_TOPIC", "new-api-logs-test"))
	t.Setenv("KAFKA_LOG_GROUP_ID", fmt.Sprintf("new-api-log-consumer-shutdown-%d", time.Now().UnixNano()))
	t.Setenv("KAFKA_LOG_SPOOL_DIR", spoolDir)
	t.Setenv("KAFKA_LOG_REQUEST_TIMEOUT_MS", "2000")
	// Long enough that nothing has failed yet: these rows are still in the
	// producer's buffer when the shutdown starts.
	t.Setenv("KAFKA_LOG_DELIVERY_TIMEOUT_MS", "300000")
	t.Setenv("KAFKA_LOG_SPOOL_SEGMENT_SECONDS", "1")

	// Registered after the temporary directories so it runs before their removal:
	// cleanups are last-in-first-out, and on Windows a directory cannot be removed
	// while the spool still holds its active segment open.
	t.Cleanup(func() { stopLogKafkaForTest(t) })
	require.NoError(t, StartLogKafka())
	const rows = 12
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000300+int64(i), requestID)))
	}
	assert.Zero(t, spooledRowsContaining(t, spoolDir, requestID),
		"nothing has failed yet, so nothing should be on the disk before the shutdown")

	StopLogKafka()

	assert.Equal(t, rows, spooledRowsContaining(t, spoolDir, requestID),
		"the rows a shutdown flush could not place have to reach the disk: closing the spool before closing the client would drop every one of them")
}

func captureStartupLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var captured bytes.Buffer
	common.LogWriterMu.Lock()
	previous := gin.DefaultWriter
	gin.DefaultWriter = &captured
	common.LogWriterMu.Unlock()
	t.Cleanup(func() {
		common.LogWriterMu.Lock()
		gin.DefaultWriter = previous
		common.LogWriterMu.Unlock()
	})
	return &captured
}
