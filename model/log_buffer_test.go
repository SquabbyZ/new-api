package model

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// useLogDatabase points the package at one log database for the duration of a
// test and restores the previous one afterwards.
func useLogDatabase(t *testing.T, db *gorm.DB, dbType common.DatabaseType) {
	t.Helper()
	previousLogDB := LOG_DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		LOG_DB = previousLogDB
		common.SetDatabaseTypes(previousMain, previousLog)
		initCol()
	})
	LOG_DB = db
	common.SetDatabaseTypes(previousMain, dbType)
	// logGroupCol follows the log database dialect; the batched ClickHouse
	// INSERT quotes the reserved `group` column with it.
	initCol()
}

// resetLogBuffer clears the process-local buffer so one test cannot see the
// rows, drops or failure count of another.
func resetLogBuffer(t *testing.T) {
	t.Helper()
	clear := func() {
		logBufferMu.Lock()
		logBufferRows = nil
		logBufferDropped = 0
		logDropReported = 0
		logBufferMu.Unlock()
		logFlushFailures = 0
		// A wake-up sent while no loop was running would otherwise survive into
		// the next test and flush half of its rows behind the test's back.
		for {
			select {
			case <-logFlushSignal:
			default:
				return
			}
		}
	}
	clear()
	t.Cleanup(clear)
}

// startLogFlushForTest starts the flush loop for one test with freshly created
// loop state: the loop is a process-lifetime singleton guarded by a sync.Once,
// so a test that starts and stops it itself has to reset that state first. No
// loop may be running when it is called.
func startLogFlushForTest(t *testing.T) {
	t.Helper()
	logFlushStop = make(chan struct{})
	logFlushDone = make(chan struct{})
	logFlushStartOnce = sync.Once{}
	logFlushStopOnce = sync.Once{}
	StartLogFlush()
	t.Cleanup(StopLogFlush)
}

// captureSysErrors points the process error log at a buffer for the rest of the
// test. Reports about dropped and unwritable log rows are the observable
// behavior under test, and common.SysError is where they land.
func captureSysErrors(t *testing.T) *bytes.Buffer {
	t.Helper()
	var captured bytes.Buffer
	common.LogWriterMu.Lock()
	previous := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = &captured
	common.LogWriterMu.Unlock()
	t.Cleanup(func() {
		common.LogWriterMu.Lock()
		gin.DefaultErrorWriter = previous
		common.LogWriterMu.Unlock()
	})
	return &captured
}

func sampleLogRow(createdAt int64, requestID string) *Log {
	return &Log{
		UserId:            7,
		CreatedAt:         createdAt,
		Type:              LogTypeConsume,
		Content:           "consume ok",
		Username:          "alice",
		TokenName:         "default",
		ModelName:         "gpt-4o",
		Quota:             1234,
		PromptTokens:      11,
		CompletionTokens:  22,
		UseTime:           3,
		IsStream:          true,
		ChannelId:         9,
		TokenId:           5,
		Group:             "vip",
		Ip:                "10.0.0.1",
		RequestId:         requestID,
		UpstreamRequestId: "upstream-1",
		Other:             `{"model_ratio":1}`,
	}
}

func recordedInserts(recorder *sqlRecorder) []string {
	var inserts []string
	for _, statement := range recorder.recorded() {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "INSERT INTO LOGS") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "INSERT INTO `LOGS`") {
			inserts = append(inserts, statement)
		}
	}
	return inserts
}

// openClickHouseLogTestDB opens the real ClickHouse instance with an empty
// `logs` table created by the production DDL.
func openClickHouseLogTestDB(t *testing.T, recorder *sqlRecorder) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("TEST_CLICKHOUSE_DSN is not configured")
	}
	db, err := gorm.Open(clickhouse.Open(normalizeClickHouseDSN(dsn)), &gorm.Config{Logger: recorder})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	var version string
	require.NoError(t, db.Raw("SELECT version()").Scan(&version).Error)
	t.Logf("clickhouse version: %s", version)

	recreateLogsTable(t, db)
	t.Cleanup(func() { _ = db.Exec("DROP TABLE IF EXISTS logs").Error })
	return db
}

func recreateLogsTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)
	require.NoError(t, db.Exec(clickHouseLogCreateTableSQL(0)).Error)
}

func activeLogParts(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var parts int64
	require.NoError(t, db.Raw(
		"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'logs' AND active",
	).Scan(&parts).Error)
	return parts
}

// countServerInserts counts the INSERT statements ClickHouse actually received
// for the `logs` table, which is the server-side form of "the insert is
// batched" that system.parts can only approximate: background merges rewrite
// parts asynchronously, but every received statement is logged verbatim.
func countServerInserts(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	require.NoError(t, db.Exec("SYSTEM FLUSH LOGS").Error)
	var statements int64
	// QueryFinish counts one row per finished query. Both forms are matched:
	// the batched statement names the table unquoted, GORM's per-row INSERT
	// quotes it.
	require.NoError(t, db.Raw(
		"SELECT count() FROM system.query_log WHERE event_time >= now() - 300 AND type = 'QueryFinish' AND query_kind = 'Insert' AND (query LIKE 'INSERT INTO logs%' OR query LIKE 'INSERT INTO `logs`%')",
	).Scan(&statements).Error)
	return statements
}

// TestLogWriteRoundTripOnEveryLogDatabase proves the write path still lands a
// complete row that the existing readers return field by field, on all four
// supported engines. Only ClickHouse buffers, so only ClickHouse needs a flush
// before the row is visible.
func TestLogWriteRoundTripOnEveryLogDatabase(t *testing.T) {
	const requestID = "rid-round-trip"

	cases := []struct {
		name string
		env  string
		typ  common.DatabaseType
	}{
		{"sqlite", "", common.DatabaseTypeSQLite},
		{"mysql", "TEST_MYSQL_DSN", common.DatabaseTypeMySQL},
		{"postgres", "TEST_POSTGRES_DSN", common.DatabaseTypePostgreSQL},
		{"clickhouse", "TEST_CLICKHOUSE_DSN", common.DatabaseTypeClickHouse},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := os.Getenv(tc.env)
			if tc.env != "" && dsn == "" {
				t.Skip(tc.env + " is not configured")
			}
			resetLogBuffer(t)

			recorder := &sqlRecorder{}
			var db *gorm.DB
			if tc.name == "clickhouse" {
				db = openClickHouseLogTestDB(t, recorder)
			} else {
				db = openLogTestDB(t, tc.name, dsn, recorder)
			}
			useLogDatabase(t, db, tc.typ)

			createdAt := int64(1700000000)
			require.NoError(t, createLog(sampleLogRow(createdAt, requestID)))

			if tc.name == "clickhouse" {
				// ClickHouse is the buffered deployment form: the request path
				// must not have reached the database yet.
				assert.Empty(t, recordedInserts(recorder), "the row must be buffered, not written")
			}
			flushLogBuffer()

			logs, total, err := GetAllLogs(LogTypeConsume, 0, 0, "", "", "", 0, 10, 0, "", requestID, "")
			require.NoError(t, err)
			require.EqualValues(t, 1, total)
			require.Len(t, logs, 1)

			stored := logs[0]
			assert.Equal(t, 7, stored.UserId)
			assert.Equal(t, createdAt, stored.CreatedAt)
			assert.Equal(t, LogTypeConsume, stored.Type)
			assert.Equal(t, "consume ok", stored.Content)
			assert.Equal(t, "alice", stored.Username)
			assert.Equal(t, "default", stored.TokenName)
			assert.Equal(t, "gpt-4o", stored.ModelName)
			assert.Equal(t, 1234, stored.Quota)
			assert.Equal(t, 11, stored.PromptTokens)
			assert.Equal(t, 22, stored.CompletionTokens)
			assert.Equal(t, 3, stored.UseTime)
			assert.True(t, stored.IsStream)
			assert.Equal(t, 5, stored.TokenId)
			assert.Equal(t, "vip", stored.Group)
			assert.Equal(t, "10.0.0.1", stored.Ip)
			assert.Equal(t, requestID, stored.RequestId)
			assert.Equal(t, "upstream-1", stored.UpstreamRequestId)
			assert.JSONEq(t, `{"model_ratio":1}`, stored.Other)

			// The channel column is not part of the readers' filters here, so
			// read the row back directly to assert it on every engine.
			var channelID int
			require.NoError(t, db.Model(&Log{}).Where("request_id = ?", requestID).Select("channel_id").Scan(&channelID).Error)
			assert.Equal(t, 9, channelID)

			// The user-scoped reader must see the row too.
			userLogs, userTotal, err := GetUserLogs(7, LogTypeConsume, 0, 0, "", "", 0, 10, "", requestID, "")
			require.NoError(t, err)
			require.EqualValues(t, 1, userTotal)
			require.Len(t, userLogs, 1)
			assert.Equal(t, "alice", userLogs[0].Username)
			assert.Equal(t, 1234, userLogs[0].Quota)
		})
	}
}

// TestClickHouseLogInsertIsBatched proves on the real ClickHouse instance that a
// flushed batch reaches the server as one INSERT statement, and that the
// per-row path this replaces reaches it as one statement per row.
func TestClickHouseLogInsertIsBatched(t *testing.T) {
	const rows = 64
	createdAt := int64(1700000000)

	batchedRecorder := &sqlRecorder{}
	db := openClickHouseLogTestDB(t, batchedRecorder)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	resetLogBuffer(t)

	baseline := countServerInserts(t, db)
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(createdAt+int64(i), "rid-batched")))
	}
	flushLogBuffer()

	batchedStatements := recordedInserts(batchedRecorder)
	require.Len(t, batchedStatements, 1, "a flush must reach ClickHouse as one INSERT statement")
	assert.Contains(t, batchedStatements[0], "INSERT INTO logs")
	serverBatched := countServerInserts(t, db) - baseline
	batchedParts := activeLogParts(t, db)

	var stored int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", "rid-batched").Scan(&stored).Error)
	assert.EqualValues(t, rows, stored)
	assert.Greater(t, serverBatched, int64(0), "the batched INSERT must be visible in system.query_log")

	// The same rows through one INSERT per row, which is what the buffer
	// replaces: the driver's Create callback cannot batch, so this is what a
	// ClickHouse deployment did for every single log row.
	recreateLogsTable(t, db)
	baseline = countServerInserts(t, db)
	perRowRecorder := &sqlRecorder{}
	perRowDB := db.Session(&gorm.Session{Logger: perRowRecorder})
	for i := range rows {
		require.NoError(t, perRowDB.Create(sampleLogRow(createdAt+int64(i), "rid-per-row")).Error)
	}
	perRowStatements := recordedInserts(perRowRecorder)
	require.Len(t, perRowStatements, rows, "the per-row path must issue one statement per row")
	serverPerRow := countServerInserts(t, db) - baseline
	perRowParts := activeLogParts(t, db)

	t.Logf("clickhouse %d rows: batched=%d client statement(s)/%d server INSERT(s)/%d active part(s), per-row=%d client statement(s)/%d server INSERT(s)/%d active part(s)",
		rows, len(batchedStatements), serverBatched, batchedParts, len(perRowStatements), serverPerRow, perRowParts)
	assert.Less(t, serverBatched, serverPerRow,
		"ClickHouse must receive fewer INSERT statements for the batched flush")
	assert.Less(t, len(batchedStatements), len(perRowStatements),
		"the batched flush must emit far fewer statements than the per-row path")
	assert.Less(t, batchedParts, int64(rows), "a batch must not produce one part per row")
}

// TestLogBufferDropsWhenFullAndReports covers the overflow policy: a full buffer
// drops the incoming row instead of blocking the request, and the drops are
// counted rather than silently discarded.
func TestLogBufferDropsWhenFullAndReports(t *testing.T) {
	resetLogBuffer(t)

	for i := range logBufferCapacity {
		enqueueLog(sampleLogRow(int64(i), "rid-cap"))
	}
	require.Len(t, logBufferRows, logBufferCapacity)

	enqueueLog(sampleLogRow(0, "rid-dropped"))
	assert.Len(t, logBufferRows, logBufferCapacity, "the buffer must stay bounded")
	assert.EqualValues(t, 1, logBufferDropped)

	for i := range logDropReportEvery {
		enqueueLog(sampleLogRow(int64(i), "rid-dropped"))
	}
	assert.EqualValues(t, 1+logDropReportEvery, logBufferDropped)
	assert.Len(t, logBufferRows, logBufferCapacity)
}

// TestLogBufferRetainsBatchAfterFlushFailure covers the flush failure policy: a
// failed batch is put back in front of the buffer in FIFO order, so one
// transient failure never discards rows.
func TestLogBufferRetainsBatchAfterFlushFailure(t *testing.T) {
	resetLogBuffer(t)

	// A database without the `logs` table fails every flush deterministically.
	db := openLogTestDB(t, "sqlite", "", &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeSQLite)
	require.NoError(t, db.Migrator().DropTable(&Log{}))

	first := sampleLogRow(1700000000, "rid-first")
	second := sampleLogRow(1700000001, "rid-second")
	enqueueLog(first)
	flushLogBuffer()

	require.Len(t, logBufferRows, 1)
	assert.Same(t, first, logBufferRows[0], "the failed batch must be retained for retry")

	enqueueLog(second)
	assert.Len(t, logBufferRows, 2)
	require.NoError(t, db.AutoMigrate(&Log{}))

	flushLogBuffer()
	assert.Empty(t, logBufferRows)
	assert.Zero(t, logFlushFailures, "a successful flush resets the failure count")

	var stored int64
	require.NoError(t, db.Model(&Log{}).Where("request_id IN ?", []string{"rid-first", "rid-second"}).Count(&stored).Error)
	assert.EqualValues(t, 2, stored, "both the retried and the newly enqueued row must land")
}

// TestLogBufferKeepsEveryRowUnderConcurrentProducers exercises the production
// shape — relays enqueueing while the flush loop drains — and asserts the
// buffer's contract: every accepted row is written exactly once.
func TestLogBufferKeepsEveryRowUnderConcurrentProducers(t *testing.T) {
	resetLogBuffer(t)

	db := openLogTestDB(t, "sqlite", "", &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeSQLite)

	const producers, perProducer = 4, 100

	stopFlushing := make(chan struct{})
	var flusher sync.WaitGroup
	flusher.Go(func() {
		for {
			select {
			case <-stopFlushing:
				return
			default:
				flushLogBuffer()
			}
		}
	})

	var producersDone sync.WaitGroup
	for producer := range producers {
		producersDone.Go(func() {
			for i := range perProducer {
				enqueueLog(sampleLogRow(1700000000+int64(i), fmt.Sprintf("rid-%d-%d", producer, i)))
			}
		})
	}
	producersDone.Wait()
	close(stopFlushing)
	flusher.Wait()
	flushLogBuffer()

	assert.Zero(t, logBufferDropped, "no row may be dropped while the buffer is under its bound")

	var stored, distinct int64
	require.NoError(t, db.Model(&Log{}).Where("request_id LIKE ?", "rid-%").Count(&stored).Error)
	require.NoError(t, db.Model(&Log{}).Select("COUNT(DISTINCT request_id)").Scan(&distinct).Error)
	assert.EqualValues(t, producers*perProducer, stored, "every enqueued row must be written")
	assert.EqualValues(t, stored, distinct, "no row may be written twice")
	assert.Empty(t, logBufferRows)
}

// TestClickHouseFailedBatchIsRetriedWithoutDuplicating proves on the real
// instance that a refused batch leaves nothing behind and is re-inserted
// exactly once. One INSERT statement is atomic, so requeueing the whole batch
// can never duplicate the rows a partial write would have committed.
func TestClickHouseFailedBatchIsRetriedWithoutDuplicating(t *testing.T) {
	resetLogBuffer(t)

	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	const rows = 5
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), "rid-retried")))
	}

	// No table: the flush fails and must retain the whole batch.
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)
	flushLogBuffer()
	require.Len(t, logBufferRows, rows, "a failed batch must stay buffered")
	assert.Equal(t, 1, logFlushFailures, "the failed flush must be counted")
	assert.EqualValues(t, 0, logBufferDropped, "a flushed-and-failed batch is retained, not dropped")

	var beforeRetry int64
	require.NoError(t, db.Raw("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'logs'").Scan(&beforeRetry).Error)
	require.Zero(t, beforeRetry, "the refused INSERT must leave no table behind")

	recreateLogsTable(t, db)
	flushLogBuffer()
	assert.Empty(t, logBufferRows)

	var stored int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", "rid-retried").Scan(&stored).Error)
	assert.EqualValues(t, rows, stored, "the retried batch must land exactly once, with no duplicates")
	assert.Zero(t, logBufferDropped)
}

// TestLogBufferDropsBatchAfterRepeatedFlushFailures covers the other half of the
// flush failure policy: a batch the server refuses every time is dropped and
// reported after a bounded number of attempts, so one refused batch cannot pin
// the buffer and stall every later log forever.
func TestLogBufferDropsBatchAfterRepeatedFlushFailures(t *testing.T) {
	resetLogBuffer(t)

	db := openLogTestDB(t, "sqlite", "", &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeSQLite)
	require.NoError(t, db.Migrator().DropTable(&Log{}))

	enqueueLog(sampleLogRow(1700000000, "rid-refused"))
	for attempt := 1; attempt < logFlushMaxAttempts; attempt++ {
		flushLogBuffer()
		require.Len(t, logBufferRows, 1, "the batch must survive until the attempt limit")
	}

	// The final attempt gives up on the batch and releases the buffer.
	flushLogBuffer()
	assert.Empty(t, logBufferRows, "the batch must be released once the attempts are exhausted")
	assert.EqualValues(t, 1, logBufferDropped, "the dropped batch must be counted")
	assert.Zero(t, logFlushFailures, "the failure counter restarts with the next batch")
}

// TestLogBufferFlushesOnShutdown covers the graceful shutdown path: rows already
// accepted must reach the database when the process stops.
func TestLogBufferFlushesOnShutdown(t *testing.T) {
	// A flush interval far beyond the test keeps the background loop out of the
	// way, so what lands is what StopLogFlush wrote.
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "60000")
	assert.Equal(t, 60000*time.Millisecond, logFlushInterval())

	recorder := &sqlRecorder{}
	db := openClickHouseLogTestDB(t, recorder)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	resetLogBuffer(t)

	StartLogFlush()

	const rows = 40
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), "rid-shutdown")))
	}
	assert.Empty(t, recordedInserts(recorder), "nothing may be written while the buffer only accumulates")

	StopLogFlush()

	var stored int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", "rid-shutdown").Scan(&stored).Error)
	assert.EqualValues(t, rows, stored, "a graceful shutdown must flush every buffered row")
	assert.Len(t, recordedInserts(recorder), 1, "the shutdown flush must batch, not write row by row")
}

// TestLogBufferPreservesQuotaSaturationMarker covers the billing audit
// invariant: the clamp marker attachQuotaSaturation nests under
// other.admin_info must travel with the buffered row and be stored unchanged.
func TestLogBufferPreservesQuotaSaturationMarker(t *testing.T) {
	resetLogBuffer(t)

	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	const requestID = "rid-clamped"
	row := sampleLogRow(1700000000, requestID)
	other := NewLogOther()
	other.SetPublic("model_ratio", 1)
	// Exactly the shape attachQuotaSaturationToOther writes.
	other.SetAdmin("quota_saturation", map[string]any{"op": "mul", "kind": "overflow", "clamped": 2147483647})
	row.Other = other.JSONString()

	require.NoError(t, createLog(row))
	flushLogBuffer()

	logs, _, err := GetAllLogs(LogTypeConsume, 0, 0, "", "", "", 0, 10, 0, "", requestID, "")
	require.NoError(t, err)
	require.Len(t, logs, 1)

	var decoded struct {
		AdminInfo map[string]struct {
			Clamped int `json:"clamped"`
		} `json:"admin_info"`
	}
	require.NoError(t, common.UnmarshalJsonStr(logs[0].Other, &decoded))
	require.Contains(t, decoded.AdminInfo, "quota_saturation")
	assert.Equal(t, 2147483647, decoded.AdminInfo["quota_saturation"].Clamped)

	// The same row also has to survive the user-facing formatter, which strips
	// admin_info by design: the marker must be admin-only, not public.
	userVisible := NewLogOther()
	userVisible.SetPublic("model_ratio", 1)
	userVisible.SetAdmin("quota_saturation", map[string]any{"op": "mul"})
	assert.NotContains(t, formatLogOtherJSON(userVisible.JSONString(), logOtherVisibilityUser), "quota_saturation")
}

// TestLogBufferFlushesWholeBufferOnShutdown covers the graceful shutdown path at
// a size the buffer actually holds: it keeps several batches, so a shutdown that
// flushed only the first one would lose everything past it. The 40-row case
// below fits in a single batch and therefore cannot reach that boundary.
func TestLogBufferFlushesWholeBufferOnShutdown(t *testing.T) {
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "60000")

	recorder := &sqlRecorder{}
	db := openClickHouseLogTestDB(t, recorder)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	resetLogBuffer(t)

	// A buffer holding more than one batch is what a shutdown finds when the log
	// database was refusing writes, or when a burst outran the flush loop. Rows
	// are enqueued before the loop starts here so the case is deterministic:
	// otherwise the wake-up at logFlushBatchSize pending rows drains the buffer
	// again before StopLogFlush is reached.
	const rows = 5000
	require.Greater(t, rows, logFlushBatchSize, "the case must span more than one batch")
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), "rid-shutdown-drain")))
	}
	require.Len(t, logBufferRows, rows)

	startLogFlushForTest(t)
	StopLogFlush()

	var stored int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", "rid-shutdown-drain").Scan(&stored).Error)
	t.Logf("shutdown drain: enqueued=%d stored=%d buffered=%d dropped=%d", rows, stored, len(logBufferRows), logBufferDropped)
	assert.EqualValues(t, rows, stored, "a graceful shutdown must drain the whole buffer, not just the first batch")
	assert.Equal(t, 0, len(logBufferRows), "the buffer must be empty once the shutdown drain is done")
	assert.Zero(t, logBufferDropped)
}

// TestLogBufferReportsRowsItCannotFlushOnShutdown covers the other half of the
// shutdown contract: rows the final drain cannot write are reported at error
// level instead of vanishing with the process.
func TestLogBufferReportsRowsItCannotFlushOnShutdown(t *testing.T) {
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "60000")

	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	resetLogBuffer(t)
	startLogFlushForTest(t)

	// Below logFlushBatchSize, so no wake-up reaches the flush loop and the
	// shutdown drain is the only writer.
	const rows = 1500
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), "rid-shutdown-lost")))
	}
	output := captureSysErrors(t)
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)

	StopLogFlush()

	assert.Contains(t, output.String(), fmt.Sprintf("shutting down with %d buffered log rows that could not be written", rows),
		"rows the shutdown drain cannot write must be reported, not silently dropped")
	assert.Len(t, logBufferRows, rows, "the refused batch must be retained until the process exits")
	assert.Zero(t, logBufferDropped, "a batch that is retained is not a dropped row")
}

// TestLogBufferStartLogFlushIsIdempotent covers the lifecycle guard: starting
// the loop twice must be a no-op, not a second goroutine that closes an already
// closed channel. A panic there happens in a background goroutine, which no
// caller can recover, so it takes the process down.
func TestLogBufferStartLogFlushIsIdempotent(t *testing.T) {
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "60000")

	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	resetLogBuffer(t)
	startLogFlushForTest(t)

	StartLogFlush()

	const rows = 40
	for i := range rows {
		require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), "rid-double-start")))
	}
	StopLogFlush()

	var stored int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", "rid-double-start").Scan(&stored).Error)
	assert.EqualValues(t, rows, stored, "the one flush loop must still write every row")
}

// TestLogBufferReportsEveryDroppedBatch asserts the number of reports, not only
// the counter: rows abandoned after repeated flush failures are dropped a whole
// batch at a time, and the report has to keep firing for them.
func TestLogBufferReportsEveryDroppedBatch(t *testing.T) {
	resetLogBuffer(t)
	output := captureSysErrors(t)

	// The first drop is always reported.
	addDroppedLogRows(1)
	require.Equal(t, 1, strings.Count(output.String(), "log rows dropped so far"))

	// An abandoned batch drops logFlushBatchSize rows at once. Five of them take
	// the count to 10001, crossing logDropReportEvery once per batch.
	for range 5 {
		addDroppedLogRows(logFlushBatchSize)
	}
	reports := strings.Count(output.String(), "log rows dropped so far")
	t.Logf("dropped=%d reports=%d", logBufferDropped, reports)
	assert.EqualValues(t, 1+5*logFlushBatchSize, logBufferDropped)
	assert.Equal(t, 6, reports, "every dropped batch after the first must be reported")
}
