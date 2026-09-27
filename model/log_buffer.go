package model

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
)

// Consume logs used to reach ClickHouse one INSERT at a time, and every
// MergeTree INSERT writes a new data part. At request rate the table reaches
// parts_to_throw_insert and starts refusing writes, so the insert has to be
// batched somewhere. gorm.io/driver/clickhouse@v0.6.0 cannot do it: its Create
// callback prepares a single-row INSERT and Execs it once per row, so a batch
// has to be built here, in front of the driver.
//
// Buffering is enabled only when the log database is ClickHouse. On SQLite,
// MySQL and PostgreSQL a single-row INSERT is one local round trip that costs a
// request little and creates no parts, so those deployments keep the existing
// synchronous, immediately readable write.
const (
	// logBufferCapacity bounds the rows held in memory (a row is a few hundred
	// bytes to a couple of KB, so the ceiling is single-digit MB per node). It
	// is what keeps a database outage from growing the heap without limit.
	logBufferCapacity = 8192
	// logFlushBatchSize is both the largest single INSERT and the pending count
	// that wakes the flush loop before the interval elapses, so a burst turns
	// into an extra batched INSERT instead of a dropped row.
	logFlushBatchSize = 2000
	// logFlushMaxAttempts bounds the retries of one batch. A refused batch is
	// put back so a transient failure never discards rows, but a batch the
	// server refuses every time would otherwise pin the buffer forever and
	// silently stall every later log, so it is dropped after this many
	// consecutive failures and reported.
	logFlushMaxAttempts = 3
	// logDropReportEvery rate-limits the report of dropped rows: the first drop
	// is always reported, then one report per this many further drops.
	logDropReportEvery = 1000

	logDefaultFlushMs = 1000
	logMinFlushMs     = 100
	logMaxFlushMs     = 60000
)

var (
	logBufferMu      sync.Mutex
	logBufferRows    []*Log
	logBufferDropped int64
	// logDropReported is the drop count as of the last report. The next report is
	// driven by the increase since then rather than by the absolute count: rows
	// are also dropped a whole batch at a time, and a batch-sized step can land
	// on the same remainder of every threshold forever.
	logDropReported int64

	logFlushSignal    = make(chan struct{}, 1)
	logFlushStop      = make(chan struct{})
	logFlushDone      = make(chan struct{})
	logFlushStartOnce sync.Once
	logFlushStopOnce  sync.Once

	// logFlushFailures counts the consecutive failures of the batch currently
	// at the front of the buffer. It is touched only by the flush loop and, for
	// the final flush, by StopLogFlush after the loop has exited.
	logFlushFailures int
)

// logBufferingEnabled reports whether createLog hands rows to the buffer
// instead of inserting them itself. It is the single condition that selects
// both the buffered write path and the batched INSERT that drains it.
func logBufferingEnabled() bool {
	return common.UsingLogDatabase(common.DatabaseTypeClickHouse)
}

// StartLogFlush begins the background flush loop. Call it once, after
// InitLogDB. When the log database is not ClickHouse nothing is ever enqueued
// and the loop only ticks. Starting twice is a no-op rather than a panic, which
// mirrors StopLogFlush: the loop is a process-lifetime singleton, and a stopped
// one is not restarted.
func StartLogFlush() {
	logFlushStartOnce.Do(func() {
		go logFlushLoop()
	})
}

// StopLogFlush stops the flush loop and writes everything still buffered, so a
// graceful shutdown does not discard accepted logs. It must follow
// StartLogFlush, and calling it more than once is a no-op rather than a panic.
// The buffer holds many batches, so the final drain runs until it is empty
// rather than flushing a single batch. The first batch that cannot be written
// ends it: a log database that is refusing writes will not accept the next
// batch either, and retrying only delays the exit. Rows left behind are
// reported at error level; they live only in memory and are lost with the
// process, so shutting down with a non-empty buffer is never silent.
func StopLogFlush() {
	logFlushStopOnce.Do(func() {
		close(logFlushStop)
		<-logFlushDone
	})
	for {
		logBufferMu.Lock()
		remaining := len(logBufferRows)
		logBufferMu.Unlock()
		if remaining == 0 {
			return
		}
		if !flushLogBuffer() {
			common.SysError(fmt.Sprintf("shutting down with %d buffered log rows that could not be written; the log database refused the final write, so these rows are lost with the process", remaining))
			return
		}
	}
}

func logFlushLoop() {
	defer close(logFlushDone)
	ticker := time.NewTicker(logFlushInterval())
	defer ticker.Stop()
	for {
		select {
		case <-logFlushStop:
			return
		case <-ticker.C:
		case <-logFlushSignal:
		}
		flushLogBuffer()
	}
}

// logFlushInterval bounds the log visibility delay: a row is visible no later
// than this interval plus one INSERT after the request that produced it.
func logFlushInterval() time.Duration {
	ms := common.GetEnvOrDefault("LOG_FLUSH_INTERVAL_MS", logDefaultFlushMs)
	ms = max(ms, logMinFlushMs)
	ms = min(ms, logMaxFlushMs)
	return time.Duration(ms) * time.Millisecond
}

// enqueueLog hands one row to the buffer. A full buffer drops the row rather
// than blocking: the request path must never wait on the log database, which is
// the whole point of the buffer. Every drop is counted and reported at error
// level, never silently.
func enqueueLog(log *Log) {
	logBufferMu.Lock()
	if len(logBufferRows) >= logBufferCapacity {
		logBufferDropped++
		dropped := logBufferDropped
		logBufferMu.Unlock()
		reportDroppedLogRows(dropped)
		return
	}
	logBufferRows = append(logBufferRows, log)
	full := len(logBufferRows) >= logFlushBatchSize
	logBufferMu.Unlock()

	if full {
		// Coalescing wake-up: a wake-up already pending covers this burst.
		select {
		case logFlushSignal <- struct{}{}:
		default:
		}
	}
}

// drainLogBuffer takes the oldest rows out of the buffer. The returned slice is
// capped so appending to it can never write into the rows left behind.
func drainLogBuffer() []*Log {
	logBufferMu.Lock()
	defer logBufferMu.Unlock()
	if len(logBufferRows) == 0 {
		return nil
	}
	n := min(len(logBufferRows), logFlushBatchSize)
	batch := logBufferRows[:n:n]
	logBufferRows = logBufferRows[n:]
	return batch
}

// requeueLogBuffer puts a failed batch back in front of the buffer, keeping
// FIFO order. Rows that no longer fit are dropped and counted: the buffer is
// bounded, so a long outage cannot grow it without limit.
func requeueLogBuffer(batch []*Log) {
	logBufferMu.Lock()
	// batch is capped, so this always allocates and never aliases the rows that
	// stayed in the buffer.
	rows := append(batch, logBufferRows...)
	var dropped int64
	if len(rows) > logBufferCapacity {
		dropped = int64(len(rows) - logBufferCapacity)
		rows = rows[:logBufferCapacity]
	}
	logBufferDropped += dropped
	totalDropped := logBufferDropped
	logBufferRows = rows
	logBufferMu.Unlock()

	if dropped > 0 {
		reportDroppedLogRows(totalDropped)
	}
}

func addDroppedLogRows(n int64) {
	logBufferMu.Lock()
	logBufferDropped += n
	total := logBufferDropped
	logBufferMu.Unlock()
	reportDroppedLogRows(total)
}

// reportDroppedLogRows reports the running drop count, at most once per
// logDropReportEvery dropped rows. The threshold is the increase since the last
// report, not the absolute total: a caller can drop a whole batch at once
// (logFlushBatchSize is a multiple of logDropReportEvery), and a batch-sized
// step leaves an absolute total stuck on the same remainder of every threshold,
// which would silence every report after the first. The first drop is always
// reported.
func reportDroppedLogRows(total int64) {
	logBufferMu.Lock()
	report := logDropReported == 0 || total-logDropReported >= logDropReportEvery
	if report {
		logDropReported = total
	}
	logBufferMu.Unlock()
	if !report {
		return
	}
	common.SysError(fmt.Sprintf("log buffer is full (%d rows); %d log rows dropped so far. The log database is not keeping up: raise LOG_FLUSH_INTERVAL_MS only if it can accept larger INSERTs, otherwise check the log database", logBufferCapacity, total))
}

// flushLogBuffer writes one batch and reports whether it was written. A batch
// that fails is put back so a single transient failure never discards rows; a
// batch that keeps failing is dropped after logFlushMaxAttempts and reported,
// so one row the server refuses cannot pin the buffer forever.
func flushLogBuffer() bool {
	batch := drainLogBuffer()
	if len(batch) == 0 {
		return true
	}
	if err := insertLogBatch(batch); err != nil {
		logFlushFailures++
		err = sanitizeDBError(err)
		if logFlushFailures >= logFlushMaxAttempts {
			addDroppedLogRows(int64(len(batch)))
			common.SysError(fmt.Sprintf("dropping %d buffered log rows after %d consecutive flush failures: %s", len(batch), logFlushFailures, err.Error()))
			logFlushFailures = 0
			return false
		}
		requeueLogBuffer(batch)
		common.SysError(fmt.Sprintf("failed to flush %d buffered log rows (attempt %d/%d), rows retained for retry: %s", len(batch), logFlushFailures, logFlushMaxAttempts, err.Error()))
		return false
	}
	logFlushFailures = 0
	return true
}

func insertLogBatch(logs []*Log) error {
	if logBufferingEnabled() {
		return insertClickHouseLogBatch(logs)
	}
	return LOG_DB.Create(&logs).Error
}

// clickHouseLogInsertColumns is the column list of the multi-row INSERT, in
// table order. `group` is a reserved word and is quoted through logGroupCol,
// which initCol sets for the log database dialect.
const clickHouseLogInsertColumns = "id, user_id, created_at, type, content, username, token_name, " +
	"model_name, quota, prompt_tokens, completion_tokens, use_time, is_stream, channel_id, " +
	"token_id, %s, ip, request_id, upstream_request_id, other"

// clickHouseLogInsertRow is one row of values; its length must match
// clickHouseLogInsertColumns.
const clickHouseLogInsertRow = "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

// insertClickHouseLogBatch writes the whole batch as one statement. ClickHouse
// treats one INSERT as one data part, which is what keeps the part count far
// below the row count.
func insertClickHouseLogBatch(logs []*Log) error {
	header := fmt.Sprintf("INSERT INTO logs ("+clickHouseLogInsertColumns+") VALUES ", logGroupCol)
	var query strings.Builder
	query.Grow(len(header) + len(logs)*len(clickHouseLogInsertRow))
	query.WriteString(header)

	args := make([]any, 0, len(logs)*20)
	for i, log := range logs {
		if i > 0 {
			query.WriteString(",")
		}
		query.WriteString(clickHouseLogInsertRow)
		args = append(args,
			log.Id, log.UserId, log.CreatedAt, log.Type, log.Content, log.Username,
			log.TokenName, log.ModelName, log.Quota, log.PromptTokens, log.CompletionTokens,
			log.UseTime, log.IsStream, log.ChannelId, log.TokenId, log.Group, log.Ip,
			log.RequestId, log.UpstreamRequestId, log.Other,
		)
	}
	return LOG_DB.Exec(query.String(), args...).Error
}
