package logkafka

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// spoolRows reads back every row the spool directory holds, in file order. It is
// how a test asks "did this row reach the disk" without depending on the writer
// goroutine's timing.
func spoolRows(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var rows []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		for _, line := range strings.Split(string(data), "\n") {
			if line != "" {
				rows = append(rows, line)
			}
		}
	}
	return rows
}

// captureSysErrors points the process error log at a buffer for the rest of the
// test. Drop and failure reports are the observable behavior under test, and
// common.SysError is where they land.
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

// captureSysLogs does the same for the ordinary log stream, which is where the
// spool reports what it resolved and where it is writing.
func captureSysLogs(t *testing.T) *bytes.Buffer {
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

// silentBroker listens on a real port and never answers. Accepted connections
// stay open and mute, so an implementation that waits for a broker response
// blocks until its own timeout expires.
func silentBroker(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Keep the connection open: closing it would look like a broker
			// that answered and hung up, which is a different failure.
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, conn := range held {
			_ = conn.Close()
		}
		mu.Unlock()
	})
	return listener.Addr().String()
}

func TestLoadConfigIsDisabledWithoutBrokers(t *testing.T) {
	t.Setenv(envBrokers, "")

	cfg, err := LoadConfig()
	require.NoError(t, err, "an unconfigured Kafka is the existing deployment, not a misconfiguration")
	assert.False(t, cfg.Enabled())
	assert.False(t, Configured())
	assert.Empty(t, cfg.Brokers)
	assert.Empty(t, cfg.Topic, "nothing has to be resolved while the transport is off")
}

func TestLoadConfigAppliesTheDocumentedDefaults(t *testing.T) {
	t.Setenv(envBrokers, " 127.0.0.1:9092 , 127.0.0.1:9093 ")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.True(t, cfg.Enabled())
	assert.Equal(t, []string{"127.0.0.1:9092", "127.0.0.1:9093"}, cfg.Brokers, "each entry is trimmed and kept in order")
	assert.Equal(t, "new-api-logs", cfg.Topic)
	assert.Equal(t, "new-api-log-consumer", cfg.GroupID)
	assert.Equal(t, 33554432, cfg.MaxBufferBytes)
	assert.Equal(t, 5*time.Millisecond, cfg.Linger)
	assert.Equal(t, 120000*time.Millisecond, cfg.DeliveryTimeout)
	assert.Equal(t, 30000*time.Millisecond, cfg.RequestTimeout)
	assert.Equal(t, 200*time.Millisecond, cfg.RetryBackoff)
	assert.Equal(t, 48, cfg.RetentionHours)
	assert.Equal(t, 2000, cfg.ConsumerBatchSize)
	// The spool directory must not be os.TempDir(): on Linux that is usually a
	// tmpfs, so the disk fallback would be memory pretending to be disk.
	assert.Equal(t, "new-api-log-spool", cfg.SpoolDir)
}

func TestLoadConfigRejectsMalformedBrokerLists(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"no port", "localhost"},
		{"trailing comma", "127.0.0.1:9092,"},
		{"leading comma", ",127.0.0.1:9092"},
		{"empty element", "127.0.0.1:9092,,127.0.0.1:9093"},
		{"non numeric port", "127.0.0.1:kafka"},
		{"port zero", "127.0.0.1:0"},
		{"port out of range", "127.0.0.1:70000"},
		{"no host", ":9092"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envBrokers, tc.raw)
			cfg, err := LoadConfig()
			require.Error(t, err, "%q must fail startup rather than silently dropping an element", tc.raw)
			assert.False(t, cfg.Enabled())
			assert.Contains(t, err.Error(), envBrokers, "the message has to name the variable the operator has to fix")
		})
	}
}

func TestLoadConfigKeepsNumericValuesUsable(t *testing.T) {
	t.Setenv(envBrokers, "127.0.0.1:9092")
	t.Setenv(envConsumerBatchSize, "-5")
	t.Setenv(envMaxBufferBytes, "-1")
	t.Setenv(envLingerMs, "-1")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 1, cfg.ConsumerBatchSize, "a non-positive batch size would never flush")
	assert.Equal(t, 1, cfg.MaxBufferBytes)
	assert.Zero(t, cfg.Linger, "a negative linger is clamped to zero, which means no waiting")
}

// TestProducerSendDoesNotWaitForTheBroker is the load-bearing property of the
// producer: the request path hands a row over and moves on. Against a broker
// that accepts the connection and never answers, a synchronous send blocks for
// request.timeout.ms. The gap between the assertion and that timeout is what
// makes this check discriminate rather than measure.
func TestProducerSendDoesNotWaitForTheBroker(t *testing.T) {
	const requestTimeout = 5 * time.Second

	cfg := Config{
		Brokers:         []string{silentBroker(t)},
		Topic:           "new-api-logs",
		MaxBufferBytes:  defaultMaxBufferBytes,
		DeliveryTimeout: 5 * time.Minute,
		RequestTimeout:  requestTimeout,
		RetryBackoff:    200 * time.Millisecond,
		SpoolDir:        t.TempDir(),
	}
	producer, err := NewProducer(cfg)
	require.NoError(t, err, "an unreachable broker must not fail startup")
	t.Cleanup(producer.Close)

	const rows = 2000
	start := time.Now()
	for range rows {
		producer.Send([]byte(`{"id":1,"content":"a log row"}`))
	}
	elapsed := time.Since(start)
	t.Logf("%d sends against a broker that never answers took %s (request.timeout.ms is %s)", rows, elapsed, requestTimeout)

	assert.Less(t, elapsed, requestTimeout/10,
		"accepting a row must not wait for the broker; a synchronous send would take at least request.timeout.ms")
	assert.Zero(t, producer.spool.stashedRows.Load(), "the buffer holds far more than this, so nothing had to reach the disk")
}

// TestProducerWritesRowsItCannotBufferToTheSpool is the half of the contract
// this slice changes: a row the producer's buffer cannot take is no longer a
// drop, it is a write to the spool. The byte bound is deliberately just above
// one row, so the second row is the one that no longer fits.
func TestProducerWritesRowsItCannotBufferToTheSpool(t *testing.T) {
	payload := []byte(`{"id":1,"content":"a log row"}`)
	require.Greater(t, len(payload), 8)

	spoolDir := t.TempDir()
	cfg := Config{
		Brokers:         []string{silentBroker(t)},
		Topic:           "new-api-logs",
		MaxBufferBytes:  len(payload) + 8,
		DeliveryTimeout: 5 * time.Minute,
		RequestTimeout:  200 * time.Millisecond,
		RetryBackoff:    200 * time.Millisecond,
		SpoolDir:        spoolDir,
	}
	producer, err := NewProducer(cfg)
	require.NoError(t, err)

	output := captureSysErrors(t)
	for range 10 {
		producer.Send(payload)
	}
	// Closing flushes, fails what it could not place, drains the spool queue and
	// seals the segment, so when it returns every accepted row is on the disk.
	// The assertion is then about what was written, not about how long to wait.
	producer.Close()

	rows := spoolRows(t, spoolDir)
	assert.Len(t, rows, 10, "a row the producer buffer could not take has to reach the disk instead of being dropped")
	for _, row := range rows {
		assert.Equal(t, string(payload), row,
			"the spool stores the payload the producer would have sent, byte for byte; re-encoding it would deform anything a caller encoded")
	}
	assert.Contains(t, output.String(), "KAFKA_LOG_MAX_BUFFER_BYTES", "the report has to say which bound was hit")
}

// TestProducerSendAfterCloseIsStillCountedAndReported covers the shutdown race:
// a row offered after the spool has closed has nowhere left to go, and that is
// counted and reported rather than silently discarded.
func TestProducerSendAfterCloseIsStillCountedAndReported(t *testing.T) {
	cfg := Config{
		Brokers:         []string{silentBroker(t)},
		Topic:           "new-api-logs",
		MaxBufferBytes:  defaultMaxBufferBytes,
		DeliveryTimeout: 5 * time.Minute,
		RequestTimeout:  200 * time.Millisecond,
		RetryBackoff:    200 * time.Millisecond,
		SpoolDir:        t.TempDir(),
	}
	producer, err := NewProducer(cfg)
	require.NoError(t, err)

	output := captureSysErrors(t)
	producer.Close()
	producer.Close()
	producer.Send([]byte(`{"id":1}`))

	assert.EqualValues(t, 1, producer.spool.droppedRows.Load())
	assert.Contains(t, output.String(), "spool is already closed")
}

// TestConsumerKeepsTheBatchWhenTheWriteFails is the ordering guard for the
// consumer's contract: a batch the log database refused stays pending, so its
// offsets are never committed and recovery still has something to replay.
//
// The consumer here has no client at all, which is what makes the assertion
// discriminate: committing before writing would reach the nil client and panic
// before the assertions could pass, so this cannot be satisfied by an
// implementation that reorders the two steps.
func TestConsumerKeepsTheBatchWhenTheWriteFails(t *testing.T) {
	consumer := &Consumer{
		topic:  "new-api-logs",
		handle: func([][]byte) error { return errors.New("the log database refused the insert") },
		pending: []*kgo.Record{
			{Topic: "new-api-logs", Value: []byte(`{"id":1}`)},
			{Topic: "new-api-logs", Value: []byte(`{"id":2}`)},
		},
	}
	output := captureSysErrors(t)

	require.False(t, consumer.flush(context.Background()),
		"a batch the database refused must not be reported as written")
	assert.Len(t, consumer.pending, 2, "the refused batch has to stay pending, because its offsets must not be committed")
	assert.Zero(t, consumer.written.Load())
	assert.EqualValues(t, 1, consumer.writeFailed.Load())
	assert.Contains(t, output.String(), "their offsets are not committed")
}

func TestConsumerStopsWhenItsContextIsCancelled(t *testing.T) {
	cfg := Config{Brokers: []string{silentBroker(t)}, Topic: "new-api-logs", GroupID: "g", FlushInterval: 200 * time.Millisecond}
	consumer, err := NewConsumer(cfg, func([][]byte) error { return nil })
	require.NoError(t, err, "an unreachable broker must not fail consumer creation")
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		consumer.Run(ctx)
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Run must return when its context is cancelled; a shutdown blocks on this")
	}
}

func TestNewConsumerSurvivesAZeroFlushInterval(t *testing.T) {
	consumer, err := NewConsumer(Config{Brokers: []string{"127.0.0.1:1"}, Topic: "t", GroupID: "g", FlushInterval: 0}, nil)
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	// time.NewTicker panics on a non-positive period, and that panic would
	// happen inside Run's goroutine, where no caller could recover it.
	require.Greater(t, consumer.flushInterval, time.Duration(0))
}

func TestDescribeDeliveryErrorNamesTheMissingTopicRemedy(t *testing.T) {
	message := describeDeliveryError("new-api-logs", kerr.UnknownTopicOrPartition)
	assert.Contains(t, message, "kafka-topics.sh", "the operator needs the command, not the error code")
	assert.Contains(t, message, "does not create topics")

	assert.Equal(t, "boom", describeDeliveryError("t", errors.New("boom")), "any other error is passed through")
}

func TestShouldReportAlwaysReportsTheFirstAndThenSteps(t *testing.T) {
	var reported atomic.Int64
	assert.True(t, shouldReport(&reported, 1), "the first failure is always reported")
	assert.False(t, shouldReport(&reported, 2))
	assert.False(t, shouldReport(&reported, reportEvery))
	assert.True(t, shouldReport(&reported, reportEvery+1))
	assert.False(t, shouldReport(&reported, reportEvery+2))
}
