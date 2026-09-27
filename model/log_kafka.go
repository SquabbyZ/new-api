package model

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/logkafka"
)

// The Kafka log transport is wired here rather than inside the transport
// package because this is the package that owns the row type and the batched
// INSERT's column order. The transport moves opaque payloads, so it can depend
// on nothing here and there is no import cycle.
var (
	// logKafkaProducer is written once during startup, before the server
	// accepts requests, and read on every log write. The atomic pointer keeps
	// that read race-free.
	logKafkaProducer atomic.Pointer[logkafka.Producer]

	logKafkaConsumer       *logkafka.Consumer
	logKafkaConsumerCancel context.CancelFunc
	logKafkaConsumerDone   chan struct{}

	logKafkaStartOnce sync.Once
	logKafkaStartErr  error
	logKafkaStopOnce  sync.Once

	// logKafkaUndecodableRows counts consumed payloads that could not be
	// decoded. It is kept because such a row is dropped, and a drop that is
	// only ever written to the log cannot be told from a quiet period.
	logKafkaUndecodableRows atomic.Int64
)

// StartLogKafka starts the log transport when KAFKA_BROKERS is configured and
// does nothing when it is not. Call it after InitLogDB, next to StartLogFlush.
//
// A misconfiguration that only exists because Kafka was switched on fails
// startup, because failing is the only way an operator ever sees it. A broker
// that is merely unreachable does not: a missing dependency must not become an
// outage. Rows lost while it is down are counted and reported, never silent.
func StartLogKafka() error {
	logKafkaStartOnce.Do(func() {
		logKafkaStartErr = startLogKafka()
	})
	return logKafkaStartErr
}

func startLogKafka() error {
	cfg, err := logkafka.LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled() {
		return nil
	}
	if !common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		return fmt.Errorf("KAFKA_BROKERS is set but LOG_SQL_DSN does not point at ClickHouse: the Kafka log transport writes ClickHouse's batched INSERT, so it needs the ClickHouse log database")
	}
	// The consumer reuses LOG_FLUSH_INTERVAL_MS rather than adding a second
	// knob, so the clamp that already bounds the buffered path bounds this one.
	cfg.FlushInterval = logFlushInterval()

	if err := startLogKafkaProducer(cfg); err != nil {
		return err
	}
	if err := startLogKafkaConsumer(cfg); err != nil {
		stopLogKafkaProducer()
		return err
	}
	common.SysLog(fmt.Sprintf("kafka log transport enabled: brokers=%v topic=%s group=%s linger=%s delivery_timeout=%s request_timeout=%s retry_backoff=%s max_buffer_bytes=%d consumer_batch_size=%d flush_interval=%s",
		cfg.Brokers, cfg.Topic, cfg.GroupID, cfg.Linger, cfg.DeliveryTimeout, cfg.RequestTimeout, cfg.RetryBackoff, cfg.MaxBufferBytes, cfg.ConsumerBatchSize, cfg.FlushInterval))
	// These two are declarations, not client settings: the application does not
	// create the topic or write a spool yet, so an operator reading them back
	// has to be told they are what to configure, not what this process does.
	common.SysLog(fmt.Sprintf("kafka log transport declarations (this process does not act on them): KAFKA_LOG_RETENTION_HOURS=%d is the retention the topic should carry, KAFKA_LOG_SPOOL_DIR=%s is where the disk fallback will be written",
		cfg.RetentionHours, cfg.SpoolDir))
	return nil
}

func startLogKafkaProducer(cfg logkafka.Config) error {
	producer, err := logkafka.NewProducer(cfg)
	if err != nil {
		return err
	}
	logKafkaProducer.Store(producer)
	return nil
}

func startLogKafkaConsumer(cfg logkafka.Config) error {
	consumer, err := logkafka.NewConsumer(cfg, handleKafkaLogBatch)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	logKafkaConsumer = consumer
	logKafkaConsumerCancel = cancel
	logKafkaConsumerDone = done
	go func() {
		defer close(done)
		consumer.Run(ctx)
	}()
	return nil
}

// StopLogKafka closes the producer and then stops the consumer. The order is
// the contract: closing the producer is what puts the last accepted rows into
// the topic, and the consumer has to still be running to see them.
//
// It must run before model.CloseDB, because the consumer writes through the log
// database handle. Call it once; a second call is a no-op.
func StopLogKafka() {
	logKafkaStopOnce.Do(func() {
		stopLogKafkaProducer()
		stopLogKafkaConsumer()
	})
}

func stopLogKafkaProducer() {
	if producer := logKafkaProducer.Load(); producer != nil {
		producer.Close()
	}
}

func stopLogKafkaConsumer() {
	if logKafkaConsumer == nil {
		return
	}
	logKafkaConsumerCancel()
	<-logKafkaConsumerDone
	logKafkaConsumer.Close()
}

// sendLogToKafka encodes one row and hands it to the producer without waiting
// for the broker, so a slow or unreachable broker cannot slow a request down.
// Delivery failures are counted and reported by the producer rather than
// returned here: accepting the row must not depend on the log store, which is
// the same contract the buffered path has.
func sendLogToKafka(log *Log) {
	payload, err := common.Marshal(log)
	if err != nil {
		// A row of strings and integers cannot fail to encode; reporting it is
		// still better than dropping it quietly.
		common.SysError(fmt.Sprintf("failed to encode a log row for kafka, so it was dropped: %s", err.Error()))
		return
	}
	producer := logKafkaProducer.Load()
	if producer == nil {
		common.SysError("KAFKA_BROKERS is set but the kafka log producer was never started, so the log row was dropped; StartLogKafka has to run during startup")
		return
	}
	producer.Send(payload)
}

// handleKafkaLogBatch turns one consumed batch into one ClickHouse INSERT. It
// reuses insertClickHouseLogBatch, so the column order has a single definition
// and cannot drift away from the buffered path's.
func handleKafkaLogBatch(rows [][]byte) error {
	logs := make([]*Log, 0, len(rows))
	undecodable := 0
	for _, row := range rows {
		var log Log
		if err := common.Unmarshal(row, &log); err != nil {
			// A payload this application encoded but cannot decode is corrupt
			// or from an incompatible version. Failing the batch would retry it
			// forever and stall every later log behind it, so the row is
			// dropped and counted instead and the rest of the batch is written.
			undecodable++
			continue
		}
		logs = append(logs, &log)
	}
	if undecodable > 0 {
		common.SysError(fmt.Sprintf("dropped %d kafka log row(s) that could not be decoded (%d so far)", undecodable, logKafkaUndecodableRows.Add(int64(undecodable))))
	}
	if len(logs) == 0 {
		return nil
	}
	return sanitizeDBError(insertClickHouseLogBatch(logs))
}
