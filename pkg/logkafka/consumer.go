package logkafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Consumer drains the log topic and writes rows to the log database in batches.
//
// It commits an offset only after the batch containing it has been written, so
// a database outage leaves the rows in the topic instead of marking them
// consumed. That ordering is the guarantee this path exists for and is the
// reason auto-commit is off.
type Consumer struct {
	client         *kgo.Client
	topic          string
	groupID        string
	retentionHours int
	batchSize      int
	flushInterval  time.Duration
	requestTimeout time.Duration
	handle         func(rows [][]byte) error

	// pending is the batch that has been fetched and not yet written. It is
	// touched only by Run, so it needs no lock.
	pending []*kgo.Record

	// lastConsumedAt is, per partition, the timestamp of the newest record this
	// consumer has written. It is the process's half of the time lag: the other
	// half is the log-end timestamp the broker poll returns, and subtracting
	// them is the only way to get seconds, which no offset arithmetic yields.
	// Run writes it and the metrics collector reads it, so it is behind a
	// mutex; a partition with no entry has never been consumed and is reported
	// as absent rather than as zero lag.
	lastConsumedMu sync.Mutex
	lastConsumedAt map[int32]time.Time

	written      atomic.Int64
	writeFailed  atomic.Int64
	commitFailed atomic.Int64
	reported     atomic.Int64
	missingTopic atomic.Bool
	closeOnce    sync.Once
}

// NewConsumer builds the consumer. handle receives each batch of payloads and
// returns an error when the batch was not written; the consumer then keeps the
// batch and leaves its offsets uncommitted.
func NewConsumer(cfg Config, handle func(rows [][]byte) error) (*Consumer, error) {
	client, err := kgo.NewClient(consumerOpts(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create the kafka log consumer for %v: %w", cfg.Brokers, err)
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		// time.NewTicker panics on a non-positive period, and a panic in a
		// background goroutine takes the process down with it.
		flushInterval = time.Second
	}
	return &Consumer{
		client:         client,
		topic:          cfg.Topic,
		groupID:        cfg.GroupID,
		retentionHours: cfg.RetentionHours,
		batchSize:      cfg.ConsumerBatchSize,
		flushInterval:  flushInterval,
		requestTimeout: cfg.RequestTimeout,
		handle:         handle,
		lastConsumedAt: make(map[int32]time.Time),
	}, nil
}

// consumerOpts maps the configuration onto the client.
func consumerOpts(cfg Config) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.MaxVersions(requestVersions),
		// The group id is a shared constant by design. Adding a host or node
		// name would turn every node into its own group, and every node would
		// then consume every partition, multiplying duplicate rows by the node
		// count. A single-node development setup cannot expose that mistake.
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		// auto.offset.reset=earliest: a group with no committed offset starts
		// from the beginning of the topic rather than skipping what is already
		// there.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// enable.auto.commit=false. With auto-commit on, a batch would be
		// marked consumed while the database is down, and recovery would have
		// nothing left to replay.
		kgo.DisableAutoCommit(),
	}
}

// Run consumes until ctx is done. It is one goroutine consuming in offset
// order: the order is what lets the acceptance check assert that ClickHouse
// received rows in the order the topic holds them.
func (c *Consumer) Run(ctx context.Context) {
	// One metadata lookup before consuming. A topic that does not exist is the
	// only startup condition with a specific remedy and no other symptom: this
	// application never creates topics, so without the lookup the consumer just
	// drains nothing, which an operator reads as a hang rather than as a
	// missing topic.
	c.checkTopic(ctx)

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		if len(c.pending) >= c.batchSize {
			// The batch is full and the log database is not accepting it.
			// Retry this batch instead of fetching more, so an outage cannot
			// grow the batch without bound; waiting the flush interval keeps
			// the retry from becoming a spin.
			if !c.flush(ctx) {
				sleepUntil(ctx, c.flushInterval)
			}
			continue
		}

		// The poll deadline is the flush interval: a poll that returns nothing
		// still falls through to the flush below, so a partial batch is written
		// on time instead of waiting for more traffic to arrive.
		pollCtx, cancel := context.WithTimeout(ctx, c.flushInterval)
		fetches := c.client.PollRecords(pollCtx, c.batchSize-len(c.pending))
		cancel()
		if ctx.Err() != nil {
			// Shutting down. Rows already fetched stay uncommitted, so they are
			// delivered again rather than lost.
			return
		}
		// The fake fetch that carries this poll's expired deadline is how the
		// poll returns on time, so it is filtered out of the reports.
		fetches.EachError(c.reportFetchError)

		c.pending = append(c.pending, fetches.Records()...)
		if len(c.pending) >= c.batchSize {
			c.flush(ctx)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flush(ctx)
		}
	}
}

// flush writes the pending batch and then commits it, in that order. Committing
// first would mark a batch consumed that an outage kept out of the table, and
// recovery would have nothing to replay, which is exactly the guarantee this
// path exists to provide. It reports whether the batch was written.
func (c *Consumer) flush(ctx context.Context) bool {
	if len(c.pending) == 0 {
		return true
	}

	rows := make([][]byte, len(c.pending))
	for i, record := range c.pending {
		rows[i] = record.Value
	}
	if err := c.handle(rows); err != nil {
		c.writeFailed.Add(1)
		c.report(fmt.Sprintf("the kafka log consumer could not write %d row(s) to the log database, so their offsets are not committed and the batch will be retried: %s", len(rows), err.Error()))
		return false
	}
	c.written.Add(int64(len(rows)))
	c.recordConsumed(c.pending)

	if err := c.client.CommitRecords(ctx, c.pending...); err != nil {
		// The rows are already in the database, so a failed commit only means
		// they are delivered again. That is the at-least-once cost this design
		// accepts; it is never a lost row.
		c.commitFailed.Add(1)
		c.report(fmt.Sprintf("the kafka log consumer wrote %d row(s) but could not commit their offsets, so they will be delivered again: %s", len(rows), err.Error()))
	}
	c.pending = c.pending[:0]
	return true
}

// Close stops the client and is safe to call more than once. It must follow the
// end of Run: closing the client while Run is polling would end the poll in the
// same breath, and the caller is the one that owns the run context.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.client.Close()
		if written, failed, commits := c.written.Load(), c.writeFailed.Load(), c.commitFailed.Load(); written > 0 || failed > 0 || commits > 0 {
			common.SysLog(fmt.Sprintf("kafka log consumer stopped: %d row(s) written, %d batch write failure(s), %d offset commit failure(s)", written, failed, commits))
		}
	})
}

// recordConsumed keeps the newest record timestamp seen per partition. It runs
// on the Run goroutine, and the metrics collector reads the map through
// lastConsumed for the time lag, so both sides take the mutex.
func (c *Consumer) recordConsumed(records []*kgo.Record) {
	c.lastConsumedMu.Lock()
	defer c.lastConsumedMu.Unlock()
	for _, record := range records {
		if previous, ok := c.lastConsumedAt[record.Partition]; !ok || record.Timestamp.After(previous) {
			c.lastConsumedAt[record.Partition] = record.Timestamp
		}
	}
}

// lastConsumed copies the per-partition newest-consumed timestamps. A partition
// with no entry has never been consumed by this process.
func (c *Consumer) lastConsumed() map[int32]time.Time {
	c.lastConsumedMu.Lock()
	defer c.lastConsumedMu.Unlock()
	out := make(map[int32]time.Time, len(c.lastConsumedAt))
	maps.Copy(out, c.lastConsumedAt)
	return out
}

// ConsumerSnapshot is a read-only view of the consumer's counters. WrittenRows
// is deliberately not a metric of its own -- stashedRows and the lag pair cover
// the same events -- but it is what proves, in a test, that a drained topic
// really passed through this consumer.
type ConsumerSnapshot struct {
	WrittenRows  int64
	WriteFailed  int64
	CommitFailed int64
}

// Snapshot reads the consumer's counters. It is safe to call from any goroutine
// while Run is consuming.
func (c *Consumer) Snapshot() ConsumerSnapshot {
	return ConsumerSnapshot{
		WrittenRows:  c.written.Load(),
		WriteFailed:  c.writeFailed.Load(),
		CommitFailed: c.commitFailed.Load(),
	}
}

// reportFetchError surfaces one partition error. A topic that does not exist
// gets its remedy spelled out, once: this application never creates topics, so
// the operator is the only one who can.
func (c *Consumer) reportFetchError(_ string, _ int32, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		c.reportMissingTopic()
		return
	}
	c.report(fmt.Sprintf("the kafka log consumer failed to fetch topic %q: %s", c.topic, err.Error()))
}

// checkTopic asks the broker about the one topic this consumer drains, and
// reports the missing-topic remedy if the broker does not have it. A broker
// that is merely unreachable is not reported here: that is a declared
// non-failure, and the poll loop reports it once it has something to say.
func (c *Consumer) checkTopic(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	name := c.topic
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	response, err := c.client.Request(checkCtx, request)
	if err != nil {
		return
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok {
		return
	}
	for _, described := range metadata.Topics {
		if errors.Is(kerr.ErrorForCode(described.ErrorCode), kerr.UnknownTopicOrPartition) {
			c.reportMissingTopic()
		}
	}
}

// reportMissingTopic says what to create, once. The application deliberately
// does not create topics, so the operator is the only one who can, and the
// partition count is theirs to choose because it caps throughput and is the
// multiplier in the retention sizing.
func (c *Consumer) reportMissingTopic() {
	if c.missingTopic.Swap(true) {
		return
	}
	common.SysError(fmt.Sprintf("the kafka log consumer found no topic %q, and this application does not create topics; create it before the consumer can drain anything, e.g. %s", c.topic, createTopicRemedy(c.topic)))
}

// createTopicRemedy is the one place the create-topic command is written. Both
// the consumer's startup check and the producer's delivery failure end here, so
// the command an operator reads cannot differ between them.
func createTopicRemedy(topic string) string {
	return fmt.Sprintf("kafka-topics.sh --bootstrap-server BROKER:PORT --create --topic %s --partitions NODE_COUNT_OR_MORE --replication-factor 2_OR_3_NEVER_3_OF_3", topic)
}

// report emits one rate-limited line carrying the running totals, using the
// same rule as the producer and the in-memory buffer.
func (c *Consumer) report(why string) {
	total := c.writeFailed.Load() + c.commitFailed.Load()
	if !shouldReport(&c.reported, total) {
		return
	}
	common.SysError(fmt.Sprintf("%s (topic %q, %d row(s) written, %d write failure(s), %d commit failure(s) so far)", why, c.topic, c.written.Load(), c.writeFailed.Load(), c.commitFailed.Load()))
}

// sleepUntil waits for the interval or until the context ends, whichever comes
// first.
func sleepUntil(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
