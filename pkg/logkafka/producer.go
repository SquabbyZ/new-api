package logkafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// reportEvery rate-limits the transport's reports the same way the in-memory
// buffer rate-limits its drop report: the first failure is always reported,
// then one report per this many further failures. A broker that is down fails
// every row, and one line per row would drown the log it is trying to describe.
const reportEvery = 1000

// Producer is an asynchronous Kafka producer for encoded log rows.
//
// Send never blocks. When the broker cannot keep up, the row is dropped,
// counted and reported rather than making a request wait, which is the same
// trade the in-memory buffer makes when it is full: the request path must not
// be paced by the log store.
//
// The temporary contract of this slice is drop-and-report. The spool slice
// replaces the drop with a write to KAFKA_LOG_SPOOL_DIR; until it lands, this is
// the same level of guarantee the buffer gives when the log database refuses
// writes for good, so it is not a regression. Do not read it as the final
// design.
type Producer struct {
	client       *kgo.Client
	topic        string
	flushTimeout time.Duration

	dropped   atomic.Int64
	failed    atomic.Int64
	reported  atomic.Int64
	closed    atomic.Bool
	closeOnce sync.Once
}

// NewProducer builds the producer. It does not contact the broker: an
// unreachable broker must not stop startup, so a bad address shows up later as
// delivery failures, which are reported rather than hidden.
func NewProducer(cfg Config) (*Producer, error) {
	client, err := kgo.NewClient(producerOpts(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create the kafka log producer for %v: %w", cfg.Brokers, err)
	}
	return &Producer{client: client, topic: cfg.Topic, flushTimeout: cfg.RequestTimeout}, nil
}

// producerOpts maps the configuration onto the client. The settings the design
// pins are constants rather than variables: acks=all plus idempotent production
// is what makes a retry safe to repeat, and there is no supported reason to run
// the log transport without them.
func producerOpts(cfg Config) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.MaxVersions(requestVersions),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// franz-go produces idempotently by default; DisableIdempotentWrite is
		// deliberately not called. That removes the duplicates a retry would
		// otherwise create between the producer and the broker.
		kgo.RequestRetries(math.MaxInt32),
		// A constant backoff is what retry.backoff.ms names. The design only
		// requires it to be at least 200 ms; RecordDeliveryTimeout is what
		// bounds the total, so an unbounded retry count cannot retry forever.
		kgo.RetryBackoffFn(func(int) time.Duration { return cfg.RetryBackoff }),
		kgo.ProducerLinger(cfg.Linger),
		kgo.MaxBufferedBytes(cfg.MaxBufferBytes),
		kgo.ProduceRequestTimeout(cfg.RequestTimeout),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
	}
}

// Send hands one encoded row to the broker without blocking on it. A row the
// client cannot buffer -- its buffer is full because the broker is slow or
// unreachable -- is dropped, counted and reported.
func (p *Producer) Send(value []byte) {
	if p.closed.Load() {
		p.dropped.Add(1)
		p.report("the kafka log producer is closed, so the row was dropped")
		return
	}
	// TryProduce is the non-blocking half of Produce: at the buffer bound it
	// fails the record immediately instead of waiting for space, so this cannot
	// turn into the request path waiting on the broker.
	p.client.TryProduce(context.Background(), &kgo.Record{Topic: p.topic, Value: value}, p.delivered)
}

// delivered is the per-record delivery callback. Every failure path ends in a
// counted, reported drop; nothing here is silent.
func (p *Producer) delivered(_ *kgo.Record, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, kgo.ErrMaxBuffered) {
		p.dropped.Add(1)
		p.report("the kafka log producer buffer is full (KAFKA_LOG_MAX_BUFFER_BYTES), so the row was dropped; the broker is not keeping up")
		return
	}
	p.failed.Add(1)
	p.report(fmt.Sprintf("the kafka log producer could not deliver a row to topic %q: %s", p.topic, describeDeliveryError(p.topic, err)))
}

// Close flushes what the client still holds and stops it, and is safe to call
// more than once.
//
// The flush is bounded by request.timeout.ms rather than by the delivery
// timeout, so a shutdown that has to wait for an unreachable broker still
// returns inside the process shutdown budget. Rows the flush does not place are
// failed by the close and reported through delivered, so a shutdown never
// discards rows silently.
func (p *Producer) Close() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), p.flushTimeout)
		defer cancel()
		p.client.Flush(ctx)
		p.client.Close()
		if dropped, failed := p.dropped.Load(), p.failed.Load(); dropped > 0 || failed > 0 {
			common.SysLog(fmt.Sprintf("kafka log producer stopped: %d row(s) dropped, %d row(s) failed to deliver", dropped, failed))
		}
	})
}

// report emits one rate-limited line carrying the running totals, so an
// operator can tell "one row was too large" from "the broker has been down for
// an hour" without a metrics endpoint.
func (p *Producer) report(why string) {
	total := p.dropped.Load() + p.failed.Load()
	if !shouldReport(&p.reported, total) {
		return
	}
	common.SysError(fmt.Sprintf("%s (topic %q, %d row(s) dropped, %d row(s) failed so far)", why, p.topic, p.dropped.Load(), p.failed.Load()))
}

// shouldReport applies the "first failure always, then one per reportEvery"
// rule to a running total. The step is the increase since the last report, not
// the absolute total: a whole batch can arrive at once, and an absolute total
// would then sit on the same remainder of every threshold forever and silence
// every report after the first.
func shouldReport(reported *atomic.Int64, total int64) bool {
	last := reported.Load()
	if last != 0 && total-last < reportEvery {
		return false
	}
	reported.Store(total)
	return true
}

// describeDeliveryError turns a broker rejection into something an operator can
// act on. An unknown topic is the one failure with a specific remedy, and the
// application deliberately does not create topics, so it has to say what to
// create instead.
func describeDeliveryError(topic string, err error) string {
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return "the topic does not exist and this application does not create topics; create it before starting, e.g. " + createTopicRemedy(topic)
	}
	return err.Error()
}
