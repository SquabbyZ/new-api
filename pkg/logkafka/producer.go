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
// Send never blocks. A row the client cannot buffer, and a row the client gives
// up on, is written to the spool on the disk instead of being dropped, so the
// bound on the producer's buffer is no longer a reason for a row to disappear.
// The request path's whole cost is one atomic read plus a non-blocking send on a
// channel: the disk is a fallback for the failure path, not a second write path.
type Producer struct {
	client       *kgo.Client
	topic        string
	flushTimeout time.Duration
	spool        *Spool

	// inFlight is the bytes handed to the client and not yet reported back by a
	// delivery callback. It is the single measurement behind both the hysteresis
	// band Send branches on and the replay health criterion, because two
	// measurements of the same thing drift apart and then disagree about what
	// "keeping up" means.
	inFlight  atomic.Int64
	highWater int64
	lowWater  int64

	failed    atomic.Int64
	spooled   atomic.Int64
	reported  atomic.Int64
	closed    atomic.Bool
	closeOnce sync.Once
}

// NewProducer builds the producer and starts the disk fallback it writes to.
//
// It does not contact the broker: an unreachable broker must not stop startup,
// so a bad address shows up later as delivery failures, which land in the spool.
// An unwritable spool directory is the opposite case and does stop startup,
// because it is a deterministic misconfiguration whose cost is only paid during
// an outage.
func NewProducer(cfg Config) (*Producer, error) {
	spool, err := NewSpool(cfg)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(producerOpts(cfg)...)
	if err != nil {
		spool.Close()
		return nil, fmt.Errorf("failed to create the kafka log producer for %v: %w", cfg.Brokers, err)
	}
	limit := int64(max(cfg.MaxBufferBytes, 1))
	p := &Producer{
		client:       client,
		topic:        cfg.Topic,
		flushTimeout: cfg.RequestTimeout,
		spool:        spool,
		highWater:    limit * 9 / 10,
		lowWater:     limit / 2,
	}
	spool.StartReplay(context.Background(), p.deliverReplay, p.Healthy)
	return p, nil
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
// client cannot take -- its buffer is full because the broker is slow or
// unreachable, or the edge of the buffer was reached -- is written to the spool
// instead, so a full buffer is no longer a reason for a row to be lost.
func (p *Producer) Send(value []byte) {
	if p.closed.Load() {
		// The window this covers is the shutdown one: the spool is still open
		// while the client flushes, which is the whole reason it is closed after
		// the client rather than before it.
		p.spool.Stash(value)
		return
	}
	if p.inFlight.Load() >= p.highWater {
		// The band, not the bound: past this point the partition is already full
		// and the next row would fail its way to the disk after a round trip
		// anyway, so it goes straight there. The band is what keeps the producer
		// from oscillating between "buffer full" and "buffer empty" while the
		// broker is only just keeping up.
		p.spooled.Add(1)
		p.spool.Stash(value)
		p.report("the kafka log producer is at 90% of KAFKA_LOG_MAX_BUFFER_BYTES, so rows are written to the spool until it drains below 50%")
		return
	}
	p.inFlight.Add(int64(len(value)))
	// TryProduce is the non-blocking half of Produce: at the buffer bound it
	// fails the record immediately instead of waiting for space, so this cannot
	// turn into the request path waiting on the broker.
	p.client.TryProduce(context.Background(), &kgo.Record{Topic: p.topic, Value: value}, p.delivered)
}

// Healthy reports whether the producer is keeping up. The threshold is the lower
// edge of the band Send uses, over the same measurement, so "the producer has
// recovered" and "the producer has room again" are the same statement.
func (p *Producer) Healthy() bool {
	return p.inFlight.Load() <= p.lowWater
}

// delivered is the per-record delivery callback of the request path, and it is
// the only entry in the package that writes to the disk.
func (p *Producer) delivered(record *kgo.Record, err error) {
	p.inFlight.Add(-int64(len(record.Value)))
	if err == nil {
		return
	}
	p.spooled.Add(1)
	p.spool.Stash(record.Value)
	if errors.Is(err, kgo.ErrMaxBuffered) {
		p.report("the kafka log producer buffer is full (KAFKA_LOG_MAX_BUFFER_BYTES) and the broker is not keeping up, so the row was written to the spool instead of dropped")
		return
	}
	p.failed.Add(1)
	p.report(fmt.Sprintf("the kafka log producer could not deliver a row to topic %q, so it was written to the spool instead: %s", p.topic, describeDeliveryError(p.topic, err)))
}

// deliverReplay is the replay path's own way into the same client, and it is a
// separate entry on purpose.
//
// If a replayed row that failed to deliver reached the callback above, it would
// be written back to the spool, and a broker that stays down would then replay a
// segment, fail, write every row out again next to the original, and finally
// deliver both copies when the broker returns: a live lock with unbounded
// duplication. Keeping the two entries apart makes that impossible by
// construction rather than by a flag somebody has to remember to check. This one
// reports the result and nothing else.
func (p *Producer) deliverReplay(value []byte, done func(error)) {
	p.inFlight.Add(int64(len(value)))
	p.client.TryProduce(context.Background(), &kgo.Record{Topic: p.topic, Value: value}, func(record *kgo.Record, err error) {
		p.inFlight.Add(-int64(len(record.Value)))
		done(err)
	})
}

// Close stops the replay, flushes and closes the client, and only then closes
// the spool. It is safe to call more than once.
//
// The last two steps are in that order for a reason that is easy to lose: a
// flush that cannot place its rows fails them through delivered, which writes
// them to the spool. Closing the spool first would leave exactly those rows --
// the ones a shutdown could not deliver -- with nowhere to go.
//
// The flush is bounded by request.timeout.ms rather than by the delivery
// timeout, so a shutdown that has to wait for an unreachable broker still
// returns inside the process shutdown budget.
func (p *Producer) Close() {
	p.closeOnce.Do(func() {
		// The replay shares this client, so it has to be off it before the
		// client goes away.
		p.spool.StopReplay()

		p.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), p.flushTimeout)
		defer cancel()
		p.client.Flush(ctx)
		p.client.Close()

		p.spool.Close()

		if failed, spooled := p.failed.Load(), p.spooled.Load(); failed > 0 || spooled > 0 {
			common.SysLog(fmt.Sprintf("kafka log producer stopped: %d row(s) written to the spool, %d row(s) failed to deliver", spooled, failed))
		}
	})
}

// report emits one rate-limited line carrying the running totals, so an
// operator can tell "one row was too large" from "the broker has been down for
// an hour" without a metrics endpoint. Writes to the spool are part of the total
// so the first one is always reported and the thousandth is not.
func (p *Producer) report(why string) {
	total := p.failed.Load() + p.spooled.Load()
	if !shouldReport(&p.reported, total) {
		return
	}
	common.SysError(fmt.Sprintf("%s (topic %q, %d row(s) written to the spool, %d row(s) failed so far)", why, p.topic, p.spooled.Load(), p.failed.Load()))
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
