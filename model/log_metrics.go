package model

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/logkafka"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The log pipeline's metrics endpoint.
//
// It is a separate listener on purpose, and it is off by default. This
// deployment is a gateway that faces the internet, and the series below amount
// to a map of where the billing and audit pipeline is weakest: the topic's
// partition topology, how far behind the consumer is and how fast that is
// growing, and how much of the write path has fallen back to disk. Serving that
// on the API port would put operational detail on the same surface as user
// traffic.
//
// The security boundary is therefore the listen address itself, proved by three
// properties: the endpoint is not on the API port, it does not listen unless
// METRICS_ADDR names an address, and a malformed address fails startup rather
// than degrading to every interface. There is no HTTP authentication here, and
// that is not a bypass: this listener is not on the gin engine at all, so
// middleware.AdminAuth (a browser session or access token) and
// middleware.RequirePermission (a role for a user) do not apply to a scraper that
// has neither.
const envMetricsAddr = "METRICS_ADDR"

// logMetricsNamespace prefixes every family this endpoint exposes.
const logMetricsNamespace = "newapi_log_"

// logMetricsReadHeaderTimeout bounds how long a connection may hold the
// listener open without sending a request. The endpoint serves scrapers on a
// trusted network, but an unbounded read would still be a way to occupy the
// listener.
const logMetricsReadHeaderTimeout = 5 * time.Second

var (
	logMetricsRegistry *prometheus.Registry
	logMetricsServer   *http.Server

	logMetricsStartOnce sync.Once
	logMetricsStartErr  error
	logMetricsStopOnce  sync.Once
)

// logMetricsName builds one family name from its suffix, so the whole exposed
// set can be read in one place and every name carries the same prefix.
func logMetricsName(suffix string) string {
	return logMetricsNamespace + suffix
}

// logMetricsAddr validates METRICS_ADDR. An empty value turns the endpoint off;
// anything else must be an explicit host:port.
//
// The validation is strict for the same reason KAFKA_BROKERS' is: "9001" and
// ":" both look like a port someone meant, and both would silently mean every
// interface. Making the operator write the host turns "which interface is this
// exposed on" into a decision instead of a default.
func logMetricsAddr() (string, error) {
	raw := strings.TrimSpace(common.GetEnvOrDefaultString(envMetricsAddr, ""))
	if raw == "" {
		return "", nil
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return "", fmt.Errorf("%s %q is not host:port: %w; write an explicit address such as 127.0.0.1:9000, or leave it unset to serve no metrics endpoint", envMetricsAddr, raw, err)
	}
	if host == "" {
		return "", fmt.Errorf("%s %q has no host: an address without a host would listen on every interface, so write the interface explicitly (e.g. 127.0.0.1:9000), or leave it unset to serve no metrics endpoint", envMetricsAddr, raw)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("%s %q has no usable port: expected 1-65535", envMetricsAddr, raw)
	}
	return net.JoinHostPort(host, strconv.Itoa(portNumber)), nil
}

// StartLogMetrics serves the log pipeline's metrics when METRICS_ADDR is set,
// and does nothing when it is not. Call it after StartLogKafka, which is what
// creates the transport this endpoint reports on.
//
// When METRICS_ADDR is empty there is no listener, no goroutine and no broker
// query: a deployment that upgrades without setting it reaches exactly the I/O
// it reached before.
func StartLogMetrics() error {
	// The address is validated on every call, before anything is started, so a
	// misconfiguration is reported as itself rather than as a bind failure.
	addr, err := logMetricsAddr()
	if err != nil {
		return err
	}
	if addr == "" {
		return nil
	}
	logMetricsStartOnce.Do(func() {
		logMetricsStartErr = startLogMetrics(addr)
	})
	return logMetricsStartErr
}

// startLogMetrics is the one-shot half: the registry, the broker poll that
// feeds the derived series, and the listener.
func startLogMetrics(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("%s %q could not be bound: %w", envMetricsAddr, addr, err)
	}

	logMetricsRegistry = newLogMetricsRegistry()
	// The broker poll is part of the endpoint, not of the transport: with no
	// endpoint there is nobody to read the derived series, so the four extra
	// requests a minute are not spent.
	if consumer := logKafkaConsumer; consumer != nil {
		startLogMetricsPoll(consumer)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(logMetricsRegistry, promhttp.HandlerOpts{}))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: logMetricsReadHeaderTimeout,
	}
	logMetricsServer = server
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			common.SysError(fmt.Sprintf("the %s listener %s stopped: %s", envMetricsAddr, addr, err.Error()))
		}
	}()
	common.SysLog(fmt.Sprintf("log metrics endpoint listening on %s/metrics", addr))
	return nil
}

// StopLogMetrics shuts the metrics listener down. It is safe to call more than
// once, and safe to call when the endpoint was never started.
func StopLogMetrics() {
	logMetricsStopOnce.Do(func() {
		if logMetricsServer == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), logMetricsReadHeaderTimeout)
		defer cancel()
		_ = logMetricsServer.Shutdown(ctx)
	})
}

// newLogMetricsRegistry builds the exposition as a set with a fixed size: a
// dedicated registry, not prometheus.DefaultRegisterer, so the families on this
// externally reachable surface are an assertable contract, and so a third-party
// package that registers itself into the default registry cannot leak here.
//
// No Go or process collector is registered. They are not this endpoint's
// subject, and their families would turn "the exposed set" into something no
// test can state in full.
func newLogMetricsRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()

	gauge := func(suffix, help string, value func() float64) {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: logMetricsName(suffix), Help: help}, value))
	}
	counter := func(suffix, help string, value func() float64) {
		registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: logMetricsName(suffix), Help: help}, value))
	}

	// Two families are meaningful in every deployment, so they are always here.
	gauge("kafka_enabled", "1 when this process runs the Kafka log transport and 0 when it does not. When it is 0 every other newapi_log_kafka_* family is absent by construction, not healthy.", func() float64 {
		return boolValue(logKafkaConsumer != nil)
	})
	counter("log_buffer_dropped_rows_total", "Rows dropped by the in-memory log buffer because it was full. This is the only series here that is meaningful without Kafka: it covers the deployment whose log database cannot keep up.", func() float64 {
		return float64(logBufferDropped.Load())
	})

	// Everything below describes the Kafka transport, so none of it is
	// registered when there is no transport. A counter that reads 0 for a
	// transport that does not exist is a claim about nothing, and the families
	// that would have to invent a value (how far behind a consumer that is not
	// running is) are exactly the ones an operator must not be shown.
	if logKafkaConsumer == nil {
		return registry
	}

	// The rows that are gone for good. Each is a different action: rejected is
	// a full spool queue, dropped is a row or segment that could not be kept,
	// evicted is capacity, expired is the spool retention, torn is a crashed
	// append, and undecodable is an incompatible payload.
	counter("kafka_spool_rejected_rows_total", "Rows the spool refused because the payload carried a raw newline, which the line delimited segment format cannot carry.", func() float64 {
		return float64(logKafkaSpoolSnapshot().RejectedRows)
	})
	counter("kafka_spool_dropped_rows_total", "Rows the spool dropped without writing: its queue was full, its write failed, it was already closed, or the row did not fit the configured capacity.", func() float64 {
		return float64(logKafkaSpoolSnapshot().DroppedRows)
	})
	counter("kafka_spool_evicted_rows_total", "Rows removed with their sealed segment when the spool reached KAFKA_LOG_SPOOL_MAX_BYTES.", func() float64 {
		return float64(logKafkaSpoolSnapshot().EvictedRows)
	})
	counter("kafka_spool_expired_rows_total", "Rows removed with their sealed segment when it passed KAFKA_LOG_SPOOL_RETENTION_HOURS without being replayed.", func() float64 {
		return float64(logKafkaSpoolSnapshot().ExpiredRows)
	})
	counter("kafka_spool_torn_rows_total", "Segments whose last append was interrupted and whose truncated tail lost a row.", func() float64 {
		return float64(logKafkaSpoolSnapshot().TornRows)
	})
	counter("kafka_undecodable_rows_total", "Consumed rows dropped because the payload could not be decoded, which means a corrupt row or a producer from an incompatible version.", func() float64 {
		return float64(logKafkaUndecodableRows.Load())
	})

	// The rows still being caught, on a path that is already degrading.
	counter("kafka_spool_stashed_rows_total", "Rows written to the disk fallback because the producer's buffer was at its high-water mark or a delivery failed. This is the series to watch first in this group: it covers both ways into the spool.", func() float64 {
		return float64(logKafkaSpoolSnapshot().StashedRows)
	})
	counter("kafka_consumer_write_failed_total", "Consumer batches the log database refused. Their offsets stay uncommitted, so the rows are retried.", func() float64 {
		return float64(logKafkaConsumerSnapshot().WriteFailed)
	})
	counter("kafka_consumer_commit_failed_total", "Consumer batches that were written but whose offsets could not be committed, so they will be delivered again. This is a duplicate, never a lost row.", func() float64 {
		return float64(logKafkaConsumerSnapshot().CommitFailed)
	})
	counter("kafka_spool_replay_failed_total", "Replay attempts from the disk fallback that failed, which leave the segment in place to accumulate toward the capacity.", func() float64 {
		return float64(logKafkaSpoolSnapshot().ReplayFailed)
	})

	// The state, and the liveness of the poller that produces part of it.
	gauge("kafka_spool_bytes", "Bytes currently held by the disk fallback, which is the load a counter cannot express.", func() float64 {
		return float64(logKafkaSpoolSnapshot().Bytes)
	})
	gauge("kafka_broker_poll_success", "1 when the most recent broker poll succeeded and 0 when it did not. It is 0 before the first poll as well, so a process that restarts is never briefly reported as healthy.", func() float64 {
		return boolValue(logMetricsPollSuccess.Load())
	})
	counter("kafka_broker_poll_errors_total", "Broker polls that returned an error. While this grows, the lag, retention and group state series are absent rather than stale.", func() float64 {
		return float64(logMetricsPollErrors.Load())
	})

	registry.MustRegister(&logDerivedCollector{
		lagSeconds: prometheus.NewDesc(logMetricsName("kafka_lag_seconds"), "Wall clock lag per partition: the age, measured against now, of the oldest record this consumer still has to process. Zero means the group has committed everything the topic holds. The series is absent for a partition this process has never consumed, when the broker could not answer for it, and altogether when the broker poll fails. It assumes message.timestamp.type=CreateTime and that this topic's only producer is this application; with a third party producing into the topic the value stops being trustworthy.", []string{"partition"}, nil),
		lagRecords: prometheus.NewDesc(logMetricsName("kafka_lag_records"), "Records not yet committed, per partition: log-end offset minus the group's committed offset. It is the low-noise carrier of \"still growing\"; it is always present, because a group with no commit starts at the beginning of the topic.", []string{"partition"}, nil),
		retention:  prometheus.NewDesc(logMetricsName("kafka_retention_used_ratio"), "How much of the time based retention window the oldest retained record has used: (now - oldest record timestamp) / (KAFKA_LOG_RETENTION_HOURS * 3600), clamped to [0,1]. It is absent while the topic holds no records and when the broker poll fails. It describes TIME retention only: a topic configured with retention.bytes is bounded by size, and this ratio does not describe that bound.", nil, nil),
		groupState: prometheus.NewDesc(logMetricsName("kafka_consumer_group_state"), "The consumer group's state, 1 for the state the broker reports. Absent means the broker does not know the group at all, which is what a deployment that has never committed looks like; it is deliberately not reported as Dead, which would alert on every first start.", []string{"state"}, nil),
	})
	return registry
}

// boolValue renders a Go bool as the 0 or 1 a Prometheus gauge carries.
func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// logKafkaSpoolSnapshot reads the disk fallback's counters, or the zero
// snapshot when KAFKA_BROKERS is not configured: no producer exists then, and
// the spool it owns does not either.
func logKafkaSpoolSnapshot() logkafka.SpoolSnapshot {
	producer := logKafkaProducer.Load()
	if producer == nil {
		return logkafka.SpoolSnapshot{}
	}
	return producer.SpoolSnapshot()
}

// logKafkaConsumerSnapshot reads the consumer's counters, or the zero snapshot
// when the transport is not running.
func logKafkaConsumerSnapshot() logkafka.ConsumerSnapshot {
	if logKafkaConsumer == nil {
		return logkafka.ConsumerSnapshot{}
	}
	return logKafkaConsumer.Snapshot()
}

// logDerivedCollector reports the series that come from the broker poll. It
// emits nothing until a poll has succeeded, and nothing again after one fails:
// a frozen "lag 5s" looks like health, and these are the series an operator
// reads as health. Deleting them makes `lag > 30` evaluate on no data, which
// raises no false alert, while a poller that has stopped is reported by
// kafka_broker_poll_success on its own.
type logDerivedCollector struct {
	lagSeconds *prometheus.Desc
	lagRecords *prometheus.Desc
	retention  *prometheus.Desc
	groupState *prometheus.Desc
}

func (c *logDerivedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lagSeconds
	ch <- c.lagRecords
	ch <- c.retention
	ch <- c.groupState
}

func (c *logDerivedCollector) Collect(ch chan<- prometheus.Metric) {
	status, ok := logMetricsStatusSnapshot()
	if !ok {
		return
	}
	for _, partition := range status.Partitions {
		label := strconv.Itoa(int(partition.Partition))
		ch <- prometheus.MustNewConstMetric(c.lagRecords, prometheus.GaugeValue, float64(partition.LagRecords), label)
		if partition.LagSeconds != nil {
			ch <- prometheus.MustNewConstMetric(c.lagSeconds, prometheus.GaugeValue, *partition.LagSeconds, label)
		}
	}
	if status.RetentionUsedRatio != nil {
		ch <- prometheus.MustNewConstMetric(c.retention, prometheus.GaugeValue, *status.RetentionUsedRatio)
	}
	// Only the state the group is actually in is emitted, with the value 1.
	// Emitting all six states with five zeroes would read the same to every
	// rule above, and it would put a `state="Dead"` sample in the exposition of
	// a deployment whose group simply does not exist yet.
	if status.GroupState != "" {
		ch <- prometheus.MustNewConstMetric(c.groupState, prometheus.GaugeValue, 1, status.GroupState)
	}
}
