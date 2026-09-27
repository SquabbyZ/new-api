package model

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/logkafka"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
)

// The metrics endpoint's contract, checked on the exposition itself.
//
// Every assertion here goes through scrapeMetrics and reads family names, types,
// label keys and values. None of it can be done with a substring search over the
// body: "newapi_log_kafka_lag_seconds" appears in a HELP line, so a body search
// would pass against an exposition that never carries the series, and the
// endpoint's whole job is to be absent in exactly the situations where a
// naive check would say "present".
func scrapeMetrics(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	body, err := renderMetrics(registry)
	require.NoError(t, err)
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	require.NoError(t, err, "the exposition must be parseable prometheus text")
	return families
}

// renderMetrics is the same scrape without the test handle, so the race leg can
// scrape from a worker goroutine, where require's FailNow is not allowed.
func renderMetrics(registry *prometheus.Registry) ([]byte, error) {
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(
		recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		return nil, fmt.Errorf("the metrics handler answered %d", recorder.Code)
	}
	return recorder.Body.Bytes(), nil
}

// contractFamilies is the exposed set: 11 counters of loss and degradation, and
// 8 state gauges. The design calls it "18 families", but the list it enumerates
// has 7 state gauges and names 8; this map follows the enumerated names, which
// are the actual acceptance contract.
var contractFamilies = map[string]dto.MetricType{
	"newapi_log_kafka_spool_rejected_rows_total":    dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_dropped_rows_total":     dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_evicted_rows_total":     dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_expired_rows_total":     dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_torn_rows_total":        dto.MetricType_COUNTER,
	"newapi_log_kafka_undecodable_rows_total":       dto.MetricType_COUNTER,
	"newapi_log_log_buffer_dropped_rows_total":      dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_stashed_rows_total":     dto.MetricType_COUNTER,
	"newapi_log_kafka_consumer_write_failed_total":  dto.MetricType_COUNTER,
	"newapi_log_kafka_consumer_commit_failed_total": dto.MetricType_COUNTER,
	"newapi_log_kafka_spool_replay_failed_total":    dto.MetricType_COUNTER,
	"newapi_log_kafka_enabled":                      dto.MetricType_GAUGE,
	"newapi_log_kafka_spool_bytes":                  dto.MetricType_GAUGE,
	"newapi_log_kafka_lag_seconds":                  dto.MetricType_GAUGE,
	"newapi_log_kafka_lag_records":                  dto.MetricType_GAUGE,
	"newapi_log_kafka_retention_used_ratio":         dto.MetricType_GAUGE,
	"newapi_log_kafka_consumer_group_state":         dto.MetricType_GAUGE,
	"newapi_log_kafka_broker_poll_success":          dto.MetricType_GAUGE,
	"newapi_log_kafka_broker_poll_errors_total":     dto.MetricType_COUNTER,
}

// The families the design deliberately leaves out, because nobody would act on
// them: byte counters that restate a row counter in another unit, and internal
// bookkeeping whose only remedy is already written into a log line.
var deliberatelyUnexposed = []string{
	"newapi_log_kafka_producer_in_flight",
	"newapi_log_kafka_spool_sealed_segments_total",
	"newapi_log_kafka_spool_queue_full_total",
	"newapi_log_kafka_missing_topic",
	"newapi_log_kafka_spool_sealed_segments",
	"newapi_log_kafka_producer_failed_total",
	"newapi_log_kafka_spool_active_path",
}

func metricsPtr[T any](value T) *T { return &value }

// withKafkaTransport makes the process believe it runs the Kafka log transport,
// which is what the registry branches on to decide whether the transport's
// families exist at all.
//
// The consumer is real but never used: kgo.NewClient does not contact a broker,
// so nothing is ever dialled. The address is deliberately dead -- if anything
// here did dial it, the test would fail instead of passing against a live
// cluster.
func withKafkaTransport(t *testing.T) {
	t.Helper()
	previous := logKafkaConsumer
	consumer, err := logkafka.NewConsumer(logkafka.Config{
		Brokers:           []string{"127.0.0.1:1"},
		Topic:             "metrics-contract",
		GroupID:           "metrics-contract",
		ConsumerBatchSize: 1,
		RetentionHours:    48,
		FlushInterval:     time.Second,
		RequestTimeout:    time.Second,
	}, func([][]byte) error { return nil })
	require.NoError(t, err)
	logKafkaConsumer = consumer
	t.Cleanup(func() {
		logKafkaConsumer = previous
		consumer.Close()
	})
}

// resetLogMetricsState clears the process-wide state the collectors read, so one
// leg cannot see another's counters.
func resetLogMetricsState(t *testing.T) {
	t.Helper()
	logMetricsPollStatus.Store(nil)
	logMetricsPollSuccess.Store(false)
	logMetricsPollErrors.Store(0)
	logKafkaUndecodableRows.Store(0)
	logKafkaProducer.Store(nil)
	t.Cleanup(func() {
		logMetricsPollStatus.Store(nil)
		logMetricsPollSuccess.Store(false)
		logMetricsPollErrors.Store(0)
		logKafkaUndecodableRows.Store(0)
		logKafkaProducer.Store(nil)
	})
}

// spoolOnlyProducer is a real producer configured so that every Send takes the
// disk path: MaxBufferBytes of one byte puts the high-water mark at zero, so the
// producer's buffer is always "at its mark" and no row is ever handed to a
// broker. It needs no broker and no log database.
func spoolOnlyProducer(t *testing.T) *logkafka.Producer {
	t.Helper()
	producer, err := logkafka.NewProducer(logkafka.Config{
		Brokers:             []string{"127.0.0.1:1"},
		Topic:               "metrics-contract",
		MaxBufferBytes:      1,
		SpoolDir:            t.TempDir(),
		SpoolMaxBytes:       1 << 30,
		SpoolSegmentBytes:   1 << 30,
		SpoolSegmentSeconds: time.Hour,
		SpoolRetentionHours: 48,
		DeliveryTimeout:     time.Second,
		RequestTimeout:      time.Second,
		FlushInterval:       time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	return producer
}

// familySamples returns every sample of a family, keyed by its label values in
// declaration order. A single sample keyed "" means the family is label free.
func familySamples(t *testing.T, families map[string]*dto.MetricFamily, name string) map[string]float64 {
	t.Helper()
	family, ok := families[name]
	require.True(t, ok, "family %s must be in the exposition", name)
	samples := make(map[string]float64, len(family.Metric))
	for _, metric := range family.Metric {
		values := make([]string, 0, len(metric.Label))
		for _, label := range metric.Label {
			values = append(values, label.GetValue())
		}
		if family.GetType() == dto.MetricType_COUNTER {
			samples[strings.Join(values, ",")] = metric.GetCounter().GetValue()
			continue
		}
		samples[strings.Join(values, ",")] = metric.GetGauge().GetValue()
	}
	return samples
}

// familyLabelKeys returns the label names a family carries, which is the other
// half of the contract: an extra label multiplies the series and breaks the
// alerting rules that select on it.
func familyLabelKeys(t *testing.T, families map[string]*dto.MetricFamily, name string) []string {
	t.Helper()
	family, ok := families[name]
	require.True(t, ok, "family %s must be in the exposition", name)
	require.NotEmpty(t, family.Metric, "family %s must carry at least one sample", name)
	keys := make([]string, 0, len(family.Metric[0].Label))
	for _, label := range family.Metric[0].Label {
		keys = append(keys, label.GetName())
	}
	slices.Sort(keys)
	return keys
}

// TestLogMetricsFamilyContract is the exposure contract: exactly these families,
// with these types and these label keys. The length assertion is what makes
// "exactly" mean something -- an added family would otherwise be invisible.
func TestLogMetricsFamilyContract(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)
	logMetricsPollStatus.Store(&logkafka.BrokerStatus{
		Partitions: []logkafka.PartitionStatus{
			{Partition: 0, LagRecords: 12, LagSeconds: metricsPtr(4.5)},
			// Never consumed by this process: the records lag is real, the time
			// lag is unknown and must be missing rather than zero.
			{Partition: 1, LagRecords: 0, LagSeconds: nil},
		},
		GroupState:         "Stable",
		RetentionUsedRatio: metricsPtr(0.25),
	})

	families := scrapeMetrics(t, newLogMetricsRegistry())

	require.Len(t, families, len(contractFamilies),
		"the exposed set is a contract; an extra or missing family changes it")
	for name, metricType := range contractFamilies {
		family, ok := families[name]
		require.True(t, ok, "family %s must be exposed", name)
		assert.Equal(t, metricType, family.GetType(), "family %s has the wrong type", name)
	}

	assert.Equal(t, []string{"partition"}, familyLabelKeys(t, families, "newapi_log_kafka_lag_seconds"))
	assert.Equal(t, []string{"partition"}, familyLabelKeys(t, families, "newapi_log_kafka_lag_records"))
	assert.Equal(t, []string{"state"}, familyLabelKeys(t, families, "newapi_log_kafka_consumer_group_state"))
	for name := range contractFamilies {
		switch name {
		case "newapi_log_kafka_lag_seconds", "newapi_log_kafka_lag_records", "newapi_log_kafka_consumer_group_state":
			continue
		}
		assert.Empty(t, familyLabelKeys(t, families, name), "family %s must be label free", name)
	}

	for _, name := range deliberatelyUnexposed {
		_, ok := families[name]
		assert.False(t, ok, "family %s is deliberately not exposed", name)
	}

	for name, family := range families {
		assert.NotEqual(t, dto.MetricType_HISTOGRAM, family.GetType(), "family %s must not be a histogram", name)
		assert.NotEqual(t, dto.MetricType_SUMMARY, family.GetType(), "family %s must not be a summary", name)
	}

	// The two absences this fixture encodes, in the same family pair.
	assert.Equal(t, map[string]float64{"0": 4.5},
		familySamples(t, families, "newapi_log_kafka_lag_seconds"),
		"a partition this process never consumed has no time lag, and its sibling must not be invented")
	assert.Equal(t, map[string]float64{"0": 12, "1": 0},
		familySamples(t, families, "newapi_log_kafka_lag_records"),
		"the records lag is known for every partition")
}

// TestLogMetricsCountersAreExact drives both drop counters to a known count and
// asserts the exposition carries exactly that number. "> 0" would pass for an
// implementation that counted something else entirely.
func TestLogMetricsCountersAreExact(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)
	resetLogBuffer(t)

	// The buffer path. It is the one loss counter that is meaningful with Kafka
	// off, so it is the one that has to survive a deployment that never
	// configures Kafka.
	const droppedRows = 37
	for i := range logBufferCapacity {
		enqueueLog(sampleLogRow(int64(i), "rid-exact"))
	}
	for i := range droppedRows {
		enqueueLog(sampleLogRow(int64(i), fmt.Sprintf("rid-exact-dropped-%d", i)))
	}
	require.EqualValues(t, droppedRows, logBufferDropped.Load(), "the fixture must drop exactly this many rows")

	// The disk path, through the real producer: every row sent to a producer
	// whose buffer is at its mark is written to the spool instead.
	const stashedRows = 23
	producer := spoolOnlyProducer(t)
	logKafkaProducer.Store(producer)
	for i := range stashedRows {
		producer.Send([]byte(fmt.Sprintf(`{"request_id":"rid-exact-stashed-%d"}`, i)))
	}
	require.EqualValues(t, stashedRows, producer.SpoolSnapshot().StashedRows,
		"the fixture must hand exactly this many rows to the disk")

	families := scrapeMetrics(t, newLogMetricsRegistry())

	assert.Equal(t, map[string]float64{"": droppedRows},
		familySamples(t, families, "newapi_log_log_buffer_dropped_rows_total"))
	assert.Equal(t, map[string]float64{"": stashedRows},
		familySamples(t, families, "newapi_log_kafka_spool_stashed_rows_total"))

	// One counter, one truth: the number the metric reports is the number the
	// spool's own accessor reports, and the log line reads the same field.
	// A second counter kept for metrics would drift and then disagree with the
	// log line about whether an outage happened.
	assert.EqualValues(t, producer.SpoolSnapshot().StashedRows,
		familySamples(t, families, "newapi_log_kafka_spool_stashed_rows_total")[""])
	assert.EqualValues(t, logBufferDropped.Load(),
		familySamples(t, families, "newapi_log_log_buffer_dropped_rows_total")[""])
}

// TestLogMetricsScrapeDuringWriteIsRaceFree is the leg that makes -race
// meaningful for the collector.
//
// A -race run over a test that only writes, or only scrapes, never has the
// collector and the writer on the same memory at the same time. Here a writer
// goroutine keeps the spool, the buffer and the poll state moving while a
// scraper goroutine gathers the registry, which is the arrangement a real
// scraped endpoint is in and the one that exposes a counter that is not atomic.
func TestLogMetricsScrapeDuringWriteIsRaceFree(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)
	resetLogBuffer(t)

	producer := spoolOnlyProducer(t)
	logKafkaProducer.Store(producer)
	for i := range logBufferCapacity {
		enqueueLog(sampleLogRow(int64(i), "rid-race"))
	}

	registry := newLogMetricsRegistry()
	writesDone := make(chan struct{})
	scrapeFailed := make(chan error, 1)

	var writers sync.WaitGroup
	writers.Go(func() {
		defer close(writesDone)
		for i := range 500 {
			// The buffer is already full, so this drops and bumps the counter
			// the collector reads.
			enqueueLog(sampleLogRow(int64(i), "rid-race"))
			// The spool writer goroutine appends to the active segment, which is
			// what moves the spool's byte account the collector reads.
			producer.Send([]byte(fmt.Sprintf(`{"request_id":"rid-race-%d"}`, i)))
			logKafkaUndecodableRows.Add(1)
			logMetricsPollErrors.Add(1)
			logMetricsPollSuccess.Store(i%2 == 0)
			logMetricsPollStatus.Store(&logkafka.BrokerStatus{
				Partitions:         []logkafka.PartitionStatus{{Partition: int32(i % 4), LagRecords: int64(i), LagSeconds: metricsPtr(float64(i))}},
				GroupState:         "Stable",
				RetentionUsedRatio: metricsPtr(0.5),
			})
		}
	})

	var scrapers sync.WaitGroup
	scrapers.Go(func() {
		for {
			select {
			case <-writesDone:
				return
			default:
			}
			if _, err := renderMetrics(registry); err != nil {
				select {
				case scrapeFailed <- err:
				default:
				}
				return
			}
		}
	})
	scrapers.Wait()
	writers.Wait()

	select {
	case err := <-scrapeFailed:
		require.NoError(t, err, "every scrape during the writes must succeed")
	default:
	}
}

// TestLogMetricsDefaultsAreNotHealth covers the three default values that look
// like good news and are not.
func TestLogMetricsDefaultsAreNotHealth(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)

	t.Run("before the first poll the poller is not healthy", func(t *testing.T) {
		families := scrapeMetrics(t, newLogMetricsRegistry())
		assert.Equal(t, map[string]float64{"": 0},
			familySamples(t, families, "newapi_log_kafka_broker_poll_success"),
			"a process that has not polled yet must not report a successful poll")
		for _, name := range []string{
			"newapi_log_kafka_lag_seconds",
			"newapi_log_kafka_lag_records",
			"newapi_log_kafka_retention_used_ratio",
			"newapi_log_kafka_consumer_group_state",
		} {
			_, ok := families[name]
			assert.False(t, ok, "family %s must not exist before the first successful poll", name)
		}
	})

	t.Run("an empty topic has no retention ratio and no time lag", func(t *testing.T) {
		logMetricsPollStatus.Store(&logkafka.BrokerStatus{
			Partitions: []logkafka.PartitionStatus{{Partition: 0, LagRecords: 0, LagSeconds: nil}},
			GroupState: "Stable",
		})
		families := scrapeMetrics(t, newLogMetricsRegistry())
		_, ok := families["newapi_log_kafka_retention_used_ratio"]
		assert.False(t, ok, "an empty topic has no oldest record, so the ratio must be absent rather than 1 or NaN")
		_, ok = families["newapi_log_kafka_lag_seconds"]
		assert.False(t, ok, "a partition nothing has consumed has no time lag, and zero would read as health")
		assert.Equal(t, map[string]float64{"0": 0},
			familySamples(t, families, "newapi_log_kafka_lag_records"))
	})

	t.Run("a group with no commits is absent, not dead", func(t *testing.T) {
		logMetricsPollStatus.Store(&logkafka.BrokerStatus{
			Partitions: []logkafka.PartitionStatus{{Partition: 0, LagRecords: 0, LagSeconds: metricsPtr(0.0)}},
			GroupState: logkafka.GroupStateAbsent,
		})
		families := scrapeMetrics(t, newLogMetricsRegistry())
		states := familySamples(t, families, "newapi_log_kafka_consumer_group_state")
		assert.Equal(t, map[string]float64{logkafka.GroupStateAbsent: 1}, states,
			"a group the broker does not know is Absent; reporting Dead would alert on every first start")
		_, dead := states["Dead"]
		assert.False(t, dead, "a state the group is not in must not appear at all")
	})

	t.Run("without the transport only the two always meaningful families are served", func(t *testing.T) {
		previous := logKafkaConsumer
		logKafkaConsumer = nil
		t.Cleanup(func() { logKafkaConsumer = previous })

		families := scrapeMetrics(t, newLogMetricsRegistry())
		assert.Equal(t, map[string]float64{"": 0},
			familySamples(t, families, "newapi_log_kafka_enabled"))
		_, ok := families["newapi_log_log_buffer_dropped_rows_total"]
		assert.True(t, ok, "the buffer drop counter is the one loss series that works without Kafka")
		for _, name := range []string{
			"newapi_log_kafka_lag_seconds",
			"newapi_log_kafka_lag_records",
			"newapi_log_kafka_spool_bytes",
			"newapi_log_kafka_consumer_group_state",
			"newapi_log_kafka_retention_used_ratio",
			"newapi_log_kafka_broker_poll_success",
			"newapi_log_kafka_spool_stashed_rows_total",
			"newapi_log_kafka_broker_poll_errors_total",
		} {
			_, ok := families[name]
			assert.False(t, ok, "family %s describes a transport that is not running, so it must be absent", name)
		}
		require.Len(t, families, 2, "with no transport the endpoint serves exactly enabled and the buffer drop counter")
	})
}

// TestLogMetricsAddressMustBeExplicit covers the listen address rules: empty
// means no endpoint, and anything that looks like a port without a host fails
// rather than binding every interface.
func TestLogMetricsAddressMustBeExplicit(t *testing.T) {
	cases := []struct {
		value string
		want  string
		ok    bool
	}{
		{"", "", true},
		{"127.0.0.1:9001", "127.0.0.1:9001", true},
		{"0.0.0.0:9000", "0.0.0.0:9000", true},
		{"[::1]:9000", "[::1]:9000", true},
		{"  127.0.0.1:9001  ", "127.0.0.1:9001", true},
		{"9001", "", false},
		{":", "", false},
		{":9001", "", false},
		{"127.0.0.1:", "", false},
		{"127.0.0.1:0", "", false},
		{"127.0.0.1:99999", "", false},
		{"127.0.0.1:http", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(envMetricsAddr, tc.value)
			addr, err := logMetricsAddr()
			if !tc.ok {
				require.Error(t, err, "%q must be refused rather than degraded to another address", tc.value)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, addr)
		})
	}

	t.Run("a refused address fails startup", func(t *testing.T) {
		// The validation returns before anything is started, so this leg does
		// not consume the one-shot start and does not open a port.
		before := logMetricsServer
		t.Setenv(envMetricsAddr, "9001")
		require.Error(t, StartLogMetrics(), "a malformed METRICS_ADDR must fail startup")
		assert.Same(t, before, logMetricsServer, "a refused address must not start a listener")
	})
}

// TestLogMetricsDefaultIsNoListener is the default-deployment leg: with
// METRICS_ADDR unset the process opens no port. It asserts that no server is
// created, which is order independent, and the process-level proof (the same
// listening set as before the change) is the netstat comparison recorded with
// the slice's evidence.
func TestLogMetricsDefaultIsNoListener(t *testing.T) {
	t.Setenv(envMetricsAddr, "")
	before := logMetricsServer
	require.NoError(t, StartLogMetrics())
	assert.Same(t, before, logMetricsServer, "an unset METRICS_ADDR must not create a listener")

	// And the poller is not started either: without an endpoint there is nobody
	// to read the broker-derived series, so no request is made to the broker.
	assert.Nil(t, logMetricsPollCancel, "the broker poll must not run when the endpoint is off")
}

// TestLogMetricsListenerServesOnlyTheEndpoint starts the endpoint for real.
//
// It must run after TestLogMetricsDefaultIsNoListener, which asserts that no
// listener exists: StartLogMetrics is a process-lifetime singleton, as it is in
// main, so this is the one leg that consumes it.
func TestLogMetricsListenerServesOnlyTheEndpoint(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)
	logMetricsPollStatus.Store(&logkafka.BrokerStatus{
		Partitions:         []logkafka.PartitionStatus{{Partition: 0, LagRecords: 3, LagSeconds: metricsPtr(1.5)}},
		GroupState:         "Stable",
		RetentionUsedRatio: metricsPtr(0.1),
	})

	addr := freeLoopbackAddr(t)
	t.Setenv(envMetricsAddr, addr)
	require.NoError(t, StartLogMetrics())
	t.Cleanup(StopLogMetrics)

	body, status := httpGet(t, "http://"+addr+"/metrics")
	require.Equal(t, http.StatusOK, status)
	families := parseExposition(t, body)
	assert.Contains(t, families, "newapi_log_kafka_enabled",
		"the endpoint must serve the exposition on its own address")

	for _, path := range []string{"/", "/metrics/", "/debug/pprof/", "/api/status"} {
		_, status := httpGet(t, "http://"+addr+path)
		assert.Equal(t, http.StatusNotFound, status,
			"the listener carries one handler, so %s must be a 404", path)
	}
}

// TestLogMetricsIsNotOnTheAPIPort is the exposure leg that matters most, and it
// asserts the right thing: the API port's response carries no newapi_ family.
//
// It must not assert a status code. The API port answers /metrics with the SPA
// index page and a 200, because the web router's NoRoute fallback sends every
// path outside /v1, /api and /assets to the app shell. An assertion written
// against 404 would fail against the working code, and one written against 200
// would pass against a deployment that had exposed the metrics on the API port.
func TestLogMetricsIsNotOnTheAPIPort(t *testing.T) {
	port := "3000"
	if configured := common.GetEnvOrDefaultString("PORT", ""); configured != "" {
		port = configured
	}
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 500*time.Millisecond)
	if err != nil {
		t.Skipf("no server is listening on the API port %s, so this leg cannot observe it", port)
	}
	_ = conn.Close()

	body, status := httpGet(t, "http://127.0.0.1:"+port+"/metrics")
	t.Logf("API port /metrics answered %d with %d bytes", status, len(body))
	assert.NotContains(t, string(body), "newapi_log_",
		"the API port must not carry any newapi_log_ family: that is the exposure this slice exists to prevent")
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func httpGet(t *testing.T, url string) ([]byte, int) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	var body bytes.Buffer
	_, err = body.ReadFrom(response.Body)
	require.NoError(t, err)
	return body.Bytes(), response.StatusCode
}

func parseExposition(t *testing.T, body []byte) map[string]*dto.MetricFamily {
	t.Helper()
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	require.NoError(t, err)
	return families
}

// --- the alerting rules ---

// alertRule is one of the alerting expressions the design fixes, together with
// what it references. The check below is mechanical: every family and label key
// a rule names has to exist in a real exposition, or the rule would never fire
// and nothing would say so.
type alertRule struct {
	name       string
	expression string
	families   []string
	labelKeys  map[string][]string
}

var alertRules = []alertRule{
	{
		name:       "time lag warning",
		expression: `max by (partition) (newapi_log_kafka_lag_seconds) > 30`,
		families:   []string{"newapi_log_kafka_lag_seconds"},
		labelKeys:  map[string][]string{"newapi_log_kafka_lag_seconds": {"partition"}},
	},
	{
		name:       "time lag severe",
		expression: `max by (partition) (newapi_log_kafka_lag_seconds) > 120`,
		families:   []string{"newapi_log_kafka_lag_seconds"},
		labelKeys:  map[string][]string{"newapi_log_kafka_lag_seconds": {"partition"}},
	},
	{
		name: "high and still growing",
		expression: `(max by (partition) (newapi_log_kafka_lag_seconds) > 30)
  and on (partition)
(delta(newapi_log_kafka_lag_records[5m]) > 0)`,
		families: []string{"newapi_log_kafka_lag_seconds", "newapi_log_kafka_lag_records"},
		labelKeys: map[string][]string{
			"newapi_log_kafka_lag_seconds": {"partition"},
			"newapi_log_kafka_lag_records": {"partition"},
		},
	},
	{
		name:       "consumer group state",
		expression: `newapi_log_kafka_consumer_group_state{state=~"Empty|Dead"} == 1`,
		families:   []string{"newapi_log_kafka_consumer_group_state"},
		labelKeys:  map[string][]string{"newapi_log_kafka_consumer_group_state": {"state"}},
	},
	{
		name:       "retention nearly used",
		expression: `newapi_log_kafka_retention_used_ratio > 0.8`,
		families:   []string{"newapi_log_kafka_retention_used_ratio"},
	},
	{
		name:       "poller alive",
		expression: `newapi_log_kafka_enabled == 1 and newapi_log_kafka_broker_poll_success == 0`,
		families:   []string{"newapi_log_kafka_enabled", "newapi_log_kafka_broker_poll_success"},
	},
}

// validateAlertRules reports every family and label key a rule names that the
// exposition does not carry. It returns an error rather than failing a test so
// the reverse leg below can assert on the error itself.
func validateAlertRules(families map[string]*dto.MetricFamily) error {
	for _, rule := range alertRules {
		for _, name := range rule.families {
			family, ok := families[name]
			if !ok {
				return fmt.Errorf("rule %q references %s, which is not in the exposition", rule.name, name)
			}
			if len(family.Metric) == 0 {
				return fmt.Errorf("rule %q references %s, which has no samples", rule.name, name)
			}
			for _, key := range rule.labelKeys[name] {
				found := false
				for _, label := range family.Metric[0].Label {
					if label.GetName() == key {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("rule %q selects %s on %q, which that family does not carry", rule.name, name, key)
				}
			}
		}
	}
	return nil
}

// TestLogMetricsAlertRulesResolveOnTheExposition implements the rules' mechanical
// validation and the two legs that give it teeth.
func TestLogMetricsAlertRulesResolveOnTheExposition(t *testing.T) {
	withKafkaTransport(t)
	resetLogMetricsState(t)

	partitions := make([]logkafka.PartitionStatus, 0, 3)
	for partition := range 3 {
		partitions = append(partitions, logkafka.PartitionStatus{
			Partition:  int32(partition),
			LagRecords: int64(partition),
			LagSeconds: metricsPtr(float64(partition)),
		})
	}
	logMetricsPollStatus.Store(&logkafka.BrokerStatus{
		Partitions:         partitions,
		GroupState:         "Stable",
		RetentionUsedRatio: metricsPtr(0.1),
	})
	families := scrapeMetrics(t, newLogMetricsRegistry())

	t.Run("every referenced family and label key exists", func(t *testing.T) {
		require.NoError(t, validateAlertRules(families))
	})

	t.Run("dropping a referenced family fails the check", func(t *testing.T) {
		without := maps.Clone(families)
		require.Contains(t, without, "newapi_log_kafka_lag_records")
		delete(without, "newapi_log_kafka_lag_records")
		require.Error(t, validateAlertRules(without),
			"the validator must fail when a rule references a family the exposition does not carry; otherwise it is decoration")
	})

	t.Run("the forbidden average stays silent on a hot partition", func(t *testing.T) {
		// Twenty partitions, one badly behind and nineteen idle. This is the
		// shape a threshold on an average cannot see, and the reason the design
		// requires the partition label rather than a topic level number.
		hot := make([]logkafka.PartitionStatus, 0, 20)
		for partition := range 20 {
			seconds := 0.0
			if partition == 7 {
				seconds = 600
			}
			hot = append(hot, logkafka.PartitionStatus{
				Partition:  int32(partition),
				LagRecords: int64(seconds),
				LagSeconds: metricsPtr(seconds),
			})
		}
		logMetricsPollStatus.Store(&logkafka.BrokerStatus{Partitions: hot, GroupState: "Stable"})
		hotFamilies := scrapeMetrics(t, newLogMetricsRegistry())

		lag := familySamples(t, hotFamilies, "newapi_log_kafka_lag_seconds")
		require.Len(t, lag, 20)

		// avg(...) > 30: the expression the design forbids.
		var sum float64
		for _, seconds := range lag {
			sum += seconds
		}
		average := sum / float64(len(lag))
		assert.Equal(t, 30.0, average, "twenty partitions with one at 600 seconds average to 30")
		assert.False(t, average > 30, "the forbidden average is silently below its own threshold on a hot partition")

		// max by (partition) (...): the expression the design requires.
		hits := make([]string, 0, 20)
		for partition, seconds := range lag {
			if seconds > 30 {
				hits = append(hits, partition)
			}
		}
		slices.Sort(hits)
		assert.Equal(t, []string{"7"}, hits, "max by (partition) must find exactly the hot partition")
		t.Logf("hot partition leg: avg=%.1f (silent) hits=%v (max by (partition): %v)",
			average, hits, lag["7"])
	})
}

// --- the real broker leg ---

// TestLogMetricsLagAgainstRealBroker runs the derived series against the live
// broker, with the consumer's batch handler stubbed so not one row reaches a log
// database: the lag arithmetic is what is under test here, not the write path.
//
// It establishes the two facts no in-memory fixture can: that the broker really
// answers a timestamp query with a record's timestamp (which is what makes
// seconds possible at all), and that a group which has committed nothing is
// reported as absent rather than as Empty.
func TestLogMetricsLagAgainstRealBroker(t *testing.T) {
	rawBrokers := common.GetEnvOrDefaultString("TEST_KAFKA_BROKERS", "")
	if rawBrokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not configured")
	}
	brokers := strings.Split(rawBrokers, ",")
	topic := isolatedKafkaTopic(t, brokers)

	cfg := logkafka.Config{
		Brokers:           brokers,
		Topic:             topic,
		GroupID:           fmt.Sprintf("new-api-log-metrics-%d", time.Now().UnixNano()),
		RetentionHours:    48,
		ConsumerBatchSize: 1,
		RequestTimeout:    10 * time.Second,
		DeliveryTimeout:   10 * time.Second,
		FlushInterval:     200 * time.Millisecond,
	}

	// Built before it runs, so the first poll observes the group before any
	// member has ever joined it and before anything has been committed.
	consumer, err := logkafka.NewConsumer(cfg, func([][]byte) error { return nil })
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	pollCtx, cancelPoll := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelPoll()

	t.Run("a group that committed nothing is absent, not empty", func(t *testing.T) {
		status, err := consumer.PollBroker(pollCtx)
		require.NoError(t, err)
		assert.Equal(t, logkafka.GroupStateAbsent, status.GroupState,
			"the broker reports a group that has done nothing as Empty, which is a state operators alert on, so a first start would page about itself")
		require.Len(t, status.Partitions, 1)
		assert.Nil(t, status.Partitions[0].LagSeconds,
			"nothing has been consumed, so the time lag is unknown rather than zero")
	})

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.MaxVersions(kversion.V3_7_0()))
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	produce := func(payload string) {
		t.Helper()
		result := producer.ProduceSync(pollCtx, &kgo.Record{Topic: topic, Value: []byte(payload)})
		require.NoError(t, result.FirstErr(), "the test could not produce to its own topic")
	}

	const rows = 12
	for i := range rows {
		produce(fmt.Sprintf(`{"request_id":"rid-lag-%d"}`, i))
	}

	t.Run("before anything is consumed the records lag is the whole topic", func(t *testing.T) {
		status, err := consumer.PollBroker(pollCtx)
		require.NoError(t, err)
		require.Len(t, status.Partitions, 1)
		assert.EqualValues(t, rows, status.Partitions[0].LagRecords,
			"a group that has never committed starts at the beginning of the topic, so its lag is the whole topic")
		assert.Nil(t, status.Partitions[0].LagSeconds,
			"a partition this process never consumed must have no time lag at all")
		require.NotNil(t, status.RetentionUsedRatio,
			"the oldest record's timestamp comes back from a timestamp query, so a topic with records has an age to report")
		assert.GreaterOrEqual(t, *status.RetentionUsedRatio, 0.0)
		assert.LessOrEqual(t, *status.RetentionUsedRatio, 1.0,
			"the ratio is clamped: a record older than the declared window is not a ratio above one")
	})

	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		consumer.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		return consumer.Snapshot().WrittenRows >= rows
	}, 60*time.Second, 100*time.Millisecond, "the consumer must drain the rows the test produced")

	// The row count lives in the consumer's own snapshot rather than in a family
	// of its own: writtenRows and stashedRows describe the same events from two
	// sides, and a third counter would be a second truth about the same thing.
	assert.EqualValues(t, rows, consumer.Snapshot().WrittenRows)

	require.Eventually(t, func() bool {
		status, err := consumer.PollBroker(pollCtx)
		return err == nil && status.Partitions[0].LagRecords == 0
	}, 30*time.Second, 200*time.Millisecond, "the committed offset must catch up with the log end offset")

	t.Run("a caught up partition is zero lag, not absent", func(t *testing.T) {
		status, err := consumer.PollBroker(pollCtx)
		require.NoError(t, err)
		require.NotNil(t, status.Partitions[0].LagSeconds)
		assert.Equal(t, 0.0, *status.Partitions[0].LagSeconds)
	})

	// Stopped before it can drain the next row, so there is a real backlog to
	// measure and it cannot be mistaken for a scaled record count.
	stopRun()
	<-runDone
	produce(`{"request_id":"rid-lag-stalled"}`)

	t.Run("the time lag grows while the record lag stands still", func(t *testing.T) {
		first, err := consumer.PollBroker(pollCtx)
		require.NoError(t, err)
		require.Len(t, first.Partitions, 1)
		require.EqualValues(t, 1, first.Partitions[0].LagRecords)
		require.NotNil(t, first.Partitions[0].LagSeconds,
			"the process consumed this partition earlier, so the time lag is known once it is behind again")

		time.Sleep(3 * time.Second)

		second, err := consumer.PollBroker(pollCtx)
		require.NoError(t, err)
		require.Len(t, second.Partitions, 1)
		// Nothing was produced in between, so the records lag is unchanged...
		require.EqualValues(t, 1, second.Partitions[0].LagRecords)
		require.NotNil(t, second.Partitions[0].LagSeconds)
		t.Logf("stalled consumer: lag_records=%d lag_seconds %.3f -> %.3f",
			second.Partitions[0].LagRecords, *first.Partitions[0].LagSeconds, *second.Partitions[0].LagSeconds)
		// ...and the time lag still moved. An implementation that multiplied
		// the record count by a constant would report the same number twice.
		assert.Greater(t, *second.Partitions[0].LagSeconds, *first.Partitions[0].LagSeconds,
			"the time lag must be a wall clock quantity, not the record count scaled")
		assert.Greater(t, *second.Partitions[0].LagSeconds, 3.0,
			"three seconds passed with the consumer stopped, so the oldest unprocessed row is more than three seconds old")
	})
}
