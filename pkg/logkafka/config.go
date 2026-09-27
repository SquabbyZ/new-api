// Package logkafka carries the log write path over Kafka: an asynchronous
// producer on the request path, and a resident consumer that batches rows into
// the log database.
//
// The package is deliberately transport-only. It moves opaque byte payloads and
// never names the log row type or ClickHouse, so the model package can depend on
// it without an import cycle, and the batched INSERT (with its single column
// order) stays in the one package that already owns it.
package logkafka

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/twmb/franz-go/pkg/kversion"
)

// requestVersions is the newest set of Kafka request versions the client will
// use, and it has to be pinned.
//
// franz-go defaults to the newest versions it knows about. Measured against
// Kafka 3.7.0 with franz-go v1.19.0, that default sends ApiVersions v4 and
// Metadata v13, and the broker closes the connection on both instead of
// answering: ApiVersions is the exchange in which the client would learn it has
// to downgrade, so the negotiation never completes and every connection dies
// before one request succeeds. Pinning the target to the broker this project is
// built and measured against restores it, and the client still lowers the
// version further per broker from that broker's own ApiVersions answer.
var requestVersions = kversion.V3_7_0()

// Environment variables. KAFKA_BROKERS is the master switch: empty means Kafka
// is not configured and the caller keeps the write path it already had.
//
// The client settings the design pins (acks=all, idempotent production,
// maximum retries, auto offset commit off) are constants in producerOpts and
// consumerOpts rather than variables here, so there is no way to configure the
// log transport into a state that loses rows on retry.
const (
	envBrokers           = "KAFKA_BROKERS"
	envTopic             = "KAFKA_LOG_TOPIC"
	envGroupID           = "KAFKA_LOG_GROUP_ID"
	envMaxBufferBytes    = "KAFKA_LOG_MAX_BUFFER_BYTES"
	envLingerMs          = "KAFKA_LOG_LINGER_MS"
	envDeliveryTimeoutMs = "KAFKA_LOG_DELIVERY_TIMEOUT_MS"
	envRequestTimeoutMs  = "KAFKA_LOG_REQUEST_TIMEOUT_MS"
	envRetryBackoffMs    = "KAFKA_LOG_RETRY_BACKOFF_MS"
	envRetentionHours    = "KAFKA_LOG_RETENTION_HOURS"
	envConsumerBatchSize = "KAFKA_LOG_CONSUMER_BATCH_SIZE"
	envSpoolDir          = "KAFKA_LOG_SPOOL_DIR"

	envSpoolMaxBytes       = "KAFKA_LOG_SPOOL_MAX_BYTES"
	envSpoolRetentionHours = "KAFKA_LOG_SPOOL_RETENTION_HOURS"
	envSpoolSegmentBytes   = "KAFKA_LOG_SPOOL_SEGMENT_BYTES"
	envSpoolSegmentSeconds = "KAFKA_LOG_SPOOL_SEGMENT_SECONDS"

	defaultTopic             = "new-api-logs"
	defaultGroupID           = "new-api-log-consumer"
	defaultMaxBufferBytes    = 33554432 // 32 MiB
	defaultLingerMs          = 5
	defaultDeliveryTimeoutMs = 120000
	defaultRequestTimeoutMs  = 30000
	defaultRetryBackoffMs    = 200
	defaultRetentionHours    = 48
	defaultConsumerBatchSize = 2000

	defaultSpoolMaxBytes       = 1073741824 // 1 GiB
	defaultSpoolRetentionHours = 48
	defaultSpoolSegmentBytes   = 8388608 // 8 MiB
	defaultSpoolSegmentSeconds = 60

	// defaultSpoolDir is relative to the process working directory, which is the
	// application directory: it sits next to the default SQLite file. It is
	// deliberately not os.TempDir(): on Linux that is usually a tmpfs, so a
	// spool there would be memory pretending to be disk and would vanish with
	// the host, which is exactly what the disk fallback exists to prevent.
	defaultSpoolDir = "new-api-log-spool"
)

// Config is the resolved Kafka transport configuration.
type Config struct {
	Brokers           []string
	Topic             string
	GroupID           string
	MaxBufferBytes    int
	Linger            time.Duration
	DeliveryTimeout   time.Duration
	RequestTimeout    time.Duration
	RetryBackoff      time.Duration
	RetentionHours    int
	ConsumerBatchSize int
	SpoolDir          string

	// The spool settings. SpoolMaxBytes bounds the whole directory; eviction and
	// retention both drop whole sealed segments, never part of one, because the
	// segment format is append-only.
	SpoolMaxBytes       int64
	SpoolRetentionHours int
	SpoolSegmentBytes   int64
	SpoolSegmentSeconds time.Duration

	// FlushInterval is the longest a consumer batch waits before it is written.
	// It is not a KAFKA_* setting: the consumer reuses LOG_FLUSH_INTERVAL_MS,
	// and the caller owns that variable's clamp, so the caller fills this in
	// rather than this package reading and clamping it a second time.
	FlushInterval time.Duration
}

// Enabled reports whether Kafka is configured. KAFKA_BROKERS is the only
// switch, and an empty value means the caller keeps its previous write path.
func (c Config) Enabled() bool {
	return len(c.Brokers) > 0
}

// Configured reports whether KAFKA_BROKERS is set. It is the master switch the
// write path branches on, and the only condition: a deployment that upgrades
// without configuring Kafka must reach exactly the code it reached before.
func Configured() bool {
	return common.GetEnvOrDefaultString(envBrokers, "") != ""
}

// LoadConfig reads the KAFKA_* environment variables. An empty KAFKA_BROKERS
// returns the zero Config and a nil error: a deployment that does not configure
// Kafka is the existing deployment, not a misconfiguration. A syntax error in
// KAFKA_BROKERS is returned as an error, because that is a new
// misconfiguration and failing at startup is the only way an operator sees it.
func LoadConfig() (Config, error) {
	brokers, err := parseBrokers(common.GetEnvOrDefaultString(envBrokers, ""))
	if err != nil {
		return Config{}, err
	}
	if len(brokers) == 0 {
		return Config{}, nil
	}

	return Config{
		Brokers: brokers,
		Topic:   common.GetEnvOrDefaultString(envTopic, defaultTopic),
		GroupID: common.GetEnvOrDefaultString(envGroupID, defaultGroupID),
		// max(..., 1) rather than the raw value: strconv accepts a negative
		// number, and a negative bound would turn every send or batch into an
		// overflow instead of a limit.
		MaxBufferBytes:      max(common.GetEnvOrDefault(envMaxBufferBytes, defaultMaxBufferBytes), 1),
		Linger:              time.Duration(max(common.GetEnvOrDefault(envLingerMs, defaultLingerMs), 0)) * time.Millisecond,
		DeliveryTimeout:     time.Duration(max(common.GetEnvOrDefault(envDeliveryTimeoutMs, defaultDeliveryTimeoutMs), 1)) * time.Millisecond,
		RequestTimeout:      time.Duration(max(common.GetEnvOrDefault(envRequestTimeoutMs, defaultRequestTimeoutMs), 1)) * time.Millisecond,
		RetryBackoff:        time.Duration(max(common.GetEnvOrDefault(envRetryBackoffMs, defaultRetryBackoffMs), 1)) * time.Millisecond,
		RetentionHours:      max(common.GetEnvOrDefault(envRetentionHours, defaultRetentionHours), 1),
		ConsumerBatchSize:   max(common.GetEnvOrDefault(envConsumerBatchSize, defaultConsumerBatchSize), 1),
		SpoolDir:            common.GetEnvOrDefaultString(envSpoolDir, defaultSpoolDir),
		SpoolMaxBytes:       int64(max(common.GetEnvOrDefault(envSpoolMaxBytes, defaultSpoolMaxBytes), 1)),
		SpoolRetentionHours: max(common.GetEnvOrDefault(envSpoolRetentionHours, defaultSpoolRetentionHours), 1),
		// The effective segment size is min(segment, capacity), so "at least one
		// segment fits" always holds. Capacity smaller than a single row is not
		// expressible here -- row size is the log content's business -- so that
		// case is handled where the row is written: it is dropped and reported.
		SpoolSegmentBytes:   int64(max(min(common.GetEnvOrDefault(envSpoolSegmentBytes, defaultSpoolSegmentBytes), common.GetEnvOrDefault(envSpoolMaxBytes, defaultSpoolMaxBytes)), 1)),
		SpoolSegmentSeconds: time.Duration(max(common.GetEnvOrDefault(envSpoolSegmentSeconds, defaultSpoolSegmentSeconds), 1)) * time.Second,
	}, nil
}

// parseBrokers splits and validates the bootstrap list. One bad element fails
// the whole list: silently dropping an entry would point the producer at a
// different cluster than the operator wrote down.
func parseBrokers(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	entries := strings.Split(raw, ",")
	brokers := make([]string, 0, len(entries))
	for i, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("%s entry %d is empty: expected a comma separated list of host:port with no empty or trailing element (got %q)", envBrokers, i+1, raw)
		}
		host, port, err := net.SplitHostPort(entry)
		if err != nil {
			return nil, fmt.Errorf("%s entry %d (%q) is not host:port: %w", envBrokers, i+1, entry, err)
		}
		if host == "" {
			return nil, fmt.Errorf("%s entry %d (%q) has no host", envBrokers, i+1, entry)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("%s entry %d (%q) has no usable port: expected 1-65535", envBrokers, i+1, entry)
		}
		brokers = append(brokers, net.JoinHostPort(host, strconv.Itoa(portNumber)))
	}
	return brokers, nil
}
