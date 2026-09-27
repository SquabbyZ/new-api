package model

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/logkafka"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// withLogDatabaseType sets the log database type for one test and restores the
// previous pair afterwards. The predicate tests need only the type; a real
// handle would add nothing.
func withLogDatabaseType(t *testing.T, logType common.DatabaseType) {
	t.Helper()
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		common.SetDatabaseTypes(previousMain, previousLog)
		initCol()
	})
	common.SetDatabaseTypes(previousMain, logType)
	initCol()
}

// stopLogKafkaForTest returns the Kafka lifecycle to its zero state. The
// lifecycle is a process-lifetime singleton guarded by a sync.Once, so a test
// that starts it has to reset that state before the next one can.
func stopLogKafkaForTest(t *testing.T) {
	t.Helper()
	StopLogKafka()
	logKafkaStartOnce = sync.Once{}
	logKafkaStartErr = nil
	logKafkaStopOnce = sync.Once{}
	logKafkaProducer.Store(nil)
	logKafkaConsumer = nil
	logKafkaConsumerCancel = nil
	logKafkaConsumerDone = nil
}

// TestLogKafkaIsOffWithoutBrokersOnEveryLogDatabase is the predicate
// equivalence the backwards-compatibility acceptance rests on. It pins both
// halves: the new switch is off for all four log databases when KAFKA_BROKERS
// is empty, and logBufferingEnabled still answers exactly what it answered
// before the change -- ClickHouse only.
//
// Folding Kafka into logBufferingEnabled would make every existing ClickHouse
// deployment demand a broker it does not run, so this pair is the guard.
func TestLogKafkaIsOffWithoutBrokersOnEveryLogDatabase(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")

	cases := []struct {
		name             string
		typ              common.DatabaseType
		wantLogBuffering bool
	}{
		{"sqlite", common.DatabaseTypeSQLite, false},
		{"mysql", common.DatabaseTypeMySQL, false},
		{"postgres", common.DatabaseTypePostgreSQL, false},
		{"clickhouse", common.DatabaseTypeClickHouse, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withLogDatabaseType(t, tc.typ)
			assert.False(t, logKafkaEnabled(), "Kafka must stay off until KAFKA_BROKERS is set")
			assert.Equal(t, tc.wantLogBuffering, logBufferingEnabled(),
				"logBufferingEnabled must keep answering what it answered before Kafka existed")
		})
	}
}

// TestLogKafkaIsOnForEveryLogDatabaseWhenTheSwitchIsSet separates the two
// predicates in the other direction: the switch answers only about Kafka.
func TestLogKafkaIsOnForEveryLogDatabaseWhenTheSwitchIsSet(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")

	for _, typ := range []common.DatabaseType{
		common.DatabaseTypeSQLite, common.DatabaseTypeMySQL,
		common.DatabaseTypePostgreSQL, common.DatabaseTypeClickHouse,
	} {
		withLogDatabaseType(t, typ)
		assert.True(t, logKafkaEnabled())
	}
}

// TestStartLogKafkaRefusesAKafkaThatCannotReachClickHouse covers the
// deliberately asymmetric failure semantics. A new misconfiguration -- Kafka
// switched on where the log database is not ClickHouse -- stops startup,
// because the consumer writes ClickHouse's batched INSERT and nothing else.
func TestStartLogKafkaRefusesAKafkaThatCannotReachClickHouse(t *testing.T) {
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	withLogDatabaseType(t, common.DatabaseTypePostgreSQL)

	err := StartLogKafka()
	require.Error(t, err, "Kafka with a non-ClickHouse log database is a misconfiguration")
	assert.Contains(t, err.Error(), "LOG_SQL_DSN")
	assert.Contains(t, err.Error(), "ClickHouse")
	assert.EqualError(t, StartLogKafka(), err.Error(), "startup is attempted once; the error is remembered")
}

// TestStartLogKafkaRefusesAMalformedBrokerList covers the other startup
// failure: a broker list that cannot be parsed is a new misconfiguration too,
// and it has to name the variable to fix.
func TestStartLogKafkaRefusesAMalformedBrokerList(t *testing.T) {
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	t.Setenv("KAFKA_BROKERS", "localhost,127.0.0.1:9092")
	withLogDatabaseType(t, common.DatabaseTypeClickHouse)

	err := StartLogKafka()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_BROKERS")
}

// TestStartLogKafkaDoesNothingWithoutBrokers is the upgrade case: an existing
// deployment that has never heard of Kafka starts exactly as it did before.
func TestStartLogKafkaDoesNothingWithoutBrokers(t *testing.T) {
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	t.Setenv("KAFKA_BROKERS", "")
	withLogDatabaseType(t, common.DatabaseTypeClickHouse)

	require.NoError(t, StartLogKafka())
	assert.Nil(t, logKafkaProducer.Load(), "no producer may be created when Kafka is off")
	assert.Nil(t, logKafkaConsumer, "no consumer may be created when Kafka is off")
}

// baselineClickHouseInsertPrefix and baselineClickHouseLogRow are the statement
// the pre-change tree records for a fixed input, captured verbatim from
// `git show HEAD`'s tree on a recorder-backed SQLite handle with the ClickHouse
// log database type. They are a literal on purpose: rebuilding them from the
// production constants would only compare the code with itself.
const baselineClickHouseInsertPrefix = "INSERT INTO logs (id, user_id, created_at, type, content, username, token_name, model_name, quota, prompt_tokens, completion_tokens, use_time, is_stream, channel_id, token_id, `group`, ip, request_id, upstream_request_id, other) VALUES "

func baselineClickHouseLogRow(createdAt int64, requestID string) string {
	return fmt.Sprintf(`(0,7,%d,2,"consume ok","alice","default","gpt-4o",1234,11,22,3,true,9,5,"vip","10.0.0.1","%s","upstream-1","{""model_ratio"":1}")`, createdAt, requestID)
}

// TestCreateLogWithoutKafkaWritesTheSameStatementAsBefore is the executable
// half of the backwards-compatibility claim: with KAFKA_BROKERS empty, the
// ClickHouse deployment still buffers, and the statement a flush produces is
// byte for byte the one the pre-change tree produced for the same rows.
func TestCreateLogWithoutKafkaWritesTheSameStatementAsBefore(t *testing.T) {
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	t.Setenv("KAFKA_BROKERS", "")
	resetLogBuffer(t)

	recorder := &sqlRecorder{}
	db := openLogTestDB(t, "sqlite", "", recorder)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	require.NoError(t, createLog(sampleLogRow(1700000000, "rid-baseline-1")))
	require.NoError(t, createLog(sampleLogRow(1700000001, "rid-baseline-2")))
	flushLogBuffer()

	statements := recordedInserts(recorder)
	require.Len(t, statements, 1, "the buffered path stays batched")
	t.Logf("STATEMENT: %s", statements[0])

	expected := baselineClickHouseInsertPrefix +
		baselineClickHouseLogRow(1700000000, "rid-baseline-1") + "," +
		baselineClickHouseLogRow(1700000001, "rid-baseline-2")
	assert.Equal(t, expected, statements[0],
		"without KAFKA_BROKERS the ClickHouse write path must produce the pre-change statement byte for byte")
	assert.Zero(t, logBufferDropped.Load())
}

// TestLogKafkaPayloadRoundTripKeepsEveryField covers the billing audit
// invariant: the row travels as an opaque encoding, so the quota saturation
// marker the billing path nested under other.admin_info has to arrive byte for
// byte, and the integer fields that carry the charge have to survive intact.
func TestLogKafkaPayloadRoundTripKeepsEveryField(t *testing.T) {
	original := sampleLogRow(1700000000, "rid-saturation")
	other := NewLogOther()
	other.SetPublic("model_ratio", 1)
	other.SetAdmin("quota_saturation", map[string]any{"op": "mul", "kind": "overflow", "clamped": 2147483647})
	original.Other = other.JSONString()
	original.Quota = 2147483647
	original.PromptTokens = 2000000000
	original.CompletionTokens = 147483647
	original.CreatedAt = 1<<62 + 7

	payload, err := common.Marshal(original)
	require.NoError(t, err)

	var decoded Log
	require.NoError(t, common.Unmarshal(payload, &decoded))

	assert.Equal(t, original.Other, decoded.Other,
		"the raw JSON column has to survive the round trip byte for byte; re-marshalling it would deform the saturation marker")
	assert.Contains(t, decoded.Other, "quota_saturation")
	assert.Equal(t, original.Quota, decoded.Quota)
	assert.Equal(t, original.PromptTokens, decoded.PromptTokens)
	assert.Equal(t, original.CompletionTokens, decoded.CompletionTokens)
	assert.EqualValues(t, original.CreatedAt, decoded.CreatedAt, "created_at is an int64 and must not lose precision")
	assert.Equal(t, *original, decoded, "every column the batched INSERT writes has to survive")
}

// openIsolatedClickHouseLogDB creates a throwaway ClickHouse database and the
// production `logs` table inside it, and returns it with the function that drops
// it again.
//
// The shared fixture in log_buffer_test.go recreates the bare name `logs` in
// whatever database TEST_CLICKHOUSE_DSN points at, which is why pointing that
// variable at the backend's own log database once destroyed a live table. Every
// ClickHouse assertion in this file therefore runs in a database this helper
// created, and a DSN that names the backend's database is refused outright.
func openIsolatedClickHouseLogDB(t *testing.T) (*gorm.DB, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("TEST_CLICKHOUSE_DSN is not configured")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.NotEqual(t, "newapi_logs", strings.TrimPrefix(parsed.Path, "/"),
		"refusing to run against the database the running backend serves logs from")

	admin, err := gorm.Open(clickhouse.Open(normalizeClickHouseDSN(dsn)), &gorm.Config{})
	require.NoError(t, err)

	database := fmt.Sprintf("newapi_logs_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+database).Error)

	scoped := *parsed
	scoped.Path = "/" + database
	db, err := gorm.Open(clickhouse.Open(normalizeClickHouseDSN(scoped.String())), &gorm.Config{Logger: &sqlRecorder{}})
	require.NoError(t, err)
	require.NoError(t, db.Exec(clickHouseLogCreateTableSQL(0)).Error)

	drop := func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		if err := admin.Exec("DROP DATABASE IF EXISTS " + database).Error; err != nil {
			t.Logf("failed to drop the temporary ClickHouse database %s: %v", database, err)
		}
		if sqlDB, err := admin.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return db, drop
}

func clickHouseLogRowCount(t *testing.T, db *gorm.DB, requestID string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Raw("SELECT count() FROM logs WHERE request_id = ?", requestID).Scan(&count).Error)
	return count
}

// kafkaTestConsumer drives one logkafka.Consumer in the background and counts
// how many of the rows carrying the test's request id it saw.
type kafkaTestConsumer struct {
	seen atomic.Int64
	stop func()
}

func startKafkaTestConsumer(t *testing.T, cfg logkafka.Config, requestID string, fail error) *kafkaTestConsumer {
	t.Helper()
	consumer := &kafkaTestConsumer{}
	handle := func(rows [][]byte) error {
		for _, row := range rows {
			if bytes.Contains(row, []byte(requestID)) {
				consumer.seen.Add(1)
			}
		}
		if fail != nil {
			return fail
		}
		return handleKafkaLogBatch(rows)
	}

	client, err := logkafka.NewConsumer(cfg, handle)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()

	var once sync.Once
	consumer.stop = func() {
		once.Do(func() {
			cancel()
			<-done
			client.Close()
		})
	}
	t.Cleanup(consumer.stop)
	return consumer
}

// The four functions below let a test read the consumer group's committed
// offsets and the topic's log end offsets with the Kafka protocol itself, from a
// client that never joins the group.
//
// A second member of the group is not an option: it would take partitions away
// from the consumer under test and change the thing being observed. franz-go's
// Client.CommittedOffsets() is not an option either, because it reports the
// client's local view of its *own* assignment, so it can only answer for a
// member. Client.Request routes OffsetFetch to the group coordinator and
// ListOffsets to the partition leaders on its own, so a client with no group and
// no consume topics configured can ask both questions and disturb nothing.
func newKafkaOffsetObserver(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// The same pin the transport uses: franz-go's newest defaults do not
		// negotiate with this broker, so an unpinned observer would only ever
		// report a connection failure.
		kgo.MaxVersions(kversion.V3_7_0()),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

// isolatedKafkaTopic creates a topic this test owns, and removes it again when
// the test ends.
//
// A leg that needs a fresh consumer group to be redelivered its own rows cannot
// be run against the topic the rest of the package shares. It resets to
// earliest, so it starts at offset 0 and fetches one ConsumerBatchSize budget;
// on a topic that has accumulated other tests' rows the rows it is looking for
// are past that budget, and the leg fails for a reason that has nothing to do
// with what it asserts. An owned topic makes the starting state explicit
// instead of leaving it to how many tests ran first.
//
// Creating and deleting go through a client with no group and no consume
// topics, which is the same client the offset observers use: it disturbs
// nothing.
func isolatedKafkaTopic(t *testing.T, brokers []string) string {
	t.Helper()
	client := newKafkaOffsetObserver(t, brokers)

	name := fmt.Sprintf("new-api-logs-test-%d", time.Now().UnixNano())
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             name,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	response, err := client.Request(ctx, create)
	require.NoError(t, err)
	created, ok := response.(*kmsg.CreateTopicsResponse)
	require.True(t, ok, "create topics answered with %T", response)
	require.Len(t, created.Topics, 1)
	require.NoError(t, kerr.ErrorForCode(created.Topics[0].ErrorCode),
		"the topic this test owns could not be created, so the leg would run against the shared one and assert the wrong thing")

	// Registered after the client's own cleanup so that it runs first: cleanups
	// are last-in-first-out and the delete needs a live client.
	t.Cleanup(func() {
		request := kmsg.NewPtrDeleteTopicsRequest()
		request.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &name}}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := client.Request(ctx, request); err != nil {
			t.Logf("failed to delete the test topic %s: %v", name, err)
		}
	})
	return name
}

// observedTopicPartitions asks the broker which partitions the topic has. It is
// what gives "every partition" a boundary instead of being a figure of speech.
func observedTopicPartitions(ctx context.Context, client *kgo.Client, topic string) ([]int32, error) {
	name := topic
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("metadata request answered with %T", response)
	}
	for _, described := range metadata.Topics {
		if described.Topic != nil && *described.Topic == topic {
			partitions := make([]int32, 0, len(described.Partitions))
			for _, partition := range described.Partitions {
				partitions = append(partitions, partition.Partition)
			}
			return partitions, nil
		}
	}
	return nil, fmt.Errorf("the metadata response does not describe topic %q", topic)
}

// observedGroupOffsets is CURRENT-OFFSET for every partition: the offset a
// member of the group resumes at. A partition the group never committed reports
// -1, which is exactly the state a missing commit leaves behind.
func observedGroupOffsets(ctx context.Context, client *kgo.Client, group, topic string, partitions []int32) (map[int32]int64, error) {
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: partitions}}
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	fetched, ok := response.(*kmsg.OffsetFetchResponse)
	if !ok {
		return nil, fmt.Errorf("offset fetch answered with %T", response)
	}
	offsets := make(map[int32]int64, len(partitions))
	for _, described := range fetched.Topics {
		if described.Topic != topic {
			continue
		}
		for _, partition := range described.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return nil, err
			}
			offsets[partition.Partition] = partition.Offset
		}
	}
	return offsets, nil
}

// observedLogEndOffsets is LOG-END-OFFSET for every partition: the offset the
// next row written to that partition will carry.
func observedLogEndOffsets(ctx context.Context, client *kgo.Client, topic string, partitions []int32) (map[int32]int64, error) {
	described := kmsg.NewListOffsetsRequestTopic()
	described.Topic = topic
	for _, partition := range partitions {
		requested := kmsg.NewListOffsetsRequestTopicPartition()
		requested.Partition = partition
		// -1 is "latest", so the answer is the log end offset.
		requested.Timestamp = -1
		// -1 is "no epoch", which is what a plain client has to say; 0 would
		// claim to know the leader's epoch and can be fenced.
		requested.CurrentLeaderEpoch = -1
		described.Partitions = append(described.Partitions, requested)
	}

	request := kmsg.NewPtrListOffsetsRequest()
	request.ReplicaID = -1
	request.Topics = []kmsg.ListOffsetsRequestTopic{described}
	response, err := client.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	listed, ok := response.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("list offsets answered with %T", response)
	}
	offsets := make(map[int32]int64, len(partitions))
	for _, describedTopic := range listed.Topics {
		if describedTopic.Topic != topic {
			continue
		}
		for _, partition := range describedTopic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return nil, err
			}
			offsets[partition.Partition] = partition.Offset
		}
	}
	return offsets, nil
}

// observedGroupHasJoinedMember reports whether the group has a member that has
// finished joining, which is the moment franz-go sets that member's fetch
// position from the committed offsets.
//
// It is what keeps the negative assertion in phase C from being true for the
// wrong reason: a member that has not joined yet has also "seen nothing", and a
// fixed wait cannot tell the two apart -- which is the defect phase C used to
// have.
func observedGroupHasJoinedMember(ctx context.Context, client *kgo.Client, group string) (bool, error) {
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{group}
	response, err := client.Request(ctx, request)
	if err != nil {
		return false, err
	}
	described, ok := response.(*kmsg.DescribeGroupsResponse)
	if !ok {
		return false, fmt.Errorf("describe groups answered with %T", response)
	}
	for _, reported := range described.Groups {
		if reported.Group == group {
			return reported.State == "Stable" && len(reported.Members) > 0, nil
		}
	}
	return false, nil
}

// TestLogKafkaConsumerCommitsOnlyAfterTheLogDatabaseAcceptedTheBatch is the
// ordering proof for the consumer, and the reason this slice is worth shipping:
// an offset is committed only once the rows at or before it are in ClickHouse.
//
// It runs the same group id through three consumers. The first refuses every
// batch; the second starts where the group left off and must therefore receive
// the rows again, which is only true if the first one committed nothing. The
// third sees nothing, which is only true if the second one committed after a
// successful write. Committing before the write inverts the second assertion;
// dropping the commit after the write inverts the third.
func TestLogKafkaConsumerCommitsOnlyAfterTheLogDatabaseAcceptedTheBatch(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not configured")
	}
	db, dropDatabase := openIsolatedClickHouseLogDB(t)
	t.Cleanup(dropDatabase)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	requestID := fmt.Sprintf("rid-kafka-order-%d", time.Now().UnixNano())
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("KAFKA_LOG_GROUP_ID", fmt.Sprintf("new-api-log-consumer-test-%d", time.Now().UnixNano()))
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "200")
	t.Setenv("KAFKA_LOG_REQUEST_TIMEOUT_MS", "5000")
	t.Setenv("KAFKA_LOG_DELIVERY_TIMEOUT_MS", "10000")
	// The producer owns a spool directory now, and it creates it at startup. The
	// tests that build one have to say where it goes, or every run would leave a
	// segment directory behind in the package directory.
	t.Setenv("KAFKA_LOG_SPOOL_DIR", t.TempDir())

	cfg, err := logkafka.LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled())
	// Phase A asserts that a fresh group is redelivered a batch the log database
	// refused, and a fresh group resets to earliest: it fetches one
	// ConsumerBatchSize budget from offset 0. On the topic this package shares,
	// that budget is spent on other tests' rows long before the group reaches its
	// own, so this leg runs on a topic it owns rather than on one whose size
	// depends on what ran first.
	topic := isolatedKafkaTopic(t, cfg.Brokers)
	cfg.Topic = topic
	cfg.FlushInterval = logFlushInterval()

	producer, err := logkafka.NewProducer(cfg)
	require.NoError(t, err)
	logKafkaProducer.Store(producer)
	t.Cleanup(func() { logKafkaProducer.Store(nil) })
	output := captureSysErrors(t)

	const rows = 25
	// One row carries the billing path's saturation marker, so the assertion
	// below can compare the bytes that went in with the bytes ClickHouse holds.
	saturated := NewLogOther()
	saturated.SetPublic("model_ratio", 1)
	saturated.SetAdmin("quota_saturation", map[string]any{"op": "mul", "kind": "overflow", "clamped": 2147483647})
	saturatedOther := saturated.JSONString()

	for i := range rows {
		row := sampleLogRow(1700000000+int64(i), requestID)
		if i == 0 {
			row.Other = saturatedOther
		}
		require.NoError(t, createLog(row))
	}
	// Closing flushes, which is what makes "the rows are in the topic" an
	// observed precondition rather than a sleep. Anything the flush could not
	// place is reported, so its absence is the evidence that it placed all of it.
	producer.Close()
	require.NotContains(t, output.String(), "could not deliver",
		"every row must have reached the broker before the consumer part of the experiment starts")

	// Phase A: the log database refuses every batch.
	refused := startKafkaTestConsumer(t, cfg, requestID, errors.New("the log database is down"))
	require.Eventually(t, func() bool { return refused.seen.Load() > 0 }, 30*time.Second, 100*time.Millisecond,
		"the consumer must have reached the topic")
	time.Sleep(time.Second)
	assert.Zero(t, clickHouseLogRowCount(t, db, requestID), "a refused batch must not reach ClickHouse")
	refused.stop()

	// Phase B: the same group id, resuming where phase A left off.
	accepted := startKafkaTestConsumer(t, cfg, requestID, nil)
	require.Eventually(t, func() bool { return accepted.seen.Load() >= rows }, 60*time.Second, 100*time.Millisecond,
		"a batch whose write failed has to be delivered again; committing before the write would have hidden it")
	require.Eventually(t, func() bool { return clickHouseLogRowCount(t, db, requestID) >= rows }, 60*time.Second, 100*time.Millisecond,
		"the delivered batch has to reach ClickHouse")
	// The offset commit runs immediately after the INSERT, and it takes the run
	// context. Stopping inside that window would cancel the commit and leave the
	// rows to be delivered again, which is the at-least-once cost the design
	// accepts but would make phase C below assert the wrong thing.
	time.Sleep(time.Second)
	assert.GreaterOrEqual(t, clickHouseLogRowCount(t, db, requestID), int64(rows))

	// P8 / the billing audit invariant: the raw JSON column has to arrive byte
	// for byte, because that is where the quota saturation marker the billing
	// path nested under other.admin_info lives. Re-marshalling it anywhere along
	// produce -> consume -> ClickHouse would make a billing anomaly disappear
	// from the admin log view without a trace.
	var storedOther string
	require.NoError(t, db.Raw("SELECT other FROM logs WHERE request_id = ? ORDER BY created_at LIMIT 1", requestID).Scan(&storedOther).Error)
	assert.Equal(t, saturatedOther, storedOther,
		"the saturation marker must survive the trip through the topic byte for byte")
	assert.Contains(t, storedOther, "quota_saturation")
	accepted.stop()

	// Phase C: the same group id again, and now there is nothing left.
	//
	// "A new member sees nothing" is only evidence about the commit if the
	// member is demonstrably up and fetching. A fresh member needs about 3.4 s
	// to join, be assigned its partitions and fetch from the committed offset,
	// so asserting two seconds after starting it was equally true of a member
	// that was still joining -- and deleting the commit left this assertion
	// passing. The state is polled here instead of waited for: on every
	// partition, CURRENT-OFFSET must equal LOG-END-OFFSET. A member resumes at
	// the committed offset, so that equality *is* "the member receives nothing",
	// and it is false the moment the commit is missing (the group then reports
	// -1, never having committed anything).
	observer := newKafkaOffsetObserver(t, cfg.Brokers)

	partitionsCtx, cancelPartitions := context.WithTimeout(context.Background(), 10*time.Second)
	partitions, err := observedTopicPartitions(partitionsCtx, observer, topic)
	cancelPartitions()
	require.NoError(t, err)
	require.NotEmpty(t, partitions, "the topic has to have partitions for 'every partition' to have a boundary")

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		committed, err := observedGroupOffsets(ctx, observer, cfg.GroupID, topic, partitions)
		if err != nil {
			return false
		}
		end, err := observedLogEndOffsets(ctx, observer, topic, partitions)
		if err != nil {
			return false
		}
		for _, partition := range partitions {
			// A partition the group has never read from reports no committed
			// offset at all (-1). That is the same place as "resumed at the end"
			// only while the partition is empty, which the log end offset then
			// reports as 0 -- the member resets to earliest, which is also 0. A
			// partition that holds rows and was never committed is the state a
			// missing commit leaves behind, and it is rejected here.
			if committed[partition] < 0 {
				if end[partition] != 0 {
					return false
				}
				continue
			}
			if committed[partition] != end[partition] {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond,
		"the consumer group never reached LOG-END-OFFSET on every partition, which is what a write without a commit leaves behind: the rows are in the log database, so their offsets were not committed after the successful write")

	settled := startKafkaTestConsumer(t, cfg, requestID, nil)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		joined, err := observedGroupHasJoinedMember(ctx, observer, cfg.GroupID)
		return err == nil && joined
	}, 30*time.Second, 100*time.Millisecond,
		"the fresh consumer never finished joining the group, so 'it saw nothing' would not yet be a statement about the committed offsets")

	// The member has joined and its fetch position is the log end, so there is
	// nothing to deliver. This window is what makes the negative claim cover the
	// same span a missing commit would have delivered rows in; the causal claim
	// itself is the offset equality polled above.
	require.Never(t, func() bool { return settled.seen.Load() > 0 }, 3*time.Second, 100*time.Millisecond,
		"the offsets must have been committed after the successful write, not before it")
	settled.stop()
}

// TestLogKafkaHarness drives the experiments that need a process of their own:
// a hard kill with rows already acknowledged by the broker, and two processes
// sharing one consumer group. It is not a unit test. Compile it once with
//
//	go test -c -o harness.test ./model/
//
// and start it with KAFKA_HARNESS_MODE set; without that variable it skips, so
// an ordinary test run pays nothing for it.
//
//	inject   write HARNESS_COUNT rows through createLog, then hold for
//	         HARNESS_HOLD_SECONDS so the caller can hard-kill the process
//	consume  run the consumer for HARNESS_SECONDS and then shut down
//	count    print how many rows under HARNESS_PREFIX reached ClickHouse, and
//	         how many of them are distinguishable
func TestLogKafkaHarness(t *testing.T) {
	mode := os.Getenv("KAFKA_HARNESS_MODE")
	if mode == "" {
		t.Skip("KAFKA_HARNESS_MODE is not set; this is an experiment driver, not a unit test")
	}
	common.InitEnv()
	if spoolDir := os.Getenv("KAFKA_LOG_SPOOL_DIR"); spoolDir == "" {
		// The spool is a real directory, so a harness run says where it goes
		// rather than scattering segments through the working directory the test
		// binary happened to start in.
		t.Setenv("KAFKA_LOG_SPOOL_DIR", t.TempDir())
	}
	if os.Getenv("HARNESS_SKIP_LOGDB") != "" {
		// The producer path needs the log database *type* to be ClickHouse but
		// never touches the handle, so a producer-only leg can run while the
		// database is down, which is what the outage experiment needs.
		common.SetLogDatabaseType(common.DatabaseTypeClickHouse)
		initCol()
	} else {
		require.NoError(t, InitLogDB())
	}
	prefix := os.Getenv("HARNESS_PREFIX")
	like := prefix + "%"

	switch mode {
	case "count":
		var total, distinct int64
		require.NoError(t, LOG_DB.Raw("SELECT count() FROM logs WHERE request_id LIKE ?", like).Scan(&total).Error)
		require.NoError(t, LOG_DB.Raw("SELECT uniqExact((created_at, request_id, content)) FROM logs WHERE request_id LIKE ?", like).Scan(&distinct).Error)
		fmt.Printf("HARNESS count=%d distinct=%d\n", total, distinct)
	case "inject":
		if os.Getenv("HARNESS_SYNC") == "" && os.Getenv("HARNESS_PRODUCER_ONLY") != "" {
			// Producer only, no consumer: this process fills the topic and
			// nothing drains it, which is what isolates "the rows reached the
			// broker" from "the rows reached ClickHouse" in the crash and
			// outage experiments.
			cfg, err := logkafka.LoadConfig()
			require.NoError(t, err)
			require.NoError(t, startLogKafkaProducer(cfg))
			StartLogFlush()
		} else if os.Getenv("HARNESS_SYNC") == "" {
			require.NoError(t, StartLogKafka())
			StartLogFlush()
		} else {
			// The contrast leg: the same injection through the synchronous
			// write, with Kafka off and the log database not ClickHouse, which
			// is the path createLog takes before this change on every engine
			// except ClickHouse.
			useLogDatabase(t, openLogTestDB(t, "sqlite", "", &sqlRecorder{}), common.DatabaseTypeSQLite)
		}
		count, err := strconv.Atoi(os.Getenv("HARNESS_COUNT"))
		require.NoError(t, err)
		started := time.Now()
		for i := range count {
			require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), fmt.Sprintf("%s-%d", prefix, i))))
		}
		fmt.Printf("HARNESS injected=%d elapsed_ms=%d\n", count, time.Since(started).Milliseconds())
		hold, err := strconv.Atoi(os.Getenv("HARNESS_HOLD_SECONDS"))
		require.NoError(t, err)
		if hold > 0 {
			// Hold with nothing shut down, so the caller can hard-kill this
			// process while the rows are already acknowledged by the broker.
			deadline := time.Now().Add(time.Duration(hold) * time.Second)
			every, _ := strconv.Atoi(os.Getenv("HARNESS_INJECT_EVERY_MS"))
			round := 0
			for time.Now().Before(deadline) {
				if every <= 0 {
					time.Sleep(time.Duration(hold) * time.Second)
					break
				}
				// Keep producing for the length of the outage, so the consumer
				// group's lag has something to grow against.
				time.Sleep(time.Duration(every) * time.Millisecond)
				round++
				for i := range count {
					require.NoError(t, createLog(sampleLogRow(1700000000+int64(i), fmt.Sprintf("%s-r%d-%d", prefix, round, i))))
				}
				fmt.Printf("HARNESS streamed=%d\n", count)
			}
		}
		StopLogKafka()
		StopLogFlush()
		fmt.Println("HARNESS shutdown=graceful")
	case "consume":
		if delay, err := strconv.Atoi(os.Getenv("HARNESS_START_DELAY_SECONDS")); err == nil && delay > 0 {
			// The log database is opened above, while it is still up; the
			// delay is what lets the outage experiment take it down before this
			// process starts consuming.
			time.Sleep(time.Duration(delay) * time.Second)
		}
		require.NoError(t, StartLogKafka())
		seconds, err := strconv.Atoi(os.Getenv("HARNESS_SECONDS"))
		require.NoError(t, err)
		time.Sleep(time.Duration(seconds) * time.Second)
		StopLogKafka()
		fmt.Println("HARNESS consumer=stopped")
	default:
		t.Fatalf("unknown KAFKA_HARNESS_MODE %q", mode)
	}
}
