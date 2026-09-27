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
	assert.Zero(t, logBufferDropped)
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
	topic := common.GetEnvOrDefaultString("TEST_KAFKA_LOG_TOPIC", "new-api-logs-test")

	db, dropDatabase := openIsolatedClickHouseLogDB(t)
	t.Cleanup(dropDatabase)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	stopLogKafkaForTest(t)
	t.Cleanup(func() { stopLogKafkaForTest(t) })

	requestID := fmt.Sprintf("rid-kafka-order-%d", time.Now().UnixNano())
	t.Setenv("KAFKA_BROKERS", brokers)
	t.Setenv("KAFKA_LOG_TOPIC", topic)
	t.Setenv("KAFKA_LOG_GROUP_ID", fmt.Sprintf("new-api-log-consumer-test-%d", time.Now().UnixNano()))
	t.Setenv("LOG_FLUSH_INTERVAL_MS", "200")
	t.Setenv("KAFKA_LOG_REQUEST_TIMEOUT_MS", "5000")
	t.Setenv("KAFKA_LOG_DELIVERY_TIMEOUT_MS", "10000")

	cfg, err := logkafka.LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled())
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
	settled := startKafkaTestConsumer(t, cfg, requestID, nil)
	time.Sleep(2 * time.Second)
	assert.Zero(t, settled.seen.Load(),
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
