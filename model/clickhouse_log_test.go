package model

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestIsClickHouseDSN(t *testing.T) {
	cases := []struct {
		dsn  string
		want bool
	}{
		{"clickhouse://default:pass@localhost:9000/logs", true},
		{"tcp://localhost:9000/logs", true},
		{"http://localhost:8123/logs", true},
		{"https://localhost:8443/logs", true},
		{"postgres://root:pass@localhost:5432/db", false},
		{"postgresql://root:pass@localhost:5432/db", false},
		{"root:pass@tcp(localhost:3306)/db", false},
		{"local", false},
		{"", false},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, isClickHouseDSN(c.dsn), "dsn=%q", c.dsn)
	}
}

func TestNormalizeClickHouseDSN(t *testing.T) {
	// https without secure gets secure=true appended
	normalized := normalizeClickHouseDSN("https://default:pass@localhost:8443/logs")
	assert.Contains(t, normalized, "secure=true")
	assert.True(t, strings.HasPrefix(normalized, "https://"))

	// https that already specifies secure is left untouched
	assert.Equal(t,
		"https://localhost:8443/logs?secure=false",
		normalizeClickHouseDSN("https://localhost:8443/logs?secure=false"),
	)

	// non-https schemes are returned verbatim
	assert.Equal(t, "clickhouse://localhost:9000/logs", normalizeClickHouseDSN("clickhouse://localhost:9000/logs"))
	assert.Equal(t, "tcp://localhost:9000/logs", normalizeClickHouseDSN("tcp://localhost:9000/logs"))
}

func TestChooseDBRejectsClickHouseForMainDatabase(t *testing.T) {
	original, had := os.LookupEnv("SQL_DSN")
	t.Cleanup(func() {
		if had {
			require.NoError(t, os.Setenv("SQL_DSN", original))
		} else {
			require.NoError(t, os.Unsetenv("SQL_DSN"))
		}
	})
	require.NoError(t, os.Setenv("SQL_DSN", "clickhouse://default:pass@localhost:9000/logs"))

	db, dbType, err := chooseDB("SQL_DSN", false)
	require.Error(t, err)
	assert.Nil(t, db)
	assert.Equal(t, common.DatabaseType(""), dbType)
	assert.Contains(t, err.Error(), "does not support ClickHouse")
}

func TestClickHouseLogTTLExpression(t *testing.T) {
	assert.Equal(t, "", clickHouseLogTTLExpression(0))
	assert.Equal(t, "", clickHouseLogTTLExpression(-5))
	assert.Equal(t, "toDateTime(created_at) + INTERVAL 30 DAY DELETE", clickHouseLogTTLExpression(30))
}

func TestClickHouseLogTTLClause(t *testing.T) {
	assert.Equal(t, "", clickHouseLogTTLClause(0))
	assert.Equal(t, "\nTTL toDateTime(created_at) + INTERVAL 7 DAY DELETE", clickHouseLogTTLClause(7))
}

func TestClickHouseLogCreateTableSQL(t *testing.T) {
	withoutTTL := clickHouseLogCreateTableSQL(0)
	assert.Contains(t, withoutTTL, "CREATE TABLE IF NOT EXISTS logs")
	assert.Contains(t, withoutTTL, "ENGINE = MergeTree()")
	assert.Contains(t, withoutTTL, "PARTITION BY toYYYYMM(toDateTime(created_at))")
	assert.Contains(t, withoutTTL, "ORDER BY (created_at, request_id)")
	assert.NotContains(t, withoutTTL, "TTL ")

	// quota is a 64-bit column, matching the domain it carries. A 32-bit column
	// would not reject an out-of-range insert: ClickHouse wraps it silently
	// (measured on 24.8.14.39, 3000000000 reads back as -1294967296), so the
	// narrow type was a silent-corruption trap rather than a loud failure. The
	// quota domain is clamped to int32 today, which is the only reason it was
	// unreachable.
	assert.Contains(t, withoutTTL, "quota Int64 DEFAULT 0")
	assert.NotContains(t, withoutTTL, "quota Int32")

	// Fresh installs declare the same pruning indexes the upgrade path adds, so
	// both starting points answer the readers' filters the same way.
	assert.Contains(t, withoutTTL, "INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1")
	assert.Contains(t, withoutTTL, "INDEX idx_type type TYPE bloom_filter(0.01) GRANULARITY 1")

	withTTL := clickHouseLogCreateTableSQL(30)
	assert.Contains(t, withTTL, "ORDER BY (created_at, request_id)")
	assert.Contains(t, withTTL, "TTL toDateTime(created_at) + INTERVAL 30 DAY DELETE")
	assert.Contains(t, withTTL, "quota Int64 DEFAULT 0")
	assert.Contains(t, withTTL, "INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1")
}

func TestClickHouseLogSkippingIndexClause(t *testing.T) {
	assert.Equal(t,
		"INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1",
		clickHouseLogSkippingIndexClause(clickHouseLogSkippingIndexes[0]),
	)
	assert.Equal(t,
		"INDEX idx_type type TYPE bloom_filter(0.01) GRANULARITY 1",
		clickHouseLogSkippingIndexClause(clickHouseLogSkippingIndexes[1]),
	)
}

// TestClickHouseLogAddIndexSQL pins the statement the upgrade path issues to its
// idempotent form. The ClickHouse integration tests prove the behaviour; this
// keeps the contract checked when no ClickHouse DSN is configured.
func TestClickHouseLogAddIndexSQL(t *testing.T) {
	assert.Equal(t,
		"ALTER TABLE logs ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 1",
		clickHouseLogAddIndexSQL(clickHouseLogSkippingIndexes[0]),
	)
}

// legacyClickHouseLogsDDL is the `logs` shape shipped before this slice: a
// 32-bit `quota` and no skipping indexes. It is frozen on purpose — deriving it
// from the current DDL would make the upgrade tests describe the code instead
// of the upgrade.
const legacyClickHouseLogsDDL = `
CREATE TABLE logs (
	id Int64 DEFAULT 0,
	user_id Int32 DEFAULT 0,
	created_at Int64 DEFAULT 0,
	type Int32 DEFAULT 0,
	content String DEFAULT '',
	username String DEFAULT '',
	token_name String DEFAULT '',
	model_name String DEFAULT '',
	quota Int32 DEFAULT 0,
	prompt_tokens Int32 DEFAULT 0,
	completion_tokens Int32 DEFAULT 0,
	use_time Int32 DEFAULT 0,
	is_stream UInt8 DEFAULT 0,
	channel_id Int32 DEFAULT 0,
	token_id Int32 DEFAULT 0,
	` + "`group`" + ` String DEFAULT '',
	ip String DEFAULT '',
	request_id String DEFAULT '',
	upstream_request_id String DEFAULT '',
	other String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(created_at))
ORDER BY (created_at, request_id)`

// openClickHouseLegacyLogTestDB opens the real ClickHouse instance with an
// empty `logs` table in the shape the previous release created.
func openClickHouseLegacyLogTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)
	require.NoError(t, db.Exec(legacyClickHouseLogsDDL).Error)
	return db
}

// clickHouseLogsShape renders the structural identity of `logs`: its columns in
// order, its skipping indexes, and its partition / sorting key. Two tables that
// render the same shape answer the same queries with the same pruning.
func clickHouseLogsShape(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var columns []struct {
		Name string `gorm:"column:name"`
		Type string `gorm:"column:type"`
	}
	require.NoError(t, db.Raw(
		"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'logs' ORDER BY position",
	).Scan(&columns).Error)
	var indexes []struct {
		Name        string `gorm:"column:name"`
		Expr        string `gorm:"column:expr"`
		Type        string `gorm:"column:type"`
		Granularity int    `gorm:"column:granularity"`
	}
	require.NoError(t, db.Raw(
		"SELECT name, expr, type, granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'logs' ORDER BY name",
	).Scan(&indexes).Error)
	var keys []struct {
		PartitionKey string `gorm:"column:partition_key"`
		SortingKey   string `gorm:"column:sorting_key"`
	}
	require.NoError(t, db.Raw(
		"SELECT partition_key, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'logs'",
	).Scan(&keys).Error)
	require.Len(t, keys, 1)

	var builder strings.Builder
	for _, column := range columns {
		fmt.Fprintf(&builder, "column %s %s\n", column.Name, column.Type)
	}
	for _, index := range indexes {
		fmt.Fprintf(&builder, "index %s %s %s GRANULARITY %d\n", index.Name, index.Expr, index.Type, index.Granularity)
	}
	fmt.Fprintf(&builder, "partition %s\nsorting %s\n", keys[0].PartitionKey, keys[0].SortingKey)
	return builder.String()
}

func clickHouseLogsQuotaType(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var columns []struct {
		Type string `gorm:"column:type"`
	}
	require.NoError(t, db.Raw(
		"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'logs' AND name = 'quota'",
	).Scan(&columns).Error)
	require.Len(t, columns, 1, "logs.quota must exist")
	return columns[0].Type
}

type clickHouseQuotaRow struct {
	Id     int64 `gorm:"column:id"`
	UserId int   `gorm:"column:user_id"`
	Quota  int64 `gorm:"column:quota"`
}

// clickHouseLogSkippingIndexCount counts the pruning indexes on `logs`, so a
// migration run can be asserted not to have added a duplicate one.
func clickHouseLogSkippingIndexCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Raw(
		"SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'logs'",
	).Scan(&count).Error)
	return count
}

// TestClickHouseLogUpgradeFromLegacyTable proves on the real ClickHouse instance
// that migrating a table created by the previous release widens `quota` to
// Int64 while leaving every existing row byte-for-byte the same, and that the
// migration is a no-op when it runs again.
func TestClickHouseLogUpgradeFromLegacyTable(t *testing.T) {
	db := openClickHouseLegacyLogTestDB(t)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "0")

	// The extreme ends of the old 32-bit range are the values the widening must
	// carry over unchanged.
	require.NoError(t, db.Exec(`
		INSERT INTO logs (id, user_id, created_at, type, quota, request_id) VALUES
		(1, 11, 1700000000, 2, 2147483647, 'legacy-max'),
		(2, 22, 1700000060, 2, -2147483648, 'legacy-min'),
		(3, 33, 1700000120, 5, 0, 'legacy-zero'),
		(4, 44, 1700000180, 2, 123456789, 'legacy-mid')`).Error)

	var before []clickHouseQuotaRow
	require.NoError(t, db.Raw("SELECT id, user_id, quota FROM logs ORDER BY id").Scan(&before).Error)
	require.Len(t, before, 4)

	require.NoError(t, migrateClickHouseLogDB())

	assert.Equal(t, "Int64", clickHouseLogsQuotaType(t, db))
	var after []clickHouseQuotaRow
	require.NoError(t, db.Raw("SELECT id, user_id, quota FROM logs ORDER BY id").Scan(&after).Error)
	assert.Equal(t, before, after, "widening quota must not change any existing value")

	// The upgrade path and the fresh-install DDL must agree, and the skipping
	// indexes must cover the parts that already existed.
	shapeAfterUpgrade := clickHouseLogsShape(t, db)
	assert.Contains(t, shapeAfterUpgrade, "index idx_user_id user_id bloom_filter GRANULARITY 1")
	assert.Contains(t, shapeAfterUpgrade, "index idx_type type bloom_filter GRANULARITY 1")

	// Running the migration again must change neither the structure nor the rows.
	require.NoError(t, migrateClickHouseLogDB())
	assert.Equal(t, shapeAfterUpgrade, clickHouseLogsShape(t, db))
	var afterSecondRun []clickHouseQuotaRow
	require.NoError(t, db.Raw("SELECT id, user_id, quota FROM logs ORDER BY id").Scan(&afterSecondRun).Error)
	assert.Equal(t, before, afterSecondRun)

	// The same migration applies a configured retention to the upgraded table.
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "7")
	require.NoError(t, migrateClickHouseLogDB())
	hasTTL, err := clickHouseLogTableHasTTL()
	require.NoError(t, err)
	assert.True(t, hasTTL, "a configured TTL must reach an existing table")
}

// TestClickHouseLogFreshInstallMatchesUpgradedTable proves the two supported
// starting points converge: a database the current DDL created and a database
// upgraded from the previous release end up structurally identical.
func TestClickHouseLogFreshInstallMatchesUpgradedTable(t *testing.T) {
	db := openClickHouseLegacyLogTestDB(t)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "0")

	// Upgraded starting point.
	require.NoError(t, migrateClickHouseLogDB())
	upgraded := clickHouseLogsShape(t, db)

	// Fresh install: the startup migration runs against a database that has no
	// `logs` table at all.
	require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)
	require.NoError(t, migrateClickHouseLogDB())
	fresh := clickHouseLogsShape(t, db)

	assert.Equal(t, fresh, upgraded)
	assert.Equal(t, "Int64", clickHouseLogsQuotaType(t, db))
}

// TestClickHouseLogSkippingIndexesSurviveRepeatRun proves on the real ClickHouse
// instance that applying the pruning indexes twice cannot fail. The probe in
// ensureClickHouseLogSkippingIndexes and the ALTER it guards are separate
// statements, so two masters starting in the same window (NODE_TYPE unset means
// every node is a master) can both decide an index is missing and both add it.
// A bare ADD INDEX answers the loser with "index with this name already exists"
// (Code 44), which migrateClickHouseLogDB returns to main's FatalLog — a node
// that dies at startup. That race is between processes, so -race cannot see it;
// only the idempotence of the statement removes it.
func TestClickHouseLogSkippingIndexesSurviveRepeatRun(t *testing.T) {
	db := openClickHouseLegacyLogTestDB(t)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "0")

	require.NoError(t, migrateClickHouseLogDB())
	shapeAfterFirstRun := clickHouseLogsShape(t, db)
	indexCountAfterFirstRun := clickHouseLogSkippingIndexCount(t, db)
	require.Equal(t, int64(len(clickHouseLogSkippingIndexes)), indexCountAfterFirstRun)

	// Re-running the migration must neither fail nor add a second index.
	require.NoError(t, migrateClickHouseLogDB())
	assert.Equal(t, shapeAfterFirstRun, clickHouseLogsShape(t, db))
	assert.Equal(t, indexCountAfterFirstRun, clickHouseLogSkippingIndexCount(t, db))

	// The losing side of the race: the index is already there, and the ALTER is
	// issued anyway, exactly as the migration would if it had probed a moment
	// earlier.
	for _, index := range clickHouseLogSkippingIndexes {
		require.NoError(t, db.Exec(clickHouseLogAddIndexSQL(index)).Error)
		require.NoError(t, db.Exec("ALTER TABLE logs MATERIALIZE INDEX "+index.Name).Error)
	}
	assert.Equal(t, shapeAfterFirstRun, clickHouseLogsShape(t, db))
	assert.Equal(t, indexCountAfterFirstRun, clickHouseLogSkippingIndexCount(t, db))
}

// clickHouseExplainSkip runs EXPLAIN indexes=1 and returns the granules the
// named skipping index left in the reading range, plus the granules it saw.
func clickHouseExplainSkip(t *testing.T, db *gorm.DB, query string, indexName string) (int64, int64) {
	t.Helper()
	rows, err := db.Raw("EXPLAIN indexes=1 " + query).Rows()
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	selected, total := int64(-1), int64(0)
	seenIndex := false
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		trimmed := strings.TrimSpace(line)
		if trimmed == "Name: "+indexName {
			seenIndex = true
			continue
		}
		if !seenIndex || !strings.HasPrefix(trimmed, "Granules:") {
			continue
		}
		_, err := fmt.Sscanf(trimmed, "Granules: %d/%d", &selected, &total)
		require.NoError(t, err)
		break
	}
	require.NoError(t, rows.Err())
	require.Truef(t, seenIndex, "EXPLAIN did not use skipping index %s", indexName)
	require.NotEqual(t, int64(-1), selected)
	return selected, total
}

// TestClickHouseLogSkippingIndexPrunesGranules proves on the real ClickHouse
// instance that the planner reads the new indexes, and measures how much they
// actually prune. A granule-level index can only skip a granule whose values
// are narrow, and neither filtered column is correlated with the
// (created_at, request_id) sort key, so the numbers below are the honest ceiling
// for this data shape, not a promise that the filter became cheap.
func TestClickHouseLogSkippingIndexPrunesGranules(t *testing.T) {
	db := openClickHouseLogTestDB(t, &sqlRecorder{})
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)

	// 200k rows appended in time order from a 5k-user pool: the same shape real
	// logs have, with user_id unrelated to the append order.
	require.NoError(t, db.Exec(`
		INSERT INTO logs (id, user_id, created_at, type, request_id, quota)
		SELECT number, (cityHash64(number) % 5000) + 1, now() - 90*86400 + number,
		       (cityHash64(number * 7) % 8) + 1, toString(number), 0
		FROM numbers(200000)`).Error)

	selected, total := clickHouseExplainSkip(t, db, "SELECT count() FROM logs WHERE user_id = 42", "idx_user_id")
	require.Greater(t, total, int64(10), "the probe needs enough granules to be meaningful")
	assert.Less(t, selected, total, "user_id must prune at least one granule")
	t.Logf("user_id: %d/%d granules selected (%.1f%% pruned)", selected, total, 100*float64(total-selected)/float64(total))

	typeSelected, typeTotal := clickHouseExplainSkip(t, db, "SELECT count() FROM logs WHERE type = 3", "idx_type")
	t.Logf("type: %d/%d granules selected (%.1f%% pruned) — a value that occurs in far more than one granule's worth of rows (here eight values over the range, so ~1/8 of every granule) lands in essentially every granule, and no granule-level index can prune that",
		typeSelected, typeTotal, 100*float64(typeTotal-typeSelected)/float64(typeTotal))
}

// TestClickHouseLogTTLRetention proves on the real ClickHouse instance that a
// configured TTL reclaims expired monthly partitions by dropping them — no
// mutation, no part rewrite — and that leaving the TTL unset deletes nothing.
func TestClickHouseLogTTLRetention(t *testing.T) {
	const probeRows = 9000

	// 9000 rows over the last 90 days, 100 per day. "Expired" is measured at a
	// seven-day margin so no assertion rides on the one-second granularity of
	// now(), which is what the probe rows are stamped with.
	const insertProbe = `
		INSERT INTO logs (id, user_id, created_at, type, request_id, quota)
		SELECT number, 1, toInt64(now() - (number % 90) * 86400), 2, toString(number), 100
		FROM numbers(9000)`
	const expiredExpr = "created_at <= now() - 7 * 86400"

	cases := []struct {
		name          string
		ttlDays       int
		wantRows      int64
		wantExpired   int64
		wantNoRewrite bool
	}{
		{"ttl configured drops expired partitions", 1, 100, 0, true},
		{"ttl unset deletes nothing", 0, probeRows, 8300, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openClickHouseLogTestDB(t, &sqlRecorder{})
			useLogDatabase(t, db, common.DatabaseTypeClickHouse)
			require.NoError(t, db.Exec("DROP TABLE IF EXISTS logs").Error)
			require.NoError(t, db.Exec(clickHouseLogCreateTableSQL(tc.ttlDays)).Error)
			require.NoError(t, db.Exec(insertProbe).Error)

			require.NoError(t, db.Exec("OPTIMIZE TABLE logs FINAL").Error)

			var remaining, expired int64
			require.NoError(t, db.Raw(
				"SELECT count(), countIf("+expiredExpr+") FROM logs",
			).Row().Scan(&remaining, &expired))
			assert.Equal(t, tc.wantRows, remaining)
			assert.Equal(t, tc.wantExpired, expired)

			if !tc.wantNoRewrite {
				return
			}
			// Reclaiming expired data must drop whole parts: a mutation would
			// mean ClickHouse rewrote the table instead of discarding it.
			var mutations int64
			require.NoError(t, db.Raw(
				"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'logs'",
			).Scan(&mutations).Error)
			assert.Zero(t, mutations, "TTL must drop parts, not rewrite them")

			// The expired monthly partitions are the ones holding rows; the
			// OPTIMIZE leaves them empty for the background cleaner to unlink,
			// so the timing-independent form of "the partitions were dropped" is
			// that no part still carries their rows.
			var partsWithRows, activeParts int64
			require.NoError(t, db.Raw(
				"SELECT countIf(rows > 0), count() FROM system.parts WHERE database = currentDatabase() AND table = 'logs' AND active",
			).Row().Scan(&partsWithRows, &activeParts))
			assert.Equal(t, int64(1), partsWithRows, "the expired monthly partitions must no longer hold rows")
			t.Logf("active parts after OPTIMIZE: %d, of which holding rows: %d", activeParts, partsWithRows)
		})
	}
}

// TestClickHouseLogReadersAndCleanupWorkOnUpgradedTable is the control for the
// existing query paths: on a table this migration upgraded, the user-scoped
// reader still returns the widened quota and the cleanup path still removes
// exactly the rows below its cutoff.
func TestClickHouseLogReadersAndCleanupWorkOnUpgradedTable(t *testing.T) {
	db := openClickHouseLegacyLogTestDB(t)
	useLogDatabase(t, db, common.DatabaseTypeClickHouse)
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "0")
	require.NoError(t, migrateClickHouseLogDB())

	const oldCreatedAt = int64(1600000000)
	const recentCreatedAt = int64(1700000000)
	require.NoError(t, db.Exec(`
		INSERT INTO logs (id, user_id, created_at, type, quota, request_id, username) VALUES
		(1, 77, 1600000000, 2, 2000000000, 'old-row', 'alice'),
		(2, 77, 1700000000, 2, 1234, 'recent-row', 'alice')`).Error)

	logs, total, err := GetUserLogs(77, LogTypeConsume, 0, 0, "", "", 0, 10, "", "", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Len(t, logs, 2)
	assert.Equal(t, "alice", logs[0].Username)
	assert.Equal(t, 1234, logs[0].Quota, "the most recent row comes first")
	assert.Equal(t, 2000000000, logs[1].Quota, "a value near the old 32-bit ceiling must survive the widening")

	deleted, err := DeleteOldLogBatch(context.Background(), oldCreatedAt+1, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	remaining, total, err := GetUserLogs(77, LogTypeConsume, 0, 0, "", "", 0, 10, "", "", "")
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, remaining, 1)
	assert.Equal(t, "recent-row", remaining[0].RequestId)
}

func TestClickHouseCreateTableHasTTL(t *testing.T) {
	assert.True(t, clickHouseCreateTableHasTTL("CREATE TABLE logs (...)\nTTL toDateTime(created_at) + INTERVAL 30 DAY DELETE"))
	assert.True(t, clickHouseCreateTableHasTTL("CREATE TABLE logs (...) TTL toDateTime(created_at)"))
	assert.False(t, clickHouseCreateTableHasTTL("CREATE TABLE logs (...)\nORDER BY (created_at, request_id)"))
}

func TestClickHouseLogOrder(t *testing.T) {
	assert.Equal(t, "created_at desc, request_id desc", clickHouseLogOrder(""))
	assert.Equal(t, "logs.created_at desc, logs.request_id desc", clickHouseLogOrder("logs."))
}

func TestBuildLogLikeConditionUsesStandardEscape(t *testing.T) {
	originalLogDatabaseType := common.LogDatabaseType()
	t.Cleanup(func() {
		common.SetLogDatabaseType(originalLogDatabaseType)
	})
	common.SetLogDatabaseType(common.DatabaseTypeSQLite)

	condition, pattern, err := buildLogLikeCondition("logs.model_name", "gpt_4%")

	require.NoError(t, err)
	assert.Equal(t, "logs.model_name LIKE ? ESCAPE '!'", condition)
	assert.Equal(t, "gpt!_4%", pattern)
}

func TestBuildLogLikeConditionUsesClickHouseEscaping(t *testing.T) {
	originalLogDatabaseType := common.LogDatabaseType()
	t.Cleanup(func() {
		common.SetLogDatabaseType(originalLogDatabaseType)
	})
	common.SetLogDatabaseType(common.DatabaseTypeClickHouse)

	condition, pattern, err := buildLogLikeCondition("logs.model_name", `gpt_4\mini%`)

	require.NoError(t, err)
	assert.Equal(t, "logs.model_name LIKE ?", condition)
	assert.Equal(t, `gpt\_4\\mini%`, pattern)
}

func TestEnsureLogRequestId(t *testing.T) {
	empty := &Log{}
	ensureLogRequestId(empty)
	assert.NotEmpty(t, empty.RequestId, "empty request id should be backfilled")

	existing := &Log{RequestId: "fixed-request-id"}
	ensureLogRequestId(existing)
	assert.Equal(t, "fixed-request-id", existing.RequestId, "existing request id must be preserved")

	assert.NotPanics(t, func() { ensureLogRequestId(nil) })
}

func TestAssignDisplayLogIds(t *testing.T) {
	logs := []*Log{{}, {}, {}}

	assignDisplayLogIds(logs, 0)
	assert.Equal(t, []int{1, 2, 3}, []int{logs[0].Id, logs[1].Id, logs[2].Id})

	assignDisplayLogIds(logs, 20)
	assert.Equal(t, []int{21, 22, 23}, []int{logs[0].Id, logs[1].Id, logs[2].Id})

	assert.NotPanics(t, func() { assignDisplayLogIds(nil, 0) })
}
