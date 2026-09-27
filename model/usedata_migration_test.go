package model

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// quotaDataBeforeUniqueness mirrors the delivered quota_data schema: the same
// columns and the same non-unique indexes, without the unique index on the
// business key. AutoMigrating it builds the "database created by the previous
// release" the migration has to upgrade, duplicates included.
type quotaDataBeforeUniqueness struct {
	Id        int
	UserID    int    `gorm:"index"`
	Username  string `gorm:"index:idx_qdt_model_user_name,priority:2;size:64;default:''"`
	ModelName string `gorm:"index:idx_qdt_model_user_name,priority:1;size:64;default:''"`
	CreatedAt int64  `gorm:"bigint;index:idx_qdt_created_at,priority:2"`
	UseGroup  string `gorm:"index;size:64;default:''"`
	TokenID   int    `gorm:"index;default:0"`
	ChannelID int    `gorm:"index;default:0"`
	NodeName  string `gorm:"index;size:64;default:''"`
	TokenUsed int    `gorm:"default:0"`
	Count     int    `gorm:"default:0"`
	Quota     int    `gorm:"default:0"`
}

func (quotaDataBeforeUniqueness) TableName() string { return "quota_data" }

type quotaDataMigrationCase struct {
	name string
	env  string
	typ  common.DatabaseType
}

func quotaDataMigrationCases() []quotaDataMigrationCase {
	return []quotaDataMigrationCase{
		{"sqlite", "", common.DatabaseTypeSQLite},
		{"mysql", "TEST_MYSQL_DSN", common.DatabaseTypeMySQL},
		{"postgres", "TEST_POSTGRES_DSN", common.DatabaseTypePostgreSQL},
	}
}

// openQuotaDataMigrationDB opens one engine with a pool large enough for the
// concurrency leg, where each simulated master holds a connection of its own.
func openQuotaDataMigrationDB(t *testing.T, dialect string, dsn string) *gorm.DB {
	t.Helper()
	var dialector gorm.Dialector
	switch dialect {
	case "sqlite":
		previousPath := common.SQLitePath
		common.SQLitePath = filepath.Join(t.TempDir(), "quota_data_migration.db") +
			"?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"
		t.Cleanup(func() { common.SQLitePath = previousPath })
		dialector = sqlite.Open(common.SQLitePath)
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres":
		dialector = postgres.Open(dsn)
	}
	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(16)
	t.Cleanup(func() {
		require.NoError(t, db.Migrator().DropTable("quota_data"))
		// Leave the shared test database holding the delivered schema, which is
		// what the other `model` tests expect to find.
		require.NoError(t, db.Table("quota_data").AutoMigrate(&QuotaData{}))
		require.NoError(t, sqlDB.Close())
	})
	require.NoError(t, db.Migrator().DropTable("quota_data"))
	return db
}

// resetQuotaDataBeforeUniqueness rebuilds the pre-migration `quota_data` table.
func resetQuotaDataBeforeUniqueness(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Migrator().DropTable("quota_data"))
	require.NoError(t, db.Table("quota_data").AutoMigrate(&quotaDataBeforeUniqueness{}))
	require.False(t, db.Migrator().HasIndex(&QuotaData{}, quotaDataBusinessKeyIndex),
		"the fixture must start without the business key index")
}

func quotaDataRow(t *testing.T, db *gorm.DB, id int) QuotaData {
	t.Helper()
	var row QuotaData
	require.NoError(t, db.Table("quota_data").Where("id = ?", id).First(&row).Error)
	return row
}

func quotaDataTotalRows(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var total int64
	require.NoError(t, db.Table("quota_data").Count(&total).Error)
	return total
}

// quotaDataIndexColumns reads the columns of the business key index in index
// order from each engine's own catalog, so the assertion does not depend on
// GORM's index parser.
func quotaDataIndexColumns(t *testing.T, db *gorm.DB) []string {
	t.Helper()
	var columns []string
	var err error
	switch db.Dialector.Name() {
	case "postgres":
		err = db.Raw(`
SELECT attribute_meta.attname AS name
FROM pg_catalog.pg_index AS index_meta
JOIN pg_catalog.pg_class AS index_class ON index_class.oid = index_meta.indexrelid
JOIN pg_catalog.pg_attribute AS attribute_meta
  ON attribute_meta.attrelid = index_meta.indrelid
 AND attribute_meta.attnum = ANY (index_meta.indkey)
WHERE index_meta.indrelid = to_regclass('quota_data') AND index_class.relname = ?
ORDER BY array_position(index_meta.indkey, attribute_meta.attnum)`, quotaDataBusinessKeyIndex).Scan(&columns).Error
	case "mysql":
		err = db.Raw(`
SELECT COLUMN_NAME AS name
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'quota_data' AND INDEX_NAME = ?
ORDER BY SEQ_IN_INDEX`, quotaDataBusinessKeyIndex).Scan(&columns).Error
	default:
		err = db.Raw(`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, quotaDataBusinessKeyIndex).Scan(&columns).Error
	}
	require.NoError(t, err)
	return columns
}

func quotaDataIndexIsUnique(t *testing.T, db *gorm.DB) bool {
	t.Helper()
	var unique bool
	var err error
	switch db.Dialector.Name() {
	case "postgres":
		err = db.Raw(`
SELECT index_meta.indisunique
FROM pg_catalog.pg_index AS index_meta
JOIN pg_catalog.pg_class AS index_class ON index_class.oid = index_meta.indexrelid
WHERE index_meta.indrelid = to_regclass('quota_data') AND index_class.relname = ?`,
			quotaDataBusinessKeyIndex).Scan(&unique).Error
	case "mysql":
		err = db.Raw(`
SELECT NON_UNIQUE = 0
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'quota_data' AND INDEX_NAME = ? AND SEQ_IN_INDEX = 1`,
			quotaDataBusinessKeyIndex).Scan(&unique).Error
	default:
		err = db.Raw(`SELECT "unique" FROM pragma_index_list(?) WHERE name = ?`,
			"quota_data", quotaDataBusinessKeyIndex).Scan(&unique).Error
	}
	require.NoError(t, err)
	return unique
}

// quotaDataMigrationWrites returns the DDL and DML statements a migration pass
// emitted. "It did not fail" is not idempotency; an empty slice is.
func quotaDataMigrationWrites(recorder *migrationSQLRecorder) []string {
	writes := recorder.schemaMutations()
	recorder.mu.Lock()
	statements := append([]string(nil), recorder.statements...)
	recorder.mu.Unlock()
	for _, statement := range statements {
		normalized := strings.ToUpper(strings.TrimSpace(statement))
		if strings.HasPrefix(normalized, "UPDATE ") || strings.HasPrefix(normalized, "DELETE ") {
			writes = append(writes, statement)
		}
	}
	return writes
}

// withQuotaDataFlushDB points the flush and read paths at the dialect under test
// so the assertions exercise the production code rather than a copy of it.
func withQuotaDataFlushDB(t *testing.T, db *gorm.DB, typ common.DatabaseType) {
	t.Helper()
	previousDB := DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	DB = db
	common.SetDatabaseTypes(typ, previousLog)
	t.Cleanup(func() {
		DB = previousDB
		common.SetDatabaseTypes(previousMain, previousLog)
		resetQuotaDataCache(t)
	})
	resetQuotaDataCache(t)
}

// seedQuotaDataDuplicates inserts two groups of rows that share a business key,
// plus two rows whose key is unique. The smallest id of each group is inserted
// last, so "keep the row that came first" and "keep MIN(id)" cannot both pass.
func seedQuotaDataDuplicates(t *testing.T, db *gorm.DB) {
	t.Helper()
	rows := []QuotaData{
		{Id: 7, UserID: 1, Username: "alice", ModelName: "gpt-a", CreatedAt: 3600, UseGroup: "vip", TokenID: 11, ChannelID: 1, NodeName: "node-a", Count: 2, Quota: 150, TokenUsed: 60},
		{Id: 5, UserID: 1, Username: "alice", ModelName: "gpt-a", CreatedAt: 3600, UseGroup: "vip", TokenID: 11, ChannelID: 1, NodeName: "node-a", Count: 4, Quota: 30, TokenUsed: 10},
		{Id: 3, UserID: 1, Username: "alice", ModelName: "gpt-a", CreatedAt: 3600, UseGroup: "vip", TokenID: 11, ChannelID: 1, NodeName: "node-a", Count: 1, Quota: 50, TokenUsed: 20},
		{Id: 21, UserID: 2, Username: "bob", ModelName: "gpt-b", CreatedAt: 7200, UseGroup: "default", TokenID: 22, ChannelID: 2, NodeName: "node-b", Count: 3, Quota: 70, TokenUsed: 30},
		{Id: 20, UserID: 2, Username: "bob", ModelName: "gpt-b", CreatedAt: 7200, UseGroup: "default", TokenID: 22, ChannelID: 2, NodeName: "node-b", Count: 2, Quota: 30, TokenUsed: 10},
		{Id: 40, UserID: 3, Username: "carol", ModelName: "gpt-c", CreatedAt: 3600, UseGroup: "vip", TokenID: 33, ChannelID: 1, NodeName: "node-a", Count: 9, Quota: 900, TokenUsed: 90},
		{Id: 41, UserID: 4, Username: "dave", ModelName: "gpt-d", CreatedAt: 3600, UseGroup: "vip", TokenID: 44, ChannelID: 1, NodeName: "node-a", Count: 8, Quota: 800, TokenUsed: 80},
	}
	for _, row := range rows {
		require.NoError(t, db.Table("quota_data").Create(&row).Error)
	}
	require.EqualValues(t, 7, quotaDataTotalRows(t, db))
}

// TestMigrateDBRepairsQuotaDataBeforeAutoMigrate pins the ordering the upgrade
// depends on. AutoMigrate issues CREATE UNIQUE INDEX for the business key and
// fails when the table still holds duplicate rows, and a failing AutoMigrate
// aborts startup. The deduplication therefore has to run first, so an upgrade
// over a database that already holds duplicates must reach the end of the real
// startup migration.
func TestMigrateDBRepairsQuotaDataBeforeAutoMigrate(t *testing.T) {
	previousPath := common.SQLitePath
	common.SQLitePath = filepath.Join(t.TempDir(), "startup.db") +
		"?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"
	t.Cleanup(func() { common.SQLitePath = previousPath })

	legacy, err := gorm.Open(sqlite.Open(common.SQLitePath), &gorm.Config{})
	require.NoError(t, err)
	legacySQLDB, err := legacy.DB()
	require.NoError(t, err)
	require.NoError(t, legacy.Table("quota_data").AutoMigrate(&quotaDataBeforeUniqueness{}))
	seedQuotaDataDuplicates(t, legacy)
	require.NoError(t, legacySQLDB.Close())

	db, err := gorm.Open(sqlite.Open(common.SQLitePath), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	previousDB := DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	DB = db
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, previousLog)
	t.Cleanup(func() {
		DB = previousDB
		common.SetDatabaseTypes(previousMain, previousLog)
	})

	require.NoError(t, migrateDB(), "an upgrade over duplicate rows must not abort startup")
	assert.EqualValues(t, 4, quotaDataTotalRows(t, db))
	assert.True(t, quotaDataIndexIsUnique(t, db))
}

func TestMigrateQuotaDataUniqueness(t *testing.T) {
	for _, tc := range quotaDataMigrationCases() {
		t.Run(tc.name, func(t *testing.T) {
			dsn := os.Getenv(tc.env)
			if tc.env != "" && dsn == "" {
				t.Skip(tc.env + " is not configured")
			}
			db := openQuotaDataMigrationDB(t, tc.name, dsn)
			versionQuery := "SELECT version()"
			if tc.name == "sqlite" {
				versionQuery = "SELECT sqlite_version()"
			}
			var version string
			require.NoError(t, db.Raw(versionQuery).Scan(&version).Error)
			t.Logf("%s version: %s", tc.name, version)

			t.Run("a fresh database gets the unique index", func(t *testing.T) {
				require.NoError(t, db.Migrator().DropTable("quota_data"))
				require.NoError(t, migrateQuotaDataUniqueness(db),
					"a database without the table has nothing to migrate")
				require.NoError(t, db.Table("quota_data").AutoMigrate(&QuotaData{}))
				assert.Equal(t, quotaDataBusinessKeyColumns, quotaDataIndexColumns(t, db))
				assert.True(t, quotaDataIndexIsUnique(t, db))
				// Column set unchanged: no hash column was introduced.
				columns, err := db.Migrator().ColumnTypes(&QuotaData{})
				require.NoError(t, err)
				names := make([]string, 0, len(columns))
				for _, column := range columns {
					names = append(names, column.Name())
				}
				assert.Equal(t, []string{
					"id", "user_id", "username", "model_name", "created_at",
					"use_group", "token_id", "channel_id", "node_name",
					"token_used", "count", "quota",
				}, names)
			})

			t.Run("an existing table without duplicates only gains the index", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				seed := QuotaData{UserID: 1, Username: "alice", ModelName: "gpt-a", CreatedAt: 3600,
					UseGroup: "vip", TokenID: 11, ChannelID: 1, NodeName: "node-a", Count: 5, Quota: 500, TokenUsed: 50}
				require.NoError(t, db.Table("quota_data").Create(&seed).Error)

				require.NoError(t, migrateQuotaDataUniqueness(db))

				assert.True(t, quotaDataIndexIsUnique(t, db))
				assert.Equal(t, quotaDataBusinessKeyColumns, quotaDataIndexColumns(t, db))
				stored := quotaDataRow(t, db, seed.Id)
				assert.Equal(t, quotaDataCounters(&seed), quotaDataCounters(&stored))
				assert.EqualValues(t, 1, quotaDataTotalRows(t, db))
			})

			t.Run("duplicate rows are merged by sum on the smallest id", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				seedQuotaDataDuplicates(t, db)
				recorder := &migrationSQLRecorder{}

				require.NoError(t, migrateQuotaDataUniqueness(db.Session(&gorm.Session{Logger: recorder})))

				// The deduplication branch really ran: 7 rows collapsed into 4 keys.
				assert.EqualValues(t, 4, quotaDataTotalRows(t, db))
				var deletes int
				for _, statement := range recorder.statements {
					if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "DELETE ") {
						deletes++
					}
				}
				assert.Positive(t, deletes, "the merge must have deleted the redundant rows")

				merged := quotaDataRow(t, db, 3)
				assert.Equal(t, QuotaData{Count: 7, Quota: 230, TokenUsed: 90}, quotaDataCounters(&merged))
				assert.Equal(t, 3, merged.Id, "the kept row must be the smallest id of the group")
				var groupRows int64
				require.NoError(t, db.Table("quota_data").
					Where("user_id = ? AND username = ? AND model_name = ?", 1, "alice", "gpt-a").
					Count(&groupRows).Error)
				assert.EqualValues(t, 1, groupRows)

				second := quotaDataRow(t, db, 20)
				assert.Equal(t, QuotaData{Count: 5, Quota: 100, TokenUsed: 40}, quotaDataCounters(&second))
				// Rows whose key was already unique are untouched.
				for id, want := range map[int]QuotaData{
					40: {Count: 9, Quota: 900, TokenUsed: 90},
					41: {Count: 8, Quota: 800, TokenUsed: 80},
				} {
					untouched := quotaDataRow(t, db, id)
					assert.Equal(t, want, quotaDataCounters(&untouched))
				}

				assert.True(t, quotaDataIndexIsUnique(t, db))
				assert.Equal(t, quotaDataBusinessKeyColumns, quotaDataIndexColumns(t, db))
			})

			t.Run("running it again emits no ddl or dml", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				seedQuotaDataDuplicates(t, db)
				require.NoError(t, migrateQuotaDataUniqueness(db))
				before := quotaDataRow(t, db, 3)

				recorder := &migrationSQLRecorder{}
				require.NoError(t, migrateQuotaDataUniqueness(db.Session(&gorm.Session{Logger: recorder})))

				assert.Empty(t, quotaDataMigrationWrites(recorder), "the second pass must change nothing")
				after := quotaDataRow(t, db, 3)
				assert.Equal(t, quotaDataCounters(&before), quotaDataCounters(&after),
					"a second pass must not add the counters up twice")
				assert.EqualValues(t, 4, quotaDataTotalRows(t, db))
			})

			t.Run("null business keys are normalized before merging", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				// A unique index does not constrain keys containing NULL on any of
				// the three engines, and `username = ?` never matches NULL, so this
				// pair would otherwise survive as two rows forever.
				for _, row := range []struct {
					id     int
					count  int
					quota  int
					tokens int
				}{{60, 2, 150, 60}, {61, 1, 50, 20}} {
					require.NoError(t, db.Exec(
						"INSERT INTO quota_data (id, user_id, username, model_name, created_at, use_group, token_id, channel_id, node_name, token_used, count, quota) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						row.id, 1, "gpt-a", 3600, "vip", 11, 1, "node-a", row.tokens, row.count, row.quota).Error)
				}

				require.NoError(t, migrateQuotaDataUniqueness(db))

				var nulls int64
				require.NoError(t, db.Table("quota_data").Where("username IS NULL").Count(&nulls).Error)
				assert.EqualValues(t, 0, nulls, "NULL must be rewritten to the column default")
				assert.EqualValues(t, 1, quotaDataTotalRows(t, db))
				merged := quotaDataRow(t, db, 60)
				assert.Equal(t, QuotaData{Count: 3, Quota: 200, TokenUsed: 80}, quotaDataCounters(&merged))
				assert.Empty(t, merged.Username)

				// The index now really covers this key: a flush accumulates instead
				// of adding a second row.
				withQuotaDataFlushDB(t, db, tc.typ)
				LogQuotaData(QuotaDataLogParams{
					UserID: 1, Username: "", ModelName: "gpt-a", CreatedAt: 3600,
					UseGroup: "vip", TokenID: 11, ChannelID: 1, NodeName: "node-a",
					Quota: 25, TokenUsed: 10,
				})
				SaveQuotaDataCache()
				assert.EqualValues(t, 1, quotaDataTotalRows(t, db))
				flushed := quotaDataRow(t, db, 60)
				assert.Equal(t, QuotaData{Count: 4, Quota: 225, TokenUsed: 90}, quotaDataCounters(&flushed))
			})

			t.Run("concurrent masters merge exactly once", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				seedQuotaDataDuplicates(t, db)

				const masters = 3
				errs := make([]error, masters)
				var wg sync.WaitGroup
				for i := range masters {
					wg.Go(func() { errs[i] = migrateQuotaDataUniqueness(db) })
				}
				wg.Wait()
				for i, err := range errs {
					require.NoError(t, err, "master %d failed", i)
				}

				assert.Equal(t, quotaDataBusinessKeyColumns, quotaDataIndexColumns(t, db))
				assert.True(t, quotaDataIndexIsUnique(t, db))
				// Every key keeps its rows, summed exactly once: a lost update would
				// write back a smaller sum, a duplicated merge a larger one.
				assert.EqualValues(t, 4, quotaDataTotalRows(t, db))
				first := quotaDataRow(t, db, 3)
				second := quotaDataRow(t, db, 20)
				assert.Equal(t, QuotaData{Count: 7, Quota: 230, TokenUsed: 90}, quotaDataCounters(&first))
				assert.Equal(t, QuotaData{Count: 5, Quota: 100, TokenUsed: 40}, quotaDataCounters(&second))
			})

			t.Run("the dashboard reads the same numbers before and after", func(t *testing.T) {
				resetQuotaDataBeforeUniqueness(t, db)
				seedQuotaDataDuplicates(t, db)
				withQuotaDataFlushDB(t, db, tc.typ)

				before, err := GetQuotaDataByUserId(1, 0, 1<<40)
				require.NoError(t, err)
				require.NotEmpty(t, before)

				require.NoError(t, migrateQuotaDataUniqueness(db))

				after, err := GetQuotaDataByUserId(1, 0, 1<<40)
				require.NoError(t, err)
				byModel := func(rows []*QuotaData) []QuotaData {
					sorted := make([]QuotaData, 0, len(rows))
					for _, row := range rows {
						sorted = append(sorted, *row)
					}
					slices.SortFunc(sorted, func(a, b QuotaData) int {
						return cmp.Or(cmp.Compare(a.ModelName, b.ModelName), cmp.Compare(a.CreatedAt, b.CreatedAt))
					})
					return sorted
				}
				assert.Equal(t, byModel(before), byModel(after),
					"summing on merge is what keeps the dashboard readings unchanged")
			})
		})
	}
}
