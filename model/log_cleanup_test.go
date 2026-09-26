package model

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// sqlRecorder captures every statement GORM executes so the test can assert on
// the SQL that reaches the database, not only on its row counts.
type sqlRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *sqlRecorder) LogMode(gormlogger.LogLevel) gormlogger.Interface { return r }
func (r *sqlRecorder) Info(context.Context, string, ...any)             {}
func (r *sqlRecorder) Warn(context.Context, string, ...any)             {}
func (r *sqlRecorder) Error(context.Context, string, ...any)            {}

func (r *sqlRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	statement, _ := fc()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, statement)
}

func (r *sqlRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statements...)
}

func (r *sqlRecorder) deletes() []string {
	var deletes []string
	for _, statement := range r.recorded() {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "DELETE") {
			deletes = append(deletes, statement)
		}
	}
	return deletes
}

// TestDeleteOldLogBatchDatabaseMatrix covers the log retention batch delete on
// every supported engine. GORM only lists LIMIT among its MySQL delete clauses,
// so the previous Delete().Limit() form deleted every expired row in one
// statement on PostgreSQL and SQLite instead of honouring the batch size.
func TestDeleteOldLogBatchDatabaseMatrix(t *testing.T) {
	previousLogDB := LOG_DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		LOG_DB = previousLogDB
		common.SetDatabaseTypes(previousMain, previousLog)
	})

	const (
		expiredRows = 250
		freshRows   = 40
		batchSize   = 100
	)
	cutoff := time.Now().Unix() - 1800

	cases := []struct {
		name string
		env  string
		typ  common.DatabaseType
	}{
		{"sqlite", "", common.DatabaseTypeSQLite},
		{"mysql", "TEST_MYSQL_DSN", common.DatabaseTypeMySQL},
		{"postgres", "TEST_POSTGRES_DSN", common.DatabaseTypePostgreSQL},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := os.Getenv(tc.env)
			if tc.env != "" && dsn == "" {
				t.Skip(tc.env + " is not configured")
			}
			recorder := &sqlRecorder{}
			db := openLogTestDB(t, tc.name, dsn, recorder)
			LOG_DB = db
			common.SetDatabaseTypes(previousMain, tc.typ)

			versionQuery := "SELECT version()"
			if tc.name == "sqlite" {
				versionQuery = "SELECT sqlite_version()"
			}
			var version string
			require.NoError(t, db.Raw(versionQuery).Scan(&version).Error)
			t.Logf("%s version: %s", tc.name, version)

			rows := make([]Log, 0, expiredRows+freshRows)
			for i := range expiredRows {
				rows = append(rows, Log{UserId: 1, Type: LogTypeConsume, Content: "expired", CreatedAt: cutoff - int64(expiredRows) + int64(i)})
			}
			for i := range freshRows {
				rows = append(rows, Log{UserId: 1, Type: LogTypeConsume, Content: "fresh", CreatedAt: cutoff + int64(i) + 1})
			}
			require.NoError(t, db.Create(&rows).Error)

			// Acceptance 1 + 2: every call is bounded by `limit`, and looping
			// until the batch comes back empty removes exactly the expired rows.
			var deletedTotal int64
			for batch := range expiredRows/batchSize + 2 {
				deleted, err := DeleteOldLogBatch(context.Background(), cutoff, batchSize)
				require.NoError(t, err)
				assert.LessOrEqual(t, deleted, int64(batchSize), "batch %d must not exceed the requested limit", batch)
				if deleted == 0 {
					break
				}
				deletedTotal += deleted
			}
			assert.EqualValues(t, expiredRows, deletedTotal)

			var remainingExpired, remainingFresh int64
			require.NoError(t, db.Model(&Log{}).Where("created_at < ?", cutoff).Count(&remainingExpired).Error)
			require.NoError(t, db.Model(&Log{}).Where("created_at >= ?", cutoff).Count(&remainingFresh).Error)
			assert.Zero(t, remainingExpired, "no expired row may survive the cleanup loop")
			assert.EqualValues(t, freshRows, remainingFresh, "rows inside the retention window must not be touched")

			// Acceptance 5: the call is idempotent once nothing is left to delete.
			deleted, err := DeleteOldLogBatch(context.Background(), cutoff, batchSize)
			require.NoError(t, err)
			assert.Zero(t, deleted)

			require.NoError(t, db.Migrator().DropTable(&Log{}))
			require.NoError(t, db.AutoMigrate(&Log{}))
			deleted, err = DeleteOldLogBatch(context.Background(), cutoff, batchSize)
			require.NoError(t, err)
			assert.Zero(t, deleted, "an empty log table reports zero without error")

			// Acceptance 3: the statement that reaches the engine must not rely on
			// DELETE ... LIMIT, which PostgreSQL and SQLite do not accept.
			deletes := recorder.deletes()
			require.NotEmpty(t, deletes, "the batch delete must reach the database")
			for _, statement := range deletes {
				assert.NotContains(t, strings.ToUpper(statement), "LIMIT", "%s must not emit DELETE ... LIMIT", tc.name)
			}
			assert.Contains(t, strings.ToUpper(strings.TrimSpace(deletes[0])), "IN (", "the batch must be deleted by primary key")
			t.Logf("%s emitted delete: %s", tc.name, deletes[0])

			// Acceptance 6: the index the retention scan depends on exists and the
			// delete path still resolves its batch through it.
			assert.True(t, db.Migrator().HasIndex(&Log{}, "idx_created_at_id"), "%s must keep idx_created_at_id", tc.name)
			assertLogCleanupScansCreatedAtIndex(t, db, tc.name)
		})
	}
}

// openLogTestDB opens one engine with a fresh `logs` table and a logger that
// records every statement it executes.
func openLogTestDB(t *testing.T, dialect string, dsn string, recorder *sqlRecorder) *gorm.DB {
	t.Helper()
	var dialector gorm.Dialector
	switch dialect {
	case "sqlite":
		dialector = sqlite.Open(":memory:")
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres":
		dialector = postgres.Open(dsn)
	}
	db, err := gorm.Open(dialector, &gorm.Config{Logger: recorder})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() {
		require.NoError(t, db.Migrator().DropTable(&Log{}))
		require.NoError(t, sqlDB.Close())
	})
	require.NoError(t, db.Migrator().DropTable(&Log{}))
	require.NoError(t, db.AutoMigrate(&Log{}))
	return db
}

// assertLogCleanupScansCreatedAtIndex asserts the batch select resolves through
// idx_created_at_id. The probe mirrors the production statement but keeps the
// matching rows a small fraction of the table so the planner has a reason to
// prefer the index over a sequential scan on every engine.
func assertLogCleanupScansCreatedAtIndex(t *testing.T, db *gorm.DB, dialect string) {
	t.Helper()
	const probeRows = 20000
	rows := make([]Log, 0, probeRows)
	for i := range probeRows {
		rows = append(rows, Log{UserId: 1, Type: LogTypeConsume, Content: "probe", CreatedAt: int64(probeRows) + int64(i)})
	}
	require.NoError(t, db.CreateInBatches(&rows, 200).Error)
	probe := "SELECT id FROM logs WHERE created_at < ? ORDER BY created_at, id LIMIT 100"
	selectiveCutoff := int64(probeRows) + 30

	switch dialect {
	case "mysql":
		var plan []map[string]any
		require.NoError(t, db.Raw("EXPLAIN "+probe, selectiveCutoff).Scan(&plan).Error)
		require.Len(t, plan, 1)
		t.Logf("mysql plan: %v", plan[0])
		assert.Equal(t, "idx_created_at_id", plan[0]["key"])
	case "postgres":
		var plan []map[string]any
		require.NoError(t, db.Raw("EXPLAIN "+probe, selectiveCutoff).Scan(&plan).Error)
		require.NotEmpty(t, plan)
		lines := ""
		for _, step := range plan {
			for _, line := range step {
				lines += fmt.Sprint(line) + "\n"
			}
		}
		t.Logf("postgres plan:\n%s", lines)
		assert.Contains(t, lines, "idx_created_at_id")
	default:
		var plan []struct {
			Detail string `gorm:"column:detail"`
		}
		require.NoError(t, db.Raw("EXPLAIN QUERY PLAN "+probe, selectiveCutoff).Scan(&plan).Error)
		require.NotEmpty(t, plan)
		details := ""
		for _, step := range plan {
			details += step.Detail + "\n"
		}
		t.Logf("sqlite plan:\n%s", details)
		assert.Contains(t, details, "idx_created_at_id")
	}
	require.NoError(t, db.Where("content = ?", "probe").Delete(&Log{}).Error)
}
