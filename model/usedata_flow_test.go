package model

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// errInjectedQuotaDataWrite is injected into the engine rather than produced by
// pointing the flush path at a broken database, so one key can fail while the
// rest of the same flush still reaches a healthy one.
var errInjectedQuotaDataWrite = errors.New("injected quota_data write failure")

func seedFlowQuotaData(t *testing.T, quotaData QuotaData) {
	t.Helper()
	require.NoError(t, DB.Create(&quotaData).Error)
}

func seedFlowLookupData(t *testing.T) {
	t.Helper()
	require.NoError(t, DB.Create(&Channel{Id: 1, Name: "east"}).Error)
	require.NoError(t, DB.Create(&Channel{Id: 2, Name: "west"}).Error)
	require.NoError(t, DB.Create(&Token{Id: 11, UserId: 1, Key: "sk-primary", Name: "primary"}).Error)
	require.NoError(t, DB.Create(&Token{Id: 22, UserId: 2, Key: "sk-backup", Name: "backup"}).Error)
	require.NoError(t, DB.Delete(&Token{Id: 11}).Error)
}

func TestGetFlowQuotaDataUsesQuotaDataRoleSpecificDimensions(t *testing.T) {
	truncateTables(t)
	seedFlowLookupData(t)

	seedFlowQuotaData(t, QuotaData{
		UserID:    1,
		Username:  "alice",
		NodeName:  "node-a",
		TokenID:   11,
		UseGroup:  "vip",
		ModelName: "gpt-a",
		ChannelID: 1,
		CreatedAt: 1000,
		Count:     2,
		Quota:     100,
		TokenUsed: 40,
	})
	seedFlowQuotaData(t, QuotaData{
		UserID:    1,
		Username:  "alice",
		NodeName:  "node-a",
		TokenID:   11,
		UseGroup:  "vip",
		ModelName: "gpt-a",
		ChannelID: 1,
		CreatedAt: 1100,
		Count:     1,
		Quota:     50,
		TokenUsed: 20,
	})
	seedFlowQuotaData(t, QuotaData{
		UserID:    1,
		Username:  "alice",
		NodeName:  "node-a",
		TokenID:   11,
		UseGroup:  "vip",
		ModelName: "gpt-a",
		ChannelID: 2,
		CreatedAt: 1200,
		Count:     1,
		Quota:     25,
		TokenUsed: 10,
	})
	seedFlowQuotaData(t, QuotaData{
		UserID:    2,
		Username:  "bob",
		NodeName:  "node-b",
		TokenID:   22,
		UseGroup:  "default",
		ModelName: "gpt-b",
		ChannelID: 1,
		CreatedAt: 1300,
		Count:     3,
		Quota:     70,
		TokenUsed: 30,
	})
	seedFlowQuotaData(t, QuotaData{
		UserID:    1,
		Username:  "alice",
		ModelName: "legacy",
		CreatedAt: 1400,
		Count:     99,
		Quota:     999,
		TokenUsed: 999,
	})

	rootRows, err := GetFlowQuotaData(900, 2000, "", 0, common.RoleRootUser)
	require.NoError(t, err)
	require.Len(t, rootRows, 3)
	// Token 11 was soft-deleted, so its name is intentionally left empty for the
	// frontend to render a localized "deleted (id)" label instead.
	require.Equal(t, FlowQuotaData{
		UserID:      1,
		Username:    "alice",
		NodeName:    "node-a",
		TokenID:     11,
		TokenName:   "",
		UseGroup:    "vip",
		ChannelID:   1,
		ChannelName: "east",
		ModelName:   "gpt-a",
		TokenUsed:   60,
		Count:       3,
		Quota:       150,
	}, *rootRows[0])
	// A token that still exists resolves to its current name.
	require.Equal(t, 22, rootRows[1].TokenID)
	require.Equal(t, "backup", rootRows[1].TokenName)

	adminRows, err := GetFlowQuotaData(900, 2000, "alice", 0, common.RoleAdminUser)
	require.NoError(t, err)
	require.Len(t, adminRows, 2)
	require.Equal(t, 0, adminRows[0].TokenID)
	require.Empty(t, adminRows[0].TokenName)
	require.Empty(t, adminRows[0].NodeName)
	require.Equal(t, "alice", adminRows[0].Username)
	require.Equal(t, "vip", adminRows[0].UseGroup)
	require.Equal(t, "east", adminRows[0].ChannelName)
	require.Equal(t, 150, adminRows[0].Quota)

	selfRows, err := GetFlowQuotaData(900, 2000, "", 1, common.RoleCommonUser)
	require.NoError(t, err)
	require.Len(t, selfRows, 1)
	require.Empty(t, selfRows[0].Username)
	require.Equal(t, 0, selfRows[0].ChannelID)
	require.Empty(t, selfRows[0].ChannelName)
	require.Empty(t, selfRows[0].TokenName)
	require.Equal(t, "vip", selfRows[0].UseGroup)
	require.Equal(t, 175, selfRows[0].Quota)
}

func TestLogQuotaDataSplitsRowsByUseGroupTokenChannelAndNode(t *testing.T) {
	truncateTables(t)
	CacheQuotaDataLock.Lock()
	CacheQuotaData = make(map[string]*QuotaData)
	CacheQuotaDataLock.Unlock()

	LogQuotaData(QuotaDataLogParams{
		UserID:    1,
		Username:  "alice",
		ModelName: "gpt-a",
		CreatedAt: 3661,
		UseGroup:  "vip",
		TokenID:   11,
		ChannelID: 1,
		NodeName:  "node-a",
		Quota:     100,
		TokenUsed: 40,
	})
	LogQuotaData(QuotaDataLogParams{
		UserID:    1,
		Username:  "alice",
		ModelName: "gpt-a",
		CreatedAt: 3700,
		UseGroup:  "vip",
		TokenID:   11,
		ChannelID: 1,
		NodeName:  "node-a",
		Quota:     50,
		TokenUsed: 20,
	})
	LogQuotaData(QuotaDataLogParams{
		UserID:    1,
		Username:  "alice",
		ModelName: "gpt-a",
		CreatedAt: 3700,
		UseGroup:  "default",
		TokenID:   11,
		ChannelID: 1,
		NodeName:  "node-a",
		Quota:     25,
		TokenUsed: 10,
	})

	SaveQuotaDataCache()

	var rows []QuotaData
	require.NoError(t, DB.Order("quota DESC").Find(&rows).Error)
	require.Len(t, rows, 2)
	require.Equal(t, int64(3600), rows[0].CreatedAt)
	require.Equal(t, "vip", rows[0].UseGroup)
	require.Equal(t, 11, rows[0].TokenID)
	require.Equal(t, 1, rows[0].ChannelID)
	require.Equal(t, "node-a", rows[0].NodeName)
	require.Equal(t, 2, rows[0].Count)
	require.Equal(t, 150, rows[0].Quota)
	require.Equal(t, 60, rows[0].TokenUsed)
	require.Equal(t, "default", rows[1].UseGroup)
	require.Equal(t, 25, rows[1].Quota)
}

// TestSaveQuotaDataCacheKeepsDataThatFailedToFlush covers the dashboard flush on
// every supported engine. A write that failed used to be logged and then dropped
// with the unconditional cache reset, so a single transient error could silently
// lose an hour of dashboard data. A key that fails must instead stay in the cache
// for the next flush, and must not stop the rest of the same flush. A probe that
// failed counts as a failure too: swallowing its error reads as "this key is new"
// and inserts a duplicate row for a key that already has one.
func TestSaveQuotaDataCacheKeepsDataThatFailedToFlush(t *testing.T) {
	previousDB := DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		DB = previousDB
		common.SetDatabaseTypes(previousMain, previousLog)
		resetQuotaDataCache(t)
	})

	// Every LogQuotaData call is one request: it adds one to count and carries
	// the quota and tokens it is handed.
	logKey := func(modelName string, quota, tokenUsed int) {
		LogQuotaData(QuotaDataLogParams{
			UserID:    1,
			Username:  "alice",
			ModelName: modelName,
			CreatedAt: 3600,
			UseGroup:  "default",
			TokenID:   1,
			ChannelID: 1,
			NodeName:  "node-a",
			Quota:     quota,
			TokenUsed: tokenUsed,
		})
	}

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
			db := openQuotaDataTestDB(t, tc.name, dsn, recorder)
			DB = db
			common.SetDatabaseTypes(tc.typ, previousLog)

			versionQuery := "SELECT version()"
			if tc.name == "sqlite" {
				versionQuery = "SELECT sqlite_version()"
			}
			var version string
			require.NoError(t, db.Raw(versionQuery).Scan(&version).Error)
			t.Logf("%s version: %s", tc.name, version)

			t.Run("every key is written and the cache is cleared", func(t *testing.T) {
				clearQuotaDataTable(t, db)
				resetQuotaDataCache(t)

				logKey("alpha", 100, 40)
				logKey("alpha", 50, 20)
				logKey("beta", 75, 30)
				SaveQuotaDataCache()

				assert.Empty(t, CacheQuotaData, "a fully successful flush clears the cache")
				rows := quotaDataRows(t, db)
				require.Len(t, rows, 2)
				assert.Equal(t, QuotaData{Count: 2, Quota: 150, TokenUsed: 60}, quotaDataCounters(rows["alpha"]))
				assert.Equal(t, QuotaData{Count: 1, Quota: 75, TokenUsed: 30}, quotaDataCounters(rows["beta"]))
			})

			t.Run("a failed insert keeps its key and blocks nothing", func(t *testing.T) {
				clearQuotaDataTable(t, db)
				resetQuotaDataCache(t)
				// Fail whichever insert the flush reaches first. Go randomises map
				// iteration, so failing a named key would sometimes put it last and
				// let a flush that aborts on the first error still look correct.
				failedModel := failFirstQuotaDataCreate(t, db)
				errorLog := captureSysError(t)

				logKey("alpha", 100, 40)
				logKey("alpha", 50, 20)
				logKey("beta", 75, 30)
				SaveQuotaDataCache()

				require.NotEmpty(t, *failedModel, "the injected failure must have been reached")
				cached := cachedQuotaData(t)
				rows := quotaDataRows(t, db)
				// Acceptance 1 + 3 + 6: the failed key keeps its full accumulated
				// counters and stays cached, while the other key still landed.
				assert.Len(t, cached, 1, "only the failed key may remain cached")
				assert.Len(t, rows, 1, "the other key must still be written")
				expected := map[string]QuotaData{
					"alpha": {Count: 2, Quota: 150, TokenUsed: 60},
					"beta":  {Count: 1, Quota: 75, TokenUsed: 30},
				}
				require.Contains(t, cached, *failedModel, "the failed key must stay cached")
				assert.Equal(t, expected[*failedModel], quotaDataCounters(cached[*failedModel]))
				for modelName, row := range rows {
					assert.NotEqual(t, *failedModel, modelName, "the failed key must not reach the database")
					assert.Equal(t, expected[modelName], quotaDataCounters(row))
				}
				// Acceptance 4: the failure is reported at error level with the cause.
				assert.Contains(t, errorLog.String(), errInjectedQuotaDataWrite.Error())
				t.Logf("%s retained=%v rows=%v", tc.name, cached[*failedModel], rows)
			})

			t.Run("a failed increment keeps its key and blocks nothing", func(t *testing.T) {
				clearQuotaDataTable(t, db)
				resetQuotaDataCache(t)
				// An existing row sends this key down the increment branch.
				require.NoError(t, db.Create(&QuotaData{
					UserID: 1, Username: "alice", ModelName: "existing", CreatedAt: 3600,
					UseGroup: "default", TokenID: 1, ChannelID: 1, NodeName: "node-a",
					Count: 5, Quota: 500, TokenUsed: 50,
				}).Error)
				failQuotaDataUpdates(t, db)
				errorLog := captureSysError(t)

				logKey("existing", 100, 40)
				logKey("fresh", 75, 30)
				SaveQuotaDataCache()

				// Acceptance 2 + 3 + 6: the increment that failed keeps the data it
				// could not apply, and the key without a row is still inserted.
				cached := cachedQuotaData(t)
				rows := quotaDataRows(t, db)
				assert.Len(t, cached, 1, "only the failed key may remain cached")
				require.Len(t, rows, 2)
				require.Contains(t, cached, "existing", "the key whose increment failed must stay cached")
				assert.Equal(t, QuotaData{Count: 1, Quota: 100, TokenUsed: 40}, quotaDataCounters(cached["existing"]))
				assert.Equal(t, QuotaData{Count: 1, Quota: 75, TokenUsed: 30}, quotaDataCounters(rows["fresh"]))
				assert.Equal(t, QuotaData{Count: 5, Quota: 500, TokenUsed: 50}, quotaDataCounters(rows["existing"]),
					"a failed increment must not apply partially")
				// Acceptance 4: the failure is reported at error level with the cause.
				assert.Contains(t, errorLog.String(), errInjectedQuotaDataWrite.Error())

				for _, statement := range recorder.recorded() {
					if strings.Contains(statement, "INSERT INTO") || strings.Contains(statement, "UPDATE ") {
						t.Logf("%s write statement: %s", tc.name, strings.TrimSpace(statement))
					}
				}
			})

			t.Run("a failed probe keeps its key and inserts nothing", func(t *testing.T) {
				clearQuotaDataTable(t, db)
				resetQuotaDataCache(t)
				// The row already exists, so a probe that succeeds routes this key
				// into the increment branch. A probe error mistaken for "no row"
				// instead inserts a second row for the same business key, which the
				// dashboard then counts twice.
				require.NoError(t, db.Create(&QuotaData{
					UserID: 1, Username: "alice", ModelName: "existing", CreatedAt: 3600,
					UseGroup: "default", TokenID: 1, ChannelID: 1, NodeName: "node-a",
					Count: 5, Quota: 500, TokenUsed: 50,
				}).Error)
				failQuotaDataProbes(t, db)
				errorLog := captureSysError(t)

				logKey("existing", 100, 40)
				SaveQuotaDataCache()

				cached := cachedQuotaData(t)
				rows := quotaDataRows(t, db)
				var rowCount int64
				require.NoError(t, db.Model(&QuotaData{}).Count(&rowCount).Error)
				// Acceptance 1 + 3 + 6: an unreadable probe is a failure, never a new
				// key, so nothing is inserted, the key stays cached with its counters,
				// and the failure is reported at error level.
				assert.Equal(t, int64(1), rowCount, "a failed probe must not insert a duplicate row")
				require.Contains(t, cached, "existing", "the key whose probe failed must stay cached")
				assert.Equal(t, QuotaData{Count: 1, Quota: 100, TokenUsed: 40}, quotaDataCounters(cached["existing"]))
				assert.Equal(t, QuotaData{Count: 5, Quota: 500, TokenUsed: 50}, quotaDataCounters(rows["existing"]),
					"the existing row must be left untouched")
				assert.Contains(t, errorLog.String(), errInjectedQuotaDataWrite.Error())
				t.Logf("%s probe-failure retained=%v rows=%d", tc.name, cached["existing"], rowCount)
			})
		})
	}
}

// openQuotaDataTestDB opens one engine with a fresh `quota_data` table and a
// logger that records every statement it executes.
func openQuotaDataTestDB(t *testing.T, dialect string, dsn string, recorder *sqlRecorder) *gorm.DB {
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
		require.NoError(t, db.Migrator().DropTable(&QuotaData{}))
		require.NoError(t, sqlDB.Close())
	})
	require.NoError(t, db.Migrator().DropTable(&QuotaData{}))
	require.NoError(t, db.AutoMigrate(&QuotaData{}))
	return db
}

func clearQuotaDataTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec("DELETE FROM quota_data").Error)
}

func resetQuotaDataCache(t *testing.T) {
	t.Helper()
	CacheQuotaDataLock.Lock()
	defer CacheQuotaDataLock.Unlock()
	CacheQuotaData = make(map[string]*QuotaData)
}

func cachedQuotaData(t *testing.T) map[string]*QuotaData {
	t.Helper()
	CacheQuotaDataLock.Lock()
	defer CacheQuotaDataLock.Unlock()
	byModelName := make(map[string]*QuotaData, len(CacheQuotaData))
	for _, quotaData := range CacheQuotaData {
		byModelName[quotaData.ModelName] = quotaData
	}
	return byModelName
}

func quotaDataRows(t *testing.T, db *gorm.DB) map[string]*QuotaData {
	t.Helper()
	var rows []*QuotaData
	require.NoError(t, db.Find(&rows).Error)
	byModelName := make(map[string]*QuotaData, len(rows))
	for _, row := range rows {
		byModelName[row.ModelName] = row
	}
	return byModelName
}

// quotaDataCounters drops the identity columns so a stored row can be compared
// against the counters the flush was expected to write.
func quotaDataCounters(quotaData *QuotaData) QuotaData {
	return QuotaData{Count: quotaData.Count, Quota: quotaData.Quota, TokenUsed: quotaData.TokenUsed}
}

// captureSysError redirects the error-level writer so the test can assert that a
// failed flush was reported rather than swallowed.
func captureSysError(t *testing.T) *bytes.Buffer {
	t.Helper()
	buffer := &bytes.Buffer{}
	previous := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = buffer
	t.Cleanup(func() { gin.DefaultErrorWriter = previous })
	return buffer
}

// failFirstQuotaDataCreate rejects the first `quota_data` insert of the flush and
// records which key it rejected.
func failFirstQuotaDataCreate(t *testing.T, db *gorm.DB) *string {
	t.Helper()
	failedModel := new(string)
	db.Callback().Create().Before("gorm:create").Register("test:fail_first_quota_data_create", func(tx *gorm.DB) {
		quotaData, ok := tx.Statement.Dest.(*QuotaData)
		if !ok || tx.Statement.Table != "quota_data" || *failedModel != "" {
			return
		}
		*failedModel = quotaData.ModelName
		tx.AddError(errInjectedQuotaDataWrite)
	})
	t.Cleanup(func() { db.Callback().Create().Remove("test:fail_first_quota_data_create") })
	return failedModel
}

// failQuotaDataProbes rejects the `quota_data` existence probe of the flush, so a key
// that actually has a row is looked up as if the lookup itself had failed. The probe
// is singled out by its destination type: the test's own reads of `quota_data` scan
// into a slice or a counter, so they still reach the engine.
func failQuotaDataProbes(t *testing.T, db *gorm.DB) {
	t.Helper()
	db.Callback().Query().Before("gorm:query").Register("test:fail_quota_data_probe", func(tx *gorm.DB) {
		if tx.Statement.Table != "quota_data" {
			return
		}
		if _, ok := tx.Statement.Dest.(*QuotaData); !ok {
			return
		}
		tx.AddError(errInjectedQuotaDataWrite)
	})
	t.Cleanup(func() { db.Callback().Query().Remove("test:fail_quota_data_probe") })
}

// failQuotaDataUpdates rejects every `quota_data` increment of the flush, which is
// the branch a key takes when its row already exists.
func failQuotaDataUpdates(t *testing.T, db *gorm.DB) {
	t.Helper()
	db.Callback().Update().Before("gorm:update").Register("test:fail_quota_data_update", func(tx *gorm.DB) {
		if tx.Statement.Table != "quota_data" {
			return
		}
		tx.AddError(errInjectedQuotaDataWrite)
	})
	t.Cleanup(func() { db.Callback().Update().Remove("test:fail_quota_data_update") })
}
