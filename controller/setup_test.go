package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetSetupReportsLogDatabaseType(t *testing.T) {
	previousDB, previousLogDB := model.DB, model.LOG_DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	previousSetup := constant.Setup
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.User{}))
	model.DB, model.LOG_DB = db, db
	constant.Setup = false
	t.Cleanup(func() {
		model.DB, model.LOG_DB = previousDB, previousLogDB
		common.SetDatabaseTypes(previousMain, previousLog)
		constant.Setup = previousSetup
	})

	tests := []struct {
		name     string
		mainType common.DatabaseType
		logType  common.DatabaseType
		wantMain string
		wantLog  string
	}{
		{
			name:     "clickhouse log database is reported alongside the postgres primary",
			mainType: common.DatabaseTypePostgreSQL,
			logType:  common.DatabaseTypeClickHouse,
			wantMain: "postgres",
			wantLog:  "clickhouse",
		},
		{
			name:     "shared log database reports the primary type twice",
			mainType: common.DatabaseTypeMySQL,
			logType:  common.DatabaseTypeMySQL,
			wantMain: "mysql",
			wantLog:  "mysql",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			common.SetDatabaseTypes(testCase.mainType, testCase.logType)
			response := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(response)
			context.Request = httptest.NewRequest(http.MethodGet, "/api/setup", nil)

			GetSetup(context)

			var payload struct {
				Success bool  `json:"success"`
				Data    Setup `json:"data"`
			}
			require.NoError(t, common.Unmarshal(response.Body.Bytes(), &payload))
			assert.True(t, payload.Success)
			assert.Equal(t, testCase.wantMain, payload.Data.DatabaseType)
			assert.Equal(t, testCase.wantLog, payload.Data.LogDatabaseType)
		})
	}
}

func TestGetSetupInitializedReportsNoDatabaseTypes(t *testing.T) {
	previousSetup := constant.Setup
	constant.Setup = true
	t.Cleanup(func() { constant.Setup = previousSetup })

	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	context.Request = httptest.NewRequest(http.MethodGet, "/api/setup", nil)

	GetSetup(context)

	var payload struct {
		Success bool  `json:"success"`
		Data    Setup `json:"data"`
	}
	require.NoError(t, common.Unmarshal(response.Body.Bytes(), &payload))
	assert.True(t, payload.Success)
	assert.True(t, payload.Data.Status)
	assert.Empty(t, payload.Data.DatabaseType)
	assert.Empty(t, payload.Data.LogDatabaseType)
}
