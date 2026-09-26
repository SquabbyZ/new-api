package model

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lastStatementContaining returns the most recent recorded statement that
// contains the given fragment, so a test can assert on the SQL that reached the
// engine rather than only on the value it returned.
func lastStatementContaining(t *testing.T, recorder *sqlRecorder, fragment string) string {
	t.Helper()
	statements := recorder.recorded()
	for i := len(statements) - 1; i >= 0; i-- {
		if strings.Contains(statements[i], fragment) {
			return statements[i]
		}
	}
	require.FailNow(t, "no recorded statement contains "+fragment, "recorded: %v", statements)
	return ""
}

// normalizeIdentQuotes removes the only quoting difference between PostgreSQL
// and MySQL/SQLite, so one expected statement literal can be compared across
// all three engines.
func normalizeIdentQuotes(statement string) string {
	return strings.ReplaceAll(statement, "`", `"`)
}

// TestLogStatsApplyDefaultTimeWindow covers the dashboard log statistics on
// every supported engine. `quota`, `prompt_tokens` and `completion_tokens` are
// indexed by nothing on `logs`, so an aggregate that the caller never gave a
// lower `created_at` bound for cannot be answered from an index and turns into
// a full-table scan. The aggregates must therefore always carry a lower bound,
// while an explicit range from the caller keeps producing exactly the statement
// and the numbers it produced before.
func TestLogStatsApplyDefaultTimeWindow(t *testing.T) {
	previousLogDB := LOG_DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	t.Cleanup(func() {
		LOG_DB = previousLogDB
		common.SetDatabaseTypes(previousMain, previousLog)
	})

	// Fixed timestamps, so the statement emitted for an explicit range is a
	// constant that can be compared literally on any engine and any run.
	const (
		rangeStart = int64(1700000000)
		rangeEnd   = int64(1700086400)
		rangeQuota = 5
		rangeRows  = 4
	)
	// Rows a default-window query must include: recent, an hour old, and one
	// just inside the window edge.
	windowOffsets := []int64{60, 3600, logStatWindowSeconds - 120}
	const (
		windowQuota      = 100
		windowPrompt     = 10
		windowCompletion = 5
	)
	// Rows only an explicit range may reach.
	historicOffsets := []int64{logStatWindowSeconds + 86400, logStatWindowSeconds + 2*86400}
	const (
		historicQuota      = 7
		historicPrompt     = 1000
		historicCompletion = 1000
	)
	const (
		rangePrompt     = 1
		rangeCompletion = 1
	)

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

			now := time.Now().Unix()
			rows := make([]Log, 0, len(windowOffsets)+len(historicOffsets)+rangeRows)
			for _, offset := range windowOffsets {
				rows = append(rows, Log{
					UserId:           1,
					Type:             LogTypeConsume,
					CreatedAt:        now - offset,
					Quota:            windowQuota,
					PromptTokens:     windowPrompt,
					CompletionTokens: windowCompletion,
				})
			}
			for _, offset := range historicOffsets {
				rows = append(rows, Log{
					UserId:           1,
					Type:             LogTypeConsume,
					CreatedAt:        now - offset,
					Quota:            historicQuota,
					PromptTokens:     historicPrompt,
					CompletionTokens: historicCompletion,
				})
			}
			for offset := range int64(rangeRows) {
				rows = append(rows, Log{
					UserId:           1,
					Type:             LogTypeConsume,
					CreatedAt:        rangeStart + offset,
					Quota:            rangeQuota,
					PromptTokens:     rangePrompt,
					CompletionTokens: rangeCompletion,
				})
			}
			require.NoError(t, db.Create(&rows).Error)

			// Acceptance 1: with no start_timestamp at all the aggregate still
			// carries a lower created_at bound, and that bound is the start of
			// the default window rather than the epoch.
			stat, err := SumUsedQuota(LogTypeConsume, 0, 0, "", "", "", 0, "")
			require.NoError(t, err)
			quotaStatement := lastStatementContaining(t, recorder, "sum(quota)")
			t.Logf("%s emitted quota statement: %s", tc.name, quotaStatement)
			const boundPrefix = "created_at >= "
			boundAt := strings.Index(quotaStatement, boundPrefix)
			require.GreaterOrEqual(t, boundAt, 0, "%s must bound the aggregate in time", tc.name)
			boundText, _, _ := strings.Cut(quotaStatement[boundAt+len(boundPrefix):], " ")
			bound, parseErr := strconv.ParseInt(boundText, 10, 64)
			require.NoError(t, parseErr, "%s bound must be a bare timestamp: %q", tc.name, boundText)
			assert.InDelta(t, now-logStatWindowSeconds, bound, 120, "%s must use the default window", tc.name)

			// The window is what keeps the aggregate bounded, so rows past it
			// must no longer be part of the result.
			assert.Equal(t, windowQuota*len(windowOffsets), stat.Quota, "%s must sum only the default window", tc.name)

			token := SumUsedToken(LogTypeConsume, 0, 0, "", "", "")
			assert.Equal(t, len(windowOffsets)*(windowPrompt+windowCompletion), token, "%s must bound the token sum too", tc.name)

			// The rate window is a fixed 60 seconds of its own and must not
			// inherit the caller's range.
			rateStatement := lastStatementContaining(t, recorder, "count(*)")
			assert.Equal(t, 1, strings.Count(rateStatement, "created_at >= "), "%s rpm/tpm must keep its own single 60s window: %s", tc.name, rateStatement)

			// A value that is not a usable bound must not become a way around
			// the window: `created_at >= -1` selects every row.
			stat, err = SumUsedQuota(LogTypeConsume, -1, 0, "", "", "", 0, "")
			require.NoError(t, err)
			negativeStatement := lastStatementContaining(t, recorder, "sum(quota)")
			assert.NotContains(t, negativeStatement, "created_at >= -1", "%s must not accept a negative bound: %s", tc.name, negativeStatement)
			assert.Equal(t, windowQuota*len(windowOffsets), stat.Quota, "%s must fall back to the window for a negative start", tc.name)

			// A request that states only an upper bound keeps the period it
			// asked for instead of collapsing to an empty window.
			stat, err = SumUsedQuota(LogTypeConsume, 0, rangeEnd, "", "", "", 0, "")
			require.NoError(t, err)
			endOnlyStatement := lastStatementContaining(t, recorder, "sum(quota)")
			t.Logf("%s emitted end-only statement: %s", tc.name, endOnlyStatement)
			assert.Contains(t, endOnlyStatement, "created_at >= "+strconv.FormatInt(rangeEnd-logStatWindowSeconds, 10), "%s must anchor the fallback to the caller's upper bound: %s", tc.name, endOnlyStatement)
			assert.Equal(t, rangeQuota*rangeRows, stat.Quota, "%s must return the period an end-only request asked for", tc.name)

			// Acceptance 3: an explicit range is passed through untouched. The
			// expected statement is the one the pre-change implementation
			// produced for the same arguments, so this fails if the default
			// window ever rewrites or clamps a caller-supplied bound.
			stat, err = SumUsedQuota(LogTypeConsume, rangeStart, rangeEnd, "", "", "", 0, "")
			require.NoError(t, err)
			explicitStatement := lastStatementContaining(t, recorder, "sum(quota)")
			t.Logf("%s emitted explicit statement: %s", tc.name, explicitStatement)
			assert.Equal(t,
				`SELECT COALESCE(sum(quota), 0) quota FROM "logs" WHERE created_at >= 1700000000 AND created_at <= 1700086400 AND type = 2`,
				normalizeIdentQuotes(strings.TrimSpace(explicitStatement)))
			assert.Equal(t, rangeQuota*rangeRows, stat.Quota, "%s must not clamp an explicit range", tc.name)

			explicitRateStatement := lastStatementContaining(t, recorder, "count(*)")
			assert.NotContains(t, explicitRateStatement, strconv.FormatInt(rangeStart, 10), "%s rpm/tpm must ignore the caller's range: %s", tc.name, explicitRateStatement)
			assert.NotContains(t, explicitRateStatement, strconv.FormatInt(rangeEnd, 10), "%s rpm/tpm must ignore the caller's range: %s", tc.name, explicitRateStatement)

			// A range wider than the default window must still reach the rows
			// outside it, i.e. the fallback never becomes an upper bound.
			stat, err = SumUsedQuota(LogTypeConsume, now-400*86400, now, "", "", "", 0, "")
			require.NoError(t, err)
			assert.Equal(t, windowQuota*len(windowOffsets)+historicQuota*len(historicOffsets), stat.Quota, "%s must not truncate an explicit range", tc.name)
		})
	}
}
