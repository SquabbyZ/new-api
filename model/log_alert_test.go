package model

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// 本文件是异常用量检测的唯一测试文件，覆盖：
//   - AC-1a 确定性造数（直接 INSERT 进临时 ClickHouse 库，不经 Kafka、不经 HTTP）
//   - AC-2 不误伤（正常流量零发现 + 反向确认）
//   - AC-3 变异证明规则有判别力（开关 / 阈值双向）
//   - AC-7 饱和行必须从比率运算中排除并单独计数
//   - AC-10 聚合 SQL 在 sqlite / mysql / postgres 上的可移植性
//
// ⚠️ TEST_CLICKHOUSE_DSN 必须指向**自建的临时库**（先例名 rd_probe_abuse_ch）。
// model 包的 CH 夹具用 currentDatabase() + 裸表名 DROP TABLE IF EXISTS logs，
// 本会话已因此删掉过后端正在用的 newapi_logs 真表。

const (
	abuseFixtureWindowSeconds = 600
	abuseFixtureBaselineWins  = 6
)

// abuseAlertSaturatedMarkerLiteral 是饱和标记的**键形态**字面量：与生产的
// abuseAlertSaturatedMarker 谓词去掉两侧 LIKE 通配符后同形。它直接复用生产代码的
// 键名常量，因此键名改了这里会跟着改，不会出现第二份可漂移的字面量。
const abuseAlertSaturatedMarkerLiteral = `"` + abuseAlertSaturatedKey + `":`

// abuseFixtureRow 是一条直接写进日志库的日志行。
type abuseFixtureRow struct {
	TokenID   int32
	CreatedAt int64
	Type      int
	Quota     int64
	ModelName string
	Other     string
}

// withAbuseAlertClickHouseLogDB 把 LOG_DB 指向 TEST_CLICKHOUSE_DSN 上的临时库，
// 并在用例结束后恢复。未配置该环境变量时整条用例跳过。
func withAbuseAlertClickHouseLogDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := openClickHouseLogTestDB(t, &sqlRecorder{})

	previousLogDB := LOG_DB
	previousType := common.LogDatabaseType()
	LOG_DB = db
	common.SetLogDatabaseType(common.DatabaseTypeClickHouse)
	t.Cleanup(func() {
		LOG_DB = previousLogDB
		common.SetLogDatabaseType(previousType)
	})
	return db
}

// insertAbuseFixtureRows 直接 INSERT，因此造数是确定性的 —— 不经 Kafka（异步落库）、
// 不经 HTTP、不需要初始化系统。
func insertAbuseFixtureRows(t *testing.T, db *gorm.DB, rows []abuseFixtureRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, fmt.Sprintf("(%d,%d,%d,%d,%s,%s)",
			row.TokenID, row.CreatedAt, row.Type, row.Quota,
			abuseFixtureSQLString(row.ModelName), abuseFixtureSQLString(row.Other)))
	}
	statement := "INSERT INTO logs (token_id, created_at, type, quota, model_name, other) VALUES " +
		strings.Join(values, ",")
	require.NoError(t, db.Exec(statement).Error)
}

// abuseFixtureSQLString 用单引号字面量（ClickHouse 的 "..." 是标识符，不是字符串）。
func abuseFixtureSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// abuseFixtureForgedSaturationPath 是**客户端可控**的饱和标记伪装（D7）：把标记的
// 字面量放进 `request_path` 的**值**里。它的形状就是攻击者能构造出来的形态 ——
// `request_path` 的值来自 `ctx.Request.URL.Path`（service/log_info_generate.go）。
//
// 它必须**不**触发饱和排除：谓词要匹配的是 admin_info 的**键形态**
// （`"quota_saturation":`），而 JSON 字符串值里的 `"` 会被序列化器转义成 `\"`，
// 因此值内部永远凑不出那个字节序列。
const abuseFixtureForgedSaturationPath = `{"request_path":"/v1beta/models/gemini-2.0-flash:generateContent/quota_saturation"}`

// abuseFixtureWindows 返回基线窗（由旧到新，长度 baseline_windows）与当前窗起点，
// 再附上当前窗起点，便于调用方一次拿到全部 7 个窗口。
//
// 当前窗取**最后一个完整窗口**：检测比对的是已跑完的窗口，而不是 now 所在的那个
// 只跑了几秒的不完整窗口 —— 拿一个 30 秒的窗口比 6 个完整窗口，正常流量会被判成骤降。
func abuseFixtureWindows(now int64) ([]int64, int64) {
	currentEnd := now - now%abuseFixtureWindowSeconds
	currentStart := currentEnd - abuseFixtureWindowSeconds
	windows := make([]int64, abuseFixtureBaselineWins+1)
	for i := range abuseFixtureBaselineWins + 1 {
		windows[i] = currentStart - int64(abuseFixtureBaselineWins-i)*abuseFixtureWindowSeconds
	}
	return windows, currentStart
}

// abuseFixtureRowsInWindow 生成一个窗口内的 rowsPerWindow 行，行内偏移严格落在窗口内
// （不会溢出到下一个窗口），每行额度 quotaPerRow。
func abuseFixtureRowsInWindow(tokenID int32, window int64, rowsPerWindow int, quotaPerRow int64, logType int) []abuseFixtureRow {
	rows := make([]abuseFixtureRow, 0, rowsPerWindow)
	step := int64(abuseFixtureWindowSeconds) / int64(rowsPerWindow)
	if step < 1 {
		step = 1
	}
	for i := range rowsPerWindow {
		rows = append(rows, abuseFixtureRow{
			TokenID:   tokenID,
			CreatedAt: window + int64(i)*step,
			Type:      logType,
			Quota:     quotaPerRow,
			ModelName: "gpt-4o",
		})
	}
	return rows
}

// abuseFixtureRowsInWindows 对一组窗口批量生成「同形」数据。
func abuseFixtureRowsInWindows(tokenID int32, windows []int64, rowsPerWindow int, quotaPerRow int64, logType int) []abuseFixtureRow {
	rows := make([]abuseFixtureRow, 0, len(windows)*rowsPerWindow)
	for _, window := range windows {
		rows = append(rows, abuseFixtureRowsInWindow(tokenID, window, rowsPerWindow, quotaPerRow, logType)...)
	}
	return rows
}

// buildAbuseFixture 生成 AC-1a 要求的全部令牌。所有时间相对 now 计算，W = 600s。
func buildAbuseFixture(now int64) []abuseFixtureRow {
	windows, currentStart := abuseFixtureWindows(now)
	baseline := windows[:abuseFixtureBaselineWins]
	current := []int64{currentStart}

	rows := make([]abuseFixtureRow, 0, 4096)

	// 9001 正常令牌：基线与当前窗同形（每窗 10 行 quota=1000）。期望零发现。
	rows = append(rows, abuseFixtureRowsInWindows(9001, windows, 10, 1000, LogTypeConsume)...)

	// 9002 消费突增：基线与 9001 同形，当前窗每行 200000（窗口合计 200×）。
	rows = append(rows, abuseFixtureRowsInWindows(9002, baseline, 10, 1000, LogTypeConsume)...)
	rows = append(rows, abuseFixtureRowsInWindows(9002, current, 10, 200000, LogTypeConsume)...)

	// 9003 请求突增：基线每窗 10 行，当前窗 200 行（20×）。
	rows = append(rows, abuseFixtureRowsInWindows(9003, baseline, 10, 1000, LogTypeConsume)...)
	rows = append(rows, abuseFixtureRowsInWindows(9003, current, 200, 1000, LogTypeConsume)...)

	// 9004 失败率异常：基线每窗 100 行 type=2 + 1 行 type=5（1%）；当前窗 40 + 20（33%）。
	rows = append(rows, abuseFixtureRowsInWindows(9004, baseline, 100, 1000, LogTypeConsume)...)
	for _, window := range baseline {
		rows = append(rows, abuseFixtureRow{TokenID: 9004, CreatedAt: window + 500, Type: LogTypeError, ModelName: "gpt-4o"})
	}
	rows = append(rows, abuseFixtureRowsInWindows(9004, current, 40, 1000, LogTypeConsume)...)
	for i := range 20 {
		rows = append(rows, abuseFixtureRow{TokenID: 9004, CreatedAt: currentStart + 100 + int64(i*15), Type: LogTypeError, ModelName: "gpt-4o"})
	}

	// 9005 模型首现：回看窗内全是 gpt-4o，当前窗出现 zzz-new。
	rows = append(rows, abuseFixtureRowsInWindows(9005, baseline, 10, 1000, LogTypeConsume)...)
	rows = append(rows, abuseFixtureRow{TokenID: 9005, CreatedAt: currentStart + 10, Type: LogTypeConsume, Quota: 1000, ModelName: "zzz-new"})

	// 9006 基线不足：总共 1 行。
	rows = append(rows, abuseFixtureRow{TokenID: 9006, CreatedAt: currentStart + 20, Type: LogTypeConsume, Quota: 1000, ModelName: "gpt-4o"})

	// 9007 饱和行：基线与 9001 同形，当前窗的额度是被钳到边界的极大值且带饱和标记。
	//
	// 取 2^31-1 而不是 Int64 的极大值：那是 billing.md 写的真实单请求饱和边界，
	// 而且它加总之后**不会**溢出 —— 一个会溢出的夹具会让「排除饱和行」这条断言
	// 失去判别力（不加总溢出时额度自己就变小了，规则自然不触发，测试假通过）。
	rows = append(rows, abuseFixtureRowsInWindows(9007, baseline, 10, 1000, LogTypeConsume)...)
	for i := range 10 {
		rows = append(rows, abuseFixtureRow{
			TokenID:   9007,
			CreatedAt: currentStart + int64(i*60),
			Type:      LogTypeConsume,
			Quota:     2147483647,
			ModelName: "gpt-4o",
			Other:     `{"admin_info":{"quota_saturation":{"clamped":true}}}`,
		})
	}

	// 9008 干净令牌（无基线）：只在当前窗有数据。
	rows = append(rows, abuseFixtureRowsInWindows(9008, current, 5, 1000, LogTypeConsume)...)

	// 9009 弱突增（3×）：默认比率 5.0 下不触发，比率调到 2 时开始触发。
	rows = append(rows, abuseFixtureRowsInWindows(9009, baseline, 10, 100000, LogTypeConsume)...)
	rows = append(rows, abuseFixtureRowsInWindows(9009, current, 10, 300000, LogTypeConsume)...)

	// 9010 饱和标记伪装（D7）：与 9001 同形的基线，当前窗每行 200000，且每行的
	// `other` 把 `quota_saturation` 放进**客户端可控的 `request_path` 值**里。
	// 谓词必须只认 admin_info 的**键形态**；若它只匹配裸子串，攻击者用这条路径就能
	// 把自己那条巨量消费行从比率运算里剔除，从而压掉自己的消费告警 —— 而本 slice
	// 的唯一目的就是发现盗刷。期望：`consume_spike` **照常命中**。
	forged := abuseFixtureRowsInWindow(9010, currentStart, 10, 200000, LogTypeConsume)
	for i := range forged {
		forged[i].Other = abuseFixtureForgedSaturationPath
	}
	rows = append(rows, abuseFixtureRowsInWindows(9010, baseline, 10, 1000, LogTypeConsume)...)
	rows = append(rows, forged...)

	// 9011 `type=5` 逐类开关被关掉的形态（PRD 数据边界，D1）：全部 7 个窗口只有
	// `type=2`，**一条 `type=5` 都没有**。该令牌**通过**基线门，所以 PRD 要求它的
	// `error_rate_spike` 记 `insufficient_baseline` —— 既不是 finding，也不是沉默。
	rows = append(rows, abuseFixtureRowsInWindows(9011, windows, 10, 1000, LogTypeConsume)...)

	// 9012 基线零错误的退化形态（D2）：基线每窗 100 行 `type=2`、**0 行** `type=5`；
	// 当前窗 100 行 `type=2` + 25 行 `type=5`（25 ≥ `min_errors`=20）。
	// 旧判定式 `currentRate >= error_rate_ratio × 0` 恒真，于是它会被当成
	// 「当前错误数 ≥ 绝对地板」而命中 —— 正是 PRD §选定 4.1 明令禁止的绝对量判定。
	// 期望：**不命中**，且记 `insufficient_baseline`。
	rows = append(rows, abuseFixtureRowsInWindows(9012, baseline, 100, 1000, LogTypeConsume)...)
	rows = append(rows, abuseFixtureRowsInWindows(9012, current, 100, 1000, LogTypeConsume)...)
	for i := range 25 {
		rows = append(rows, abuseFixtureRow{
			TokenID: 9012, CreatedAt: currentStart + 10 + int64(i*20), Type: LogTypeError, ModelName: "gpt-4o",
		})
	}

	return rows
}

func hasAbuseFinding(result AbuseAlertScanResult, rule string, tokenID int32) bool {
	return abuseFindingByKey(result, rule, tokenID) != nil
}

func abuseFindingByKey(result AbuseAlertScanResult, rule string, tokenID int32) *AbuseAlertFinding {
	for i := range result.Findings {
		if result.Findings[i].Rule == rule && result.Findings[i].TokenID == tokenID {
			return &result.Findings[i]
		}
	}
	return nil
}

func hasAbuseSkip(result AbuseAlertScanResult, rule string, tokenID int32, reason string) bool {
	for _, skip := range result.Skipped {
		if skip.Rule == rule && skip.TokenID == tokenID && skip.Reason == reason {
			return true
		}
	}
	return false
}

// countAbuseFindingsForToken 统计一个令牌命中了多少条发现 —— AC-1a 断言的是
// 「恰好命中期望的规则」，所以既要断言命中，也要断言没有多命中。
func countAbuseFindingsForToken(result AbuseAlertScanResult, tokenID int32) int {
	count := 0
	for _, finding := range result.Findings {
		if finding.TokenID == tokenID {
			count++
		}
	}
	return count
}

func scanAbuseFixture(t *testing.T, now int64) AbuseAlertScanResult {
	t.Helper()
	result, err := ScanAbuseAlerts(context.Background(), time.Unix(now, 0), 0)
	require.NoError(t, err)
	return result
}

// TestScanAbuseAlertsDetectsEachRuleOnDeterministicData 是 AC-1a 的主用例：
// 断言命中的 (规则 id, token_id) 二元组，不是发现总数。
func TestScanAbuseAlertsDetectsEachRuleOnDeterministicData(t *testing.T) {
	db := withAbuseAlertClickHouseLogDB(t)
	now := time.Now().Unix()
	insertAbuseFixtureRows(t, db, buildAbuseFixture(now))
	result := scanAbuseFixture(t, now)

	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9002),
		"AC-1a: expected consume_spike for token 9002, findings=%v", result.Findings)
	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleRequestSpike, 9003),
		"AC-1a: expected request_spike for token 9003, findings=%v", result.Findings)
	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleErrorRateSpike, 9004),
		"AC-1a: expected error_rate_spike for token 9004, findings=%v", result.Findings)
	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleNewModel, 9005),
		"AC-1a: expected new_model for token 9005, findings=%v", result.Findings)

	// 9002 的证据比率 ≈ 200（当前窗 10×200000 / 基线中位数 10×1000）。
	consumeFinding := abuseFindingByKey(result, operation_setting.AbuseRuleConsumeSpike, 9002)
	require.NotNil(t, consumeFinding, "AC-1a: consume_spike finding for token 9002 is missing")
	assert.InDelta(t, 200.0, consumeFinding.Ratio, 1.0,
		"AC-1a: consume_spike evidence ratio must be ~200 for token 9002")
	assert.Equal(t, "up", consumeFinding.Direction,
		"AC-1a: rules report the upward direction only")

	newModelFinding := abuseFindingByKey(result, operation_setting.AbuseRuleNewModel, 9005)
	require.NotNil(t, newModelFinding, "AC-1a: new_model finding for token 9005 is missing")
	require.NotNil(t, newModelFinding.ModelName, "AC-1a: new_model finding must name the model")
	assert.Equal(t, "zzz-new", *newModelFinding.ModelName,
		"AC-1a: new_model must name the newly seen model")

	// AC-1a：每个期望命中的令牌**恰好**命中期望的那一条规则。
	assert.Equal(t, 1, countAbuseFindingsForToken(result, 9002), "AC-1a: token 9002 must fire exactly one rule")
	assert.Equal(t, 1, countAbuseFindingsForToken(result, 9003), "AC-1a: token 9003 must fire exactly one rule")
	assert.Equal(t, 1, countAbuseFindingsForToken(result, 9004), "AC-1a: token 9004 must fire exactly one rule")
	assert.Equal(t, 1, countAbuseFindingsForToken(result, 9005), "AC-1a: token 9005 must fire exactly one rule")

	// AC-1a / AC-5：9006 / 9008 出现在 skipped 且 reason 正确，且不在 findings。
	for _, tokenID := range []int32{9006, 9008} {
		assert.Equal(t, 0, countAbuseFindingsForToken(result, tokenID),
			"AC-1a: token %d has no usable baseline and must not appear in findings", tokenID)
		for _, rule := range []string{
			operation_setting.AbuseRuleConsumeSpike,
			operation_setting.AbuseRuleRequestSpike,
			operation_setting.AbuseRuleErrorRateSpike,
		} {
			assert.True(t, hasAbuseSkip(result, rule, tokenID, AbuseAlertSkipInsufficientBaseline),
				"AC-5: token %d must be reported as insufficient_baseline for %s, not silently dropped (skipped=%v)",
				tokenID, rule, result.Skipped)
		}
	}

	// AC-5 的第三种状态（D1，PRD 数据边界）：`type=5` 的逐类开关被关掉时，该令牌
	// 一条 `type=5` 都读不到 ⇒ 失败率没有可比对象。它**通过**基线门，因此老的实现
	// 会让它既不产 finding 也不产 skip（运维者什么也看不到，与「没有异常」无法区分）。
	assert.Equal(t, 0, countAbuseFindingsForToken(result, 9011),
		"AC-5: token 9011 has no type=5 row at all, so it must not produce findings=%v", result.Findings)
	assert.True(t, hasAbuseSkip(result, operation_setting.AbuseRuleErrorRateSpike, 9011, AbuseAlertSkipInsufficientBaseline),
		"AC-5: a token whose error-log category is switched off must be reported as insufficient_baseline for error_rate_spike, not silently dropped (skipped=%v)",
		result.Skipped)

	// D2（比率的分母退化）：基线 100 行 type=2 + 0 行 type=5，当前窗 25 个错误
	// （≥ min_errors=20）。`currentRate >= error_rate_ratio × 0` 恒真，所以旧的
	// 判定式把它当成「错误数 ≥ 绝对地板」而命中 —— PRD §选定 4.1 禁止的绝对量判定。
	assert.False(t, hasAbuseFinding(result, operation_setting.AbuseRuleErrorRateSpike, 9012),
		"D2: a token whose baseline has zero errors must NOT be judged by the absolute error floor (findings=%v)",
		result.Findings)
	assert.True(t, hasAbuseSkip(result, operation_setting.AbuseRuleErrorRateSpike, 9012, AbuseAlertSkipInsufficientBaseline),
		"D2: the degenerate denominator must be reported as insufficient_baseline (skipped=%v)", result.Skipped)
}

// TestScanAbuseAlertsDoesNotFireOnNormalTraffic 是 AC-2。
func TestScanAbuseAlertsDoesNotFireOnNormalTraffic(t *testing.T) {
	db := withAbuseAlertClickHouseLogDB(t)
	now := time.Now().Unix()
	insertAbuseFixtureRows(t, db, buildAbuseFixture(now))
	result := scanAbuseFixture(t, now)

	// AC-2 第 1 条：9001（与基线同形）与 9008（无基线）都必须在 findings 中缺席。
	assert.Equal(t, 0, countAbuseFindingsForToken(result, 9001),
		"AC-2: normal traffic (token 9001) produced findings=%v", result.Findings)
	assert.Equal(t, 0, countAbuseFindingsForToken(result, 9008),
		"AC-2: token 9008 without baseline produced findings=%v", result.Findings)

	// AC-2 反向确认（防「测试永远通过」）：零发现必须能区分成「检测过但没触发」与
	// 「根本没检测」。两条断言把它钉死：
	//   1) 9008 出现在 skipped —— skip 通道确实在工作；
	//   2) 9002 的基线形状与 9001 逐行相同，只有当前窗被放大 —— 它必须命中。
	//      于是「9001 零发现」= 「同一形状被评估过且没触发」，而不是「没看」。
	assert.True(t, hasAbuseSkip(result, operation_setting.AbuseRuleConsumeSpike, 9008, AbuseAlertSkipInsufficientBaseline),
		"AC-2: token 9008 must be reported as skipped, otherwise 'no finding' is indistinguishable from 'not scanned'")
	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9002),
		"AC-2: a token whose baseline is row-for-row identical to token 9001 must fire when its "+
			"current window spikes, otherwise the zero finding for 9001 proves nothing")

	// AC-2 第 2 条：正常流量不产生任何会被推送的高危发现。用白名单断言，
	// 比断言「高危数量 == 0」精确：造数里本来就有三个刻意异常的令牌。
	for _, finding := range result.Findings {
		if finding.Severity != operation_setting.AbuseSeverityHigh {
			continue
		}
		assert.Contains(t, []int32{9002, 9004, 9010}, finding.TokenID,
			"AC-2: only the deliberately abnormal fixtures may produce a high-severity (pushed) finding, got %+v", finding)
	}
}

// TestScanAbuseAlertsExcludesSaturatedRows 是 AC-7 / R2：被钳过的 quota 是极大值，
// 计进比率会直接造出假盗刷告警，必须排除并单独计数。
func TestScanAbuseAlertsExcludesSaturatedRows(t *testing.T) {
	db := withAbuseAlertClickHouseLogDB(t)
	now := time.Now().Unix()
	insertAbuseFixtureRows(t, db, buildAbuseFixture(now))
	result := scanAbuseFixture(t, now)

	assert.False(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9007),
		"AC-7: the saturated token must not produce consume_spike; its quota is a clamp artifact, not real usage")
	assert.Equal(t, int64(10), result.ExcludedSaturatedRows,
		"AC-7: exactly the saturated rows (token 9007's current window) must be counted, no more and no fewer")

	// D7：谓词必须匹配 admin_info 的**键形态**，不能匹配裸子串。9010 把
	// `quota_saturation` 放进客户端可控的 `request_path` 值里，若谓词匹配裸子串，
	// 它那条 200 倍巨量消费行会被从比率运算里剔除 —— 攻击者于是能压掉**自己的**
	// 消费告警。这条断言与上一条一起把它钉死：既不能少算（9007 必须被剔除），
	// 也不能多算（9010 必须不被剔除）。
	assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9010),
		"D7: a token whose client-controlled request_path contains the marker text must still fire consume_spike, "+
			"otherwise the predicate is forgeable by the very token it is meant to catch (findings=%v)", result.Findings)
}

// TestScanAbuseAlertsRuleMutations 是 AC-3：变异必须让**指定的那条断言**翻转，
// 因此每条断言都带自己的说明，失败时输出直接指出是哪一条。
func TestScanAbuseAlertsRuleMutations(t *testing.T) {
	db := withAbuseAlertClickHouseLogDB(t)
	restoreAbuseAlertOptionDB(t)
	now := time.Now().Unix()
	insertAbuseFixtureRows(t, db, buildAbuseFixture(now))

	baseline := scanAbuseFixture(t, now)
	require.True(t, hasAbuseFinding(baseline, operation_setting.AbuseRuleConsumeSpike, 9002),
		"AC-3 setup: token 9002 must fire consume_spike before any mutation")
	require.False(t, hasAbuseFinding(baseline, operation_setting.AbuseRuleConsumeSpike, 9009),
		"AC-3 setup: the 3x weak spike (token 9009) must not fire at the default ratio 5.0")

	// 每个变异用例只改一个键，并在本用例结束前写回 "null" —— 变异必须彼此独立，
	// 否则「开关关掉」会一路影响后面的阈值用例，让它们全部静默通过。
	const (
		switchKey = "abuse_alert_setting.rule_consume_spike_enabled"
		ratioKey  = "abuse_alert_setting.consume_ratio"
	)
	mutate := func(key string, value string) func() {
		setAbuseAlertOption(t, key, value)
		return func() { setAbuseAlertOption(t, key, "null") }
	}

	// 变异 1：规则开关关掉 —— 发现必须消失（证明开关真的接线，不是装饰）。
	t.Run("rule switch off removes the finding", func(t *testing.T) {
		restore := mutate(switchKey, "false")
		defer restore()
		result := scanAbuseFixture(t, now)
		assert.False(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9002),
			"AC-3 mutation 1 FAILED [assertion: consume_spike must disappear for token 9002 when "+
				"abuse_alert_setting.rule_consume_spike_enabled=false]: the finding is still present, "+
				"so the rule switch is not wired to the detector")
	})

	// 变异 2a：比率调到 1000 —— 同一份数据下发现必须消失（证明阈值是判定本体）。
	t.Run("a higher ratio removes the finding", func(t *testing.T) {
		restore := mutate(ratioKey, "1000")
		defer restore()
		result := scanAbuseFixture(t, now)
		assert.False(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9002),
			"AC-3 mutation 2a FAILED [assertion: consume_spike must disappear for token 9002 when "+
				"abuse_alert_setting.consume_ratio=1000]: the finding is still present, "+
				"so the threshold is not the decision body")
	})

	// 变异 2b：比率调到 2 —— 一个原本不触发的 3× 弱突增必须开始触发
	// （证明阈值能双向移动，不是一个只会变松的数字）。
	t.Run("a lower ratio starts firing the weak spike", func(t *testing.T) {
		restore := mutate(ratioKey, "2")
		defer restore()
		result := scanAbuseFixture(t, now)
		assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9009),
			"AC-3 mutation 2b FAILED [assertion: consume_spike must appear for token 9009 when "+
				"abuse_alert_setting.consume_ratio=2]: the 3x weak spike still does not fire, "+
				"so lowering the threshold has no effect")
	})

	// 变异回收：所有键回到默认后，9002 必须重新命中、9009 必须重新沉默 ——
	// 证明前面的消失是设置造成的，而不是数据被消耗或残留状态污染。
	t.Run("restoring the defaults restores the verdict", func(t *testing.T) {
		result := scanAbuseFixture(t, now)
		assert.True(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9002),
			"AC-3 cleanup FAILED [assertion: consume_spike must come back for token 9002 once the "+
				"mutated option keys are restored to their defaults]: the finding is still missing, "+
				"so an earlier mutation leaked into this case")
		assert.False(t, hasAbuseFinding(result, operation_setting.AbuseRuleConsumeSpike, 9009),
			"AC-3 cleanup FAILED [assertion: the 3x weak spike must stop firing again once "+
				"abuse_alert_setting.consume_ratio is back to its default 5.0]: it still fires, "+
				"so a lowered threshold leaked into this case")
	})
}

// TestValidateAbuseAlertOptionRejectsOutOfRangeValues 覆盖 PRD 的异常输入边界：
// 非法值在落库前被拒并保持原值，而不是让检测带着一个恒真的阈值跑。
func TestValidateAbuseAlertOptionRejectsOutOfRangeValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
		ok    bool
	}{
		{name: "null restores the default", key: "abuse_alert_setting.window_minutes", value: "null", ok: true},
		{name: "window below the lower bound", key: "abuse_alert_setting.window_minutes", value: "0"},
		{name: "window above the upper bound", key: "abuse_alert_setting.window_minutes", value: "61"},
		{name: "baseline windows below the lower bound", key: "abuse_alert_setting.baseline_windows", value: "0"},
		// D3：baseline_windows < AbuseAlertMinBaselineWindowsGate 时基线门
		// `qualified >= Gate` 永不可能成立，三条窗口规则被这条配置**静默关闭**，
		// 而它们只报 insufficient_baseline（读起来像「数据不够」）。必须拒绝。
		{name: "baseline windows below the fixed baseline gate", key: "abuse_alert_setting.baseline_windows", value: "2"},
		{name: "baseline windows exactly at the fixed baseline gate", key: "abuse_alert_setting.baseline_windows", value: "3", ok: true},
		{name: "a ratio of 1 would make the rule always fire", key: "abuse_alert_setting.consume_ratio", value: "1"},
		{name: "a ratio below 1", key: "abuse_alert_setting.consume_ratio", value: "0.5"},
		{name: "a negative request floor", key: "abuse_alert_setting.min_requests", value: "-1"},
		{name: "a negative quota floor", key: "abuse_alert_setting.min_consume_quota", value: "-1"},
		{name: "lookback above the upper bound", key: "abuse_alert_setting.new_model_lookback_hours", value: "169"},
		{name: "lookback below the lower bound", key: "abuse_alert_setting.new_model_lookback_hours", value: "0"},
		{name: "an unknown key in this module", key: "abuse_alert_setting.not_a_field", value: "1"},
		{name: "a valid ratio", key: "abuse_alert_setting.consume_ratio", value: "5", ok: true},
		{name: "a valid switch", key: "abuse_alert_setting.rule_new_model_enabled", value: "false", ok: true},
		{name: "a key from another module is not our business", key: "error_log_setting.enabled", value: "true", ok: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := operation_setting.ValidateAbuseAlertOption(testCase.key, testCase.value)
			if testCase.ok {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err,
				"boundary case: %s=%q must be rejected before it reaches the detector", testCase.key, testCase.value)
		})
	}
}

// abuseDialectAggregate 是方言矩阵的比较口径：把聚合行折成可比较的标量集合，
// 这样断言的是「四库算出同一批数字」，而不是「四库返回同样的行序」。
type abuseDialectAggregate struct {
	Rows           int
	Requests       int64
	Quota          int64
	Consumes       int64
	Errors         int64
	SaturatedRows  int64
	SaturatedQuota int64
}

func abuseDialectActualAggregates(aggs []abuseAlertWindowAgg) abuseDialectAggregate {
	var out abuseDialectAggregate
	out.Rows = len(aggs)
	for _, agg := range aggs {
		out.Requests += agg.Requests
		out.Quota += agg.Quota
		out.Consumes += agg.Consumes
		out.Errors += agg.Errors
		out.SaturatedRows += agg.SaturatedRows
		out.SaturatedQuota += agg.SaturatedQuota
	}
	return out
}

// abuseDialectExpectedAggregates 用 Go 重算同一批数字。这不是「把生产逻辑抄进测试」：
// 它算的是 SQL 的四则运算结果（计数与求和），窗口与规则判定仍然只由生产代码负责。
func abuseDialectExpectedAggregates(rows []abuseFixtureRow) abuseDialectAggregate {
	buckets := make(map[[2]int64]struct{})
	out := abuseDialectAggregate{}
	for _, row := range rows {
		buckets[[2]int64{int64(row.TokenID), row.CreatedAt - row.CreatedAt%abuseFixtureWindowSeconds}] = struct{}{}
		out.Requests++
		if row.Type == LogTypeConsume {
			out.Consumes++
			out.Quota += row.Quota
		}
		if row.Type == LogTypeError {
			out.Errors++
		}
		if strings.Contains(row.Other, abuseAlertSaturatedMarkerLiteral) {
			out.SaturatedRows++
			out.SaturatedQuota += row.Quota
		}
	}
	out.Rows = len(buckets)
	return out
}

// TestAbuseAlertSaturatedMarkerMatchesTheStoredShape 是 AC-7 的**接线证明**：
// 检测用的 LIKE 谓词必须匹配**写入侧真实产生的**落库串。
//
// 它防的是 D7 修复可能引入的相反方向的故障 —— 谓词收得太紧而不再匹配任何行，
// 于是饱和排除在**生产**里静默失效、R2 要挡的假盗警全部回来。断言的对象是
// `LogOther.JSONString()` 的真实输出（生产序列化器），不是手写字面量。
func TestAbuseAlertSaturatedMarkerMatchesTheStoredShape(t *testing.T) {
	pattern := strings.Trim(abuseAlertSaturatedMarker, "%")

	// 写入侧形态与 service/log_info_generate.go 的
	// `other.SetAdmin("quota_saturation", clamp.AuditMap())` 一致。
	written := NewLogOther()
	require.True(t, written.SetAdmin(abuseAlertSaturatedKey, map[string]any{"clamped": true}))
	assert.Contains(t, written.JSONString(), pattern,
		"AC-7: the stored `other` must contain the LIKE pattern %q, otherwise saturated rows are never excluded in production",
		pattern)

	// 反面：攻击者把**键形态本身**塞进客户端可控的 request_path。序列化器会把值里的
	// 每一个 `"` 转义成 `\"`，所以这段字节序列凑不出来 —— 谓词不可伪造。
	forged := NewLogOther()
	forged.SetPublic("request_path", `/v1beta/models/x:generateContent/"`+abuseAlertSaturatedKey+`":`)
	assert.NotContains(t, forged.JSONString(), pattern,
		"D7: a client-controlled request_path that spells out the admin_info key form must still not match the predicate")
}

// TestAbuseAlertWindowAggregateIsPortableAcrossDialects 是 AC-10：同一份数据在
// sqlite / mysql / postgres 上必须返回相同的窗口聚合，且窗口桶的取模对齐切出相同的桶。
// 窗口桶在 SQL 里用取模对齐、统计在 Go 里算 —— 这条用例是那个选择的可执行证据。
func TestAbuseAlertWindowAggregateIsPortableAcrossDialects(t *testing.T) {
	// 饱和行的 quota 是 Int64 极大值，SUM 会溢出 —— 可移植性只关心构造，
	// 所以这里用剔掉饱和行的同一份造数。
	rows := make([]abuseFixtureRow, 0, 4096)
	for _, row := range buildAbuseFixture(time.Now().Unix()) {
		if row.Other != "" {
			continue
		}
		rows = append(rows, row)
	}
	rangeEnd := time.Now().Unix() + 3600

	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var driver gorm.Dialector
			switch dialect {
			case "sqlite":
				driver = sqlite.Open(":memory:")
			case "mysql":
				dsn := os.Getenv("TEST_MYSQL_DSN")
				if dsn == "" {
					t.Skip("TEST_MYSQL_DSN not configured")
				}
				driver = mysql.Open(dsn)
			default:
				dsn := os.Getenv("TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("TEST_POSTGRES_DSN not configured")
				}
				driver = postgres.Open(dsn)
			}

			db, err := gorm.Open(driver, &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			sqlDB.SetMaxOpenConns(1)

			previousLogDB := LOG_DB
			previousType := common.LogDatabaseType()
			LOG_DB = db
			common.SetLogDatabaseType(common.DatabaseType(dialect))
			t.Cleanup(func() {
				_ = db.Exec("DROP TABLE IF EXISTS abuse_alert_probe_logs").Error
				LOG_DB = previousLogDB
				common.SetLogDatabaseType(previousType)
				_ = sqlDB.Close()
			})

			require.NoError(t, db.Exec("DROP TABLE IF EXISTS abuse_alert_probe_logs").Error)
			require.NoError(t, db.Exec(`
CREATE TABLE abuse_alert_probe_logs (
    token_id BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL DEFAULT 0,
    type BIGINT NOT NULL DEFAULT 0,
    quota BIGINT NOT NULL DEFAULT 0,
    model_name VARCHAR(255) NOT NULL DEFAULT '',
    other VARCHAR(255) NOT NULL DEFAULT ''
)`).Error)

			values := make([]string, 0, len(rows))
			for _, row := range rows {
				values = append(values, fmt.Sprintf("(%d,%d,%d,%d,%s,%s)",
					row.TokenID, row.CreatedAt, row.Type, row.Quota,
					abuseFixtureSQLString(row.ModelName), abuseFixtureSQLString(row.Other)))
			}
			require.NoError(t, db.Exec("INSERT INTO abuse_alert_probe_logs (token_id, created_at, type, quota, model_name, other) VALUES "+
				strings.Join(values, ",")).Error)

			// 跑**生产的那条聚合 SQL**（只换表名），不是一条等价的重写 ——
			// 用更宽松的查询证明可移植性，证明不了真正上线的那条语句能跑。
			var aggs []abuseAlertWindowAgg
			require.NoError(t, db.Raw(
				strings.Replace(abuseAlertWindowAggregateSQL, abuseAlertTablePlaceholder, "abuse_alert_probe_logs", 1),
				int64(abuseFixtureWindowSeconds),
				LogTypeConsume, LogTypeConsume, LogTypeError,
				abuseAlertSaturatedMarker, abuseAlertSaturatedMarker,
				0, rangeEnd,
			).Scan(&aggs).Error, "AC-10: the production window aggregate must run on %s", dialect)

			expected := abuseDialectExpectedAggregates(rows)
			assert.Equal(t, expected, abuseDialectActualAggregates(aggs),
				"AC-10: %s must return the same window aggregate as the other dialects", dialect)

			// 窗口桶必须与 Go 侧划分一致：token 9001 的每个窗口恰好一个桶、10 行。
			bucketsFor9001 := make([]int64, 0, len(aggs))
			for _, agg := range aggs {
				if agg.TokenID == 9001 {
					bucketsFor9001 = append(bucketsFor9001, agg.Bucket)
				}
			}
			require.Len(t, bucketsFor9001, abuseFixtureBaselineWins+1,
				"AC-10: %s must cut exactly one bucket per window for token 9001, got %v", dialect, bucketsFor9001)
			previous := int64(-1)
			for _, bucket := range bucketsFor9001 {
				assert.Zero(t, bucket%abuseFixtureWindowSeconds,
					"AC-10: %s must align the bucket to the window boundary", dialect)
				require.Greater(t, bucket, previous,
					"AC-10: %s must resolve the bucket identically to the Go-side window layout", dialect)
				previous = bucket
			}

			// 步骤 2 的生产 SQL 同样要在四种方言上跑得通。
			var models []abuseAlertModelAgg
			require.NoError(t, db.Raw(
				strings.Replace(abuseAlertModelAggregateSQL, abuseAlertTablePlaceholder, "abuse_alert_probe_logs", 1),
				0, rangeEnd,
			).Scan(&models).Error, "AC-10: the production model aggregate must run on %s", dialect)
			require.NotEmpty(t, models, "AC-10: %s must return the (token, model) rows", dialect)
			for _, row := range models {
				assert.NotEmpty(t, row.ModelName,
					"AC-10: %s must not return the empty model bucket", dialect)
				require.Greater(t, row.FirstSeen, int64(0),
					"AC-10: %s must report a first-seen timestamp", dialect)
			}
		})
	}
}

// abuseAlertOptionKeys 是变异用例会改到的键。改过之后必须写回 "null"（= 未设置），
// 否则会污染同一次运行里的其它用例。
var abuseAlertOptionKeys = []string{
	"abuse_alert_setting.rule_consume_spike_enabled",
	"abuse_alert_setting.consume_ratio",
}

// restoreAbuseAlertOptionDB 给变异用例准备一个可写的主库 + OptionMap 快照。
func restoreAbuseAlertOptionDB(t *testing.T) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{NamingStrategy: schema.NamingStrategy{}})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	previousDB := DB
	previousType := common.MainDatabaseType()
	DB = db
	common.SetMainDatabaseType(common.DatabaseTypeSQLite)
	initCol()

	common.OptionMapRWMutex.Lock()
	previousOptions := maps.Clone(common.OptionMap)
	if previousOptions == nil {
		previousOptions = map[string]string{}
	}
	// 这个包里的用例不一定跑过 loadOptionsFromDatabase，OptionMap 可能是 nil；
	// updateOptionMap 会直接往它里面写，所以这里必须给一个可写的 map。
	common.OptionMap = maps.Clone(previousOptions)
	common.OptionMapRWMutex.Unlock()

	t.Cleanup(func() {
		for _, key := range abuseAlertOptionKeys {
			require.NoError(t, updateOptionMap(key, "null"))
		}
		common.OptionMapRWMutex.Lock()
		common.OptionMap = previousOptions
		common.OptionMapRWMutex.Unlock()
		DB = previousDB
		common.SetMainDatabaseType(previousType)
		initCol()
		require.NoError(t, sqlDB.Close())
	})

	require.NoError(t, db.AutoMigrate(&Option{}))
}

// setAbuseAlertOption 走生产写入路径改设置 —— 变异必须证明**真实接线**，
// 直接改包级变量证明不了任何东西。
func setAbuseAlertOption(t *testing.T, key string, value string) {
	t.Helper()
	require.NoError(t, UpdateOption(key, value))
}

// TestBuildAbuseAlertMapReportsDefaultsAndUncomputableRules 是 AC-6 的判定层：
// 全新部署（option 表里没有任何 abuse_alert_setting.* 行）下，四条可计算规则的
// switch.source 必须是 "default"、enabled 必须等于内置默认，两条不可计算规则必须
// computable=false 且 switch=null。
func TestBuildAbuseAlertMapReportsDefaultsAndUncomputableRules(t *testing.T) {
	payload := operation_setting.BuildAbuseAlertMapPayload(1790000000, false, 1790003600, 0)

	byID := make(map[string]operation_setting.AbuseAlertRule, len(payload.Rules))
	for _, rule := range payload.Rules {
		byID[rule.ID] = rule
	}
	require.Len(t, payload.Rules, 6, "AC-6: the payload must carry four computable and two uncomputable rules")

	for _, ruleID := range []string{
		operation_setting.AbuseRuleConsumeSpike,
		operation_setting.AbuseRuleRequestSpike,
		operation_setting.AbuseRuleErrorRateSpike,
		operation_setting.AbuseRuleNewModel,
	} {
		rule, ok := byID[ruleID]
		require.True(t, ok, "AC-6: rule %s is missing from the payload", ruleID)
		assert.True(t, rule.Computable, "AC-6: rule %s must be reported as computable", ruleID)
		require.NotNil(t, rule.Switch, "AC-6: rule %s must carry a switch", ruleID)
		assert.Equal(t, operation_setting.AbuseSwitchSourceDefault, rule.Switch.Source,
			"AC-6: rule %s must report source=default on a fresh deployment", ruleID)
		assert.True(t, rule.Switch.Enabled,
			"AC-6: rule %s is enabled by default; a fresh deployment must not silently lose protection", ruleID)
		assert.NotEmpty(t, rule.Switch.OptionKey,
			"AC-6: rule %s must tell the frontend which option key to write", ruleID)
	}

	for _, ruleID := range []string{
		operation_setting.AbuseRuleSourceAnomaly,
		operation_setting.AbuseRuleConcurrencyAnomaly,
	} {
		rule, ok := byID[ruleID]
		require.True(t, ok, "AC-6: rule %s is missing from the payload", ruleID)
		assert.False(t, rule.Computable, "AC-6: rule %s must be reported as not computable", ruleID)
		assert.Nil(t, rule.Switch,
			"AC-6: rule %s must not carry a switch; a disabled control would read as 'usable later'", ruleID)
		assert.NotEmpty(t, rule.ReasonKey,
			"AC-6: rule %s must explain why it cannot be computed", ruleID)
	}
}

// TestAbuseAlertOptionKeysSurviveTheSensitiveKeyFilter 钉住两个静默失效：
//
//  1. GET /api/option/ 按**后缀**丢弃 Token / Secret / Key / secret / api_key 结尾的键。
//     本模块的任何一个键只要踩中后缀，前端就再也读不到它的原始值 —— 而开关本身还能写进去，
//     表现为「保存成功但页面刷新后没变」。
//  2. 写入侧校验器必须**认识**每一个键。键名拼错或字段被砍掉时，`PUT` 会返回
//     「unknown abuse alert setting」而写入被拒，页面同样表现为「保存成功但没变」。
//
// 第 2 条曾经是一条**不可能失败的断言**：它用 `"null"` 当探针，而校验器在进入 switch
// **之前**就对 `"null"` 直接返回 nil，于是任何键（拼错的、已砍掉的
// `min_baseline_windows`、空前缀）都会 NoError（QA 实测三种输入全部 NoError）。
// 现在每个键配一个**对该键合法**的探针值，校验器必须真的走到那个键的分支。
func TestAbuseAlertOptionKeysSurviveTheSensitiveKeyFilter(t *testing.T) {
	sensitiveSuffixes := []string{"Token", "Secret", "Key", "secret", "api_key"}
	payload := operation_setting.BuildAbuseAlertMapPayload(1790000000, true, 0, 30)

	probes := map[string]string{
		"abuse_alert_setting.enabled":                  "true",
		"abuse_alert_setting.notify_enabled":           "true",
		"abuse_alert_setting.window_minutes":           "10",
		"abuse_alert_setting.baseline_windows":         "6",
		"abuse_alert_setting.scan_interval_minutes":    "5",
		"abuse_alert_setting.min_baseline_requests":    "10",
		"abuse_alert_setting.consume_ratio":            "5",
		"abuse_alert_setting.request_ratio":            "5",
		"abuse_alert_setting.error_rate_ratio":         "5",
		"abuse_alert_setting.min_consume_quota":        "500000",
		"abuse_alert_setting.min_requests":             "50",
		"abuse_alert_setting.min_errors":               "20",
		"abuse_alert_setting.new_model_lookback_hours": "24",
	}
	// 规则的 option key 由**后端下发**决定，不在这里抄一份 —— 后者是第二份可漂移的
	// 清单，正是这个用例要防的东西。规则开关全部是布尔。
	for _, rule := range payload.Rules {
		if rule.Switch != nil {
			probes[rule.Switch.OptionKey] = "true"
		}
	}
	require.NotEmpty(t, probes, "the payload must carry the option keys to check")

	for key, probe := range probes {
		for _, suffix := range sensitiveSuffixes {
			assert.False(t, strings.HasSuffix(key, suffix),
				"the option key %q ends with the sensitive suffix %q and would be dropped from GET /api/option/",
				key, suffix)
		}
		assert.NoError(t, operation_setting.ValidateAbuseAlertOption(key, probe),
			"the writer-side validator must recognise the key %q and accept its own valid probe value %q; "+
				"a misspelled or removed key name fails here instead of passing on the \"null\" shortcut",
			key, probe)
	}
}
