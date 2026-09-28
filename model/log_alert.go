package model

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"
)

// 本文件是异常用量检测的**唯一**实现：一个有界的日志库聚合（只用四库语义一致的
// 构造）+ 在 Go 层做的窗口划分、中位数与比率判定。
//
// 为什么全在这里而不是拆 service：AC-1a 要求用 model 包既有的 ClickHouse 夹具
// （openClickHouseLogTestDB）做确定性造数并断言发现的 (规则 id, token_id) 二元组，
// 检测函数必须与那个夹具同包。service/abuse_alert.go 只负责「跑一次检测 + 推送」，
// controller 只负责端点 —— 没有第二份检测逻辑。

// AbuseAlertSkipInsufficientBaseline 是「基线不足」这一第三状态的 reason 值。
// 它既不是告警也不是沉默：页面必须把它显示出来，否则一个从不触发也不解释的检测
// 与不存在无法区分。
const AbuseAlertSkipInsufficientBaseline = "insufficient_baseline"

// 只报上升方向。下降（用量骤降）对盗刷没有意义，报它只会制造噪音。
const abuseAlertDirectionUp = "up"

// AbuseAlertFinding 是一条命中。窗口与证据齐全，且**不含任何凭据** ——
// 不带 tokens.key、不带请求体、不带响应体、不回显 IP（本 slice 本来也拿不到 IP）。
type AbuseAlertFinding struct {
	Rule           string  `json:"rule"`
	Severity       string  `json:"severity"`
	TokenID        int32   `json:"token_id"`
	ModelName      *string `json:"model_name"`
	WindowStart    int64   `json:"window_start"`
	WindowEnd      int64   `json:"window_end"`
	Current        float64 `json:"current"`
	BaselineMedian float64 `json:"baseline_median"`
	Ratio          float64 `json:"ratio"`
	Direction      string  `json:"direction"`
}

// AbuseAlertSkip 是一条「被检测到但没有结论」的记录。
type AbuseAlertSkip struct {
	Rule    string `json:"rule"`
	TokenID int32  `json:"token_id"`
	Reason  string `json:"reason"`
}

// AbuseAlertScanResult 是一次检测的完整结果。GET 端点与定时任务读的是同一个结构。
type AbuseAlertScanResult struct {
	WindowMinutes         int                 `json:"window_minutes"`
	CurrentWindowStart    int64               `json:"current_window_start"`
	CurrentWindowEnd      int64               `json:"current_window_end"`
	BaselineReady         bool                `json:"baseline_ready"`
	BaselineReadyAt       int64               `json:"baseline_ready_at"`
	Findings              []AbuseAlertFinding `json:"findings"`
	Skipped               []AbuseAlertSkip    `json:"skipped"`
	ExcludedSaturatedRows int64               `json:"excluded_saturated_rows"`
}

// abuseAlertWindowAgg 是一个 (令牌, 窗口) 桶的聚合结果，直接对应步骤 1 的 SQL 行。
//
// SaturatedRows / SaturatedQuota 是带 quota_saturation 标记的行 —— 它们的 quota 是
// 被钳到边界后的**极大值**，计进比率会直接造出假盗警（PRD §R2）。
type abuseAlertWindowAgg struct {
	TokenID        int32 `gorm:"column:token_id"`
	Bucket         int64 `gorm:"column:bucket"`
	Requests       int64 `gorm:"column:requests"`
	Consumes       int64 `gorm:"column:consumes"`
	Quota          int64 `gorm:"column:quota_sum"`
	Errors         int64 `gorm:"column:errors"`
	SaturatedRows  int64 `gorm:"column:saturated_rows"`
	SaturatedQuota int64 `gorm:"column:saturated_quota"`
}

// abuseAlertModelAgg 是一个 (令牌, 模型) 在回看窗内的首现与请求数。
type abuseAlertModelAgg struct {
	TokenID   int32  `gorm:"column:token_id"`
	ModelName string `gorm:"column:model_name"`
	FirstSeen int64  `gorm:"column:first_seen"`
	Requests  int64  `gorm:"column:requests"`
}

// abuseAlertSaturatedKey 是饱和标记在 `other` JSON 里的 admin_info 键名。
// 写入侧只有一处：service/log_info_generate.go 的
// `other.SetAdmin("quota_saturation", clamp.AuditMap())`。
const abuseAlertSaturatedKey = "quota_saturation"

// abuseAlertSaturatedMarker 是饱和标记的筛选串：连**键两侧的引号与冒号**一起匹配，
// 而不是匹配裸子串。
//
// **判定方案（AC-7 / R2 要求记录）**：在 SQL 侧筛选，而不是在 SQL 里解析 JSON，
// 也不是 Go 侧对候选令牌二次确认。理由：
//   - `other` 在 ClickHouse 侧刻意是 `String`（见 model/audit_log.go 记录的那次事故），
//     `JSONExtract*` 与其余三库不通用，解析 = 放弃四库一致性；
//   - `LIKE '%...%'` 只是**筛选**，不提取值，四库语义一致，且不需要 ESCAPE 方言分支；
//   - SQL 侧筛选是一次查询就能同时算出「排除后的配额」与「饱和行计数」，
//     而 Go 侧二次确认要再发一条 `token_id IN (...)` 查询，IN 列表的长度并不受控。
//
// **为什么必须匹配键形态，而不是裸子串（D7）**：`other` 是整串 JSON，里面同时装着
// `request_path`，而它的值来自 `ctx.Request.URL.Path` —— 即**客户端可控**的输入。
// 谓词若只是 `%quota_saturation%`，攻击者只要把该子串放进自己的请求路径，就能让
// **自己**那条巨量消费行的 quota 被剔除出比率运算，从而压掉自己的消费告警 ——
// 一个可被影响的漏报通道，而本 slice 的唯一目的就是发现盗刷。
//
// 匹配 `"quota_saturation":` 关掉了这个通道：`request_path` 是 JSON **字符串值**，
// 落库串由 `LogOther.JSONString()` 经 `common.Marshal`（encoding/json）序列化，
// 值内部的每一个 `"` 都被转义成 `\"`。因此攻击者在值里写入 `quota_saturation` 时，
// 它后面跟着的永远是 `\`，**不可能**凑出「裸 `"` + 键名 + `"` + `:`」这个字节序列；
// 而键名本身只由写入侧的代码决定。谓词因此不再受客户端数据左右，且仍然是**单次
// 扫描**（不新增查询、不新增 SQL，有界性不变）。
const abuseAlertSaturatedMarker = `%"` + abuseAlertSaturatedKey + `":%`

// abuseAlertBaselineQueryTimeout 是伪键路径上那次基线探测的超时。
const abuseAlertBaselineQueryTimeout = 3 * time.Second

// abuseAlertMaxSpanSeconds 返回单次扫描允许的最大跨度。
//
// 它是「单次扫描有界」的执行点：范围只由配置决定，**不随令牌总数或日志行数增长**。
// 上限取只读端点 hours 的天花板（默认 24 小时 = new_model 的默认回看）。
func abuseAlertMaxSpanSeconds() int64 {
	return int64(operation_setting.AbuseAlertMaxEndpointHours()) * 3600
}

// abuseAlertClampHours 把 hours 参数钳到 [1, 上限]。超限取上限，不报错、不扫描更大范围。
func abuseAlertClampHours(hours int) int {
	limit := operation_setting.AbuseAlertMaxEndpointHours()
	if hours <= 0 {
		return limit
	}
	return min(hours, limit)
}

// ScanAbuseAlerts 在日志库上做一次有界扫描并返回发现。
//
// requestedHours <= 0 时取上限（因此「默认」即「配置允许的最大回看」）。参数只影响
// 回看的长度，永远不能让扫描变成无界：窗口规则需要的跨度 (baseline_windows+1)×W
// 由配置上限保证，且整体被 abuseAlertMaxSpanSeconds() 截断（截断导致的基线不足会
// 如实报成 insufficient_baseline，不会被当成「没有异常」）。
//
// 「当前窗口」取**最后一个完整窗口**（[now-W, now) 向下对齐），不是 now 所在的那个
// 不完整窗口 —— 拿一个只跑了 30 秒的窗口去比 6 个完整窗口，会把正常流量判成骤降。
func ScanAbuseAlerts(ctx context.Context, now time.Time, requestedHours int) (AbuseAlertScanResult, error) {
	windowMinutes := operation_setting.AbuseAlertWindowMinutes()
	baselineWindows := operation_setting.AbuseAlertBaselineWindows()
	windowSeconds := int64(windowMinutes) * 60

	nowUnix := now.Unix()
	currentEnd := nowUnix - nowUnix%windowSeconds
	currentStart := currentEnd - windowSeconds

	lookbackSeconds := int64(abuseAlertClampHours(requestedHours)) * 3600
	if windowNeed := int64(baselineWindows+1) * windowSeconds; lookbackSeconds < windowNeed {
		lookbackSeconds = windowNeed
	}
	if maxSpan := abuseAlertMaxSpanSeconds(); lookbackSeconds > maxSpan {
		lookbackSeconds = maxSpan
	}
	windowRangeStart := currentStart - int64(baselineWindows)*windowSeconds

	result := AbuseAlertScanResult{
		WindowMinutes:      windowMinutes,
		CurrentWindowStart: currentStart,
		CurrentWindowEnd:   currentEnd,
		Findings:           []AbuseAlertFinding{},
		Skipped:            []AbuseAlertSkip{},
	}

	if LOG_DB == nil {
		return result, errAbuseAlertNoLogDatabase
	}

	result.BaselineReady, result.BaselineReadyAt = abuseAlertBaselineStatus(ctx, nowUnix, windowRangeStart, windowSeconds)

	rows, err := queryAbuseAlertWindows(ctx, windowRangeStart, currentEnd, windowSeconds)
	if err != nil {
		return result, err
	}

	result.Findings = append(result.Findings, abuseAlertWindowRuleFindings(rows, windowRangeStart, currentStart, windowSeconds, baselineWindows)...)
	result.Skipped = append(result.Skipped, abuseAlertWindowRuleSkips(rows, currentStart, windowSeconds, baselineWindows)...)
	for _, row := range rows {
		result.ExcludedSaturatedRows += row.SaturatedRows
	}

	if operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleNewModel) {
		modelFindings, modelSkips, err := abuseAlertNewModelFindings(ctx, currentStart, currentEnd, lookbackSeconds)
		if err != nil {
			return result, err
		}
		result.Findings = append(result.Findings, modelFindings...)
		result.Skipped = append(result.Skipped, modelSkips...)
	}

	sortAbuseAlertResult(&result)
	return result, nil
}

// errAbuseAlertNoLogDatabase 表示日志库未配置。调用方把它当成可读错误返回，
// 不 panic、不返回半截结果。
var errAbuseAlertNoLogDatabase = errors.New("the log database is not configured, so abuse alerts cannot be scanned")

// abuseAlertWindowAggregateSQL 是步骤 1 的聚合。表名用 %s 注入，好让 AC-10 的
// 方言矩阵能拿**生产的这一条 SQL**（只换表名）在 sqlite / mysql / postgres 上跑一遍 ——
// 用一条更宽松的等价查询去证明可移植性，证明不了真正上线的那条语句能跑。
const abuseAlertWindowAggregateSQL = `
SELECT token_id,
       created_at - (created_at % ?) AS bucket,
       COUNT(*) AS requests,
       SUM(CASE WHEN type = ? THEN 1 ELSE 0 END) AS consumes,
       SUM(CASE WHEN type = ? THEN quota ELSE 0 END) AS quota_sum,
       SUM(CASE WHEN type = ? THEN 1 ELSE 0 END) AS errors,
       SUM(CASE WHEN other LIKE ? THEN 1 ELSE 0 END) AS saturated_rows,
       SUM(CASE WHEN other LIKE ? THEN quota ELSE 0 END) AS saturated_quota
FROM {table}
WHERE created_at >= ? AND created_at < ? AND token_id > 0
GROUP BY token_id, bucket`

// abuseAlertTablePlaceholder 是聚合 SQL 的表名占位符。用字符串替换而不是
// fmt.Sprintf：SQL 里本来就有 `created_at % ?` 的取模，Sprintf 会把那个 % 当成动词。
const abuseAlertTablePlaceholder = "{table}"

// abuseAlertModelAggregateSQL 是步骤 2 的聚合（同样注入表名）。
const abuseAlertModelAggregateSQL = `
SELECT token_id,
       model_name,
       MIN(created_at) AS first_seen,
       COUNT(*) AS requests
FROM {table}
WHERE created_at >= ? AND created_at < ? AND token_id > 0 AND model_name <> ''
GROUP BY token_id, model_name`

// queryAbuseAlertWindows 是步骤 1：以 created_at 为范围谓词的窗口聚合。
//
// 窗口桶用 `created_at - (created_at % W)` 对齐，四库都支持取模与整数减法，
// 且不需要任何日期函数（PRD 明令禁止 toStartOfInterval / date_trunc / strftime）。
// 返回行数 = 范围内活跃的 (token_id, 窗口) 组合数，与日志行数无关。
//
// 聚合别名**不能**与真实列同名：ClickHouse 会把 SELECT 列表里定义的别名代入同一
// 列表的其它表达式，于是 `SUM(... quota ...) AS quota` 之后的任何 `quota` 都会解析成
// 那个 SUM，报「aggregate function inside another aggregate function」。所以额度聚合
// 叫 quota_sum，不叫 quota。
//
// token_id > 0 是 R8 的缓解：ClickHouse 的 logs.token_id 是 Int32，超过 2^31-1 会
// 静默回绕，回绕后的值会指向一个不相干的令牌 —— 归错人比漏报更糟。
func queryAbuseAlertWindows(ctx context.Context, rangeStart int64, rangeEnd int64, windowSeconds int64) ([]abuseAlertWindowAgg, error) {
	var rows []abuseAlertWindowAgg
	err := LOG_DB.WithContext(ctx).Raw(
		strings.Replace(abuseAlertWindowAggregateSQL, abuseAlertTablePlaceholder, "logs", 1),
		windowSeconds,
		LogTypeConsume, LogTypeConsume, LogTypeError,
		abuseAlertSaturatedMarker, abuseAlertSaturatedMarker,
		rangeStart, rangeEnd,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// queryAbuseAlertModels 是步骤 2：回看窗内每个 (令牌, 模型) 的首现时间与请求数。
//
// 返回行数 = 回看窗内出现过的 (token_id, model_name) 组合数，不是日志行数 ——
// 这一步是 R4（扫描成本）唯一的高成本项，所以它必须按「模型使用」而不是按「请求」聚合。
//
// 用 MIN(created_at) 而不是两次 DISTINCT 查询：首现时间落在当前窗内即「首现」，
// 一次聚合同时给出「是不是新的」与「该令牌在回看窗里的请求数」（基线门的输入）。
func queryAbuseAlertModels(ctx context.Context, rangeStart int64, rangeEnd int64) ([]abuseAlertModelAgg, error) {
	var rows []abuseAlertModelAgg
	err := LOG_DB.WithContext(ctx).Raw(
		strings.Replace(abuseAlertModelAggregateSQL, abuseAlertTablePlaceholder, "logs", 1),
		rangeStart, rangeEnd,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// AbuseAlertBaselineStatus 返回当前系统是否已积累足够的基线，以及预计就绪时间
// （0 = 现在还无法预计，例如日志库读不到）。
//
// 它被 GET /api/option/ 调用，所以**必须**失败软处理并且有超时：一个日志库抖动
// 不应该让整个设置页打不开，更不应该让请求挂在那里。
func AbuseAlertBaselineStatus() (bool, int64) {
	if LOG_DB == nil {
		return false, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), abuseAlertBaselineQueryTimeout)
	defer cancel()

	windowSeconds := int64(operation_setting.AbuseAlertWindowMinutes()) * 60
	nowUnix := time.Now().Unix()
	currentEnd := nowUnix - nowUnix%windowSeconds
	windowRangeStart := currentEnd - windowSeconds - int64(operation_setting.AbuseAlertBaselineWindows())*windowSeconds
	return abuseAlertBaselineStatus(ctx, nowUnix, windowRangeStart, windowSeconds)
}

// abuseAlertBaselineStatus 判断日志库是否已经积累了足够的基线。
//
// 判据是 PRD 定的：范围内最早的 created_at 是否早于 now - baseline_windows×W。
// 只要有一条那么早的记录，就说明窗口规则拿得到配置要求的基线窗。
//
// 失败软处理：查询失败时返回 (false, 0) 并记一条 SysLog —— 这个方法会被
// GET /api/option/ 调用，一个日志库抖动不应该让整个设置页打不开。
func abuseAlertBaselineStatus(ctx context.Context, nowUnix int64, windowRangeStart int64, windowSeconds int64) (bool, int64) {
	baselineWindows := operation_setting.AbuseAlertBaselineWindows()
	needBefore := nowUnix - int64(baselineWindows)*windowSeconds

	var earliest int64
	if err := LOG_DB.WithContext(ctx).Raw(
		"SELECT MIN(created_at) FROM logs WHERE created_at >= ? AND created_at < ?",
		windowRangeStart, nowUnix,
	).Scan(&earliest).Error; err != nil {
		common.SysError("abuse alert: failed to read the log database baseline: " + err.Error())
		return false, 0
	}
	if earliest <= 0 {
		return false, 0
	}
	if earliest <= needBefore {
		return true, 0
	}
	// 最早的一条记录何时的年龄才会够：earliest + baseline_windows×W。
	return false, earliest + int64(baselineWindows)*windowSeconds
}

// abuseAlertWindowRuleFindings 对步骤 1 的聚合求值三条窗口规则。
func abuseAlertWindowRuleFindings(rows []abuseAlertWindowAgg, windowRangeStart int64, currentStart int64, windowSeconds int64, baselineWindows int) []AbuseAlertFinding {
	perToken := groupAbuseAlertWindows(rows, windowRangeStart, currentStart, windowSeconds, baselineWindows)

	consumeRatio := operation_setting.AbuseAlertConsumeRatio()
	requestRatio := operation_setting.AbuseAlertRequestRatio()
	errorRateRatio := operation_setting.AbuseAlertErrorRateRatio()
	minConsumeQuota := operation_setting.AbuseAlertMinConsumeQuota()
	minRequests := int64(operation_setting.AbuseAlertMinRequests())
	minErrors := int64(operation_setting.AbuseAlertMinErrors())

	findings := make([]AbuseAlertFinding, 0)
	for _, token := range sortedAbuseAlertTokens(perToken) {
		windows := perToken[token]
		if !abuseAlertWindowGateSatisfied(windows) {
			continue
		}

		if operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleConsumeSpike) {
			medianQuota := medianInt64(windows.baselineQuota)
			// 地板是抑制小额令牌噪音的门槛，不是判定本体；比率才是判定本体。
			// 比较全部在 float64 上做，不把比率结果转成 quota —— 那会变成一次 quota 换算。
			if float64(windows.currentQuota) >= consumeRatio*medianQuota && windows.currentQuota >= minConsumeQuota {
				findings = append(findings, abuseAlertWindowFinding(
					operation_setting.AbuseRuleConsumeSpike, operation_setting.AbuseSeverityHigh, token,
					currentStart, windowSeconds, float64(windows.currentQuota), medianQuota, 0,
				))
			}
		}

		if operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleRequestSpike) {
			medianRequests := medianInt64(windows.baselineRequests)
			if float64(windows.currentRequests) >= requestRatio*medianRequests && windows.currentRequests >= minRequests {
				findings = append(findings, abuseAlertWindowFinding(
					operation_setting.AbuseRuleRequestSpike, operation_setting.AbuseSeverityMedium, token,
					currentStart, windowSeconds, float64(windows.currentRequests), medianRequests, 0,
				))
			}
		}

		if operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleErrorRateSpike) {
			// 基线的**错误数**为 0 时分母退化：`currentRate >= error_rate_ratio * 0`
			// 恒真，比率判定消失，规则静默变成「当前错误数 ≥ min_errors」这个**绝对量**
			// 判定 —— 正是 PRD §选定 4.1 明令禁止的形态。
			//
			// 这个形态有两种来源，必须一起挡住：
			//   1. 基线窗口里一条 `type=5` 都读不到（PRD 边界 case：`type=5` 的逐类开关
			//      被管理员关掉，上一 slice 的开关）；
			//   2. 基线的 `type=5` 存在但全是 0 计数 —— 由 (1) 覆盖，因为
			//      `baselineErrors` 就是基线窗口里 `type=5` 的行数。
			//
			// 两者都不是「无异常」，是**观测不到**：这里不产生任何发现，由
			// abuseAlertWindowRuleSkips 记 insufficient_baseline（第三状态）。
			// 注意门必须建在 `baselineErrors` 上，**不能**建在
			// `baselineConsumes + baselineErrors` 上：后者只要还有 type=2 行就通过，
			// 于是该令牌既不产 finding 也不产 skip（实测：全部 7 窗只有 type=2 的
			// 令牌 findings=0 且无 skip —— 运维者什么也看不到）。
			if windows.baselineErrors > 0 {
				currentDenominator := windows.currentConsumes + windows.currentErrors
				baselineDenominator := windows.baselineConsumes + windows.baselineErrors
				if currentDenominator > 0 {
					currentRate := float64(windows.currentErrors) / float64(currentDenominator)
					baselineRate := float64(windows.baselineErrors) / float64(baselineDenominator)
					if currentRate >= errorRateRatio*baselineRate && windows.currentErrors >= minErrors {
						findings = append(findings, abuseAlertWindowFinding(
							operation_setting.AbuseRuleErrorRateSpike, operation_setting.AbuseSeverityHigh, token,
							currentStart, windowSeconds, currentRate, baselineRate, 0,
						))
					}
				}
			}
		}
	}
	return findings
}

// abuseAlertWindowRuleSkips 为「被评估但得不出结论」的令牌产出 skipped 记录。
//
// 它覆盖两种情形，两者的共同点是**这一条规则在这个令牌上算不出来**，而不是
// 「算出来没有异常」：
//   - 基线门不达标（基线窗口不够）⇒ 该令牌的三条窗口规则各记一条；
//   - 过了基线门，但基线的错误数为 0 ⇒ 失败率的比率判定会退化成绝对地板
//     （见 abuseAlertWindowRuleFindings），只有 `error_rate_spike` 一条记。
//
// 这是 AC-2 的反向确认：零发现必须能区分成「检测过但没触发」与「根本没检测」，
// 以及 PRD 要求的第三状态「检测了但观测不到」。**不是**「每一个被评估的令牌都会
// 产出一条 skip」：过了基线门且基线里有错误的令牌不会有任何 skip。
func abuseAlertWindowRuleSkips(rows []abuseAlertWindowAgg, currentStart int64, windowSeconds int64, baselineWindows int) []AbuseAlertSkip {
	windowRangeStart := currentStart - int64(baselineWindows)*windowSeconds
	perToken := groupAbuseAlertWindows(rows, windowRangeStart, currentStart, windowSeconds, baselineWindows)

	skips := make([]AbuseAlertSkip, 0)
	for _, token := range sortedAbuseAlertTokens(perToken) {
		windows := perToken[token]
		if abuseAlertWindowGateSatisfied(windows) {
			if operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleErrorRateSpike) && windows.baselineErrors == 0 {
				skips = append(skips, AbuseAlertSkip{
					Rule:    operation_setting.AbuseRuleErrorRateSpike,
					TokenID: token,
					Reason:  AbuseAlertSkipInsufficientBaseline,
				})
			}
			continue
		}
		for _, rule := range []struct {
			id      string
			enabled bool
		}{
			{operation_setting.AbuseRuleConsumeSpike, operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleConsumeSpike)},
			{operation_setting.AbuseRuleRequestSpike, operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleRequestSpike)},
			{operation_setting.AbuseRuleErrorRateSpike, operation_setting.IsAbuseAlertRuleEnabled(operation_setting.AbuseRuleErrorRateSpike)},
		} {
			if !rule.enabled {
				continue
			}
			skips = append(skips, AbuseAlertSkip{
				Rule:    rule.id,
				TokenID: token,
				Reason:  AbuseAlertSkipInsufficientBaseline,
			})
		}
	}
	return skips
}

// abuseAlertNewModelFindings 求值 new_model：当前窗出现的 (令牌, 模型) 在回看窗内
// 完全不存在，且该令牌在回看窗内的请求数过门槛。
func abuseAlertNewModelFindings(ctx context.Context, currentStart int64, currentEnd int64, lookbackSeconds int64) ([]AbuseAlertFinding, []AbuseAlertSkip, error) {
	lookbackStart := currentEnd - lookbackSeconds
	rows, err := queryAbuseAlertModels(ctx, lookbackStart, currentEnd)
	if err != nil {
		return nil, nil, err
	}

	minBaselineRequests := int64(operation_setting.AbuseAlertMinBaselineRequests())
	lookbackRequests := make(map[int32]int64, len(rows))
	for _, row := range rows {
		lookbackRequests[row.TokenID] += row.Requests
	}

	findings := make([]AbuseAlertFinding, 0)
	skips := make([]AbuseAlertSkip, 0)
	for _, row := range rows {
		if row.FirstSeen < currentStart {
			continue
		}
		if lookbackRequests[row.TokenID] < minBaselineRequests {
			skips = append(skips, AbuseAlertSkip{
				Rule:    operation_setting.AbuseRuleNewModel,
				TokenID: row.TokenID,
				Reason:  AbuseAlertSkipInsufficientBaseline,
			})
			continue
		}
		modelName := row.ModelName
		findings = append(findings, AbuseAlertFinding{
			Rule:           operation_setting.AbuseRuleNewModel,
			Severity:       operation_setting.AbuseSeverityMedium,
			TokenID:        row.TokenID,
			ModelName:      &modelName,
			WindowStart:    currentStart,
			WindowEnd:      currentEnd,
			Current:        float64(row.Requests),
			BaselineMedian: 0,
			Ratio:          0,
			Direction:      abuseAlertDirectionUp,
		})
	}
	return findings, skips, nil
}

// abuseAlertTokenWindows 是一个令牌在扫描范围内的窗口序列。
type abuseAlertTokenWindows struct {
	// baselineRequests/Quota/Consumes/Errors 按**由旧到新**排列，长度 = baseline_windows。
	baselineRequests []int64
	baselineQuota    []int64
	baselineConsumes int64
	baselineErrors   int64

	currentRequests int64
	currentQuota    int64
	currentConsumes int64
	currentErrors   int64
}

// groupAbuseAlertWindows 把聚合行按令牌归入窗口序列。认不出的桶（范围外的行）
// 直接忽略，不猜、不四舍五入。
func groupAbuseAlertWindows(rows []abuseAlertWindowAgg, windowRangeStart int64, currentStart int64, windowSeconds int64, baselineWindows int) map[int32]*abuseAlertTokenWindows {
	perToken := make(map[int32]*abuseAlertTokenWindows)
	for _, row := range rows {
		windows := perToken[row.TokenID]
		if windows == nil {
			windows = &abuseAlertTokenWindows{
				baselineRequests: make([]int64, baselineWindows),
				baselineQuota:    make([]int64, baselineWindows),
			}
			perToken[row.TokenID] = windows
		}

		// 饱和行的 quota 是被钳到边界后的极大值，必须从比率运算里剔除；
		// 但请求本身确实发生过，所以 requests / consumes 不剔除，只剔除额度。
		quota := row.Quota - row.SaturatedQuota

		switch {
		case row.Bucket == currentStart:
			windows.currentRequests += row.Requests
			windows.currentConsumes += row.Consumes
			windows.currentErrors += row.Errors
			windows.currentQuota += quota
		case row.Bucket >= windowRangeStart && row.Bucket < currentStart:
			index := int((row.Bucket - windowRangeStart) / windowSeconds)
			if index < 0 || index >= baselineWindows {
				continue
			}
			windows.baselineRequests[index] += row.Requests
			windows.baselineQuota[index] += quota
			windows.baselineConsumes += row.Consumes
			windows.baselineErrors += row.Errors
		}
	}
	return perToken
}

// abuseAlertWindowGateSatisfied 是基线门：基线窗口中至少
// AbuseAlertMinBaselineWindowsGate 个窗口的请求数 ≥ min_baseline_requests。
//
// 不满足 ⇒ 该令牌的窗口规则记 insufficient_baseline，既不告警也不沉默。
func abuseAlertWindowGateSatisfied(windows *abuseAlertTokenWindows) bool {
	minRequests := int64(operation_setting.AbuseAlertMinBaselineRequests())
	qualified := 0
	for _, requests := range windows.baselineRequests {
		if requests >= minRequests {
			qualified++
		}
	}
	return qualified >= operation_setting.AbuseAlertMinBaselineWindowsGate
}

// sortedAbuseAlertTokens 让输出顺序稳定 —— 否则每次扫描的发现顺序都在变，
// 测试无法比较，页面上的列表也会自己跳动。
func sortedAbuseAlertTokens(perToken map[int32]*abuseAlertTokenWindows) []int32 {
	tokens := make([]int32, 0, len(perToken))
	for token := range perToken {
		tokens = append(tokens, token)
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
	return tokens
}

// sortAbuseAlertResult 固定发现与 skipped 的顺序。
func sortAbuseAlertResult(result *AbuseAlertScanResult) {
	sort.SliceStable(result.Findings, func(i, j int) bool {
		if result.Findings[i].Rule != result.Findings[j].Rule {
			return result.Findings[i].Rule < result.Findings[j].Rule
		}
		if result.Findings[i].TokenID != result.Findings[j].TokenID {
			return result.Findings[i].TokenID < result.Findings[j].TokenID
		}
		return abuseAlertModelNameOf(result.Findings[i]) < abuseAlertModelNameOf(result.Findings[j])
	})
	sort.SliceStable(result.Skipped, func(i, j int) bool {
		if result.Skipped[i].Rule != result.Skipped[j].Rule {
			return result.Skipped[i].Rule < result.Skipped[j].Rule
		}
		return result.Skipped[i].TokenID < result.Skipped[j].TokenID
	})
}

func abuseAlertModelNameOf(finding AbuseAlertFinding) string {
	if finding.ModelName == nil {
		return ""
	}
	return *finding.ModelName
}

// abuseAlertWindowFinding 组装一条窗口规则的发现；ratio 传 0 表示按 current/median 推导。
func abuseAlertWindowFinding(rule string, severity string, tokenID int32, currentStart int64, windowSeconds int64, current float64, baselineMedian float64, ratio float64) AbuseAlertFinding {
	if ratio == 0 && baselineMedian > 0 {
		ratio = current / baselineMedian
	}
	return AbuseAlertFinding{
		Rule:           rule,
		Severity:       severity,
		TokenID:        tokenID,
		WindowStart:    currentStart,
		WindowEnd:      currentStart + windowSeconds,
		Current:        current,
		BaselineMedian: baselineMedian,
		Ratio:          ratio,
		Direction:      abuseAlertDirectionUp,
	}
}

// medianInt64 求中位数。SQL 方言没有可移植的 median，所以必须在 Go 里算。
//
// 用中位数而不是均值：一个历史尖峰不应抬高门槛（否则真异常会被自己掩盖），
// 也不应把门槛压得过低。
func medianInt64(values []int64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return float64(sorted[middle])
	}
	return float64(sorted[middle-1]+sorted[middle]) / 2
}

// AbuseAlertOwner 是发现里「归给谁」的反查结果。空值表示令牌已被删除 ——
// 发现本身必须保留（丢掉等于静默隐藏盗刷），只是名字显示不出来。
type AbuseAlertOwner struct {
	TokenName string
	UserID    int
	Username  string
}

// ResolveAbuseAlertOwners 按 token_id 反查主库拿到令牌名与用户名。
//
// 这是唯一一处读主库的地方（检测本身只读日志库），因此它在测试里可以单独跳过。
// token_id <= 0 的输入被忽略（R8）；查不到的令牌保留零值，不丢发现。
func ResolveAbuseAlertOwners(tokenIDs []int32) map[int32]AbuseAlertOwner {
	owners := make(map[int32]AbuseAlertOwner)
	unique := make([]int32, 0, len(tokenIDs))
	seen := make(map[int32]struct{}, len(tokenIDs))
	for _, tokenID := range tokenIDs {
		if tokenID <= 0 {
			continue
		}
		if _, ok := seen[tokenID]; ok {
			continue
		}
		seen[tokenID] = struct{}{}
		unique = append(unique, tokenID)
	}
	if len(unique) == 0 {
		return owners
	}

	type tokenRow struct {
		ID     int    `gorm:"column:id"`
		UserID int    `gorm:"column:user_id"`
		Name   string `gorm:"column:name"`
	}
	var tokens []tokenRow
	if err := DB.Table("tokens").Select("id", "user_id", "name").Where("id IN ?", unique).Scan(&tokens).Error; err != nil {
		common.SysError("abuse alert: failed to resolve token owners: " + err.Error())
		return owners
	}
	if len(tokens) == 0 {
		return owners
	}

	userIDs := make([]int, 0, len(tokens))
	for _, token := range tokens {
		userIDs = append(userIDs, token.UserID)
	}
	type userRow struct {
		ID       int    `gorm:"column:id"`
		Username string `gorm:"column:username"`
	}
	var users []userRow
	if err := DB.Table("users").Select("id", "username").Where("id IN ?", userIDs).Scan(&users).Error; err != nil {
		common.SysError("abuse alert: failed to resolve token owners: " + err.Error())
	}
	usernames := make(map[int]string, len(users))
	for _, user := range users {
		usernames[user.ID] = user.Username
	}

	for _, token := range tokens {
		owners[int32(token.ID)] = AbuseAlertOwner{
			TokenName: token.Name,
			UserID:    token.UserID,
			Username:  usernames[token.UserID],
		}
	}
	return owners
}

// AbuseAlertLastScheduledScanAt 返回最近一次 abuse_scan 系统任务的完成时间（0 = 从未扫描过）。
//
// R5 要求页面能把「没有通知」区分成三种情况：没触发 / 触发了但没发出去 / 根本没扫。
// 这一条数据就是第三种情况的判据 —— 它读的是系统任务框架自己的运行历史，
// 不新增任何状态表，GET 端点也不写任何东西。
func AbuseAlertLastScheduledScanAt() int64 {
	var last int64
	err := DB.Model(&SystemTask{}).
		Where("type = ? AND status = ?", SystemTaskTypeAbuseScan, SystemTaskStatusSucceeded).
		Select("COALESCE(MAX(updated_at), 0)").
		Scan(&last).Error
	if err != nil {
		common.SysError("abuse alert: failed to read the last scheduled scan time: " + err.Error())
		return 0
	}
	return last
}
