package operation_setting

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/setting/config"
)

// AbuseAlertSetting 是异常用量告警的运行时设置。
//
// 全部字段用指针（三态）：nil = 从未被管理员保存过 → 取内置默认。理由与
// ErrorLogSetting 完全相同（见 error_log_setting.go 的长注释）：普通 bool / int
// 区分不出「没设过」与「显式设为 0/false」，会让升级瞬间静默改变行为；map 查不到键
// 同样迁移不了「没设过」。
//
// 字段数与 PRD §R11 的计数（11 / 19）都不一致 —— 实际是 17 个，逐条可数。
// PRD §R11 只点名了两个「最弱」的旋钮，本 slice 的取舍见 RD 工件的 `## R11 取舍`：
// `min_baseline_windows` 被砍掉（固定为常量），其余保留。
type AbuseAlertSetting struct {
	Enabled       *bool `json:"enabled"`        // 总开关，默认 true
	NotifyEnabled *bool `json:"notify_enabled"` // 是否推送高危发现，默认 true

	WindowMinutes       *int `json:"window_minutes"`        // W，默认 10
	BaselineWindows     *int `json:"baseline_windows"`      // 默认 6
	ScanIntervalMinutes *int `json:"scan_interval_minutes"` // 默认 5
	MinBaselineRequests *int `json:"min_baseline_requests"` // 默认 10

	RuleConsumeSpikeEnabled   *bool `json:"rule_consume_spike_enabled"`    // 默认 true
	RuleRequestSpikeEnabled   *bool `json:"rule_request_spike_enabled"`    // 默认 true
	RuleErrorRateSpikeEnabled *bool `json:"rule_error_rate_spike_enabled"` // 默认 true
	RuleNewModelEnabled       *bool `json:"rule_new_model_enabled"`        // 默认 true

	ConsumeRatio          *float64 `json:"consume_ratio"`            // 默认 5.0
	RequestRatio          *float64 `json:"request_ratio"`            // 默认 5.0
	ErrorRateRatio        *float64 `json:"error_rate_ratio"`         // 默认 3.0
	MinConsumeQuota       *int64   `json:"min_consume_quota"`        // 默认 500000
	MinRequests           *int     `json:"min_requests"`             // 默认 50
	MinErrors             *int     `json:"min_errors"`               // 默认 20
	NewModelLookbackHours *int     `json:"new_model_lookback_hours"` // 默认 24
}

var abuseAlertSetting = AbuseAlertSetting{}

func init() {
	config.GlobalConfig.Register("abuse_alert_setting", &abuseAlertSetting)
}

// 规则 id。冻结契约：前端、伪键、发现载荷、开关 option key 全部引用这些常量，
// 不存在第二份可漂移的清单。
const (
	AbuseRuleConsumeSpike   = "consume_spike"
	AbuseRuleRequestSpike   = "request_spike"
	AbuseRuleErrorRateSpike = "error_rate_spike"
	AbuseRuleNewModel       = "new_model"

	// 当前数据里算不出来的两条信号。它们必须出现在下发里（computable=false、
	// switch=null、带 reasonKey），页面因此不渲染任何控件 —— 交付一个永不触发的
	// 假规则比缺失更糟，它会让运维者以为「已经在看来源了」。
	AbuseRuleSourceAnomaly      = "source_anomaly"
	AbuseRuleConcurrencyAnomaly = "concurrency_anomaly"
)

// 严重级。
const (
	AbuseSeverityHigh   = "high"
	AbuseSeverityMedium = "medium"
)

// 开关来源，随伪键下发（与 ErrorLogCategorySource* 同义）。
const (
	AbuseSwitchSourceDefault = "default"
	AbuseSwitchSourceSetting = "setting"
)

// 配置上限。这些是**常量**，不是可配项 —— 它们是「单次扫描有界」这条硬约束的
// 执行点：任何一项放开一点，扫描范围就会跟着放大。
const (
	AbuseAlertMinWindowMinutes       = 1
	AbuseAlertMaxWindowMinutes       = 60
	AbuseAlertMinBaselineWindows     = 1
	AbuseAlertMaxBaselineWindows     = 24
	AbuseAlertMaxScanSpanMinutes     = 1440 // window_minutes × baseline_windows ≤ 24h
	AbuseAlertMinLookbackHours       = 1
	AbuseAlertMaxLookbackHours       = 168 // 7 天
	AbuseAlertMinScanIntervalMinutes = 1
	AbuseAlertMaxScanIntervalMinutes = 60

	// AbuseAlertMinBaselineWindowsGate 是基线门（固定，不可配；见 R11 取舍）。
	// 该令牌的基线窗口中至少这么多窗口的请求数 ≥ min_baseline_requests。
	AbuseAlertMinBaselineWindowsGate = 3

	// abuseAlertMaxEndpointHoursCeiling 是只读端点 hours 参数的上限的绝对天花板。
	// 实际上限 = max(1, min(该天花板, new_model_lookback_hours))。
	abuseAlertMaxEndpointHoursCeiling = 24
)

// min_* 一族的上界（S1）。这一族是「一个窗口内至少要有多少事件」的**地板**，
// 语义是抑制噪音，不是流量目标：地板高到没有任何令牌能达到时，规则不是「变严」，
// 而是被**静默关闭** —— min_baseline_requests 更糟，它还会让基线门恒不成立，
// 于是四条规则一起停摆，而 API 一律报 insufficient_baseline（读起来像「基线还在
// 积累」，撒谎的是展示层）。与 D3 同族：同一个「maxValue == 0 表示无界」的写法
// 被复用在这一族的每一个键上，所以这里一次性给全族补上界。
//
// 上界 = 各自内置默认值的 AbuseAlertMinFloorCeilingMultiplier 倍：既有的合法配置
// （默认值上下）全部落在区间内，而越界值只能靠**显式写一个荒谬的大数**到达，
// 一次手滑不再可能把检测全关。倍数写成常量、上界由默认值推导，避免两处漂移。
const (
	AbuseAlertMinFloorCeilingMultiplier = 100

	AbuseAlertMaxMinBaselineRequests = abuseAlertDefaultMinBaselineRequest * AbuseAlertMinFloorCeilingMultiplier
	AbuseAlertMaxMinRequests         = abuseAlertDefaultMinRequests * AbuseAlertMinFloorCeilingMultiplier
	AbuseAlertMaxMinErrors           = abuseAlertDefaultMinErrors * AbuseAlertMinFloorCeilingMultiplier
	AbuseAlertMaxMinConsumeQuota     = abuseAlertDefaultMinConsumeQuota * AbuseAlertMinFloorCeilingMultiplier
)

// 内置默认值。三态字段为 nil 时取这里的值。
const (
	abuseAlertDefaultEnabled            = true
	abuseAlertDefaultNotifyEnabled      = true
	abuseAlertDefaultWindowMinutes      = 10
	abuseAlertDefaultBaselineWindows    = 6
	abuseAlertDefaultScanIntervalMinute = 5
	abuseAlertDefaultMinBaselineRequest = 10
	abuseAlertDefaultConsumeRatio       = 5.0
	abuseAlertDefaultRequestRatio       = 5.0
	abuseAlertDefaultErrorRateRatio     = 3.0
	abuseAlertDefaultMinConsumeQuota    = 500000
	abuseAlertDefaultMinRequests        = 50
	abuseAlertDefaultMinErrors          = 20
	abuseAlertDefaultNewModelLookbackHr = 24
)

// 阈值默认值（供测试与校验引用，避免测试里出现魔法数字）。
const (
	AbuseAlertDefaultConsumeRatio    = abuseAlertDefaultConsumeRatio
	AbuseAlertDefaultRequestRatio    = abuseAlertDefaultRequestRatio
	AbuseAlertDefaultErrorRateRatio  = abuseAlertDefaultErrorRateRatio
	AbuseAlertDefaultMinConsumeQuota = abuseAlertDefaultMinConsumeQuota
	AbuseAlertDefaultMinRequests     = abuseAlertDefaultMinRequests
	AbuseAlertDefaultMinErrors       = abuseAlertDefaultMinErrors
)

func boolOrDefault(value *bool, fallback bool) bool {
	if value != nil {
		return *value
	}
	return fallback
}

func intOrDefault(value *int, fallback int) int {
	if value != nil {
		return *value
	}
	return fallback
}

// IsAbuseAlertEnabled 返回总开关的当前生效值。
//
// 每次调用都重新解析，管理员保存后立即生效，不需要重启。不取锁：写路径在
// common.OptionMapRWMutex 写锁保护下进行，读的是一个指针加一个 bool，
// 与 DemoSiteEnabled / IsErrorLogEnabled 同形态（PRD §R10 的既知取舍）。
func IsAbuseAlertEnabled() bool {
	return boolOrDefault(abuseAlertSetting.Enabled, abuseAlertDefaultEnabled)
}

// IsAbuseAlertNotifyEnabled 返回是否推送高危发现。
func IsAbuseAlertNotifyEnabled() bool {
	return boolOrDefault(abuseAlertSetting.NotifyEnabled, abuseAlertDefaultNotifyEnabled)
}

// AbuseAlertWindowMinutes 返回 W（分钟）。
func AbuseAlertWindowMinutes() int {
	return intOrDefault(abuseAlertSetting.WindowMinutes, abuseAlertDefaultWindowMinutes)
}

// AbuseAlertBaselineWindows 返回基线的窗口个数。
func AbuseAlertBaselineWindows() int {
	return intOrDefault(abuseAlertSetting.BaselineWindows, abuseAlertDefaultBaselineWindows)
}

// AbuseAlertScanIntervalMinutes 返回扫描间隔（分钟）。
func AbuseAlertScanIntervalMinutes() int {
	return intOrDefault(abuseAlertSetting.ScanIntervalMinutes, abuseAlertDefaultScanIntervalMinute)
}

// AbuseAlertMinBaselineRequests 返回基线门里「一个窗口算数」的请求数下限。
func AbuseAlertMinBaselineRequests() int {
	return intOrDefault(abuseAlertSetting.MinBaselineRequests, abuseAlertDefaultMinBaselineRequest)
}

// AbuseAlertConsumeRatio 返回消费突增的比率。
func AbuseAlertConsumeRatio() float64 {
	if abuseAlertSetting.ConsumeRatio != nil {
		return *abuseAlertSetting.ConsumeRatio
	}
	return abuseAlertDefaultConsumeRatio
}

// AbuseAlertRequestRatio 返回请求突增的比率。
func AbuseAlertRequestRatio() float64 {
	if abuseAlertSetting.RequestRatio != nil {
		return *abuseAlertSetting.RequestRatio
	}
	return abuseAlertDefaultRequestRatio
}

// AbuseAlertErrorRateRatio 返回失败率异常的比率。
func AbuseAlertErrorRateRatio() float64 {
	if abuseAlertSetting.ErrorRateRatio != nil {
		return *abuseAlertSetting.ErrorRateRatio
	}
	return abuseAlertDefaultErrorRateRatio
}

// AbuseAlertMinConsumeQuota 返回消费突增的额度地板。
func AbuseAlertMinConsumeQuota() int64 {
	if abuseAlertSetting.MinConsumeQuota != nil {
		return *abuseAlertSetting.MinConsumeQuota
	}
	return abuseAlertDefaultMinConsumeQuota
}

// AbuseAlertMinRequests 返回请求突增的请求数地板。
func AbuseAlertMinRequests() int {
	return intOrDefault(abuseAlertSetting.MinRequests, abuseAlertDefaultMinRequests)
}

// AbuseAlertMinErrors 返回失败率异常的失败数地板。
func AbuseAlertMinErrors() int {
	return intOrDefault(abuseAlertSetting.MinErrors, abuseAlertDefaultMinErrors)
}

// AbuseAlertNewModelLookbackHours 返回模型首现的回看小时数。
func AbuseAlertNewModelLookbackHours() int {
	return intOrDefault(abuseAlertSetting.NewModelLookbackHours, abuseAlertDefaultNewModelLookbackHr)
}

// AbuseAlertMaxEndpointHours 返回只读端点 hours 参数的实际上限。
func AbuseAlertMaxEndpointHours() int {
	limit := min(abuseAlertMaxEndpointHoursCeiling, AbuseAlertNewModelLookbackHours())
	return max(1, limit)
}

// AbuseAlertBaselineGateMinutes 返回基线门所需的**最短日志保留期**（分钟）。
//
// 它是「基线窗 + 当前窗」的跨度：一个窗口的数据在当前窗判定时仍然要被读到，
// 所以保留期短于这个值就等于基线永远读不到 —— 这正是 Q2 要求页面上提示的那条。
func AbuseAlertBaselineGateMinutes() int {
	return (AbuseAlertBaselineWindows() + 1) * AbuseAlertWindowMinutes()
}

// abuseAlertRuleIsEnabled 返回某条规则的开关生效值。
// 未知规则返回 false —— 与 IsErrorLogCategoryEnabled 的 fail-open 相反，
// 因为这里的默认是「检测」，未知 id 不构成检测理由。
func abuseAlertRuleIsEnabled(rule string) bool {
	switch rule {
	case AbuseRuleConsumeSpike:
		return boolOrDefault(abuseAlertSetting.RuleConsumeSpikeEnabled, true)
	case AbuseRuleRequestSpike:
		return boolOrDefault(abuseAlertSetting.RuleRequestSpikeEnabled, true)
	case AbuseRuleErrorRateSpike:
		return boolOrDefault(abuseAlertSetting.RuleErrorRateSpikeEnabled, true)
	case AbuseRuleNewModel:
		return boolOrDefault(abuseAlertSetting.RuleNewModelEnabled, true)
	default:
		return false
	}
}

// IsAbuseAlertRuleEnabled 是 abuseAlertRuleIsEnabled 的导出形式，供 model 层
// 在扫描时逐条跳过被关掉的规则。
func IsAbuseAlertRuleEnabled(rule string) bool {
	return abuseAlertRuleIsEnabled(rule)
}

// 开关写入用的完整 option key 前缀。与 config.GlobalConfig.Register 的模块名一致。
const abuseAlertOptionKeyPrefix = "abuse_alert_setting."

// abuseAlertRuleSwitchField 返回一条规则的开关字段、默认值与是否存在。
// 三个返回值一起出现是因为它们同属一份清单：分开写会出现「规则在、开关字段漏了」的静默错配。
func abuseAlertRuleSwitchField(rule string) (value *bool, defaultOn, known bool) {
	switch rule {
	case AbuseRuleConsumeSpike:
		return abuseAlertSetting.RuleConsumeSpikeEnabled, true, true
	case AbuseRuleRequestSpike:
		return abuseAlertSetting.RuleRequestSpikeEnabled, true, true
	case AbuseRuleErrorRateSpike:
		return abuseAlertSetting.RuleErrorRateSpikeEnabled, true, true
	case AbuseRuleNewModel:
		return abuseAlertSetting.RuleNewModelEnabled, true, true
	default:
		return nil, false, false
	}
}

// AbuseAlertRuleSwitch 是一条规则的开关状态（设置值，不是与总开关的合成值）。
type AbuseAlertRuleSwitch struct {
	OptionKey string `json:"option_key"`
	Enabled   bool   `json:"enabled"`
	Source    string `json:"source"`
}

func abuseAlertRuleSwitch(rule string) *AbuseAlertRuleSwitch {
	value, defaultOn, known := abuseAlertRuleSwitchField(rule)
	if !known {
		return nil
	}
	enabled, source := defaultOn, AbuseSwitchSourceDefault
	if value != nil {
		enabled, source = *value, AbuseSwitchSourceSetting
	}
	return &AbuseAlertRuleSwitch{
		OptionKey: abuseAlertOptionKeyPrefix + abuseAlertRuleSwitchOptionSuffix(rule),
		Enabled:   enabled,
		Source:    source,
	}
}

// abuseAlertRuleSwitchOptionSuffix 把规则 id 映射到它的 option 键后缀。
func abuseAlertRuleSwitchOptionSuffix(rule string) string {
	switch rule {
	case AbuseRuleConsumeSpike:
		return "rule_consume_spike_enabled"
	case AbuseRuleRequestSpike:
		return "rule_request_spike_enabled"
	case AbuseRuleErrorRateSpike:
		return "rule_error_rate_spike_enabled"
	case AbuseRuleNewModel:
		return "rule_new_model_enabled"
	default:
		return ""
	}
}

// 规则的阈值（下发给前端的解释性快照，不参与判定 —— 判定只读上面那些 accessor）。
type AbuseAlertRuleThresholds struct {
	Ratio           *float64 `json:"ratio,omitempty"`
	MinConsumeQuota *int64   `json:"min_consume_quota,omitempty"`
	MinRequests     *int     `json:"min_requests,omitempty"`
	MinErrors       *int     `json:"min_errors,omitempty"`
	LookbackHours   *int     `json:"lookback_hours,omitempty"`
}

// AbuseAlertRule 是伪键里的一条规则。
//
// Computable=false 的两条必须带 ReasonKey 且 Switch 为 null；Switch 没有 omitempty，
// 正是为了让 null 真的出现在 JSON 里（omitempty 会把 null 整个删掉，前端就看不到
// 「这一条不可切换」这个事实）。
type AbuseAlertRule struct {
	ID         string                    `json:"id"`
	TitleKey   string                    `json:"titleKey"`
	Computable bool                      `json:"computable"`
	Severity   string                    `json:"severity,omitempty"`
	ReasonKey  string                    `json:"reasonKey,omitempty"`
	Switch     *AbuseAlertRuleSwitch     `json:"switch"`
	Thresholds *AbuseAlertRuleThresholds `json:"thresholds,omitempty"`
	DependsOn  []string                  `json:"depends_on,omitempty"`
	Source     string                    `json:"source,omitempty"`
}

// 规则说明文案。TitleKey / ReasonKey 与英文源串同值，符合仓库「key 即英文源串」的
// i18n 约定；缺翻译时 i18next 回退到英文源串，不会把 key 本身显示给用户。
const (
	AbuseAlertReasonSourceAnomaly = "Not computable: request IP is stored only when the token owner enables the record_ip_log privacy setting, and relay requests are never written to audit_logs. No ASN or region is recorded anywhere."
	AbuseAlertReasonConcurrency   = "Not computable: judging \"multiple sources\" needs a source identifier, which is the same missing IP. The system also keeps no per-token in-flight tracking; the only per-request rate limit is keyed by user, not by token, and IP allow-list rejections are not recorded."
)

// AbuseAlertRules 返回完整的规则清单（后端唯一数据源）。
//
// 4 条可计算 + 2 条如实标注为不可计算。顺序即页面顺序。
func AbuseAlertRules() []AbuseAlertRule {
	rules := []AbuseAlertRule{
		{
			ID:         AbuseRuleConsumeSpike,
			TitleKey:   "Consume rate spike",
			Computable: true,
			Severity:   AbuseSeverityHigh,
			Switch:     abuseAlertRuleSwitch(AbuseRuleConsumeSpike),
			Thresholds: &AbuseAlertRuleThresholds{
				Ratio:           abuseAlertFloatPtr(AbuseAlertConsumeRatio()),
				MinConsumeQuota: abuseAlertInt64Ptr(AbuseAlertMinConsumeQuota()),
			},
			DependsOn: []string{"logs.quota", "logs.type", "logs.token_id", "logs.created_at"},
			Source:    "model/log_alert.go",
		},
		{
			ID:         AbuseRuleRequestSpike,
			TitleKey:   "Request rate spike",
			Computable: true,
			Severity:   AbuseSeverityMedium,
			Switch:     abuseAlertRuleSwitch(AbuseRuleRequestSpike),
			Thresholds: &AbuseAlertRuleThresholds{
				Ratio:       abuseAlertFloatPtr(AbuseAlertRequestRatio()),
				MinRequests: abuseAlertIntPtr(AbuseAlertMinRequests()),
			},
			DependsOn: []string{"logs.token_id", "logs.created_at"},
			Source:    "model/log_alert.go",
		},
		{
			ID:         AbuseRuleErrorRateSpike,
			TitleKey:   "Failure rate spike (after a channel is selected)",
			Computable: true,
			Severity:   AbuseSeverityHigh,
			Switch:     abuseAlertRuleSwitch(AbuseRuleErrorRateSpike),
			Thresholds: &AbuseAlertRuleThresholds{
				Ratio:     abuseAlertFloatPtr(AbuseAlertErrorRateRatio()),
				MinErrors: abuseAlertIntPtr(AbuseAlertMinErrors()),
			},
			DependsOn: []string{"logs.type", "logs.token_id", "logs.created_at"},
			Source:    "model/log_alert.go",
		},
		{
			ID:         AbuseRuleNewModel,
			TitleKey:   "Model first seen",
			Computable: true,
			Severity:   AbuseSeverityMedium,
			Switch:     abuseAlertRuleSwitch(AbuseRuleNewModel),
			Thresholds: &AbuseAlertRuleThresholds{
				LookbackHours: abuseAlertIntPtr(AbuseAlertNewModelLookbackHours()),
			},
			DependsOn: []string{"logs.model_name", "logs.token_id", "logs.created_at"},
			Source:    "model/log_alert.go",
		},
		{
			ID:         AbuseRuleSourceAnomaly,
			TitleKey:   "Source anomaly (new IP / ASN / region)",
			Computable: false,
			ReasonKey:  AbuseAlertReasonSourceAnomaly,
			Switch:     nil,
		},
		{
			ID:         AbuseRuleConcurrencyAnomaly,
			TitleKey:   "Concurrency anomaly (one token from many sources)",
			Computable: false,
			ReasonKey:  AbuseAlertReasonConcurrency,
			Switch:     nil,
		},
	}
	for i := range rules {
		if !rules[i].Computable {
			continue
		}
		// 总开关关掉时开关的「生效值」仍然是设置值 —— 页面需要看得见管理员存过什么。
		rules[i].Switch.Enabled = abuseAlertRuleIsEnabled(rules[i].ID)
	}
	return rules
}

// AbuseAlertLogRetention 描述日志保留期与检测需求的关系（Q2）。
//
// RetentionDays == 0 表示该日志库不设 TTL（SQLite / MySQL / PostgreSQL 走日志清理
// 任务，不是 TTL），此时 RequiredMinutes 无意义，页面不提示。
type AbuseAlertLogRetention struct {
	RetentionDays   int  `json:"retention_days"`
	RequiredMinutes int  `json:"required_minutes"`
	RequiredHours   int  `json:"required_hours"`
	Sufficient      bool `json:"sufficient"`
	// AppliesToClickHouseOnly 为 true 时 RetentionDays 只对 ClickHouse 日志库有意义。
	AppliesToClickHouseOnly bool `json:"applies_to_clickhouse_only"`
}

// AbuseAlertMapPayload 是 GET /api/option/ 伪键 AbuseAlertMap 的载荷形状（前后端契约）。
//
// 前端**只能**读这个伪键来判断规则状态与阈值，不要读裸键
// abuse_alert_setting.* —— 未显式保存时那些键的值是字面量字符串 "null"，
// getOptionValue 按缺省类型解析会把 "null" 变成 false/0，于是默认开的规则显示成关闭。
//
// 字段集合与 PRD 冻结的伪键载荷逐项对应。**没有** `window_minutes`：它既不在 PRD
// 冻结的伪键里，也没有任何消费者（发现端点自己的 `window_minutes` 才是页面用的那个），
// 属投机字段，已删。
type AbuseAlertMapPayload struct {
	Enabled         bool                   `json:"enabled"`
	NotifyEnabled   bool                   `json:"notify_enabled"`
	GeneratedAt     int64                  `json:"generated_at"`
	BaselineReady   bool                   `json:"baseline_ready"`
	BaselineReadyAt int64                  `json:"baseline_ready_at"`
	LogRetention    AbuseAlertLogRetention `json:"log_retention"`
	Rules           []AbuseAlertRule       `json:"rules"`
}

// BuildAbuseAlertMapPayload 组装伪键载荷：静态规则清单 + 运行时开关状态 +
// 基线就绪状态 + 日志保留期（Q2 的提示条数据源）。
func BuildAbuseAlertMapPayload(now int64, baselineReady bool, baselineReadyAt int64, retentionDays int) AbuseAlertMapPayload {
	baselineMinutes := AbuseAlertBaselineGateMinutes()
	lookbackHours := AbuseAlertNewModelLookbackHours()
	sufficient := true
	if retentionDays > 0 {
		sufficient = retentionDays*24*60 >= baselineMinutes && retentionDays >= lookbackHours
	}
	return AbuseAlertMapPayload{
		Enabled:         IsAbuseAlertEnabled(),
		NotifyEnabled:   IsAbuseAlertNotifyEnabled(),
		GeneratedAt:     now,
		BaselineReady:   baselineReady,
		BaselineReadyAt: baselineReadyAt,
		LogRetention: AbuseAlertLogRetention{
			RetentionDays:           retentionDays,
			RequiredMinutes:         baselineMinutes,
			RequiredHours:           lookbackHours,
			Sufficient:              sufficient,
			AppliesToClickHouseOnly: true,
		},
		Rules: AbuseAlertRules(),
	}
}

// ValidateAbuseAlertOption 校验一次 abuse_alert_setting.* 的写入。
//
// 返回 nil 表示这个 key 不属于本模块（交给其它校验器）或值合法。写入路径上
// model.validateOptionValue 会调用它，因此非法值在落库前就被拒 —— 与
// ValidateChannelTestConcurrency 同形。
//
// 校验的是**边界**：上界全部来自常量（不是可配项），下界来自各字段的语义下限。
// 比率必须 > 1.0，否则规则恒真 —— 那不是「更灵敏」，那是一个永远在响的规则。
func ValidateAbuseAlertOption(key string, value string) error {
	if !strings.HasPrefix(key, abuseAlertOptionKeyPrefix) {
		return nil
	}
	field := strings.TrimPrefix(key, abuseAlertOptionKeyPrefix)

	// 裸键未保存时前端会写回字面量 "null"。它是合法的「恢复默认」。
	if value == "null" {
		return nil
	}

	switch field {
	case "enabled", "notify_enabled",
		"rule_consume_spike_enabled", "rule_request_spike_enabled",
		"rule_error_rate_spike_enabled", "rule_new_model_enabled":
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("%s must be a boolean", key)
		}
	case "window_minutes":
		return checkAbuseAlertIntRange(key, value, AbuseAlertMinWindowMinutes, AbuseAlertMaxWindowMinutes, func(windowMinutes int) error {
			baselineWindows := AbuseAlertBaselineWindows()
			if windowMinutes*baselineWindows > AbuseAlertMaxScanSpanMinutes {
				return fmt.Errorf("%s: window_minutes × baseline_windows (%d) must not exceed %d minutes", key, AbuseAlertMaxScanSpanMinutes, AbuseAlertMaxScanSpanMinutes)
			}
			return nil
		})
	case "baseline_windows":
		return checkAbuseAlertIntRange(key, value, AbuseAlertMinBaselineWindows, AbuseAlertMaxBaselineWindows, func(baselineWindows int) error {
			// 下界不只是「≥ 1 个窗口」，还要 ≥ 基线门本身。baseline_windows <
			// AbuseAlertMinBaselineWindowsGate 时，门 `qualified >= Gate` **永不可能**
			// 成立，于是 consume_spike / request_spike / error_rate_spike 三条规则被这条
			// 配置**静默关闭**，而它们在 API 上表现为 insufficient_baseline —— 读起来像
			// 「数据还不够」，而不是「这个配置把规则关了」。
			//
			// 它同时是 PRD §选定 2.6 自己的不变式 `min_baseline_windows <= baseline_windows`
			// 的执行点：R11 把 min_baseline_windows 固定为常量 3 之后，这条不变式在
			// baseline_windows < 3 时必然被违反，而原来的校验器不管。用常量而不是字面量 3，
			// 避免门与校验两处漂移。
			if baselineWindows < AbuseAlertMinBaselineWindowsGate {
				return fmt.Errorf("%s must be at least %d: below the fixed baseline gate (%d windows) the three window rules can never fire, so the value would switch them off silently instead of reporting a shortage",
					key, AbuseAlertMinBaselineWindowsGate, AbuseAlertMinBaselineWindowsGate)
			}
			if AbuseAlertWindowMinutes()*baselineWindows > AbuseAlertMaxScanSpanMinutes {
				return fmt.Errorf("%s: window_minutes × baseline_windows (%d) must not exceed %d minutes", key, AbuseAlertMaxScanSpanMinutes, AbuseAlertMaxScanSpanMinutes)
			}
			return nil
		})
	case "scan_interval_minutes":
		return checkAbuseAlertIntRange(key, value, AbuseAlertMinScanIntervalMinutes, AbuseAlertMaxScanIntervalMinutes, nil)
	case "new_model_lookback_hours":
		return checkAbuseAlertIntRange(key, value, AbuseAlertMinLookbackHours, AbuseAlertMaxLookbackHours, nil)
	case "min_baseline_requests":
		return checkAbuseAlertIntRange(key, value, 0, AbuseAlertMaxMinBaselineRequests, nil)
	case "min_requests":
		return checkAbuseAlertIntRange(key, value, 0, AbuseAlertMaxMinRequests, nil)
	case "min_errors":
		return checkAbuseAlertIntRange(key, value, 0, AbuseAlertMaxMinErrors, nil)
	case "min_consume_quota":
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("%s must be an integer", key)
		}
		if parsed < 0 {
			return fmt.Errorf("%s must not be negative", key)
		}
		if parsed > AbuseAlertMaxMinConsumeQuota {
			return fmt.Errorf("%s must be at most %d", key, AbuseAlertMaxMinConsumeQuota)
		}
	case "consume_ratio", "request_ratio", "error_rate_ratio":
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("%s must be a number", key)
		}
		if parsed <= 1 {
			return fmt.Errorf("%s must be greater than 1, otherwise the rule always fires", key)
		}
		if parsed > 1e6 {
			return fmt.Errorf("%s is out of range", key)
		}
	default:
		return fmt.Errorf("unknown abuse alert setting: %s", key)
	}
	return nil
}

// checkAbuseAlertIntRange 解析并校验一个整数键；extra 为 nil 表示只校验范围。
//
// maxValue 必须是**真实的上界**：这个校验器里不再有「无界」这种写法。
// 旧签名把 `maxValue == 0` 当「无上界」，而 min_* 一族四个键全都这么传，
// 于是一族旋钮集体无界（S1）—— 写坏一颗就能让四条规则静默全关。
func checkAbuseAlertIntRange(key string, value string, minValue int, maxValue int, extra func(int) error) error {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s must be an integer", key)
	}
	if parsed < minValue {
		return fmt.Errorf("%s must be at least %d", key, minValue)
	}
	if parsed > maxValue {
		return fmt.Errorf("%s must be at most %d", key, maxValue)
	}
	if extra != nil {
		return extra(parsed)
	}
	return nil
}

// abuseAlertFloatPtr / abuseAlertInt64Ptr / abuseAlertIntPtr 取快照指针 ——
// JSON 里要区分「阈值是 0」与「阈值没下发」，所以用指针而不是裸值。
func abuseAlertFloatPtr(value float64) *float64 { return &value }
func abuseAlertInt64Ptr(value int64) *int64     { return &value }
func abuseAlertIntPtr(value int) *int           { return &value }
