package operation_setting

import (
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/config"
)

// ErrorLogEnvVar 是错误日志总开关的环境变量名。它同时也是这个开关的初值来源：
// 读取点在 common/init.go 的 initConstantEnv()（→ constant.ErrorLogEnabled），
// 那里晚于 main.go 的 godotenv.Load(".env")，因此 .env 里的值能被读到。
const ErrorLogEnvVar = "ERROR_LOG_ENABLED"

// 生效值来源，随 ErrorLogMap 一并下发给前端，用于解释「为什么现在是这个状态」。
const (
	ErrorLogEnabledSourceEnv     = "env"
	ErrorLogEnabledSourceSetting = "setting"
)

// ErrorLogSetting 是错误日志总开关的运行时设置。
//
// Enabled 用指针以区分两种「关闭」：
//   - nil     从未被管理员显式保存 → 沿用 ERROR_LOG_ENABLED 派生的初值（constant.ErrorLogEnabled）；
//   - 非 nil  管理员已显式保存 → 以保存值为准，开、关两个方向都覆盖 env。
//
// 不能用普通 bool：它区分不出「没设过」与「显式设为 false」；改成
// `Enabled || constant.ErrorLogEnabled` 则会让 env=true 的部署无法从界面关闭。
//
// 也不要在本文件的包级初始化里直接 os.Getenv(ErrorLogEnvVar)：Go 包级变量初始化
// 一定早于 main() 函数体，而 .env 是在 main.go 里加载的，那样会静默丢掉 .env 的值，
// 也就是「升级后静默变关」。env 的读取位置必须留在 common/init.go。
//
// 六个逐类开关同样用指针，原因相同：4 类默认关、2 类默认开，两种默认都需要
// 区分「没设过」与「显式设为 false/true」。用普通 bool 会在升级瞬间把默认开的
// 2 类静默关掉。也不要用 map：查不到键返回 false，同样迁移不了「没设过」。
type ErrorLogSetting struct {
	Enabled *bool `json:"enabled"`

	// 逐类采集开关。nil = 从未被管理员保存 → 用该类的历史默认值。
	RecordRelayUpstreamError            *bool `json:"record_relay_upstream_error"`
	RecordTaskUpstreamError             *bool `json:"record_task_upstream_error"`
	RecordInsufficientWalletQuota       *bool `json:"record_insufficient_wallet_quota"`
	RecordInsufficientSubscriptionQuota *bool `json:"record_insufficient_subscription_quota"`
	RecordTokenPreconsumeFailed         *bool `json:"record_token_preconsume_failed"`

	// responses_ws_dispatch_error 没有开关：它的失败由 WS 派发 runner 直接写回客户端，
	// 从不进入中继错误路径（见 ErrorLogEntries 里该条的 source），任何开关都无法让它记录。
}

var errorLogSetting = ErrorLogSetting{}

func init() {
	config.GlobalConfig.Register("error_log_setting", &errorLogSetting)
}

// IsErrorLogEnabled 返回错误日志总开关的当前生效值。
//
// 每次调用都重新解析，所以管理员在界面上保存后，对后续请求立即生效，不需要重启。
// 不取锁：写路径在 common.OptionMapRWMutex 写锁保护下进行，读的是一个指针是否为 nil
// 加一个 bool，与仓库既有的 DemoSiteEnabled / SelfUseModeEnabled 同形态。
func IsErrorLogEnabled() bool {
	if errorLogSetting.Enabled != nil {
		return *errorLogSetting.Enabled
	}
	return constant.ErrorLogEnabled
}

// ErrorLogEnabledSource 返回当前生效值的来源（env 或 setting）。
func ErrorLogEnabledSource() string {
	if errorLogSetting.Enabled != nil {
		return ErrorLogEnabledSourceSetting
	}
	return ErrorLogEnabledSourceEnv
}

// 可切换类别的标识。取值就是映射表条目（ErrorLogEntries）的 ID ——
// 开关与条目共用同一组常量，页面上的开关和条目从结构上无法漂移。
// 它们是未定型的字符串常量，因此可以直接传给 relaykit 的
// types.ErrOptionWithErrorLogCategory，无需 relaykit 认识任何类别名。
//
// 只有「存在至少一个能到达记录点的站点」的类别才在这里。原本还列了
// responses_ws_dispatch_error，实现时逐条追踪调用链发现它的两个站点
// （controller/responses_websocket.go 的派发 runner）把错误直接写回 WebSocket 客户端，
// 从不进入 service.ProcessChannelError，因此任何开关对它都无效 —— 撤下，不发布无效开关。
const (
	ErrorLogCategoryUpstream                      = "relay_upstream_error"
	ErrorLogCategoryTaskUpstream                  = "task_upstream_error"
	ErrorLogCategoryInsufficientWalletQuota       = "insufficient_wallet_quota"
	ErrorLogCategoryInsufficientSubscriptionQuota = "insufficient_subscription_quota"
	ErrorLogCategoryTokenPreconsumeFailed         = "token_preconsume_failed"
)

// 开关写入用的完整 option key。前缀与 config.GlobalConfig.Register 注册的模块名一致。
const errorLogCategoryOptionKeyPrefix = "error_log_setting.record_"

// 逐类开关的来源，随 ErrorLogMap 下发给前端。
const (
	ErrorLogCategorySourceDefault = "default"
	ErrorLogCategorySourceSetting = "setting"
)

// errorLogCategorySwitchField 返回一个类别的开关字段、它的历史默认值，
// 以及这个类别是否可切换。三个返回值一起出现是因为它们同属一份清单：
// 分开写会出现「常量在、开关字段漏了」这种静默错配。
func errorLogCategorySwitchField(category string) (value *bool, defaultOn, known bool) {
	switch category {
	case ErrorLogCategoryUpstream:
		return errorLogSetting.RecordRelayUpstreamError, true, true
	case ErrorLogCategoryTaskUpstream:
		return errorLogSetting.RecordTaskUpstreamError, true, true
	case ErrorLogCategoryInsufficientWalletQuota:
		return errorLogSetting.RecordInsufficientWalletQuota, false, true
	case ErrorLogCategoryInsufficientSubscriptionQuota:
		return errorLogSetting.RecordInsufficientSubscriptionQuota, false, true
	case ErrorLogCategoryTokenPreconsumeFailed:
		return errorLogSetting.RecordTokenPreconsumeFailed, false, true
	default:
		return nil, false, false
	}
}

// IsErrorLogCategoryEnabled 返回某个类别开关的当前生效值。
//
// nil（从未被管理员保存过）→ 该类别的历史默认值，即本开关引入之前的记录行为；
// 未知类别 → true，fail-open：将来新增的类别标签不会因为配置里没有对应字段而静默丢日志。
//
// 每次调用都重新解析，所以管理员保存后对后续请求立即生效，不需要重启。
func IsErrorLogCategoryEnabled(category string) bool {
	value, defaultOn, known := errorLogCategorySwitchField(category)
	if !known {
		return true
	}
	if value != nil {
		return *value
	}
	return defaultOn
}

// ErrorLogCategorySwitch 是某个类别的开关状态（设置值，不是与总开关的合成值）。
type ErrorLogCategorySwitch struct {
	Category  string `json:"category"`
	OptionKey string `json:"option_key"`
	Enabled   bool   `json:"enabled"`
	Source    string `json:"source"`
}

// errorLogCategorySwitch 组装一个类别的开关状态；不可切换的类别返回 nil。
func errorLogCategorySwitch(category string) *ErrorLogCategorySwitch {
	value, defaultOn, known := errorLogCategorySwitchField(category)
	if !known {
		return nil
	}
	enabled, source := defaultOn, ErrorLogCategorySourceDefault
	if value != nil {
		enabled, source = *value, ErrorLogCategorySourceSetting
	}
	return &ErrorLogCategorySwitch{
		Category:  category,
		OptionKey: errorLogCategoryOptionKeyPrefix + category,
		Enabled:   enabled,
		Source:    source,
	}
}

// 判定机制的稳定标识。条目引用这些标识而不是行号，前端按
// GATE_LABEL_KEYS 把它们渲染成文案，因此条目的条件描述不会随重构失效。
const (
	// ErrorLogGateGlobalSwitch 全局开关
	ErrorLogGateGlobalSwitch = "global_switch"
	// ErrorLogGateCategorySwitch 该类别自己的采集开关
	ErrorLogGateCategorySwitch = "category_switch"
	// ErrorLogGateBeforeRecordPoint 失败发生在记录点之前
	ErrorLogGateBeforeRecordPoint = "before_record_point"
	// ErrorLogGateLocalError 本地校验类错误
	ErrorLogGateLocalError = "local_error"
	// ErrorLogGateOtherSwitch 由其它日志开关控制
	ErrorLogGateOtherSwitch = "other_switch"
)

// ErrorLogEntry 描述一类失败是否会往 logs 表写一行 type=5（错误日志）。
//
// 这是映射表的唯一数据源：条目集合与 i18n key 都由后端定义，前端只做
// t(TitleKey) / t(ReasonKey)，不存在可漂移的第二份清单。
// TitleKey / ReasonKey 与英文源串同值，符合仓库「key 即英文源串」的 i18n 约定；
// 缺少翻译时 i18next 回退到英文源串，不会把 key 本身显示给用户。
type ErrorLogEntry struct {
	ID        string   `json:"id"`
	TitleKey  string   `json:"titleKey"`
	Recorded  bool     `json:"recorded"`
	Gates     []string `json:"gates"`
	ReasonKey string   `json:"reasonKey"`
	Source    string   `json:"source"`

	// CategorySwitch 是该类别的采集开关；nil 表示这一类结构上不可切换，
	// 页面因此不渲染任何控件（不是渲染一个灰掉的开关）。
	CategorySwitch *ErrorLogCategorySwitch `json:"category_switch"`
}

// 条目说明文案。额度不足 / 订阅不足 / 预扣费失败属于正常业务结果而非故障，
// 记进错误日志会把「用户没额度了」和「系统坏了」混为一谈并淹没日志表。
// ReasonKey 对可切换的类别是「开启它要知道的事」，对不可切换的类别是「为什么不能」。
const (
	ErrorLogReasonBeforeRecordPoint = "This failure returns before the record point, so no error log is written."
	ErrorLogReasonLocalError        = "Excluded by the call-site condition before the record point. Recording it would require local errors to go through the channel error handler, which also disables the upstream channel."
	ErrorLogReasonNormalBusiness    = "Insufficient quota is a normal business outcome, not a fault. Enabling it may flood the error log with routine failures."
	ErrorLogReasonOutsideRelayPath  = "This failure is returned to the WebSocket client by the dispatch runner and never enters the relay error path, so no switch can make it record."
	ErrorLogReasonOtherSwitch       = "Written by its own log switch and unrelated to the error log switch."
)

// ErrorLogMapPayload 是 GET /api/option/ 伪键 ErrorLogMap 的载荷形状（前后端契约）。
//
// Enabled 是生效值，前端开关的显示值只能取它；不要读裸键
// error_log_setting.enabled —— 未显式保存时那个值是字面量字符串 "null"，
// 按 bool 解析会得到 false，于是 env=true 时页面会显示成关闭。
type ErrorLogMapPayload struct {
	Enabled       bool            `json:"enabled"`
	EnabledSource string          `json:"enabled_source"`
	EnvVar        string          `json:"env_var"`
	Entries       []ErrorLogEntry `json:"entries"`
}

// BuildErrorLogMapPayload 组装映射表载荷：静态条目 + 运行时开关状态。
func BuildErrorLogMapPayload() ErrorLogMapPayload {
	return ErrorLogMapPayload{
		Enabled:       IsErrorLogEnabled(),
		EnabledSource: ErrorLogEnabledSource(),
		EnvVar:        ErrorLogEnvVar,
		Entries:       ErrorLogEntries(),
	}
}

// ErrorLogEntries 返回完整的日志映射表（后端唯一数据源）。
//
// 记录条件即 service/relay_error.go 的双门槛：IsErrorLogEnabled() && 该错误所属类别的开关。
// Recorded 是当前合成值（全局 ∧ 该类），因此这个函数每次都要读一次运行时设置；
// CategorySwitch 是该类别的设置值，nil 表示该类结构上不可切换。
// Source 只是辅助定位串，条目的 id 与 i18n key 保持稳定、不随行号变化。
func ErrorLogEntries() []ErrorLogEntry {
	entries := []ErrorLogEntry{
		{
			ID: "relay_upstream_error",
			// 「未特别归类」的失败也走这里：记录点对没有声明类别的错误一律归入本类，
			// 于是 controller/relay.go:209、relay/responses_websocket.go:321、
			// controller/channel-test.go:953 三个站点都受本开关管辖。
			TitleKey: "Upstream channel error (after a channel is selected), including failures without a specific category",
			Gates:    []string{ErrorLogGateGlobalSwitch, ErrorLogGateCategorySwitch},
			Source:   "controller/relay.go:209, relay/responses_websocket.go:321, controller/channel-test.go:953",
		},
		{
			ID:       "task_upstream_error",
			TitleKey: "Async task submission upstream error",
			Gates:    []string{ErrorLogGateGlobalSwitch, ErrorLogGateCategorySwitch, ErrorLogGateLocalError},
			Source:   "controller/relay.go:563",
		},
		{
			ID:        "no_available_channel",
			TitleKey:  "No available channel in the group",
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:163-168 (break at :167)",
		},
		{
			ID:        "tiered_billing_prepare_failed",
			TitleKey:  "Tiered billing preparation failure",
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:170-173 (break at :172)",
		},
		{
			ID:        "request_body_read_failed",
			TitleKey:  "Request body read failure or oversized body",
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:175-184 (break at :183)",
		},
		{
			ID:        "task_local_error",
			TitleKey:  "Task submission local validation error",
			Gates:     []string{ErrorLogGateLocalError},
			ReasonKey: ErrorLogReasonLocalError,
			Source:    "controller/relay.go:562-568",
		},
		{
			ID:        "insufficient_wallet_quota",
			TitleKey:  "Insufficient wallet quota or pre-consume failure",
			Gates:     []string{ErrorLogGateGlobalSwitch, ErrorLogGateCategorySwitch},
			ReasonKey: ErrorLogReasonNormalBusiness,
			// :262 只在 Reserve 路径上被调用，而它的三个调用方里有两个会丢掉这个错误对象
			// 或根本不进记录点，因此那个站点不经过记录点。
			Source: "service/billing_session.go:237,396,402 (record point); :262 (returns before the record point)",
		},
		{
			ID:        "insufficient_subscription_quota",
			TitleKey:  "Insufficient subscription quota",
			Gates:     []string{ErrorLogGateGlobalSwitch, ErrorLogGateCategorySwitch},
			ReasonKey: ErrorLogReasonNormalBusiness,
			Source:    "service/billing_session.go:241 (record point); :284 (returns before the record point)",
		},
		{
			ID:        "token_preconsume_failed",
			TitleKey:  "Token pre-consume failure",
			Gates:     []string{ErrorLogGateGlobalSwitch, ErrorLogGateCategorySwitch},
			ReasonKey: ErrorLogReasonNormalBusiness,
			Source:    "service/billing_session.go:213 (record point); :313 (returns before the record point)",
		},
		{
			// 这两个站点把错误直接还给 WS 派发 runner，而 runner 只用它构造一条写回客户端的
			// 错误帧（relay/responses_websocket.go:186-192），从不调用 ProcessChannelError。
			// 因此这一类的失败到不了记录点，本 slice 不为它发布开关。
			ID:        "responses_ws_dispatch_error",
			TitleKey:  "Responses WebSocket internal dispatch failure",
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonOutsideRelayPath,
			Source:    "controller/responses_websocket.go:109,112 (handed to the WS runner, never to ProcessChannelError)",
		},
		{
			ID:        "other_log_types",
			TitleKey:  "Other log types (consume / login / audit / top-up / task billing)",
			Gates:     []string{ErrorLogGateOtherSwitch},
			ReasonKey: ErrorLogReasonOtherSwitch,
			Source:    "model/log.go:232,247,270,357,433",
		},
	}

	for i := range entries {
		switchForCategory := errorLogCategorySwitch(entries[i].ID)
		if switchForCategory == nil {
			// 不可切换的类别永远不记录：它们的失败根本没到达记录点。
			entries[i].Recorded, entries[i].CategorySwitch = false, nil
			continue
		}
		entries[i].CategorySwitch = switchForCategory
		entries[i].Recorded = IsErrorLogEnabled() && IsErrorLogCategoryEnabled(entries[i].ID)
	}
	return entries
}
