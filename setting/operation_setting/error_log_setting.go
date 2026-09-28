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
type ErrorLogSetting struct {
	Enabled *bool `json:"enabled"`
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

// 判定机制的稳定标识。条目引用这些标识而不是行号，前端按
// GATE_LABEL_KEYS 把它们渲染成文案，因此条目的条件描述不会随重构失效。
const (
	// ErrorLogGateGlobalSwitch 全局开关
	ErrorLogGateGlobalSwitch = "global_switch"
	// ErrorLogGatePerErrorFlag 该错误未被标记为「不记录」
	ErrorLogGatePerErrorFlag = "per_error_flag"
	// ErrorLogGateBeforeRecordPoint 失败发生在记录点之前
	ErrorLogGateBeforeRecordPoint = "before_record_point"
	// ErrorLogGateHardcodedNoRecord 硬编码的「不记录」标记
	ErrorLogGateHardcodedNoRecord = "hardcoded_no_record"
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
}

// 不记录的原因。额度不足 / 订阅不足 / 预扣费失败属于正常业务结果而非故障，
// 把它们记进错误日志会把「用户没额度了」和「系统坏了」混为一谈并淹没日志表。
const (
	ErrorLogReasonBeforeRecordPoint = "This failure returns before the record point, so no error log is written."
	ErrorLogReasonLocalError        = "Local validation errors are rejected on purpose and are not upstream failures."
	ErrorLogReasonNormalBusiness    = "Insufficient quota is a normal business outcome, not a fault."
	ErrorLogReasonHardcodedNoRecord = "Marked as not recorded because the caller already handles this failure."
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
// 记录条件即 service/relay_error.go 的双门槛：IsErrorLogEnabled() && types.IsRecordErrorLog(err)。
// Source 只是辅助定位串，条目的 id 与 i18n key 保持稳定、不随行号变化。
func ErrorLogEntries() []ErrorLogEntry {
	return []ErrorLogEntry{
		{
			ID:       "relay_upstream_error",
			TitleKey: "Upstream channel error (after a channel is selected)",
			Recorded: true,
			Gates:    []string{ErrorLogGateGlobalSwitch, ErrorLogGatePerErrorFlag},
			Source:   "controller/relay.go:297, relay/responses_websocket.go:321",
		},
		{
			ID:       "task_upstream_error",
			TitleKey: "Async task submission upstream error",
			Recorded: true,
			Gates:    []string{ErrorLogGateGlobalSwitch, ErrorLogGatePerErrorFlag, ErrorLogGateLocalError},
			Source:   "controller/relay.go:563",
		},
		{
			ID:        "no_available_channel",
			TitleKey:  "No available channel in the group",
			Recorded:  false,
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:167",
		},
		{
			ID:        "tiered_billing_prepare_failed",
			TitleKey:  "Tiered billing preparation failure",
			Recorded:  false,
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:172",
		},
		{
			ID:        "request_body_read_failed",
			TitleKey:  "Request body read failure or oversized body",
			Recorded:  false,
			Gates:     []string{ErrorLogGateBeforeRecordPoint},
			ReasonKey: ErrorLogReasonBeforeRecordPoint,
			Source:    "controller/relay.go:183",
		},
		{
			ID:        "task_local_error",
			TitleKey:  "Task submission local validation error",
			Recorded:  false,
			Gates:     []string{ErrorLogGateLocalError},
			ReasonKey: ErrorLogReasonLocalError,
			Source:    "controller/relay.go:562",
		},
		{
			ID:        "insufficient_wallet_quota",
			TitleKey:  "Insufficient wallet quota or pre-consume failure",
			Recorded:  false,
			Gates:     []string{ErrorLogGateHardcodedNoRecord},
			ReasonKey: ErrorLogReasonNormalBusiness,
			Source:    "service/billing_session.go:237,262,396,402",
		},
		{
			ID:        "insufficient_subscription_quota",
			TitleKey:  "Insufficient subscription quota",
			Recorded:  false,
			Gates:     []string{ErrorLogGateHardcodedNoRecord},
			ReasonKey: ErrorLogReasonNormalBusiness,
			Source:    "service/billing_session.go:241,284",
		},
		{
			ID:        "token_preconsume_failed",
			TitleKey:  "Token pre-consume failure",
			Recorded:  false,
			Gates:     []string{ErrorLogGateHardcodedNoRecord},
			ReasonKey: ErrorLogReasonNormalBusiness,
			Source:    "service/billing_session.go:213,313",
		},
		{
			ID:        "responses_ws_dispatch_error",
			TitleKey:  "Responses WebSocket internal dispatch failure",
			Recorded:  false,
			Gates:     []string{ErrorLogGateHardcodedNoRecord},
			ReasonKey: ErrorLogReasonHardcodedNoRecord,
			Source:    "controller/responses_websocket.go:108,111",
		},
		{
			ID:        "other_log_types",
			TitleKey:  "Other log types (consume / login / audit / top-up / task billing)",
			Recorded:  false,
			Gates:     []string{ErrorLogGateOtherSwitch},
			ReasonKey: ErrorLogReasonOtherSwitch,
			Source:    "model/log.go:232,247,270,357,433",
		},
	}
}
