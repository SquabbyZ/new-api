package service

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	kitdto "github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/system_setting"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestShouldRetryRelayErrorHonorsChannelPinOnChannelError(t *testing.T) {
	err := types.NewError(errors.New("channel failed"), types.ErrorCodeChannelNoAvailableKey)
	for _, test := range []struct {
		name      string
		pin       *dto.ChannelPin
		wantRetry bool
	}{
		{name: "unrestricted channel error", wantRetry: true},
		{
			name: "single attempt pin suppresses channel error retry",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt,
			},
			wantRetry: false,
		},
		{
			name: "origin task pin permits retry on the same channel",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceOriginTask, Rank: dto.PinRankOriginTask, RetryMode: dto.PinRetrySameChannel,
			},
			wantRetry: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if test.pin != nil {
				GetChannelConstraints(c).AddPin(*test.pin)
			}
			assert.Equal(t, test.wantRetry, ShouldRetryRelayError(c, err, 1))
		})
	}
}

func TestProcessChannelErrorMasksDisableReasonAndNotification(t *testing.T) {
	previousDB, previousType := model.DB, common.MainDatabaseType()
	previousCache, previousRedis := common.MemoryCacheEnabled, common.RedisEnabled
	previousAutoDisable, previousErrorLog := common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled
	previousNotifyLimit := constant.NotifyLimitCount
	previousClient, previousWorker := httpClient, system_setting.WorkerUrl
	fetch := system_setting.GetFetchSetting()
	previousFetch := *fetch
	t.Cleanup(func() {
		model.DB = previousDB
		common.SetMainDatabaseType(previousType)
		common.MemoryCacheEnabled, common.RedisEnabled = previousCache, previousRedis
		common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = previousAutoDisable, previousErrorLog
		constant.NotifyLimitCount = previousNotifyLimit
		httpClient, system_setting.WorkerUrl = previousClient, previousWorker
		*fetch = previousFetch
	})
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, database.AutoMigrate(&model.Channel{}, &model.Ability{}, &model.User{}))
	model.DB = database
	common.SetMainDatabaseType(common.DatabaseTypeSQLite)
	common.MemoryCacheEnabled, common.RedisEnabled = false, false
	common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = true, false
	constant.NotifyLimitCount = 10
	channel := &model.Channel{Name: "relay-review", Key: "fixture-key", Type: 1, Status: common.ChannelStatusEnabled, Group: "default", Models: "test-model"}
	require.NoError(t, channel.Insert())
	notifications := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		notifications <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	httpClient, system_setting.WorkerUrl = server.Client(), ""
	fetch.EnableSSRFProtection = false
	settings, err := common.Marshal(kitdto.UserSetting{NotifyType: kitdto.NotifyTypeWebhook, WebhookUrl: server.URL})
	require.NoError(t, err)
	root := &model.User{Username: "notification-test-root", Role: common.RoleRootUser, Status: common.UserStatusEnabled, Setting: string(settings)}
	require.NoError(t, database.Create(root).Error)
	notifyKey := fmt.Sprintf("%d:%s:%s", root.Id, formatNotifyType(channel.Id, common.ChannelStatusAutoDisabled), time.Now().Format("2006010215"))
	notifyLimitStore.Delete(notifyKey)
	t.Cleanup(func() { notifyLimitStore.Delete(notifyKey) })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	apiErr := types.NewErrorWithStatusCode(errors.New("upstream https://private.example.com/path?token=review-token api_key:review-secret"), types.ErrorCodeChannelNoAvailableKey, http.StatusUnauthorized)
	ProcessChannelError(c, types.ChannelError{ChannelId: channel.Id, ChannelName: channel.Name, AutoBan: true}, apiErr, nil)
	var notification WebhookPayload
	select {
	case payload := <-notifications:
		require.NoError(t, common.Unmarshal(payload, &notification))
	case <-time.After(5 * time.Second):
		t.Fatal("automatic channel-disable notification was not delivered")
	}
	loaded, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	assert.Equal(t, common.ChannelStatusAutoDisabled, loaded.Status)
	wantReason := "status_code=401, upstream https://***.com/***?token=*** api_key:***"
	assert.Equal(t, wantReason, loaded.GetOtherInfo()["status_reason"])
	assert.Contains(t, notification.Content, wantReason)
	assert.NotContains(t, notification.Content, "review-token")
	assert.NotContains(t, notification.Content, "review-secret")
	assert.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
}

func TestDecideRelayRetryReasons(t *testing.T) {
	upstream := func(status int) *types.NewAPIError {
		return types.NewOpenAIError(errors.New("upstream"), types.ErrorCodeBadResponseStatusCode, status)
	}
	for _, tc := range []struct {
		name    string
		err     *types.NewAPIError
		retries int
		setup   func(*gin.Context)
		want    PolicyDecision
	}{
		{name: "retry status matched", err: upstream(http.StatusTooManyRequests), retries: 1, want: PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}},
		{name: "status outside retry rules", err: upstream(http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "stop", Reason: "status_not_retryable", Source: "global"}},
		{name: "attempt budget exhausted", err: upstream(http.StatusTooManyRequests), retries: 0, want: PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}},
		{name: "always skipped status", err: upstream(http.StatusGatewayTimeout), retries: 1, want: PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}},
		{name: "success status never retries", err: upstream(http.StatusOK), retries: 1, want: PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}},
		{name: "skip retry error", err: types.NewErrorWithStatusCode(errors.New("local"), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry()), retries: 1, want: PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}},
		{name: "channel error retries without budget", err: types.NewError(errors.New("no key"), types.ErrorCodeChannelNoAvailableKey), retries: 0, want: PolicyDecision{Action: "retry", Reason: "channel_error", Source: "system"}},
		{name: "single attempt pin", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			GetChannelConstraints(c).AddPin(dto.ChannelPin{ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt})
		}, want: PolicyDecision{Action: "stop", Reason: "pinned_channel", Source: "channel_constraint"}},
		{name: "strict session", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			c.Set(ginKeyChannelAffinitySkipRetry, true)
			RequestPolicy(c).SessionModeSource = "global"
		}, want: PolicyDecision{Action: "stop", Reason: "strict_session", Source: "global"}},
		{name: "nil error", retries: 1, want: PolicyDecision{Action: "stop", Reason: "request_completed", Source: "system"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if tc.setup != nil {
				tc.setup(c)
			}
			decision := DecideRelayRetry(c, tc.err, tc.retries)
			assert.Equal(t, tc.want, decision)
			assert.Equal(t, tc.want.Action == "retry", ShouldRetryRelayError(c, tc.err, tc.retries))
		})
	}
}

func TestRequestPolicyEventsReachLogAdminInfo(t *testing.T) {
	previousAutoDisable := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticDisableChannelEnabled = previousAutoDisable })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set("auto_ban", true)
	c.Set("channel_id", 7)
	state := RequestPolicy(c)
	state.BeginAttempt(&model.Channel{Id: 7}, "default")
	apiErr := types.NewOpenAIError(errors.New("invalid credential"), types.ErrorCodeBadResponseStatusCode, http.StatusUnauthorized)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))

	failed := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, failed)
	events, ok := failed.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a failed relay exposes its decision events to administrators")
	require.Len(t, events, 3)
	assert.Equal(t, PolicyDecision{Action: "attempt", Reason: "channel_selected", Source: "routing"}, events[0].Decision)
	assert.Equal(t, "default", events[0].Group)
	assert.Equal(t, PolicyDecision{Action: "failure", Reason: "upstream_failure", Source: "upstream"}, events[1].Decision)
	assert.Equal(t, http.StatusUnauthorized, events[1].Status)
	assert.Equal(t, PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}, events[2].Decision)
	assert.Equal(t, "channel_disable_requested", events[2].Health, "the health entry follows the automatic disable rules")
	common.SetContextKey(c, constant.ContextKeyChannelIsMultiKey, true)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))
	assert.Equal(t, "key_disable_requested", state.Events()[4].Health)

	state.BeginAttempt(&model.Channel{Id: 8}, "default")
	c.Set("channel_id", 8)
	MarkRequestPolicySuccess(c, nil)
	MarkRequestPolicySuccess(c, nil)
	succeeded := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, succeeded)
	events, ok = succeeded.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a successful relay exposes its decision events to administrators")
	require.Len(t, events, 7, "the outcome is recorded once")
	assert.Equal(t, PolicyDecision{Action: "success", Reason: "request_completed", Source: "upstream"}, events[6].Decision)
	assert.Equal(t, 8, events[6].ChannelID)
	assert.Equal(t, 2, events[6].Attempt)
	assert.True(t, state.Successful)

	untouched, _ := gin.CreateTestContext(httptest.NewRecorder())
	other := model.NewLogOther()
	AppendRelayLogAdminInfo(untouched, nil, other)
	assert.NotContains(t, other.Snapshot()["admin_info"], "request_policy", "requests without decisions do not carry an empty record")
}

// errorLogTestFixture 装配记录点所需的最小环境：内存 SQLite 同时充当主库与日志库、
// 一个已初始化的 OptionMap、以及「未设 env、未保存过设置」的全新部署默认态。
//
// 返回的 errorLogRows 数当前的 type=5 行数。用例结束会把 optionKeys 里的键还原成
// "null" —— 这一步必须在 model.DB 仍指向测试库、且测试库仍打开时执行，
// 否则这条写入会落到真实数据库上。
func errorLogTestFixture(t *testing.T, optionKeys ...string) func() int64 {
	t.Helper()

	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, database.AutoMigrate(&model.Option{}, &model.Log{}, &model.User{}))

	previousDB, previousLogDB := model.DB, model.LOG_DB
	previousRedis := common.RedisEnabled
	previousMainType, previousLogType := common.MainDatabaseType(), common.LogDatabaseType()
	previousFallback := constant.ErrorLogEnabled

	model.DB, model.LOG_DB = database, database
	common.RedisEnabled = false
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	// updateOptionMap 写入全局 OptionMap，测试进程里它默认是 nil。
	common.OptionMapRWMutex.Lock()
	if common.OptionMap == nil {
		common.OptionMap = make(map[string]string)
	}
	common.OptionMapRWMutex.Unlock()
	// 未设 env、未保存过设置。
	constant.ErrorLogEnabled = false

	t.Cleanup(func() {
		for _, key := range optionKeys {
			assert.NoError(t, model.UpdateOption(key, "null"))
			common.OptionMapRWMutex.Lock()
			delete(common.OptionMap, key)
			common.OptionMapRWMutex.Unlock()
		}
		require.NoError(t, sqlDB.Close())
		model.DB, model.LOG_DB = previousDB, previousLogDB
		common.RedisEnabled = previousRedis
		common.SetDatabaseTypes(previousMainType, previousLogType)
		constant.ErrorLogEnabled = previousFallback
	})

	return func() int64 {
		var count int64
		require.NoError(t, model.LOG_DB.Model(&model.Log{}).Where("type = ?", model.LogTypeError).Count(&count).Error)
		return count
	}
}

func newErrorLogTestContext() *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set("id", 7)
	c.Set("token_name", "runtime-switch")
	c.Set("token_id", 11)
	c.Set("original_model", "gpt-test")
	c.Set("group", "default")
	common.SetContextKey(c, constant.ContextKeyRequestStartTime, time.Now())
	return c
}

// TestProcessChannelErrorFollowsTheRuntimeErrorLogSwitch 是本 slice 的核心回归。
// 记录决策必须读运行时设置，而不是启动时的 env 快照：如果读取点退回
// constant.ErrorLogEnabled，开关打开后的那条断言就会失败 —— 界面上改了开关却不生效、
// 必须重启，正是要防的那个 bug。
func TestProcessChannelErrorFollowsTheRuntimeErrorLogSwitch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	errorLogRows := errorLogTestFixture(t, "error_log_setting.enabled")

	upstreamError := func() *types.NewAPIError {
		return types.NewOpenAIError(errors.New("upstream refused"), types.ErrorCodeBadResponseStatusCode, http.StatusBadGateway)
	}
	channelError := types.ChannelError{ChannelId: 101, ChannelName: "runtime-switch", AutoBan: false}

	ProcessChannelError(newErrorLogTestContext(), channelError, upstreamError(), nil)
	assert.Zero(t, errorLogRows(), "the default stays off, so no error log row is written")

	// 与 PUT /api/option/ 完全同一条写入路径。
	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "true"))
	ProcessChannelError(newErrorLogTestContext(), channelError, upstreamError(), nil)
	assert.Equal(t, int64(1), errorLogRows(),
		"turning the switch on must take effect for the next request without a restart")

	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "false"))
	ProcessChannelError(newErrorLogTestContext(), channelError, upstreamError(), nil)
	assert.Equal(t, int64(1), errorLogRows(), "turning the switch off stops new error log rows")

	// 回到「没设过」后重新跟随 ERROR_LOG_ENABLED：只设了 env=true 的部署升级后不能静默变关。
	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "null"))
	constant.ErrorLogEnabled = true
	ProcessChannelError(newErrorLogTestContext(), channelError, upstreamError(), nil)
	assert.Equal(t, int64(2), errorLogRows(),
		"a deployment that only sets ERROR_LOG_ENABLED=true keeps recording after the upgrade")
}

// errorLogCategories 是可切换类别与它们的历史默认值。defaultOn 逐条对应
// PRD 选定 2 的表：默认值必须等于引入逐类开关之前的记录行为。
//
// 这里没有 responses_ws_dispatch_error：它的站点把错误直接还给 WS 派发 runner，
// 从不进入 ProcessChannelError，因此没有可发布的开关（见 operation_setting 的条目注释）。
func errorLogCategories() []struct {
	ID        string
	Key       string
	DefaultOn bool
	Err       func() *types.NewAPIError
} {
	openAI := func(ops ...types.NewAPIErrorOptions) func() *types.NewAPIError {
		return func() *types.NewAPIError {
			return types.NewOpenAIError(errors.New("upstream refused"), types.ErrorCodeBadResponseStatusCode, http.StatusBadGateway, ops...)
		}
	}
	quota := func(category string, message string, ops ...types.NewAPIErrorOptions) func() *types.NewAPIError {
		return func() *types.NewAPIError {
			return types.NewErrorWithStatusCode(errors.New(message), types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
				append(ops, types.ErrOptionWithErrorLogCategory(types.ErrorLogCategory(category)))...)
		}
	}
	return []struct {
		ID        string
		Key       string
		DefaultOn bool
		Err       func() *types.NewAPIError
	}{
		{
			ID:        operation_setting.ErrorLogCategoryUpstream,
			Key:       "error_log_setting.record_relay_upstream_error",
			DefaultOn: true,
			// 没有声明类别的错误 —— 缺省归类为第 1 行的那条规则。
			Err: openAI(),
		},
		{
			ID:        operation_setting.ErrorLogCategoryTaskUpstream,
			Key:       "error_log_setting.record_task_upstream_error",
			DefaultOn: true,
			Err:       openAI(types.ErrOptionWithErrorLogCategory(operation_setting.ErrorLogCategoryTaskUpstream)),
		},
		{
			ID:        operation_setting.ErrorLogCategoryInsufficientWalletQuota,
			Key:       "error_log_setting.record_insufficient_wallet_quota",
			DefaultOn: false,
			Err:       quota(operation_setting.ErrorLogCategoryInsufficientWalletQuota, "用户额度不足, 剩余额度: 0"),
		},
		{
			ID:        operation_setting.ErrorLogCategoryInsufficientSubscriptionQuota,
			Key:       "error_log_setting.record_insufficient_subscription_quota",
			DefaultOn: false,
			Err:       quota(operation_setting.ErrorLogCategoryInsufficientSubscriptionQuota, "订阅额度不足或未配置订阅: no active subscription"),
		},
		{
			ID:        operation_setting.ErrorLogCategoryTokenPreconsumeFailed,
			Key:       "error_log_setting.record_token_preconsume_failed",
			DefaultOn: false,
			Err: func() *types.NewAPIError {
				return types.NewErrorWithStatusCode(errors.New("token quota exhausted"), types.ErrorCodePreConsumeTokenQuotaFailed, http.StatusForbidden,
					types.ErrOptionWithSkipRetry(), types.ErrOptionWithErrorLogCategory(operation_setting.ErrorLogCategoryTokenPreconsumeFailed))
			},
		},
	}
}

// TestErrorLogCategorySwitchesAreIndependentAndKeepTheirDefaults 覆盖全部可切换类别：
// 默认值逐类等于改动前的行为、每类可以单独开/关、且开关之间不串扰。
func TestErrorLogCategorySwitchesAreIndependentAndKeepTheirDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)

	categories := errorLogCategories()
	keys := make([]string, 0, len(categories))
	for _, category := range categories {
		keys = append(keys, category.Key)
	}
	errorLogRows := errorLogTestFixture(t, append(keys, "error_log_setting.enabled")...)

	// 升级后的真实起点：6 个键都没被写过，option 表里也不该有它们。
	// 这一条要在夹具装好之后查，否则查的是真实库。
	for _, category := range categories {
		var stored int64
		require.NoError(t, model.DB.Model(&model.Option{}).Where("key = ?", category.Key).Count(&stored).Error)
		assert.Zero(t, stored, "升级不得写回任何一个逐类开关")
	}

	channelError := types.ChannelError{ChannelId: 101, ChannelName: "category-switch", AutoBan: false}
	record := func(err *types.NewAPIError) {
		ProcessChannelError(newErrorLogTestContext(), channelError, err, nil)
	}

	// 判定层：全部为「没设过」时，逐类等于历史默认值。
	for _, category := range categories {
		assert.Equal(t, category.DefaultOn, operation_setting.IsErrorLogCategoryEnabled(category.ID),
			"%s must keep its pre-slice default while the switch was never saved", category.ID)
	}
	// 未知类别 fail-open：将来新增的标签不会因为配置里没有字段而静默丢日志。
	assert.True(t, operation_setting.IsErrorLogCategoryEnabled("future_category"))

	// 「已有老配置」起点：库里只显式保存过总开关，没有这 6 个键。
	// 反序列化必须把它们留成 nil 而不是 false —— 否则默认开的 2 类会在升级瞬间静默变关。
	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "true"))
	for _, category := range categories {
		var stored int64
		require.NoError(t, model.DB.Model(&model.Option{}).Where("key = ?", category.Key).Count(&stored).Error)
		assert.Zero(t, stored, "%s must stay absent in an upgraded database", category.Key)
		assert.Equal(t, category.DefaultOn, operation_setting.IsErrorLogCategoryEnabled(category.ID),
			"%s must keep its pre-slice behavior when only the global switch was ever saved", category.ID)
	}
	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "null"))

	// 全局关时任何类别都不记录 —— 逐类开关不能绕过外层硬闸。
	for _, category := range categories {
		require.NoError(t, model.UpdateOption(category.Key, "true"))
	}
	record(categories[0].Err())
	assert.Zero(t, errorLogRows(), "the global switch is the outer hard gate")
	// 六类全部回到「没设过」，让下面的逐类循环从各自的真实默认态出发。
	for _, category := range categories {
		require.NoError(t, model.UpdateOption(category.Key, "null"))
	}
	require.NoError(t, model.UpdateOption("error_log_setting.enabled", "true"))

	// 行为层：逐类开 → 记录；关 → 不记录；同时证明另外 5 类不受影响。
	for i, category := range categories {
		require.NoError(t, model.UpdateOption(category.Key, "false"))
		before := errorLogRows()
		record(category.Err())
		assert.Equal(t, before, errorLogRows(), "%s is off, so it writes no row", category.ID)
		// 串扰断言：关掉 X 之后，另外 5 类的判定结果必须仍然等于它们各自的历史默认值。
		for j, other := range categories {
			if i == j {
				assert.False(t, operation_setting.IsErrorLogCategoryEnabled(other.ID))
				continue
			}
			assert.Equal(t, other.DefaultOn, operation_setting.IsErrorLogCategoryEnabled(other.ID),
				"%s must not be affected by %s", other.ID, category.ID)
		}

		require.NoError(t, model.UpdateOption(category.Key, "true"))
		before = errorLogRows()
		record(category.Err())
		assert.Equal(t, before+1, errorLogRows(), "%s is on, so it writes exactly one row", category.ID)

		// 串扰断言（行为层）：X 开着的时候，另外 5 类的记录行为仍由它们自己的设置决定。
		require.NoError(t, model.UpdateOption(category.Key, "false"))
		for j, other := range categories {
			if i == j {
				continue
			}
			before = errorLogRows()
			record(other.Err())
			if other.DefaultOn {
				assert.Equal(t, before+1, errorLogRows(), "%s stays on while %s is off", other.ID, category.ID)
			} else {
				assert.Equal(t, before, errorLogRows(), "%s stays off while %s is off", other.ID, category.ID)
			}
		}
		// 回到「没设过」，让下一轮从这个类别的真实默认态开始。
		require.NoError(t, model.UpdateOption(category.Key, "null"))
	}
}

// TestBillingSessionKeepsItsErrorFieldsAndCarriesTheLogCategory 锁住计费门禁的硬约束：
// 那 10 处调用点只换了「日志归类」这一个 option，错误码 / 状态码 / 消息文本一字未变。
func TestBillingSessionKeepsItsErrorFieldsAndCarriesTheLogCategory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	errorLogTestFixture(t)

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	// wallet_only 直接走 tryWallet，用户没有额度时就是 `:396` 那个调用点。
	relayInfo := &relaycommon.RelayInfo{UserId: 999999, UserSetting: kitdto.UserSetting{BillingPreference: "wallet_only"}}
	_, apiErr := NewBillingSession(c, relayInfo, 100)

	require.NotNil(t, apiErr)
	assert.Equal(t, types.ErrorCodeInsufficientUserQuota, apiErr.GetErrorCode())
	assert.Equal(t, http.StatusForbidden, apiErr.StatusCode)
	wantMessage := "用户额度不足, 剩余额度: " + logger.FormatQuota(0)
	assert.Equal(t, wantMessage, apiErr.Error())
	assert.Equal(t, types.ErrorLogCategory(operation_setting.ErrorLogCategoryInsufficientWalletQuota), types.GetErrorLogCategory(apiErr))
	// 归类标签不参与错误文本与对外的 OpenAI 错误体：对外表现与改动前一致。
	assert.NotContains(t, apiErr.Error(), operation_setting.ErrorLogCategoryInsufficientWalletQuota)
	assert.Equal(t, types.OpenAIError{
		Message: wantMessage,
		Type:    string(types.ErrorTypeNewAPIError),
		Code:    types.ErrorCodeInsufficientUserQuota,
	}, apiErr.ToOpenAIError())
}
