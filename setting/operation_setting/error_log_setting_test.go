package operation_setting

import (
	"slices"
	"testing"

	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// useErrorLogSetting 接管开关状态，并在用例结束时恢复，避免污染其它用例。
func useErrorLogSetting(t *testing.T, savedEnabled *bool, envFallback bool) {
	t.Helper()
	previousEnabled, previousFallback := errorLogSetting.Enabled, constant.ErrorLogEnabled
	errorLogSetting.Enabled, constant.ErrorLogEnabled = savedEnabled, envFallback
	t.Cleanup(func() {
		errorLogSetting.Enabled, constant.ErrorLogEnabled = previousEnabled, previousFallback
	})
}

func TestIsErrorLogEnabledResolvesAdminSettingAheadOfEnv(t *testing.T) {
	enabled, disabled := true, false

	for _, test := range []struct {
		name        string
		saved       *bool
		envFallback bool
		want        bool
		wantSource  string
	}{
		{
			name: "fresh deployment keeps the default off",
			want: false, wantSource: ErrorLogEnabledSourceEnv,
		},
		{
			name:        "existing deployment with env true is not silently turned off",
			envFallback: true, want: true, wantSource: ErrorLogEnabledSourceEnv,
		},
		{
			name:  "administrator turned the switch off over env true",
			saved: &disabled, envFallback: true, want: false, wantSource: ErrorLogEnabledSourceSetting,
		},
		{
			name:  "administrator turned the switch back on over env true",
			saved: &enabled, envFallback: true, want: true, wantSource: ErrorLogEnabledSourceSetting,
		},
		{
			name:  "administrator turned the switch on without env",
			saved: &enabled, want: true, wantSource: ErrorLogEnabledSourceSetting,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			useErrorLogSetting(t, test.saved, test.envFallback)

			assert.Equal(t, test.want, IsErrorLogEnabled())
			assert.Equal(t, test.wantSource, ErrorLogEnabledSource())
		})
	}
}

func TestErrorLogSettingStaysDistinguishableFromAnExplicitFalse(t *testing.T) {
	useErrorLogSetting(t, nil, false)

	values, err := config.ConfigToMap(&errorLogSetting)
	require.NoError(t, err)
	assert.Equal(t, "null", values["enabled"],
		"a never-saved switch must serialize as null, not as false")

	require.NoError(t, config.UpdateConfigFromMap(&errorLogSetting, map[string]string{"enabled": "false"}))
	require.NotNil(t, errorLogSetting.Enabled)
	assert.Equal(t, ErrorLogEnabledSourceSetting, ErrorLogEnabledSource(),
		"an explicit false is a saved setting and must override env")
	assert.False(t, IsErrorLogEnabled())

	require.NoError(t, config.UpdateConfigFromMap(&errorLogSetting, map[string]string{"enabled": "null"}))
	assert.Nil(t, errorLogSetting.Enabled, "null must restore the env fallback")
	assert.Equal(t, ErrorLogEnabledSourceEnv, ErrorLogEnabledSource())
}

// pinErrorLogSetting 接管整个设置结构体（含 6 个逐类开关），用例结束时整体恢复。
func pinErrorLogSetting(t *testing.T) {
	t.Helper()
	previous := errorLogSetting
	t.Cleanup(func() { errorLogSetting = previous })
}

// TestIsErrorLogCategoryEnabledKeepsTheHistoricalDefaults 是「升级不改变记录行为」的判定层口径：
// 从未被保存过的开关必须回落到本机制引入之前的行为，而不是回落到 false。
func TestIsErrorLogCategoryEnabledKeepsTheHistoricalDefaults(t *testing.T) {
	useErrorLogSetting(t, nil, false)
	pinErrorLogSetting(t)
	errorLogSetting.RecordRelayUpstreamError = nil
	errorLogSetting.RecordTaskUpstreamError = nil
	errorLogSetting.RecordInsufficientWalletQuota = nil
	errorLogSetting.RecordInsufficientSubscriptionQuota = nil
	errorLogSetting.RecordTokenPreconsumeFailed = nil

	for _, test := range []struct {
		category string
		want     bool
	}{
		{ErrorLogCategoryUpstream, true},
		{ErrorLogCategoryTaskUpstream, true},
		{ErrorLogCategoryInsufficientWalletQuota, false},
		{ErrorLogCategoryInsufficientSubscriptionQuota, false},
		{ErrorLogCategoryTokenPreconsumeFailed, false},
	} {
		assert.Equal(t, test.want, IsErrorLogCategoryEnabled(test.category),
			"a never-saved switch must keep the pre-slice behavior: "+test.category)
	}
	assert.True(t, IsErrorLogCategoryEnabled(""),
		"an error without a declared category is not a business category")
	assert.True(t, IsErrorLogCategoryEnabled("future_category"),
		"unknown categories fail open, so a new label never silently drops logs")
}

// TestErrorLogCategorySwitchesRoundTripThroughTheConfigMap 锁住存储形态：
// 6 个扁平 *bool 键各自独立，未设过序列化成 "null"（不是 "false"）。
func TestErrorLogCategorySwitchesRoundTripThroughTheConfigMap(t *testing.T) {
	useErrorLogSetting(t, nil, false)
	pinErrorLogSetting(t)

	values, err := config.ConfigToMap(&errorLogSetting)
	require.NoError(t, err)
	for _, key := range []string{
		"record_relay_upstream_error",
		"record_task_upstream_error",
		"record_insufficient_wallet_quota",
		"record_insufficient_subscription_quota",
		"record_token_preconsume_failed",
	} {
		assert.Equal(t, "null", values[key],
			"a never-saved category switch must serialize as null, not as false")
	}

	require.NoError(t, config.UpdateConfigFromMap(&errorLogSetting, map[string]string{
		"record_relay_upstream_error":      "false",
		"record_insufficient_wallet_quota": "true",
	}))
	assert.False(t, IsErrorLogCategoryEnabled(ErrorLogCategoryUpstream))
	assert.True(t, IsErrorLogCategoryEnabled(ErrorLogCategoryInsufficientWalletQuota))
	assert.True(t, IsErrorLogCategoryEnabled(ErrorLogCategoryTaskUpstream),
		"a key the map does not mention must stay untouched")

	require.NoError(t, config.UpdateConfigFromMap(&errorLogSetting, map[string]string{
		"record_relay_upstream_error": "null",
	}))
	assert.True(t, IsErrorLogCategoryEnabled(ErrorLogCategoryUpstream),
		"null restores the historical default instead of turning the category off")
}

// TestErrorLogEntriesFollowTheCategorySwitches 是映射表在新机制下的契约：
// 可切换的类别带自己的开关与 category_switch 门，不可切换的类别没有开关、
// 永远不记录，并且必须给出原因和代码位置。
func TestErrorLogEntriesFollowTheCategorySwitches(t *testing.T) {
	on, off := true, false
	useErrorLogSetting(t, &on, false)
	pinErrorLogSetting(t)

	errorLogSetting.RecordRelayUpstreamError = &on
	errorLogSetting.RecordTaskUpstreamError = &on
	errorLogSetting.RecordInsufficientWalletQuota = &on
	errorLogSetting.RecordInsufficientSubscriptionQuota = &on
	errorLogSetting.RecordTokenPreconsumeFailed = &on

	entries := ErrorLogEntries()
	require.Len(t, entries, 11, "the mapping table keeps all eleven rows after this slice")

	seen := make(map[string]struct{}, len(entries))
	switchable := 0
	for _, entry := range entries {
		assert.NotEmpty(t, entry.ID, "every entry needs a stable id")
		assert.NotEmpty(t, entry.TitleKey, "every entry needs a renderable title")
		assert.NotEmpty(t, entry.Gates, "every entry must name the mechanism that decides it "+entry.ID)
		assert.NotEmpty(t, entry.Source, "every entry must point at the code that decides it: "+entry.ID)
		assert.NotContains(t, seen, entry.ID, "duplicate entry id "+entry.ID)
		seen[entry.ID] = struct{}{}

		if entry.CategorySwitch == nil {
			assert.NotContains(t, entry.Gates, ErrorLogGateCategorySwitch,
				"a row without a switch must not claim a category gate: "+entry.ID)
			assert.False(t, entry.Recorded,
				"a row whose failure never reaches the record point is never recorded: "+entry.ID)
			assert.NotEmpty(t, entry.ReasonKey, "unswitchable rows must explain why: "+entry.ID)
			continue
		}

		switchable++
		assert.Contains(t, entry.Gates, ErrorLogGateGlobalSwitch,
			"the global switch is the outer gate: "+entry.ID)
		assert.Contains(t, entry.Gates, ErrorLogGateCategorySwitch,
			"switchable rows are decided by their own switch: "+entry.ID)
		assert.Equal(t, entry.ID, entry.CategorySwitch.Category)
		assert.Equal(t, "error_log_setting.record_"+entry.ID, entry.CategorySwitch.OptionKey)
		assert.Equal(t, ErrorLogCategorySourceSetting, entry.CategorySwitch.Source)
		assert.True(t, entry.CategorySwitch.Enabled, "every category was explicitly turned on: "+entry.ID)
		assert.True(t, entry.Recorded, "every category was explicitly turned on: "+entry.ID)
	}
	assert.Equal(t, 5, switchable,
		"exactly the categories with a site that reaches the record point are switchable")

	// 只关掉一个类别 ⇒ 只有它那一行变化（判定层的「不串扰」证据）。
	errorLogSetting.RecordInsufficientWalletQuota = &off
	for _, entry := range ErrorLogEntries() {
		if entry.CategorySwitch == nil {
			continue
		}
		if entry.ID == ErrorLogCategoryInsufficientWalletQuota {
			assert.False(t, entry.CategorySwitch.Enabled)
			assert.False(t, entry.Recorded)
			continue
		}
		assert.True(t, entry.Recorded,
			"turning one category off must not affect the others: "+entry.ID)
	}

	// 全局关是外层硬闸：合成值全为 false，但逐类开关仍显示各自的设置值 ——
	// 否则管理员会看不到自己保存过什么，点一下下次刷新又变回关。
	useErrorLogSetting(t, &off, false)
	for _, entry := range ErrorLogEntries() {
		assert.False(t, entry.Recorded, "the global switch is the outer hard gate: "+entry.ID)
		if entry.CategorySwitch != nil {
			assert.Equal(t, IsErrorLogCategoryEnabled(entry.CategorySwitch.Category), entry.CategorySwitch.Enabled,
				"the category setting stays visible while the global switch is off: "+entry.ID)
		}
	}
	assert.True(t, IsErrorLogCategoryEnabled(ErrorLogCategoryTaskUpstream),
		"a category the administrator left on stays on while the global switch is off")
}

func TestBuildErrorLogMapPayloadReportsTheEffectiveSwitch(t *testing.T) {
	enabled := true
	useErrorLogSetting(t, &enabled, false)

	payload := BuildErrorLogMapPayload()
	assert.True(t, payload.Enabled)
	assert.Equal(t, ErrorLogEnabledSourceSetting, payload.EnabledSource)
	assert.Equal(t, "ERROR_LOG_ENABLED", payload.EnvVar)
	assert.Equal(t, ErrorLogEntries(), payload.Entries)
	assert.True(t, slices.ContainsFunc(payload.Entries, func(entry ErrorLogEntry) bool {
		return entry.Recorded
	}), "the map must explain the rows the switch actually controls")
}
