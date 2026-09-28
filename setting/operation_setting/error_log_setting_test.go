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

func TestErrorLogEntriesOnlyExposeTheGlobalSwitchOnRecordedRows(t *testing.T) {
	entries := ErrorLogEntries()
	require.NotEmpty(t, entries)

	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		assert.NotEmpty(t, entry.ID, "every entry needs a stable id")
		assert.NotEmpty(t, entry.TitleKey, "every entry needs a renderable title")
		assert.NotEmpty(t, entry.Gates, "every entry must name the mechanism that decides it "+entry.ID)
		assert.NotContains(t, seen, entry.ID, "duplicate entry id "+entry.ID)
		seen[entry.ID] = struct{}{}

		if entry.Recorded {
			assert.Contains(t, entry.Gates, ErrorLogGateGlobalSwitch,
				"recorded rows are the ones the switch controls: "+entry.ID)
			assert.Empty(t, entry.ReasonKey, "recorded rows have nothing to explain away: "+entry.ID)
			continue
		}
		assert.NotContains(t, entry.Gates, ErrorLogGateGlobalSwitch,
			"unrecorded rows must never look switchable: "+entry.ID)
		assert.NotEmpty(t, entry.ReasonKey, "unrecorded rows must explain why: "+entry.ID)
		assert.NotEmpty(t, entry.Source, "unrecorded rows must point at the code that decides them: "+entry.ID)
	}
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
