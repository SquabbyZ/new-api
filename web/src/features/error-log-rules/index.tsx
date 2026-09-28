/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { useTranslation } from 'react-i18next'

import { ErrorState } from '@/components/error-state'
import { LoadingState } from '@/components/loading-state'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { SettingsCard } from '@/features/system-settings/components/settings-card'
import { SettingsSwitchField } from '@/features/system-settings/components/settings-form-layout'
import { useSystemOptions } from '@/features/system-settings/hooks/use-system-options'
import { useUpdateOption } from '@/features/system-settings/hooks/use-update-option'

import { ErrorLogRulesTable } from './components/error-log-rules-table'
import { parseErrorLogMap } from './lib/parse-error-log-map'

/**
 * 「系统设置 → 运维 → Error Log Rules」这一个 section 的内容主体。
 *
 * 标题与外层布局由 `SettingsPage`（`system-settings/components/settings-page.tsx`）
 * 按 section 注册表统一提供，这里只渲染内容 —— 与 `LogSettingsSection` 等既有 section
 * 的分工一致，不再自带页面骨架。
 */
export function ErrorLogRulesSection() {
  const { t } = useTranslation()
  const optionsQuery = useSystemOptions()
  const updateOption = useUpdateOption()

  const errorLogMap = parseErrorLogMap(optionsQuery.data?.data)

  if (optionsQuery.isPending) {
    return <LoadingState />
  }

  // 后端没有下发 ErrorLogMap（旧后端）或内容不合法时不渲染开关 ——
  // 展示一个可能不真实的生效值比展示错误更难排查。
  if (optionsQuery.isError || errorLogMap === null) {
    return (
      <ErrorState
        title={t('Failed to load the error log rules')}
        description={t(
          'The backend did not return the error log map. Update the backend and try again.'
        )}
        onRetry={() => {
          void optionsQuery.refetch()
        }}
      />
    )
  }

  return (
    <div className='space-y-4'>
      <SettingsCard
        title={t('Error log switch')}
        description={t(
          'When enabled, failures that reach the record point are written to the error log.'
        )}
      >
        <SettingsSwitchField
          controlId='error-log-enabled'
          checked={errorLogMap.enabled}
          disabled={updateOption.isPending}
          onCheckedChange={(checked) => {
            updateOption.mutate({
              key: 'error_log_setting.enabled',
              value: checked,
            })
          }}
          label={t('Record error logs')}
          description={
            errorLogMap.enabledSource === 'setting'
              ? t('Currently decided by the administrator setting.')
              : t('Currently following the {{envVar}} environment variable.', {
                  envVar: errorLogMap.envVar,
                })
          }
        />
      </SettingsCard>

      <SettingsCard
        title={t('Which failures leave an error log')}
        description={t(
          'Five categories can be toggled individually below. The other six cannot, and each row says why.'
        )}
      >
        {errorLogMap.enabled ? null : (
          <Alert>
            <AlertDescription>
              {t(
                'The error log switch is off, so nothing is written no matter how the category switches below are set. Your category settings are kept.'
              )}
            </AlertDescription>
          </Alert>
        )}
        <ErrorLogRulesTable
          entries={errorLogMap.entries}
          pendingOptionKey={
            updateOption.isPending ? updateOption.variables?.key : undefined
          }
          onCategoryChange={(optionKey, enabled) => {
            // 与总开关同一条写入路径：键由后端下发，值传布尔。
            updateOption.mutate({ key: optionKey, value: enabled })
          }}
        />
      </SettingsCard>

      <SettingsCard title={t('Why some failures are not recorded')}>
        <div className='text-muted-foreground space-y-3 text-sm'>
          <p>
            {t(
              'Insufficient quota, insufficient subscription quota and pre-consume failures are normal business outcomes, not faults, so they are collected off by default. Turning one on is an informed choice: it may flood the error log with routine failures.'
            )}
          </p>
          <p>
            {t(
              'Some failures return before the record point is reached — when no channel is available in the group, when tiered billing preparation fails, or when the request body cannot be read. No switch can make them record; doing so would mean moving where recording happens, which is a separate change. The same applies to local task validation errors, and recording those would also disable the upstream channel.'
            )}
          </p>
          <p>
            {t(
              'Failures that carry no specific category are counted as "Upstream channel error (after a channel is selected)". Turning that category off therefore also stops the records for relay retries and channel tests.'
            )}
          </p>
        </div>
      </SettingsCard>
    </div>
  )
}
