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

import { SectionPageLayout } from '@/components/layout'
import { Badge } from '@/components/ui/badge'
import { ErrorState } from '@/components/error-state'
import { LoadingState } from '@/components/loading-state'
import { SettingsCard } from '@/features/system-settings/components/settings-card'
import { SettingsSwitchField } from '@/features/system-settings/components/settings-form-layout'
import { useSystemOptions } from '@/features/system-settings/hooks/use-system-options'
import { useUpdateOption } from '@/features/system-settings/hooks/use-update-option'

import { ErrorLogRulesTable } from './components/error-log-rules-table'
import { parseErrorLogMap } from './lib/parse-error-log-map'

export function ErrorLogRules() {
  const { t } = useTranslation()
  const optionsQuery = useSystemOptions()
  const updateOption = useUpdateOption()

  const errorLogMap = parseErrorLogMap(optionsQuery.data?.data)

  const title = (
    <span className='inline-flex min-w-0 items-center gap-2'>
      <span className='truncate'>{t('Error Log Rules')}</span>
      <Badge variant='outline' className='shrink-0'>
        Root
      </Badge>
    </span>
  )

  if (optionsQuery.isPending) {
    return (
      <SectionPageLayout>
        <SectionPageLayout.Title>{title}</SectionPageLayout.Title>
        <SectionPageLayout.Content>
          <LoadingState />
        </SectionPageLayout.Content>
      </SectionPageLayout>
    )
  }

  // 后端没有下发 ErrorLogMap（旧后端）或内容不合法时不渲染开关 ——
  // 展示一个可能不真实的生效值比展示错误更难排查。
  if (optionsQuery.isError || errorLogMap === null) {
    return (
      <SectionPageLayout>
        <SectionPageLayout.Title>{title}</SectionPageLayout.Title>
        <SectionPageLayout.Content>
          <ErrorState
            title={t('Failed to load the error log rules')}
            description={t(
              'The backend did not return the error log map. Update the backend and try again.'
            )}
            onRetry={() => {
              void optionsQuery.refetch()
            }}
          />
        </SectionPageLayout.Content>
      </SectionPageLayout>
    )
  }

  return (
    <SectionPageLayout>
      <SectionPageLayout.Title>{title}</SectionPageLayout.Title>
      <SectionPageLayout.Content>
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
                  : t(
                      'Currently following the {{envVar}} environment variable.',
                      { envVar: errorLogMap.envVar }
                    )
              }
            />
          </SettingsCard>

          <SettingsCard
            title={t('Which failures leave an error log')}
            description={t(
              'These decisions are hardcoded in the code and cannot be toggled in this version.'
            )}
          >
            <ErrorLogRulesTable entries={errorLogMap.entries} />
          </SettingsCard>

          <SettingsCard title={t('Why some failures are not recorded')}>
            <div className='text-muted-foreground space-y-3 text-sm'>
              <p>
                {t(
                  'Insufficient quota, insufficient subscription quota and pre-consume failures are normal business outcomes, not faults. Recording them would mix "the user ran out of quota" with "the system is broken" and drown the error log.'
                )}
              </p>
              <p>
                {t(
                  'Some failures happen before the record point is reached — for example when no channel is available in the group, when tiered billing preparation fails, or when the request body cannot be read. Those paths never call the error log writer.'
                )}
              </p>
            </div>
          </SettingsCard>
        </div>
      </SectionPageLayout.Content>
    </SectionPageLayout>
  )
}
