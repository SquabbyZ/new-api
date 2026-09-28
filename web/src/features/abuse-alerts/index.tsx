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
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/error-state'
import { LoadingState } from '@/components/loading-state'
import { SettingsCard } from '@/features/system-settings/components/settings-card'
import { SettingsSwitchField } from '@/features/system-settings/components/settings-form-layout'
import { useSystemOptions } from '@/features/system-settings/hooks/use-system-options'
import { useUpdateOption } from '@/features/system-settings/hooks/use-update-option'
import { toIntlLocale } from '@/i18n/languages'

import { getAbuseAlerts } from './api'
import { AbuseFindingsTable } from './components/abuse-findings-table'
import { AbuseRulesTable } from './components/abuse-rules-table'
import { parseAbuseAlertMap } from './lib/parse-abuse-alert-map'

/** 时间戳按界面语言格式化，并吞掉 0 / 非法值 —— 它们表示「还没有」。 */
function formatAbuseAlertTime(
  timestamp: number,
  locale: string | undefined
): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return '-'
  return new Intl.DateTimeFormat(locale, {
    dateStyle: 'short',
    timeStyle: 'short',
  }).format(new Date(timestamp * 1000))
}

/**
 * 「系统设置 → 运维 → Abuse Alerts」这一个 section 的内容主体。
 *
 * 三段式：① 总开关 + 基线就绪提示条；② 规则表；③ 发现列表 + 刷新。
 * 标题与外层布局由 `SettingsPage` 按 section 注册表统一提供。
 *
 * 规则状态与阈值**只能**取伪键 `AbuseAlertMap`，不能读裸键 `abuse_alert_setting.*`
 * （未显式保存时那些键的值是字面量字符串 "null"，按缺省类型解析会得到 false/0）。
 */
export function AbuseAlertsSection() {
  const { t, i18n } = useTranslation()
  const locale = toIntlLocale(i18n.resolvedLanguage || i18n.language)
  const optionsQuery = useSystemOptions()
  const updateOption = useUpdateOption()

  const abuseAlertMap = parseAbuseAlertMap(optionsQuery.data?.data)

  // 伪键到达后这条查询**自动执行**，不等运维者点「刷新」。
  //
  // 代码此前与注释相反（注释写「首次渲染不自动跑」），本轮把**代码**定为正确的一侧：
  // 本页是发现的完整记录（PRD §R6），打开却什么都不扫，运维者只会看到一个永远转不完
  // 的加载态 —— 而「已经看过、没有异常」与「什么都没看」在页面上长得一样，正是这个
  // 页面反复要挡的那件事。
  //
  // 代价有界，且都是既有设计已经限住的：一次扫描最多 3 条日志库查询；查询异步执行、
  // 不阻塞渲染；staleTime 30s + refetchOnWindowFocus:false ⇒ 一次打开最多扫一次；
  // 只读（不写状态、不发通知）。
  const reportQuery = useQuery({
    queryKey: ['abuse-alerts', 'report'],
    queryFn: () => getAbuseAlerts(0),
    enabled: abuseAlertMap !== null,
    staleTime: 30 * 1000,
    refetchOnWindowFocus: false,
  })

  if (optionsQuery.isPending) {
    return <LoadingState />
  }

  // 后端没有下发 AbuseAlertMap（旧后端）或内容不合法时不渲染开关 ——
  // 展示一个可能不真实的规则状态比展示错误更难排查。
  if (optionsQuery.isError || abuseAlertMap === null) {
    return (
      <ErrorState
        title={t('Failed to load the abuse alert rules')}
        description={t(
          'The backend did not return the abuse alert map. Update the backend and try again.'
        )}
        onRetry={() => {
          void optionsQuery.refetch()
        }}
      />
    )
  }

  const report = reportQuery.data

  return (
    <div className='space-y-4'>
      <SettingsCard
        title={t('Abuse alert switch')}
        description={t(
          'A background scan compares each token against its own recent baseline. Findings are recorded on this page; only high-severity findings are pushed.'
        )}
      >
        <SettingsSwitchField
          controlId='abuse-alert-enabled'
          checked={abuseAlertMap.enabled}
          disabled={updateOption.isPending}
          onCheckedChange={(checked) => {
            updateOption.mutate({ key: 'abuse_alert_setting.enabled', value: checked })
          }}
          label={t('Scan for abnormal usage')}
          description={t(
            'When off, the scheduled scan creates no run at all, so this page can no longer tell you whether it ever ran.'
          )}
        />
        <SettingsSwitchField
          controlId='abuse-alert-notify-enabled'
          checked={abuseAlertMap.notifyEnabled}
          disabled={updateOption.isPending}
          onCheckedChange={(checked) => {
            updateOption.mutate({
              key: 'abuse_alert_setting.notify_enabled',
              value: checked,
            })
          }}
          label={t('Push high-severity findings to the root user')}
          description={t(
            'One summary notification per scan. Medium-severity findings are only recorded here.'
          )}
        />
      </SettingsCard>

      {/* 基线就绪是第一等状态：没有它，「还没开始判定」与「判定过、没有异常」
          在页面上长得一模一样，而这两件事的处置完全不同。 */}
      {abuseAlertMap.baselineReady ? null : (
        <Alert>
          <AlertDescription>
            {abuseAlertMap.baselineReadyAt > 0
              ? t(
                  'The log database does not yet hold enough history for a baseline. Judging starts around {{time}}.',
                  {
                    time: formatAbuseAlertTime(
                      abuseAlertMap.baselineReadyAt,
                      locale
                    ),
                  }
                )
              : t(
                  'The log database does not yet hold enough history for a baseline, so nothing can be judged yet. This is not a statement that everything is fine.'
                )}
          </AlertDescription>
        </Alert>
      )}

      {abuseAlertMap.logRetention.retentionDays > 0 &&
      !abuseAlertMap.logRetention.sufficient ? (
        <Alert>
          <AlertDescription>
            {t(
              'The log retention is {{days}} day(s), which is shorter than what detection needs ({{minutes}} minutes of baseline, {{hours}} hours of model lookback). Baseline and new-model rules will keep reporting "insufficient baseline" until you raise it.',
              {
                days: abuseAlertMap.logRetention.retentionDays,
                minutes: abuseAlertMap.logRetention.requiredMinutes,
                hours: abuseAlertMap.logRetention.requiredHours,
              }
            )}
          </AlertDescription>
        </Alert>
      ) : null}

      <SettingsCard
        title={t('Which signals are watched')}
        description={t(
          'Four signals can be computed from the log database. Two cannot, and each of those rows says why instead of offering a switch that would never fire.'
        )}
      >
        <AbuseRulesTable
          rules={abuseAlertMap.rules}
          pendingOptionKey={
            updateOption.isPending ? updateOption.variables?.key : undefined
          }
          onRuleChange={(optionKey, enabled) => {
            // 与总开关同一条写入路径：键由后端下发，值传布尔。
            updateOption.mutate({ key: optionKey, value: enabled })
          }}
        />
      </SettingsCard>

      <SettingsCard
        title={t('Findings')}
        description={t(
          'Recorded findings. Nothing here changes a token, a user or a group — this release only shows you what it sees.'
        )}
      >
        <div className='space-y-3'>
          <div className='flex flex-wrap items-center justify-between gap-2'>
            <p className='text-muted-foreground text-xs'>
              {report ? (
                <>
                  {/* PRD 边界 case：`hours` 超上限时后端钳制后正常返回，页面必须显示
                      **实际使用**的窗口 —— 否则运维者以为看的是 99999 小时，
                      实际只扫了 24 小时。 */}
                  {t('Scanning the last {{hours}} hours in {{minutes}}-minute windows', {
                    hours: report.hours,
                    minutes: report.windowMinutes,
                  })}
                  {' · '}
                  {t('Last scan on this page: {{time}}', {
                    time: formatAbuseAlertTime(report.generatedAt, locale),
                  })}
                  {report.lastScheduledScanAt > 0
                    ? ` · ${t('Last scheduled scan: {{time}}', {
                        time: formatAbuseAlertTime(
                          report.lastScheduledScanAt,
                          locale
                        ),
                      })}`
                    : ` · ${t('The scheduled scan has not run yet.')}`}
                </>
              ) : null}
            </p>
            <Button
              variant='outline'
              size='sm'
              disabled={reportQuery.isFetching}
              onClick={() => {
                void reportQuery.refetch()
              }}
            >
              {t('Refresh findings')}
            </Button>
          </div>

          {/* 没有通知的三种情况必须可区分，否则「一切正常」与「没人告诉你」一样。 */}
          {report && !report.notifyChannelReady ? (
            <Alert>
              <AlertDescription>
                {t(
                  'The root user has no notification channel configured, so pushed alerts would be dropped silently. This page is the complete record.'
                )}
              </AlertDescription>
            </Alert>
          ) : null}

          {reportQuery.isPending ? <LoadingState /> : null}

          {reportQuery.isError ? (
            <ErrorState
              title={t('Failed to scan the log database')}
              description={t(
                'The backend could not read the log database. No finding can be shown, and this is not a statement that everything is fine.'
              )}
              onRetry={() => {
                void reportQuery.refetch()
              }}
            />
          ) : null}

          {report ? (
            <AbuseFindingsTable
              findings={report.findings}
              skipped={report.skipped}
              excludedSaturatedRows={report.excludedSaturatedRows}
            />
          ) : null}
        </div>
      </SettingsCard>
    </div>
  )
}
