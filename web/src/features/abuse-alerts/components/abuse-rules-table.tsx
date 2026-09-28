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

import {
  StaticDataTable,
  type StaticDataTableColumn,
} from '@/components/data-table'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'

import type { AbuseAlertRule, AbuseAlertRuleThresholds } from '../types'

/**
 * 规则表。全部条目来自后端下发的 `AbuseAlertMap.rules`。
 *
 * 只有 `switch` 非 null 的行渲染开关 —— 其余的行是**结构上不可计算**的
 * （它们依赖当前不被记录的数据），渲染一个灰掉的开关会被读成「以后能用」。
 */
export function AbuseRulesTable(props: {
  rules: AbuseAlertRule[]
  pendingOptionKey?: string
  onRuleChange: (optionKey: string, enabled: boolean) => void
}) {
  const { t } = useTranslation()

  const columns: StaticDataTableColumn<AbuseAlertRule>[] = [
    {
      id: 'title',
      header: t('Rule'),
      cell: (rule) => (
        <div className='min-w-0 space-y-1'>
          <div className='font-medium'>{t(rule.titleKey)}</div>
          {rule.reasonKey ? (
            <p className='text-muted-foreground text-xs'>{t(rule.reasonKey)}</p>
          ) : null}
        </div>
      ),
    },
    {
      id: 'ruleSwitch',
      header: t('Enabled'),
      cellClassName: 'align-top',
      cell: (rule) => {
        const ruleSwitch = rule.switch
        if (ruleSwitch === null) return null

        return (
          <div className='flex min-w-0 flex-col items-start gap-1'>
            <Switch
              aria-label={t('Enable this abuse alert rule')}
              checked={ruleSwitch.enabled}
              disabled={props.pendingOptionKey === ruleSwitch.optionKey}
              onCheckedChange={(checked) => {
                props.onRuleChange(ruleSwitch.optionKey, checked)
              }}
            />
            <span className='text-muted-foreground text-xs'>
              {ruleSwitch.source === 'setting'
                ? t('Saved by the administrator')
                : t('Default')}
            </span>
          </div>
        )
      },
    },
    {
      id: 'severity',
      header: t('Severity'),
      cell: (rule) =>
        rule.severity ? (
          <Badge variant={rule.severity === 'high' ? 'destructive' : 'secondary'}>
            {rule.severity === 'high' ? t('High') : t('Medium')}
          </Badge>
        ) : null,
    },
    {
      id: 'dependsOn',
      header: t('Depends on'),
      cellClassName: 'align-top',
      cell: (rule) => (
        <ul className='text-muted-foreground list-inside list-disc text-xs'>
          {rule.dependsOn.map((column) => (
            <li key={column}>
              <code>{column}</code>
            </li>
          ))}
        </ul>
      ),
    },
    {
      id: 'thresholds',
      header: t('Thresholds'),
      cellClassName: 'align-top',
      cell: (rule) => (
        <span className='text-muted-foreground text-xs'>
          {formatThresholds(rule.thresholds, t)}
        </span>
      ),
    },
    {
      id: 'source',
      header: t('Code location'),
      cellClassName: 'align-top',
      cell: (rule) => (
        <code className='text-muted-foreground text-xs'>{rule.source}</code>
      ),
    },
  ]

  return (
    <StaticDataTable
      columns={columns}
      data={props.rules}
      getRowKey={(rule) => rule.id}
      emptyContent={t('No rules')}
    />
  )
}

/**
 * 阈值按「阈值是判定本体」的口径展示：没有阈值可展示的规则（当前只有固定的
 * 不可计算项）显示占位符，而不是空白 —— 空白会被读成「没有阈值」。
 */
function formatThresholds(
  thresholds: AbuseAlertRuleThresholds | null,
  t: (key: string, options?: Record<string, unknown>) => string
): string {
  if (thresholds === null) return t('Not configurable')

  const parts: string[] = []
  if (thresholds.ratio !== undefined) {
    parts.push(t('ratio {{value}}x', { value: thresholds.ratio }))
  }
  if (thresholds.minConsumeQuota !== undefined) {
    parts.push(t('quota floor {{value}}', { value: thresholds.minConsumeQuota }))
  }
  if (thresholds.minRequests !== undefined) {
    parts.push(t('request floor {{value}}', { value: thresholds.minRequests }))
  }
  if (thresholds.minErrors !== undefined) {
    parts.push(t('error floor {{value}}', { value: thresholds.minErrors }))
  }
  if (thresholds.lookbackHours !== undefined) {
    parts.push(t('lookback {{value}}h', { value: thresholds.lookbackHours }))
  }
  return parts.length > 0 ? parts.join(' · ') : t('Not configurable')
}
