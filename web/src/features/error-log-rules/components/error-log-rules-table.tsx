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

import { GATE_LABEL_KEYS, type ErrorLogEntry } from '../types'

/**
 * 日志映射表。条目全部来自后端下发的 `ErrorLogMap.entries`。
 *
 * 只有 `categorySwitch` 非 null 的行渲染开关 —— 其余的行是结构上不可切换的
 * （它们的失败根本没到达记录点），渲染一个灰掉的开关会被读成「以后能用」。
 */
export function ErrorLogRulesTable(props: {
  entries: ErrorLogEntry[]
  pendingOptionKey?: string
  onCategoryChange: (optionKey: string, enabled: boolean) => void
}) {
  const { t } = useTranslation()

  const columns: StaticDataTableColumn<ErrorLogEntry>[] = [
    {
      id: 'title',
      header: t('Category'),
      cell: (entry) => (
        <div className='min-w-0 space-y-1'>
          <div className='font-medium'>{t(entry.titleKey)}</div>
          {entry.reasonKey ? (
            <p className='text-muted-foreground text-xs'>
              {t(entry.reasonKey)}
            </p>
          ) : null}
        </div>
      ),
    },
    {
      id: 'categorySwitch',
      header: t('Collect'),
      cellClassName: 'align-top',
      cell: (entry) => {
        const categorySwitch = entry.categorySwitch
        if (categorySwitch === null) return null

        return (
          <div className='flex min-w-0 flex-col items-start gap-1'>
            <Switch
              aria-label={t('Record error logs for this category')}
              checked={categorySwitch.enabled}
              disabled={props.pendingOptionKey === categorySwitch.optionKey}
              onCheckedChange={(checked) => {
                props.onCategoryChange(categorySwitch.optionKey, checked)
              }}
            />
            <span className='text-muted-foreground text-xs'>
              {categorySwitch.source === 'setting'
                ? t('Saved by the administrator')
                : t('Default')}
            </span>
          </div>
        )
      },
    },
    {
      id: 'recorded',
      header: t('Recorded'),
      cell: (entry) => (
        <Badge variant={entry.recorded ? 'default' : 'secondary'}>
          {entry.recorded ? t('Yes') : t('No')}
        </Badge>
      ),
    },
    {
      id: 'gates',
      header: t('Decided by'),
      cell: (entry) => (
        <ul className='text-muted-foreground list-inside list-disc text-xs'>
          {entry.gates.map((gate) => (
            <li key={gate}>{t(GATE_LABEL_KEYS[gate])}</li>
          ))}
        </ul>
      ),
    },
    {
      id: 'source',
      header: t('Code location'),
      cellClassName: 'align-top',
      cell: (entry) => (
        <code className='text-muted-foreground text-xs'>{entry.source}</code>
      ),
    },
  ]

  return (
    <StaticDataTable
      columns={columns}
      data={props.entries}
      getRowKey={(entry) => entry.id}
      emptyContent={t('No entries')}
    />
  )
}
