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

import { GATE_LABEL_KEYS, type ErrorLogEntry } from '../types'

/**
 * 只读的日志映射表。条目全部来自后端下发的 `ErrorLogMap.entries`。
 *
 * 这里刻意不渲染任何可提交控件：那些判定是代码里硬编码的，本版本不提供逐类切换，
 * 任何开关或勾选框都会让管理员误以为可以逐类调整。
 */
export function ErrorLogRulesTable(props: { entries: ErrorLogEntry[] }) {
  const { t } = useTranslation()

  const columns: StaticDataTableColumn<ErrorLogEntry>[] = [
    {
      id: 'title',
      header: t('Category'),
      cell: (entry) => (
        <div className='min-w-0 space-y-1'>
          <div className='font-medium'>{t(entry.titleKey)}</div>
          {entry.recorded ? null : (
            <p className='text-muted-foreground text-xs'>
              {t(entry.reasonKey)}
            </p>
          )}
        </div>
      ),
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
