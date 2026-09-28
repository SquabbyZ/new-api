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
import { toIntlLocale } from '@/i18n/languages'
import { formatNumber } from '@/lib/format'

import type { AbuseAlertFinding, AbuseAlertSkip } from '../types'

function formatWindow(start: number, locale: string | undefined): string {
  if (!Number.isFinite(start) || start <= 0) return '-'
  return new Intl.DateTimeFormat(locale, {
    dateStyle: 'short',
    timeStyle: 'short',
  }).format(new Date(start * 1000))
}

function formatRatio(value: number): string {
  return Number.isFinite(value) ? value.toFixed(2) : '-'
}

/**
 * 发现列表。`findings` 与 `skipped` 一起展示 —— 只显示发现会让「没有发现」
 * 与「没有检测」看起来一样。
 *
 * `tokenName` / `username` 为空表示令牌已被删除：该行仍然显示并标成已删除，
 * 不隐藏。发现里本来就不含任何凭据。
 */
export function AbuseFindingsTable(props: {
  findings: AbuseAlertFinding[]
  skipped: AbuseAlertSkip[]
  excludedSaturatedRows: number
}) {
  const { t, i18n } = useTranslation()
  const locale = toIntlLocale(i18n.resolvedLanguage || i18n.language)

  const columns: StaticDataTableColumn<AbuseAlertFinding>[] = [
    {
      id: 'rule',
      header: t('Rule'),
      cell: (finding) => (
        <div className='flex min-w-0 flex-col items-start gap-1'>
          <code className='text-xs'>{finding.rule}</code>
          <Badge
            variant={finding.severity === 'high' ? 'destructive' : 'secondary'}
          >
            {finding.severity === 'high' ? t('High') : t('Medium')}
          </Badge>
        </div>
      ),
    },
    {
      id: 'token',
      header: t('Token'),
      cell: (finding) => (
        <div className='min-w-0 space-y-1'>
          <div className='font-medium'>
            {finding.tokenName || t('Deleted token #{{id}}', { id: finding.tokenId })}
          </div>
          <p className='text-muted-foreground text-xs'>
            {finding.username ||
              t('Deleted user #{{id}}', { id: finding.userId })}
          </p>
        </div>
      ),
    },
    {
      id: 'model',
      header: t('Model'),
      cell: (finding) =>
        finding.modelName ? (
          <code className='text-xs'>{finding.modelName}</code>
        ) : (
          <span className='text-muted-foreground text-xs'>{t('Any')}</span>
        ),
    },
    {
      id: 'evidence',
      header: t('Evidence'),
      cell: (finding) => (
        <div className='text-xs'>
          <div>
            {t('current {{value}}', {
              value: formatNumber(finding.evidence.current, locale),
            })}
          </div>
          <div className='text-muted-foreground'>
            {t('baseline median {{value}}', {
              value: formatNumber(finding.evidence.baselineMedian, locale),
            })}
          </div>
          <div className='text-muted-foreground'>
            {t('ratio {{value}}', { value: formatRatio(finding.evidence.ratio) })}
            {' · '}
            {t('Increasing')}
          </div>
        </div>
      ),
    },
    {
      id: 'window',
      header: t('Window'),
      cellClassName: 'align-top',
      cell: (finding) => (
        <span className='text-muted-foreground text-xs'>
          {formatWindow(finding.windowStart, locale)}
        </span>
      ),
    },
  ]

  return (
    <div className='space-y-3'>
      <StaticDataTable
        columns={columns}
        data={props.findings}
        getRowKey={(finding) =>
          `${finding.rule}:${finding.tokenId}:${finding.modelName ?? ''}`
        }
        emptyContent={t('No finding in this window')}
      />
      <div className='text-muted-foreground space-y-1 text-xs'>
        <p>
          {t('Excluded saturated rows: {{value}}', {
            value: formatNumber(props.excludedSaturatedRows, locale),
          })}
        </p>
        {props.skipped.length > 0 ? (
          <div>
            <p>
              {t(
                'Tokens that were scanned but could not be judged (baseline not ready): {{value}}',
                { value: formatNumber(props.skipped.length, locale) }
              )}
            </p>
            <p>
              {props.skipped
                .map((skip) => `${skip.rule} #${skip.tokenId} (${skip.reason})`)
                .join(' · ')}
            </p>
          </div>
        ) : null}
      </div>
    </div>
  )
}
