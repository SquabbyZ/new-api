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
import { CheckCircle2 } from 'lucide-react'
import { useTranslation } from 'react-i18next'

import { StatusBadge } from '@/components/status-badge'
import { Separator } from '@/components/ui/separator'

import { resolveDatabaseMeta } from '../lib/database-meta'
import type { SetupFormValues, SetupStatus } from '../types'

interface CompleteStepProps {
  status?: SetupStatus
  values: SetupFormValues
}

const USAGE_MODE_LABEL_KEYS: Record<SetupFormValues['usageMode'], string> = {
  external: 'External operations mode',
  self: 'Personal use mode',
  demo: 'Demo site mode',
}

export function CompleteStep({ status, values }: CompleteStepProps) {
  const { t } = useTranslation()
  const usageLabelKey = USAGE_MODE_LABEL_KEYS[values.usageMode]
  // Resolve from the raw reported type, exactly as the Database check step
  // does: resolving a trimmed type instead would render a padded type with two
  // different names across the two steps.
  const databaseMeta = resolveDatabaseMeta(status?.database_type)
  const databaseLabel = databaseMeta?.label ?? t('Unknown')
  const databaseVariant = databaseMeta?.variant ?? 'info'
  // The log database earns its own row only when it differs from the primary
  // one: an unset LOG_SQL_DSN makes both types identical, and the default
  // deployment must keep rendering exactly the rows above.
  const mainType = status?.database_type?.trim().toLowerCase() ?? ''
  const logDatabaseType = status?.log_database_type?.trim() ?? ''
  const logType = logDatabaseType.toLowerCase()
  const showLogDatabase = logType !== '' && logType !== mainType
  const logDatabaseMeta = showLogDatabase ? resolveDatabaseMeta(logType) : null

  return (
    <div className='flex flex-col items-center gap-6 text-center'>
      <div className='rounded-2xl bg-emerald-500/10 p-4 text-emerald-600 dark:bg-emerald-500/20 dark:text-emerald-300'>
        <CheckCircle2 className='size-8' />
      </div>
      <div className='space-y-2'>
        <h2 className='text-2xl font-semibold tracking-tight'>
          {t('Ready to initialize')}
        </h2>
        <p className='text-muted-foreground max-w-lg text-sm sm:text-base'>
          {t(
            'Double check the configuration below. Your system will be locked until initialization is complete.'
          )}
        </p>
      </div>

      <div className='bg-card w-full rounded-xl border p-6 text-left shadow-sm sm:p-8'>
        <dl className='grid gap-6'>
          <div className='space-y-1.5'>
            <dt className='text-muted-foreground text-xs font-medium tracking-wide uppercase'>
              {t('Database')}
            </dt>
            <dd className='flex flex-wrap items-center gap-2'>
              <span className='text-sm font-semibold'>{databaseLabel}</span>
              <StatusBadge
                label={databaseLabel}
                variant={databaseVariant}
                copyable={false}
              />
            </dd>
          </div>

          {logDatabaseMeta && (
            <>
              <Separator />

              <div className='space-y-1.5'>
                <dt className='text-muted-foreground text-xs font-medium tracking-wide uppercase'>
                  {t('Log database')}
                </dt>
                <dd className='flex flex-wrap items-center gap-2'>
                  <span className='text-sm font-semibold'>
                    {logDatabaseMeta.label}
                  </span>
                  <StatusBadge
                    label={logDatabaseMeta.label}
                    variant={logDatabaseMeta.variant}
                    copyable={false}
                  />
                </dd>
              </div>
            </>
          )}

          <Separator />

          <div className='space-y-1.5'>
            <dt className='text-muted-foreground text-xs font-medium tracking-wide uppercase'>
              {t('Administrator account')}
            </dt>
            <dd className='text-sm font-semibold'>
              {status?.root_init
                ? t('Existing account will be reused')
                : values.username || t('Not set yet')}
            </dd>
          </div>

          <Separator />

          <div className='space-y-1.5'>
            <dt className='text-muted-foreground text-xs font-medium tracking-wide uppercase'>
              {t('Usage mode')}
            </dt>
            <dd className='text-sm font-semibold'>{t(usageLabelKey)}</dd>
          </div>
        </dl>
      </div>
    </div>
  )
}
