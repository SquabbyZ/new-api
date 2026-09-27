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
import { render, screen } from '@testing-library/react'
import i18next from 'i18next'
import { expect, it } from 'vitest'

import type { SetupFormValues, SetupStatus } from '../../types'
import { CompleteStep } from '../complete-step'

// The suite's i18next instance is initialized with empty English resources, so
// t('English key') renders that key verbatim. Queries below therefore assert on
// translation keys, the same literals tracked by static-keys.ts and i18n:sync.
const VALUES: SetupFormValues = {
  username: 'admin',
  password: '',
  confirmPassword: '',
  usageMode: 'self',
}

const DATABASE_LABEL = 'Database'
const LOG_DATABASE_LABEL = 'Log database'
const PRE_CHANGE_ROWS = ['Database', 'Administrator account', 'Usage mode']

function fixture(status: Partial<SetupStatus> = {}): SetupStatus {
  return {
    status: false,
    root_init: false,
    database_type: 'postgres',
    ...status,
  }
}

function renderStep(status?: SetupStatus) {
  return render(<CompleteStep status={status} values={VALUES} />)
}

// Each database row shows its name twice - once as the row value and once as
// the badge label - so reach the badge through its row's own label instead of
// counting every occurrence in the step.
function rowBadge(rowLabel: string) {
  return screen
    .getByText(rowLabel)
    .closest('div')
    ?.querySelector('[data-slot="status-badge"]')
}

// Before this change the step always rendered three rows split by two
// separators, so an input that reports no usable log database must still match
// this shape element for element.
function expectPreChangeRendering(container: HTMLElement) {
  expect(
    [...container.querySelectorAll('dt')].map((row) => row.textContent)
  ).toEqual(PRE_CHANGE_ROWS)
  expect(container.querySelectorAll('[data-slot="separator"]')).toHaveLength(2)
  expect(screen.queryByText(LOG_DATABASE_LABEL)).not.toBeInTheDocument()
}

it('shows the friendly labels for the primary and log database when the two differ', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'clickhouse' })
  )

  // The primary row names the database the way the Database check step does,
  // as both its row value and its badge label.
  expect(screen.getAllByText('PostgreSQL')).toHaveLength(2)
  expect(screen.queryByText('postgres')).not.toBeInTheDocument()
  expect(rowBadge(DATABASE_LABEL)).toHaveClass('text-success')

  expect(screen.getAllByText('ClickHouse')).toHaveLength(2)
  expect(rowBadge(LOG_DATABASE_LABEL)).toHaveClass('text-info')

  expect(screen.getByText('admin')).toBeVisible()
  expect(screen.getByText('Personal use mode')).toBeVisible()
})

it.each([
  { name: 'identical', logDatabaseType: 'postgres' },
  { name: 'identical but differently cased', logDatabaseType: 'POSTGRES' },
  {
    name: 'identical but padded with whitespace',
    logDatabaseType: '  postgres  ',
  },
])(
  'hides the log database when it normalizes to the primary type ($name)',
  ({ logDatabaseType }) => {
    const { container } = renderStep(
      fixture({
        database_type: 'postgres',
        log_database_type: logDatabaseType,
      })
    )

    expectPreChangeRendering(container)
  }
)

it('hides the log database when the backend does not report a log database type', () => {
  const status = fixture({ database_type: 'postgres' })
  // The pre-change code never received this field, so this input shape is the
  // pre-change baseline the two assertions below compare against.
  expect('log_database_type' in status).toBe(false)

  const { container } = renderStep(status)

  expectPreChangeRendering(container)
})

it.each(['', '  '])(
  'hides the log database when the reported type is blank %j',
  (logDatabaseType) => {
    const { container } = renderStep(
      fixture({
        database_type: 'postgres',
        log_database_type: logDatabaseType,
      })
    )

    expectPreChangeRendering(container)
  }
)

it('resolves a differently cased primary database type to its friendly label', () => {
  renderStep(fixture({ database_type: 'POSTGRES' }))

  expect(screen.getAllByText('PostgreSQL')).toHaveLength(2)
  expect(screen.queryByText('POSTGRES')).not.toBeInTheDocument()
})

it('labels the primary database from the raw reported type, not a trimmed one', () => {
  const { container } = renderStep(
    fixture({ database_type: '  postgres  ', log_database_type: 'postgres' })
  )

  // The Database check step resolves this same raw string and reports a custom
  // driver, so resolving the trimmed type here would give the two steps two
  // different names for one database.
  expect(screen.queryByText('PostgreSQL')).not.toBeInTheDocument()
  expect(screen.getAllByText('postgres')).toHaveLength(2)
  expect(rowBadge(DATABASE_LABEL)).toHaveClass('text-info')

  expectPreChangeRendering(container)
})

it.each([
  { name: 'primary', status: { database_type: 'duckdb' }, row: DATABASE_LABEL },
  {
    name: 'log',
    status: { database_type: 'postgres', log_database_type: 'duckdb' },
    row: LOG_DATABASE_LABEL,
  },
])(
  'keeps the raw reported type and uses the info badge for an unknown $name database type',
  ({ status, row }) => {
    renderStep(fixture(status))

    expect(screen.getAllByText('duckdb')).toHaveLength(2)
    expect(rowBadge(row)).toHaveClass('text-info')
  }
)

it('lowercases an unrecognized log database type the way the Database check step does', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'DuckDB' })
  )

  // The Database check step resolves the trimmed, lowercased log type, so this
  // step has to resolve the same value or the two steps disagree again.
  expect(screen.getAllByText('duckdb')).toHaveLength(2)
  expect(rowBadge(LOG_DATABASE_LABEL)).toHaveClass('text-info')
})

it('resolves a differently cased log database type to its friendly label', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'ClickHouse' })
  )

  expect(screen.getAllByText('ClickHouse')).toHaveLength(2)
  expect(rowBadge(LOG_DATABASE_LABEL)).toHaveClass('text-info')
})

it('falls back to Unknown with the info badge when the status is unavailable', () => {
  const { container } = renderStep(undefined)

  expect(screen.getAllByText('Unknown')).toHaveLength(2)
  expect(rowBadge(DATABASE_LABEL)).toHaveClass('text-info')
  expectPreChangeRendering(container)
})

it('falls back to Unknown when the reported primary database type is empty', () => {
  const { container } = renderStep(fixture({ database_type: '' }))

  expect(screen.getAllByText('Unknown')).toHaveLength(2)
  expect(rowBadge(DATABASE_LABEL)).toHaveClass('text-info')
  expectPreChangeRendering(container)
})

it('renders the Unknown fallback through i18n so a translated locale shows its own wording', async () => {
  i18next.addResourceBundle('zh', 'translation', { Unknown: '未知' })
  await i18next.changeLanguage('zh')

  try {
    renderStep(undefined)

    expect(screen.getAllByText('未知')).toHaveLength(2)
  } finally {
    await i18next.changeLanguage('en')
    i18next.removeResourceBundle('zh', 'translation')
  }
})
