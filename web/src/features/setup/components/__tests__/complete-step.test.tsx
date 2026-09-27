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
import { expect, it } from 'vitest'

import type { SetupFormValues, SetupStatus } from '../../types'
import { CompleteStep } from '../complete-step'

// The suite's i18next instance is initialized with empty English resources, so
// t('English key') renders that key verbatim. Queries below therefore assert on
// translation keys, the same literals tracked by i18n:sync.
const VALUES: SetupFormValues = {
  username: 'admin',
  password: '',
  confirmPassword: '',
  usageMode: 'self',
}

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

it('shows the log database beside the primary database when the two differ', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'clickhouse' })
  )

  // The primary row is untouched: raw type text plus its badge.
  expect(screen.getAllByText('postgres')).toHaveLength(2)

  const logRow = screen.getByText(LOG_DATABASE_LABEL).closest('div')
  expect(logRow).not.toBeNull()
  expect(logRow?.querySelector('[data-slot="status-badge"]')).toHaveClass(
    'text-info'
  )
  expect(screen.getAllByText('clickhouse')).toHaveLength(2)

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

it('keeps the raw type text and the info badge for a differently cased log database', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'ClickHouse' })
  )

  expect(screen.getAllByText('ClickHouse')).toHaveLength(2)
  expect(
    screen
      .getByText(LOG_DATABASE_LABEL)
      .closest('div')
      ?.querySelector('[data-slot="status-badge"]')
  ).toHaveClass('text-info')
})

it('falls back to a neutral badge for an unknown log database type', () => {
  renderStep(
    fixture({ database_type: 'postgres', log_database_type: 'duckdb' })
  )

  expect(screen.getAllByText('duckdb')).toHaveLength(2)
  expect(
    screen
      .getByText(LOG_DATABASE_LABEL)
      .closest('div')
      ?.querySelector('[data-slot="status-badge"]')
  ).toHaveClass('text-muted-foreground')
})

it('keeps the primary row and hides the log row when the status is unavailable', () => {
  const { container } = renderStep(undefined)

  expect(screen.getAllByText('Unknown')).toHaveLength(2)
  expectPreChangeRendering(container)
})
