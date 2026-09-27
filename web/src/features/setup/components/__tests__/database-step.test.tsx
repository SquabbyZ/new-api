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

import type { SetupStatus } from '../../types'
import { DatabaseStep } from '../database-step'

// The suite's i18next instance is initialized with empty English resources, so
// t('English key') renders that key verbatim. Queries below therefore assert on
// translation keys, the same literals tracked by static-keys.ts and i18n:sync.
const POSTGRES_DESCRIPTION =
  'PostgreSQL offers advanced reliability and data integrity for production workloads.'
const CLICKHOUSE_DESCRIPTION =
  'ClickHouse stores logs and analytics data. Business and billing data stay in the primary database.'
const LOG_DATABASE_LABEL = 'Detected log database'

function fixture(status: Partial<SetupStatus>): SetupStatus {
  return {
    status: false,
    root_init: false,
    database_type: 'postgres',
    ...status,
  }
}

it('shows the ClickHouse log database beside the primary database when both are configured', () => {
  render(
    <DatabaseStep
      status={fixture({
        database_type: 'postgres',
        log_database_type: 'clickhouse',
      })}
    />
  )

  expect(screen.getByText('Detected database')).toBeVisible()
  expect(screen.getByText(POSTGRES_DESCRIPTION)).toBeVisible()
  expect(screen.getByText('PostgreSQL detected')).toBeVisible()

  expect(screen.getByText(LOG_DATABASE_LABEL)).toBeVisible()
  expect(screen.getByText(CLICKHOUSE_DESCRIPTION)).toBeVisible()
  expect(screen.getByText('ClickHouse log database detected')).toBeVisible()
})

it.each([
  {
    name: 'sqlite',
    description:
      'SQLite stores all data in a single file. Make sure that file is persisted when running in containers.',
    alertTitle: 'Persist your data file',
  },
  {
    name: 'mysql',
    description:
      'MySQL is a production-ready relational database. Keep your credentials secure.',
    alertTitle: 'MySQL detected',
  },
  {
    name: 'postgres',
    description: POSTGRES_DESCRIPTION,
    alertTitle: 'PostgreSQL detected',
  },
])(
  'keeps the $name primary database rendering unchanged when the log database is the same',
  ({ name, description, alertTitle }) => {
    render(
      <DatabaseStep
        status={fixture({
          database_type: name,
          log_database_type: name,
        })}
      />
    )

    expect(screen.getByText('Detected database')).toBeVisible()
    expect(screen.getByText(description)).toBeVisible()
    expect(screen.getByText(alertTitle)).toBeVisible()
    expect(screen.queryByText(LOG_DATABASE_LABEL)).not.toBeInTheDocument()
  }
)

it('hides the log database when the backend does not report a log database type', () => {
  render(<DatabaseStep status={fixture({ database_type: 'mysql' })} />)

  expect(screen.getByText('MySQL detected')).toBeVisible()
  expect(screen.queryByText(LOG_DATABASE_LABEL)).not.toBeInTheDocument()
})

it('hides the log database when the reported log database type is blank', () => {
  render(
    <DatabaseStep
      status={fixture({ database_type: 'mysql', log_database_type: '  ' })}
    />
  )

  expect(screen.getByText('MySQL detected')).toBeVisible()
  expect(screen.queryByText(LOG_DATABASE_LABEL)).not.toBeInTheDocument()
})

it('falls back to the custom driver copy for an unknown log database type', () => {
  render(
    <DatabaseStep
      status={fixture({
        database_type: 'postgres',
        log_database_type: 'duckdb',
      })}
    />
  )

  expect(screen.getByText(LOG_DATABASE_LABEL)).toBeVisible()
  expect(screen.getByText('Custom database driver detected.')).toBeVisible()
  expect(
    screen.queryByText('ClickHouse log database detected')
  ).not.toBeInTheDocument()
})
