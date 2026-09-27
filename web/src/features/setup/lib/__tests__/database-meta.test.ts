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
import { expect, it } from 'vitest'

import { resolveDatabaseMeta } from '../database-meta'

// Both setup steps label the same database through this one resolver, so the
// lookup semantics asserted here are the contract that keeps them identical.
it.each([
  { type: 'sqlite', label: 'SQLite', variant: 'warning' },
  { type: 'mysql', label: 'MySQL', variant: 'success' },
  { type: 'postgres', label: 'PostgreSQL', variant: 'success' },
  { type: 'clickhouse', label: 'ClickHouse', variant: 'info' },
])(
  'resolves the $type database to its friendly label and badge variant',
  ({ type, label, variant }) => {
    expect(resolveDatabaseMeta(type)).toMatchObject({ label, variant })
  }
)

it.each([
  { type: 'POSTGRES', label: 'PostgreSQL', variant: 'success' },
  { type: 'PoStGrEs', label: 'PostgreSQL', variant: 'success' },
  { type: 'MySQL', label: 'MySQL', variant: 'success' },
  { type: 'ClickHouse', label: 'ClickHouse', variant: 'info' },
])(
  'resolves the $type database to the same entry as its lowercase key',
  ({ type, label, variant }) => {
    expect(resolveDatabaseMeta(type)).toMatchObject({ label, variant })
  }
)

it('falls back to the raw reported type with the info variant when it is unknown', () => {
  expect(resolveDatabaseMeta('duckdb')).toMatchObject({
    label: 'duckdb',
    variant: 'info',
  })
})

// The lookup keys on the lowercase spelling, and the PostgreSQL key is
// `postgres` - so the canonical `PostgreSQL` spelling misses the table even
// though the resulting label looks identical. The Database check step renders
// exactly this, and this slice must not diverge from it, so the fallback is
// pinned here rather than "fixed" in the resolver.
it('keeps the PostgreSQL spelling on the unrecognized-driver fallback', () => {
  expect(resolveDatabaseMeta('PostgreSQL')).toMatchObject({
    label: 'PostgreSQL',
    descriptionKey: 'Custom database driver detected.',
    variant: 'info',
  })
})

it.each([undefined, ''])(
  'returns null for %j so the caller supplies the Unknown fallback',
  (type) => {
    expect(resolveDatabaseMeta(type)).toBeNull()
  }
)

it('passes a whitespace-only type through unresolved instead of trimming it', () => {
  expect(resolveDatabaseMeta('  ')).toMatchObject({
    label: '  ',
    variant: 'info',
  })
})
