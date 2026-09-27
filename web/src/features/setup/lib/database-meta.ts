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
export const DATABASE_META: Record<
  string,
  {
    label: string
    descriptionKey: string
    variant: 'info' | 'success' | 'warning'
  }
> = {
  sqlite: {
    label: 'SQLite',
    descriptionKey:
      'SQLite stores all data in a single file. Make sure that file is persisted when running in containers.',
    variant: 'warning',
  },
  mysql: {
    label: 'MySQL',
    descriptionKey:
      'MySQL is a production-ready relational database. Keep your credentials secure.',
    variant: 'success',
  },
  postgres: {
    label: 'PostgreSQL',
    descriptionKey:
      'PostgreSQL offers advanced reliability and data integrity for production workloads.',
    variant: 'success',
  },
  clickhouse: {
    label: 'ClickHouse',
    descriptionKey:
      'ClickHouse stores logs and analytics data. Business and billing data stay in the primary database.',
    variant: 'info',
  },
}

export function resolveDatabaseMeta(type?: string) {
  if (!type) return null
  const normalized = type.toLowerCase()
  return (
    DATABASE_META[normalized] ?? {
      label: type,
      descriptionKey: 'Custom database driver detected.',
      variant: 'info' as const,
    }
  )
}
