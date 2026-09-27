package model

import (
	"fmt"
	"slices"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

const (
	// quota_data 使用独立的锁名 / 锁 id，不得与 options 的
	// new_api_options_pk / 75820193 共用。
	quotaDataMigrationLockName = "new_api_quota_data_key"
	quotaDataMigrationLockID   = 90827351
)

// migrateQuotaDataUniqueness makes "at most one row per 8-column business key"
// a database-enforced invariant of quota_data instead of an expectation of the
// application-level probe that the flush path used to perform. It must run
// before DB.AutoMigrate(&QuotaData{}): AutoMigrate issues
// `if !HasIndex(..) { CreateIndex(..) }` for the unique index, so on a database
// that still holds duplicate rows that statement fails, and the failure
// propagates through migrateDB into InitDB and FatalLog.
//
// The migration is idempotent by definition: an existing correct unique index
// cannot coexist with duplicate rows, so its presence means there is nothing
// left to do and the function returns without emitting any DDL or DML.
func migrateQuotaDataUniqueness(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("migrate quota data uniqueness: database is nil")
	}
	if !db.Migrator().HasTable(&QuotaData{}) {
		return nil
	}
	unique, err := quotaDataBusinessKeyIsUnique(db)
	if err != nil {
		return err
	}
	if unique {
		return nil
	}
	// 去重是「读一组行、在 Go 里求和、写回保留行、删其余行」的跨语句
	// read-modify-write。不串行化时，节点 B 可能对节点 A 已经部分删除的组求和
	// 并把更小的结果写回，永久丢掉求和差额。锁内必须重新探一次索引（双检锁）：
	// 另一个 master 可能在本节点取锁之前就完成了整个迁移。
	return withMigrationLock(db, quotaDataMigrationLockName, quotaDataMigrationLockID, func(locked *gorm.DB) error {
		unique, err := quotaDataBusinessKeyIsUnique(locked)
		if err != nil {
			return err
		}
		if unique {
			return nil
		}
		if err := normalizeQuotaDataBusinessKeyNulls(locked); err != nil {
			return err
		}
		if err := dedupeQuotaDataBusinessKeyRows(locked); err != nil {
			return err
		}
		return createQuotaDataBusinessKeyIndex(locked)
	})
}

// quotaDataBusinessKeyIsUnique reports whether the business key is already
// enforced, judging the index by its definition (8 columns in order, unique)
// rather than by its name alone.
func quotaDataBusinessKeyIsUnique(db *gorm.DB) (bool, error) {
	indexes, err := db.Migrator().GetIndexes(&QuotaData{})
	if err != nil {
		return false, fmt.Errorf("inspect quota_data indexes: %w", err)
	}
	for _, index := range indexes {
		if index.Name() != quotaDataBusinessKeyIndex {
			continue
		}
		if !slices.Equal(index.Columns(), quotaDataBusinessKeyColumns) {
			return false, fmt.Errorf("quota_data index %q covers %v, want %v",
				quotaDataBusinessKeyIndex, index.Columns(), quotaDataBusinessKeyColumns)
		}
		unique, ok := index.Unique()
		if !ok || !unique {
			return false, fmt.Errorf("quota_data index %q is not unique", quotaDataBusinessKeyIndex)
		}
		return true, nil
	}
	return false, nil
}

// normalizeQuotaDataBusinessKeyNulls rewrites NULL business key columns to the
// value the write path itself would have sent (all eight columns are nullable
// and every caller passes plain int/string). This is required, not defensive:
// a unique index does not constrain rows whose key contains NULL on any of the
// three engines, and the equality match used by the deduplication below never
// matches NULL.
func normalizeQuotaDataBusinessKeyNulls(db *gorm.DB) error {
	if err := db.Exec(`UPDATE quota_data SET
	user_id = COALESCE(user_id, 0),
	username = COALESCE(username, ''),
	model_name = COALESCE(model_name, ''),
	created_at = COALESCE(created_at, 0),
	use_group = COALESCE(use_group, ''),
	token_id = COALESCE(token_id, 0),
	channel_id = COALESCE(channel_id, 0),
	node_name = COALESCE(node_name, '')
WHERE user_id IS NULL OR username IS NULL OR model_name IS NULL OR created_at IS NULL
	OR use_group IS NULL OR token_id IS NULL OR channel_id IS NULL OR node_name IS NULL`).Error; err != nil {
		return fmt.Errorf("normalize quota_data business key nulls: %w", err)
	}
	return nil
}

// dedupeQuotaDataBusinessKeyRows merges every group of rows that share a
// business key into one row: the counters are summed and the smallest id of the
// group is kept.
//
// The aggregation runs in Go, not in SQL. MySQL 5.7 has no window functions, so
// ROW_NUMBER() cannot pick the surviving row, and
// `UPDATE t SET c = (SELECT SUM(..) FROM t ..)` fails there with ERROR 1093
// while PostgreSQL and SQLite accept it - exactly the shape that passes two
// engines and fails the minimum supported one.
//
// Merging is done by primary key rather than by rebuilding the table: quota_data
// has no cleanup task and grows monotonically, so reading the whole table into
// memory (the repairOptionPrimaryKey shape) is not affordable here.
func dedupeQuotaDataBusinessKeyRows(db *gorm.DB) error {
	columns := strings.Join(quotaDataBusinessKeyColumns, ", ")
	conditions := make([]string, 0, len(quotaDataBusinessKeyColumns))
	for _, name := range quotaDataBusinessKeyColumns {
		conditions = append(conditions, name+" = ?")
	}
	where := strings.Join(conditions, " and ")

	var duplicateKeys []QuotaData
	if err := db.Model(&QuotaData{}).
		Select(columns).
		Group(columns).
		Having("COUNT(*) > 1").
		Find(&duplicateKeys).Error; err != nil {
		return fmt.Errorf("find duplicate quota_data business keys: %w", err)
	}

	mergedKeys := 0
	deletedRows := 0
	for _, key := range duplicateKeys {
		var rows []QuotaData
		if err := db.Model(&QuotaData{}).
			Where(where, key.UserID, key.Username, key.ModelName, key.CreatedAt, key.UseGroup, key.TokenID, key.ChannelID, key.NodeName).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("read duplicate quota_data rows: %w", err)
		}
		if len(rows) < 2 {
			continue
		}
		kept := rows[0]
		count, quota, tokenUsed := 0, 0, 0
		for _, row := range rows {
			if row.Id < kept.Id {
				kept = row
			}
			count += row.Count
			quota += row.Quota
			tokenUsed += row.TokenUsed
		}
		// 先写回求和结果，再删其余行。反过来（先删后写）一旦中断，保留行里
		// 只剩旧值，组内计数就永久丢了；这个顺序最坏只会让同一组被再合并一次。
		if err := db.Model(&QuotaData{}).Where("id = ?", kept.Id).Updates(map[string]any{
			"count":      count,
			"quota":      quota,
			"token_used": tokenUsed,
		}).Error; err != nil {
			return fmt.Errorf("merge duplicate quota_data rows: %w", err)
		}
		stale := make([]int, 0, len(rows)-1)
		for _, row := range rows {
			if row.Id != kept.Id {
				stale = append(stale, row.Id)
			}
		}
		if err := db.Model(&QuotaData{}).Where("id IN ?", stale).Delete(&QuotaData{}).Error; err != nil {
			return fmt.Errorf("delete duplicate quota_data rows: %w", err)
		}
		mergedKeys++
		deletedRows += len(stale)
	}
	if mergedKeys > 0 {
		common.SysLog(fmt.Sprintf("quota_data 去重：合并 %d 组重复业务键，删除 %d 行", mergedKeys, deletedRows))
	}
	return nil
}

// createQuotaDataBusinessKeyIndex creates the unique index and then verifies it,
// so a concurrent master that created it first is treated as success.
//
// A missing index is not a warning-level problem: without it PostgreSQL rejects
// every ON CONFLICT write (the key stays in the in-memory cache forever, growing
// without bound while nothing reaches the dashboard) and MySQL never triggers
// ON DUPLICATE KEY UPDATE (every flush inserts a new row). Both are worse than
// the defect being fixed, so the caller lets the error terminate startup.
func createQuotaDataBusinessKeyIndex(db *gorm.DB) error {
	if createErr := db.Migrator().CreateIndex(&QuotaData{}, quotaDataBusinessKeyIndex); createErr != nil {
		unique, inspectErr := quotaDataBusinessKeyIsUnique(db)
		if inspectErr != nil || !unique {
			return fmt.Errorf("create quota_data business key index %q: %w", quotaDataBusinessKeyIndex, createErr)
		}
	}
	unique, err := quotaDataBusinessKeyIsUnique(db)
	if err != nil {
		return err
	}
	if !unique {
		return fmt.Errorf("quota_data business key index %q is missing after creation", quotaDataBusinessKeyIndex)
	}
	return nil
}
