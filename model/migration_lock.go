package model

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// withMigrationLock runs fn while an exclusive database-wide lock is held, so
// that concurrent masters cannot run the same startup repair migration at the
// same time. NODE_TYPE is unset by default, which makes every node a master,
// so a repair that reads a group of rows, sums them and writes the sum back
// must serialize with the same repair on the other nodes; otherwise one node
// can sum a group another node has already partially rewritten and write the
// smaller result back.
//
// lockName and lockID must be unique per migration. Sharing them between two
// different repairs would serialize the two for no reason and make the name
// ambiguous.
func withMigrationLock(db *gorm.DB, lockName string, lockID int, fn func(*gorm.DB) error) error {
	switch db.Dialector.Name() {
	case "mysql":
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("lock %s: %w", lockName, err)
		}
		ctx := context.Background()
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("lock %s: %w", lockName, err)
		}
		defer conn.Close()
		var acquired int
		if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 60)", lockName).Scan(&acquired); err != nil {
			return fmt.Errorf("lock %s: %w", lockName, err)
		}
		if acquired != 1 {
			return fmt.Errorf("lock %s: timeout", lockName)
		}
		defer conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", lockName)
		return fn(db)
	case "postgres":
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", lockID).Error; err != nil {
				return fmt.Errorf("lock %s: %w", lockName, err)
			}
			return fn(tx)
		})
	default:
		return db.Transaction(func(tx *gorm.DB) error {
			return fn(tx)
		})
	}
}
