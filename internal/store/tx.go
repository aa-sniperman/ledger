package store

import (
	"context"
	"database/sql"
	"fmt"
)

func InTx(ctx context.Context, db *sql.DB, fn func(Repositories) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if panicValue := recover(); panicValue != nil {
			_ = tx.Rollback()
			panic(panicValue)
		}

		if err != nil {
			_ = tx.Rollback()
			return
		}

		if commitErr := tx.Commit(); commitErr != nil {
			err = fmt.Errorf("commit tx: %w", commitErr)
		}
	}()

	err = fn(NewRepositories(tx))
	return err
}
