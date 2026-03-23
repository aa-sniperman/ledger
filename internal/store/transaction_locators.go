package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/sharding"
)

type TransactionLocatorRepository struct {
	db DBTX
}

func NewTransactionLocatorRepository(db DBTX) *TransactionLocatorRepository {
	return &TransactionLocatorRepository{db: db}
}

func (r *TransactionLocatorRepository) Create(ctx context.Context, transactionID string, shardID sharding.ShardID, createdAt time.Time) error {
	const query = `
INSERT INTO ledger.transaction_locators (
	transaction_id,
	shard_id,
	created_at
) VALUES ($1, $2, $3)
ON CONFLICT (transaction_id) DO NOTHING
`

	if _, err := r.db.ExecContext(ctx, query, transactionID, shardID, createdAt); err != nil {
		return fmt.Errorf("insert transaction locator %s: %w", transactionID, err)
	}

	return nil
}

func (r *TransactionLocatorRepository) GetByTransactionID(ctx context.Context, transactionID string) (sharding.ShardID, error) {
	const query = `
SELECT shard_id
FROM ledger.transaction_locators
WHERE transaction_id = $1
`

	var shardID string
	if err := r.db.QueryRowContext(ctx, query, transactionID).Scan(&shardID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: transaction locator %s", ErrNotFound, transactionID)
		}
		return "", fmt.Errorf("select transaction locator %s: %w", transactionID, err)
	}

	return sharding.ShardID(shardID), nil
}
