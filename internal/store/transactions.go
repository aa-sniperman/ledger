package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/domain"
)

type TransactionRepository struct {
	db DBTX
}

func NewTransactionRepository(db DBTX) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) Create(ctx context.Context, transaction domain.Transaction) error {
	if err := transaction.ValidateForCreate(); err != nil {
		return err
	}

	const insertTransactionQuery = `
INSERT INTO ledger.ledger_transactions (
	transaction_id,
	posting_key,
	transaction_type,
	status,
	effective_at,
	created_at
) VALUES ($1, $2, $3, $4, $5, $6)
`

	if _, err := r.db.ExecContext(
		ctx,
		insertTransactionQuery,
		transaction.ID,
		transaction.PostingKey,
		transaction.Type,
		transaction.Status,
		transaction.EffectiveAt,
		transaction.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert transaction %s: %w", transaction.ID, err)
	}

	if err := r.InsertEntries(ctx, transaction.Entries); err != nil {
		return err
	}

	return nil
}

func (r *TransactionRepository) InsertEntries(ctx context.Context, entries []domain.Entry) error {
	const insertEntryQuery = `
INSERT INTO ledger.ledger_entries (
	entry_id,
	transaction_id,
	account_id,
	account_version,
	amount,
	currency,
	direction,
	status,
	effective_at,
	created_at,
	discarded_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`

	for _, entry := range entries {
		entryTransactionID, err := transactionIDForEntry(entry.TransactionID, entry)
		if err != nil {
			return err
		}

		if _, err := r.db.ExecContext(
			ctx,
			insertEntryQuery,
			entry.ID,
			entryTransactionID,
			entry.AccountID,
			entry.AccountVersion,
			entry.Money.Amount,
			entry.Money.Currency,
			entry.Direction,
			entry.Status,
			entry.EffectiveAt,
			entry.CreatedAt,
			entry.DiscardedAt,
		); err != nil {
			return fmt.Errorf("insert entry %s: %w", entry.ID, err)
		}
	}

	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, transactionID string) (domain.Transaction, error) {
	const query = `
SELECT
	transaction_id,
	posting_key,
	transaction_type,
	status,
	effective_at,
	created_at
FROM ledger.ledger_transactions
WHERE transaction_id = $1
`

	var transaction domain.Transaction
	var status string

	err := r.db.QueryRowContext(ctx, query, transactionID).Scan(
		&transaction.ID,
		&transaction.PostingKey,
		&transaction.Type,
		&status,
		&transaction.EffectiveAt,
		&transaction.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Transaction{}, fmt.Errorf("%w: transaction %s", ErrNotFound, transactionID)
		}
		return domain.Transaction{}, fmt.Errorf("select transaction %s: %w", transactionID, err)
	}

	transaction.Status = domain.TransactionStatus(status)

	entries, err := r.ListEntriesByTransactionID(ctx, transaction.ID)
	if err != nil {
		return domain.Transaction{}, err
	}

	transaction.Entries = entries
	return transaction, nil
}

func (r *TransactionRepository) GetByPostingKey(ctx context.Context, postingKey string) (domain.Transaction, error) {
	const query = `
SELECT transaction_id
FROM ledger.ledger_transactions
WHERE posting_key = $1
`

	var transactionID string
	err := r.db.QueryRowContext(ctx, query, postingKey).Scan(&transactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Transaction{}, fmt.Errorf("%w: posting key %s", ErrNotFound, postingKey)
		}
		return domain.Transaction{}, fmt.Errorf("select transaction by posting key %s: %w", postingKey, err)
	}

	return r.GetByID(ctx, transactionID)
}

func (r *TransactionRepository) ListEntriesByTransactionID(ctx context.Context, transactionID string) ([]domain.Entry, error) {
	return r.listEntriesByQuery(ctx, `
SELECT
	entry_id,
	transaction_id,
	account_id,
	account_version,
	amount,
	currency,
	direction,
	status,
	effective_at,
	created_at,
	discarded_at
FROM ledger.ledger_entries
WHERE transaction_id = $1
ORDER BY created_at, entry_id
`, transactionID)
}

func (r *TransactionRepository) ListActiveEntriesByTransactionIDAndStatus(ctx context.Context, transactionID string, status domain.EntryStatus) ([]domain.Entry, error) {
	const query = `
SELECT
	entry_id,
	transaction_id,
	account_id,
	account_version,
	amount,
	currency,
	direction,
	status,
	effective_at,
	created_at,
	discarded_at
FROM ledger.ledger_entries
WHERE transaction_id = $1
  AND status = $2
  AND discarded_at IS NULL
ORDER BY created_at, entry_id
`

	return r.listEntriesByQuery(ctx, query, transactionID, status)
}

func (r *TransactionRepository) listEntriesByQuery(ctx context.Context, query string, args ...any) ([]domain.Entry, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select transaction entries: %w", err)
	}
	defer rows.Close()

	var entries []domain.Entry

	for rows.Next() {
		var entry domain.Entry
		var direction string
		var status string
		var currency string
		var amount int64

		if err := rows.Scan(
			&entry.ID,
			&entry.TransactionID,
			&entry.AccountID,
			&entry.AccountVersion,
			&amount,
			&currency,
			&direction,
			&status,
			&entry.EffectiveAt,
			&entry.CreatedAt,
			&entry.DiscardedAt,
		); err != nil {
			return nil, fmt.Errorf("scan entry row: %w", err)
		}

		entry.Money = domain.Money{
			Amount:   amount,
			Currency: currency,
		}
		entry.Direction = domain.Direction(direction)
		entry.Status = domain.EntryStatus(status)

		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate entry rows: %w", err)
	}

	return entries, nil
}

func (r *TransactionRepository) DiscardEntries(ctx context.Context, entryIDs []string, discardedAt time.Time) error {
	if len(entryIDs) == 0 {
		return nil
	}

	query, args := buildStringInQuery(`
UPDATE ledger.ledger_entries
SET discarded_at = $1
WHERE entry_id IN (%s)
  AND discarded_at IS NULL
`, entryIDs)

	args = append([]any{discardedAt}, args...)
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("discard entries: %w", err)
	}

	return nil
}

func (r *TransactionRepository) UpdateStatus(ctx context.Context, transactionID string, status domain.TransactionStatus) error {
	const query = `
UPDATE ledger.ledger_transactions
SET status = $1
WHERE transaction_id = $2
`

	result, err := r.db.ExecContext(ctx, query, status, transactionID)
	if err != nil {
		return fmt.Errorf("update transaction status %s: %w", transactionID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for transaction status %s: %w", transactionID, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: transaction %s", ErrNotFound, transactionID)
	}

	return nil
}

func (r *TransactionRepository) TransitionStatus(ctx context.Context, transactionID string, fromStatus, toStatus domain.TransactionStatus) error {
	const query = `
UPDATE ledger.ledger_transactions
SET status = $1
WHERE transaction_id = $2
  AND status = $3
`

	result, err := r.db.ExecContext(ctx, query, toStatus, transactionID, fromStatus)
	if err != nil {
		return fmt.Errorf("transition transaction status %s: %w", transactionID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for transaction status transition %s: %w", transactionID, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: transaction %s is not in %s", domain.ErrInvalidTransition, transactionID, fromStatus)
	}

	return nil
}

func transactionIDForEntry(transactionID string, entry domain.Entry) (string, error) {
	if entry.TransactionID == "" {
		return transactionID, nil
	}

	if entry.TransactionID != transactionID {
		return "", fmt.Errorf("entry %s transaction_id %s does not match transaction %s", entry.ID, entry.TransactionID, transactionID)
	}

	return entry.TransactionID, nil
}
