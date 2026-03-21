package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sniperman/ledger/internal/domain"
)

type AccountRepository struct {
	db DBTX
}

type BalanceDelta struct {
	PostedDebits   int64
	PostedCredits  int64
	PendingDebits  int64
	PendingCredits int64
}

type BalanceChange struct {
	AccountID string
	Delta     BalanceDelta
}

func NewAccountRepository(db DBTX) *AccountRepository {
	return &AccountRepository{db: db}
}

func (r *AccountRepository) Create(ctx context.Context, state domain.AccountState) error {
	if err := state.Validate(); err != nil {
		return err
	}

	now := time.Now().UTC()
	if state.Account.CreatedAt.IsZero() {
		state.Account.CreatedAt = now
	}
	if state.Account.UpdatedAt.IsZero() {
		state.Account.UpdatedAt = state.Account.CreatedAt
	}
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = state.Account.UpdatedAt
	}

	const insertAccountQuery = `
INSERT INTO ledger.accounts (
	account_id,
	currency,
	normal_balance,
	created_at,
	updated_at
) VALUES ($1, $2, $3, $4, $5)
`

	if _, err := r.db.ExecContext(
		ctx,
		insertAccountQuery,
		state.Account.ID,
		state.Account.Currency,
		state.Account.NormalBalance,
		state.Account.CreatedAt,
		state.Account.UpdatedAt,
	); err != nil {
		return fmt.Errorf("insert account %s: %w", state.Account.ID, err)
	}

	const insertBalanceQuery = `
INSERT INTO ledger.account_current_balances (
	account_id,
	posted_debits,
	posted_credits,
	pending_debits,
	pending_credits,
	current_version,
	updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`

	if _, err := r.db.ExecContext(
		ctx,
		insertBalanceQuery,
		state.Account.ID,
		state.CurrentBalances.PostedDebits,
		state.CurrentBalances.PostedCredits,
		state.CurrentBalances.PendingDebits,
		state.CurrentBalances.PendingCredits,
		state.CurrentVersion,
		state.UpdatedAt,
	); err != nil {
		return fmt.Errorf("insert account current balances %s: %w", state.Account.ID, err)
	}

	return nil
}

func (r *AccountRepository) GetByID(ctx context.Context, accountID string) (domain.AccountState, error) {
	query, args := buildStringInQuery(accountStateBaseQuery(`
WHERE a.account_id IN (%s)
`), []string{accountID})

	states, err := r.queryStates(ctx, query, args...)
	if err != nil {
		return domain.AccountState{}, err
	}

	state, ok := states[accountID]
	if !ok {
		return domain.AccountState{}, fmt.Errorf("%w: account %s", ErrNotFound, accountID)
	}

	return state, nil
}

func (r *AccountRepository) GetByIDs(ctx context.Context, accountIDs []string) (map[string]domain.AccountState, error) {
	if len(accountIDs) == 0 {
		return map[string]domain.AccountState{}, nil
	}

	query, args := buildStringInQuery(accountStateBaseQuery(`
WHERE a.account_id IN (%s)
`), accountIDs)

	return r.queryStates(ctx, query, args...)
}

func (r *AccountRepository) ApplyBalanceChanges(ctx context.Context, changes []BalanceChange, enforceAvailableCheck bool, updatedAt time.Time) (map[string]int64, error) {
	const query = `
UPDATE ledger.account_current_balances cb
SET
	posted_debits = cb.posted_debits + $1,
	posted_credits = cb.posted_credits + $2,
	pending_debits = cb.pending_debits + $3,
	pending_credits = cb.pending_credits + $4,
	current_version = cb.current_version + 1,
	updated_at = $5
FROM ledger.accounts a
WHERE cb.account_id = $6
  AND a.account_id = cb.account_id
  AND (
  	NOT $7 OR
  	CASE
  		WHEN a.normal_balance = 'credit_normal' THEN (cb.posted_credits + $2) - (cb.pending_debits + $3) >= 0
  		WHEN a.normal_balance = 'debit_normal' THEN (cb.posted_debits + $1) - (cb.pending_credits + $4) >= 0
  		ELSE FALSE
  	END
  )
RETURNING cb.current_version
`

	versions := make(map[string]int64, len(changes))

	for _, change := range changes {
		var currentVersion int64
		err := r.db.QueryRowContext(
			ctx,
			query,
			change.Delta.PostedDebits,
			change.Delta.PostedCredits,
			change.Delta.PendingDebits,
			change.Delta.PendingCredits,
			updatedAt,
			change.AccountID,
			enforceAvailableCheck,
		).Scan(&currentVersion)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if enforceAvailableCheck {
					return nil, fmt.Errorf("%w: account %s", domain.ErrBalanceLockFailed, change.AccountID)
				}
				return nil, fmt.Errorf("%w: account current balances %s", ErrNotFound, change.AccountID)
			}
			return nil, fmt.Errorf("apply balance change %s: %w", change.AccountID, err)
		}

		versions[change.AccountID] = currentVersion
	}

	return versions, nil
}

func (r *AccountRepository) queryStates(ctx context.Context, query string, args ...any) (map[string]domain.AccountState, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select account states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]domain.AccountState)

	for rows.Next() {
		state, err := scanAccountState(rows)
		if err != nil {
			return nil, err
		}

		states[state.Account.ID] = state
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate account state rows: %w", err)
	}

	return states, nil
}

func scanAccountState(scanner interface{ Scan(dest ...any) error }) (domain.AccountState, error) {
	var state domain.AccountState
	var normalBalance string

	err := scanner.Scan(
		&state.Account.ID,
		&state.Account.Currency,
		&normalBalance,
		&state.Account.CreatedAt,
		&state.Account.UpdatedAt,
		&state.CurrentBalances.PostedDebits,
		&state.CurrentBalances.PostedCredits,
		&state.CurrentBalances.PendingDebits,
		&state.CurrentBalances.PendingCredits,
		&state.CurrentVersion,
		&state.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AccountState{}, ErrNotFound
		}
		return domain.AccountState{}, fmt.Errorf("scan account state row: %w", err)
	}

	state.Account.NormalBalance = domain.NormalBalance(normalBalance)
	return state, nil
}

func accountStateBaseQuery(suffix string) string {
	return `
SELECT
	a.account_id,
	a.currency,
	a.normal_balance,
	a.created_at,
	a.updated_at,
	cb.posted_debits,
	cb.posted_credits,
	cb.pending_debits,
	cb.pending_credits,
	cb.current_version,
	cb.updated_at
FROM ledger.accounts a
INNER JOIN ledger.account_current_balances cb
	ON cb.account_id = a.account_id
` + suffix
}

func buildStringInQuery(base string, values []string) (string, []any) {
	placeholders := make([]string, 0, len(values))
	args := make([]any, 0, len(values))

	for index, value := range values {
		placeholders = append(placeholders, fmt.Sprintf("$%d", index+1))
		args = append(args, value)
	}

	return fmt.Sprintf(base, strings.Join(placeholders, ", ")), args
}
