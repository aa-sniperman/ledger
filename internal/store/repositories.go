package store

import (
	"context"
	"database/sql"
)

type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Repositories struct {
	Accounts     *AccountRepository
	Transactions *TransactionRepository
}

func NewRepositories(db DBTX) Repositories {
	return Repositories{
		Accounts:     NewAccountRepository(db),
		Transactions: NewTransactionRepository(db),
	}
}
