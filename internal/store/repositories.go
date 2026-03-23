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
	Commands            *CommandRepository
	Accounts            *AccountRepository
	Transactions        *TransactionRepository
	TransactionLocators *TransactionLocatorRepository
}

func NewRepositories(db DBTX) Repositories {
	return Repositories{
		Commands:            NewCommandRepository(db),
		Accounts:            NewAccountRepository(db),
		Transactions:        NewTransactionRepository(db),
		TransactionLocators: NewTransactionLocatorRepository(db),
	}
}
