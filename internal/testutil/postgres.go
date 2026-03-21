package testutil

import (
	"context"
	"database/sql"
	"testing"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
)

func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}

	db, err := database.Open(context.Background(), cfg.TestDatabaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func ResetTestDB(t *testing.T, db *sql.DB) {
	t.Helper()

	const query = `
TRUNCATE TABLE
	ledger.ledger_entries,
	ledger.ledger_transactions,
	ledger.account_current_balances,
	ledger.accounts
CASCADE
`

	if _, err := db.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("reset test database: %v", err)
	}
}
