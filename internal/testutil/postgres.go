package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/migrations"
	"github.com/sniperman/ledger/internal/sharding"
)

const testDBAdvisoryLockKey int64 = 73421091

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

	runner := migrations.NewRunner(db, migrations.Files)
	if _, err := runner.Up(context.Background()); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}

	lockConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire test database connection: %v", err)
	}

	if _, err := lockConn.ExecContext(context.Background(), `SELECT pg_advisory_lock($1)`, testDBAdvisoryLockKey); err != nil {
		_ = lockConn.Close()
		t.Fatalf("acquire test database advisory lock: %v", err)
	}

	t.Cleanup(func() {
		_, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, testDBAdvisoryLockKey)
		_ = lockConn.Close()
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
	ledger.transaction_locators,
	ledger.ledger_commands,
	ledger.account_current_balances,
	ledger.accounts
CASCADE
`

	if _, err := db.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("reset test database: %v", err)
	}
}

func OpenShardTestDBs(t *testing.T) map[sharding.ShardID]*sql.DB {
	t.Helper()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	if len(cfg.TestShardDatabaseURLs) == 0 {
		t.Skip("TEST_SHARD_DATABASE_URLS is not configured")
	}

	dbs, err := database.OpenMany(context.Background(), cfg.TestShardDatabaseURLs)
	if err != nil {
		t.Fatalf("open shard test databases: %v", err)
	}

	for shardID, db := range dbs {
		runner := migrations.NewRunner(db, migrations.Files)
		if _, err := runner.Up(context.Background()); err != nil {
			database.CloseMany(dbs)
			t.Fatalf("migrate shard test database %s: %v", shardID, err)
		}

		lockConn, err := db.Conn(context.Background())
		if err != nil {
			database.CloseMany(dbs)
			t.Fatalf("acquire shard test database connection %s: %v", shardID, err)
		}

		lockKey := advisoryLockKeyForShard(shardID)
		if _, err := lockConn.ExecContext(context.Background(), `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
			_ = lockConn.Close()
			database.CloseMany(dbs)
			t.Fatalf("acquire shard test database advisory lock %s: %v", shardID, err)
		}

		currentShardID := shardID
		currentLockConn := lockConn
		t.Cleanup(func() {
			_, _ = currentLockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKeyForShard(currentShardID))
			_ = currentLockConn.Close()
		})
	}

	t.Cleanup(func() {
		database.CloseMany(dbs)
	})

	return dbs
}

func advisoryLockKeyForShard(shardID sharding.ShardID) int64 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(fmt.Sprintf("shard-test-db:%s", shardID)))
	return testDBAdvisoryLockKey + int64(hasher.Sum32())
}

func ResetShardTestDBs(t *testing.T, dbs map[sharding.ShardID]*sql.DB) {
	t.Helper()

	for _, db := range dbs {
		ResetTestDB(t, db)
	}
}
