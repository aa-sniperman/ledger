package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestFleetProcessAvailableAllIntegration(t *testing.T) {
	centralDB := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, centralDB)

	shardDBs := testutil.OpenShardTestDBs(t)
	testutil.ResetShardTestDBs(t, shardDBs)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, map[sharding.SystemAccountRole]int{
		sharding.SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	registry, err := sharding.NewDBRegistry(shardDBs)
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}

	commandService := service.NewCommandService(centralDB, router, registry)
	fleet, err := NewFleet(
		commandService,
		[]sharding.ShardID{"shard-a", "shard-b"},
		50*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("build fleet: %v", err)
	}

	userShardA := testUserIDForShard(t, router, "shard-a")
	userShardB := testUserIDForShard(t, router, "shard-b")

	systemAccountShardA, shardA, err := router.SystemAccountForUser(userShardA, sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick shard-a system account: %v", err)
	}
	systemAccountShardB, shardB, err := router.SystemAccountForUser(userShardB, sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick shard-b system account: %v", err)
	}

	seedShardAccounts(t, shardDBs[shardA], userShardA, systemAccountShardA)
	seedShardAccounts(t, shardDBs[shardB], userShardB, systemAccountShardB)

	enqueueCreateCommand(t, commandService, userShardA, systemAccountShardA, "tx_worker_fleet_a", "pk_worker_fleet_a", 70)
	enqueueCreateCommand(t, commandService, userShardB, systemAccountShardB, "tx_worker_fleet_b", "pk_worker_fleet_b", 40)

	processed, err := fleet.ProcessAvailableAll(context.Background())
	if err != nil {
		t.Fatalf("process available all: %v", err)
	}
	if processed[shardA] != 1 || processed[shardB] != 1 {
		t.Fatalf("unexpected processed counts: %+v", processed)
	}

	assertTransactionExists(t, shardDBs[shardA], "tx_worker_fleet_a")
	assertTransactionMissing(t, shardDBs[shardB], "tx_worker_fleet_a")
	assertTransactionExists(t, shardDBs[shardB], "tx_worker_fleet_b")
	assertTransactionMissing(t, shardDBs[shardA], "tx_worker_fleet_b")
}

func testUserIDForShard(t *testing.T, router sharding.Router, target sharding.ShardID) string {
	t.Helper()

	for i := 1; i <= 10_000; i++ {
		userID := fmt.Sprintf("worker_user_%d", i)
		shardID, err := router.ShardForUser(userID)
		if err != nil {
			t.Fatalf("route user %s: %v", userID, err)
		}
		if shardID == target {
			return userID
		}
	}

	t.Fatalf("could not find user for shard %s", target)
	return ""
}

func seedShardAccounts(t *testing.T, db store.DBTX, userID, systemAccountID string) {
	t.Helper()

	repos := store.NewRepositories(db)
	ctx := context.Background()
	now := time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC)

	accounts := []serviceAccountState{
		{
			AccountID:      sharding.UserAccountID(userID),
			Version:        10,
			PostedCredits:  100,
			PendingCredits: 100,
			Timestamp:      now,
		},
		{
			AccountID: systemAccountID,
			Version:   0,
			Timestamp: now,
		},
	}

	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account.toDomainAccountState()); err != nil {
			t.Fatalf("seed shard account %s: %v", account.AccountID, err)
		}
	}
}

func enqueueCreateCommand(t *testing.T, commandService *service.CommandService, userID, systemAccountID, transactionID, postingKey string, amount int64) {
	t.Helper()

	_, _, err := commandService.EnqueueTransactionCreate(context.Background(), service.EnqueueCreateTransactionCommandInput{
		UserID:         userID,
		IdempotencyKey: "idem_" + transactionID,
		CreateInput: service.CreateTransactionInput{
			Flow:          service.TransactionFlowAuthorizing,
			TransactionID: transactionID,
			PostingKey:    postingKey,
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 23, 12, 30, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 23, 12, 30, 1, 0, time.UTC),
			Entries: []service.CreateEntryInput{
				{EntryID: transactionID + "_debit", AccountID: sharding.UserAccountID(userID), Amount: amount, Currency: "USD", Direction: "debit"},
				{EntryID: transactionID + "_credit", AccountID: systemAccountID, Amount: amount, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue create command %s: %v", transactionID, err)
	}
}

func assertTransactionExists(t *testing.T, db store.DBTX, transactionID string) {
	t.Helper()

	if _, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), transactionID); err != nil {
		t.Fatalf("load transaction %s: %v", transactionID, err)
	}
}

func assertTransactionMissing(t *testing.T, db store.DBTX, transactionID string) {
	t.Helper()

	if _, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), transactionID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected transaction %s to be missing, got %v", transactionID, err)
	}
}

type serviceAccountState struct {
	AccountID      string
	Version        int64
	PostedCredits  int64
	PendingCredits int64
	Timestamp      time.Time
}

func (s serviceAccountState) toDomainAccountState() domain.AccountState {
	return domain.AccountState{
		Account: domain.Account{
			ID:            s.AccountID,
			Currency:      "USD",
			NormalBalance: domain.NormalBalanceCredit,
			CreatedAt:     s.Timestamp,
			UpdatedAt:     s.Timestamp,
		},
		CurrentVersion: s.Version,
		CurrentBalances: domain.BalanceBuckets{
			PostedCredits:  s.PostedCredits,
			PendingCredits: s.PendingCredits,
		},
		UpdatedAt: s.Timestamp,
	}
}
