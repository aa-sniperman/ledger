package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestCommandServiceProcessNextUsesShardSpecificDBIntegration(t *testing.T) {
	centralDB := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, centralDB)

	shardDBs := testutil.OpenShardTestDBs(t)
	testutil.ResetShardTestDBs(t, shardDBs)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	registry, err := sharding.NewDBRegistry(shardDBs)
	if err != nil {
		t.Fatalf("build shard registry: %v", err)
	}

	commandService := NewCommandService(centralDB, router, registry)
	queryService := NewQueryService(centralDB, router, registry)

	userShardA := testUserIDForShard(t, router, "shard-a")
	userShardB := testUserIDForShard(t, router, "shard-b")

	systemAccountShardA, shardA, err := router.SystemAccountForUser(userShardA, "USD", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick shard-a system account: %v", err)
	}
	systemAccountShardB, shardB, err := router.SystemAccountForUser(userShardB, "USD", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick shard-b system account: %v", err)
	}

	seedShardCommandExecutionAccounts(t, shardDBs[shardA], userShardA, systemAccountShardA)
	seedShardCommandExecutionAccounts(t, shardDBs[shardB], userShardB, systemAccountShardB)

	enqueuedShardA, _, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         userShardA,
		IdempotencyKey: "idem_cmd_multishard_create_a",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_multishard_a",
			PostingKey:    "pk_cmd_multishard_a",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 23, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 23, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_multishard_a_debit", AccountID: sharding.UserAccountID(userShardA, "USD"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_multishard_a_credit", AccountID: systemAccountShardA, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue shard-a command: %v", err)
	}

	enqueuedShardB, _, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         userShardB,
		IdempotencyKey: "idem_cmd_multishard_create_b",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_multishard_b",
			PostingKey:    "pk_cmd_multishard_b",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 23, 8, 1, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 23, 8, 1, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_multishard_b_debit", AccountID: sharding.UserAccountID(userShardB, "USD"), Amount: 40, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_multishard_b_credit", AccountID: systemAccountShardB, Amount: 40, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue shard-b command: %v", err)
	}

	processedShardA, ok, err := commandService.ProcessNext(context.Background(), shardA, time.Date(2026, 3, 23, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("process shard-a command: %v", err)
	}
	if !ok {
		t.Fatal("expected shard-a command to be processed")
	}
	if processedShardA.CommandID != enqueuedShardA.CommandID || processedShardA.Status != command.StatusSucceeded {
		t.Fatalf("unexpected shard-a processed envelope: %+v", processedShardA)
	}

	if _, ok, err := commandService.ProcessNext(context.Background(), shardA, time.Date(2026, 3, 23, 9, 0, 1, 0, time.UTC)); err != nil {
		t.Fatalf("reprocess shard-a queue: %v", err)
	} else if ok {
		t.Fatal("expected no shard-b work to be claimable from shard-a queue")
	}

	processedShardB, ok, err := commandService.ProcessNext(context.Background(), shardB, time.Date(2026, 3, 23, 9, 0, 2, 0, time.UTC))
	if err != nil {
		t.Fatalf("process shard-b command: %v", err)
	}
	if !ok {
		t.Fatal("expected shard-b command to be processed")
	}
	if processedShardB.CommandID != enqueuedShardB.CommandID || processedShardB.Status != command.StatusSucceeded {
		t.Fatalf("unexpected shard-b processed envelope: %+v", processedShardB)
	}

	assertTransactionLocator(t, centralDB, "tx_cmd_multishard_a", shardA)
	assertTransactionLocator(t, centralDB, "tx_cmd_multishard_b", shardB)

	assertTransactionExistsOnShard(t, shardDBs[shardA], "tx_cmd_multishard_a", domain.TransactionStatusPending)
	assertTransactionMissingOnShard(t, shardDBs[shardB], "tx_cmd_multishard_a")
	assertTransactionExistsOnShard(t, shardDBs[shardB], "tx_cmd_multishard_b", domain.TransactionStatusPending)
	assertTransactionMissingOnShard(t, shardDBs[shardA], "tx_cmd_multishard_b")

	balanceShardA, err := queryService.GetAccountBalance(context.Background(), sharding.UserAccountID(userShardA, "USD"))
	if err != nil {
		t.Fatalf("load shard-a account balance: %v", err)
	}
	if balanceShardA.Posted != 100 || balanceShardA.Pending != 30 || balanceShardA.Available != 30 {
		t.Fatalf("unexpected shard-a user balance: %+v", balanceShardA)
	}

	balanceShardB, err := queryService.GetAccountBalance(context.Background(), sharding.UserAccountID(userShardB, "USD"))
	if err != nil {
		t.Fatalf("load shard-b account balance: %v", err)
	}
	if balanceShardB.Posted != 100 || balanceShardB.Pending != 60 || balanceShardB.Available != 60 {
		t.Fatalf("unexpected shard-b user balance: %+v", balanceShardB)
	}

	transactionShardA, err := queryService.GetTransaction(context.Background(), "tx_cmd_multishard_a")
	if err != nil {
		t.Fatalf("query shard-a transaction: %v", err)
	}
	if transactionShardA.ID != "tx_cmd_multishard_a" || transactionShardA.Status != domain.TransactionStatusPending {
		t.Fatalf("unexpected shard-a transaction: %+v", transactionShardA)
	}

	transactionShardB, err := queryService.GetTransaction(context.Background(), "tx_cmd_multishard_b")
	if err != nil {
		t.Fatalf("query shard-b transaction: %v", err)
	}
	if transactionShardB.ID != "tx_cmd_multishard_b" || transactionShardB.Status != domain.TransactionStatusPending {
		t.Fatalf("unexpected shard-b transaction: %+v", transactionShardB)
	}
}

func testUserIDForShard(t *testing.T, router sharding.Router, target sharding.ShardID) string {
	t.Helper()

	for i := 1; i <= 10_000; i++ {
		userID := fmt.Sprintf("user_%d", i)
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

func seedShardCommandExecutionAccounts(t *testing.T, db store.DBTX, userID, systemAccountID string) {
	t.Helper()

	repos := store.NewRepositories(db)
	ctx := context.Background()
	now := time.Date(2026, 3, 23, 7, 0, 0, 0, time.UTC)

	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            sharding.UserAccountID(userID, "USD"),
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion: 10,
			CurrentBalances: domain.BalanceBuckets{
				PostedCredits:  100,
				PendingCredits: 100,
			},
			UpdatedAt: now,
		},
		{
			Account: domain.Account{
				ID:            systemAccountID,
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion:  0,
			CurrentBalances: domain.BalanceBuckets{},
			UpdatedAt:       now,
		},
	}

	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account); err != nil {
			t.Fatalf("seed shard account %s: %v", account.Account.ID, err)
		}
	}
}

func assertTransactionLocator(t *testing.T, centralDB store.DBTX, transactionID string, shardID sharding.ShardID) {
	t.Helper()

	actual, err := store.NewRepositories(centralDB).TransactionLocators.GetByTransactionID(context.Background(), transactionID)
	if err != nil {
		t.Fatalf("load transaction locator %s: %v", transactionID, err)
	}
	if actual != shardID {
		t.Fatalf("expected locator %s -> %s, got %s", transactionID, shardID, actual)
	}
}

func assertTransactionExistsOnShard(t *testing.T, db store.DBTX, transactionID string, status domain.TransactionStatus) {
	t.Helper()

	transaction, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), transactionID)
	if err != nil {
		t.Fatalf("load transaction %s: %v", transactionID, err)
	}
	if transaction.Status != status {
		t.Fatalf("expected transaction %s to have status %s, got %s", transactionID, status, transaction.Status)
	}
}

func assertTransactionMissingOnShard(t *testing.T, db store.DBTX, transactionID string) {
	t.Helper()

	_, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), transactionID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected transaction %s to be missing, got %v", transactionID, err)
	}
}
