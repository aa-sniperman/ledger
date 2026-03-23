package service

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestCommandServiceEnqueueTransactionCreateIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, map[sharding.SystemAccountRole]int{
		sharding.SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	commandService := NewCommandService(db, router, nil)
	systemAccountID, shardID, err := router.SystemAccountForUser("user_123", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}

	envelope, idempotent, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_create_1",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_create_1",
			PostingKey:    "pk_cmd_create_1",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_create_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_create_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue command: %v", err)
	}
	if idempotent {
		t.Fatal("expected first enqueue not to be idempotent")
	}
	if envelope.Status != command.StatusAccepted || envelope.Type != command.TypeTransactionCreate || envelope.ShardID != shardID {
		t.Fatalf("unexpected command envelope: %+v", envelope)
	}

	replayed, idempotent, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_create_1",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_create_1",
			PostingKey:    "pk_cmd_create_1",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_create_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_create_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("replay enqueue command: %v", err)
	}
	if !idempotent || replayed.CommandID != envelope.CommandID {
		t.Fatalf("expected idempotent replay to return same command, got original=%+v replayed=%+v", envelope, replayed)
	}
}

func TestCommandServiceClaimNextIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	commandService := NewCommandService(db, router, nil)
	systemAccountID, shardID, err := router.SystemAccountForUser("user_123", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}

	enqueued, _, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_claim_1",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_claim_1",
			PostingKey:    "pk_cmd_claim_1",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_claim_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_claim_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue command: %v", err)
	}

	claimed, ok, err := commandService.ClaimNext(context.Background(), shardID, time.Date(2026, 3, 22, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("claim next command: %v", err)
	}
	if !ok {
		t.Fatal("expected a command to be claimed")
	}
	if claimed.CommandID != enqueued.CommandID || claimed.Status != command.StatusProcessing {
		t.Fatalf("unexpected claimed command: %+v", claimed)
	}

	_, ok, err = commandService.ClaimNext(context.Background(), shardID, time.Date(2026, 3, 22, 9, 0, 1, 0, time.UTC))
	if err != nil {
		t.Fatalf("claim next command second time: %v", err)
	}
	if ok {
		t.Fatal("expected no further claimable commands after first claim")
	}
}

func TestCommandServiceProcessNextCreatesTransactionIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	commandService := NewCommandService(db, router, nil)
	systemAccountID, shardID, err := router.SystemAccountForUser("user_123", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}

	seedCommandExecutionAccounts(t, db, systemAccountID)

	enqueued, _, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_process_1",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_process_1",
			PostingKey:    "pk_cmd_process_1",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_process_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_process_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue command: %v", err)
	}

	processed, ok, err := commandService.ProcessNext(context.Background(), shardID, time.Date(2026, 3, 22, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("process next command: %v", err)
	}
	if !ok {
		t.Fatal("expected one command to be processed")
	}
	if processed.CommandID != enqueued.CommandID || processed.Status != command.StatusSucceeded {
		t.Fatalf("unexpected processed command: %+v", processed)
	}

	transaction, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), "tx_cmd_process_1")
	if err != nil {
		t.Fatalf("load created transaction: %v", err)
	}
	if transaction.Status != domain.TransactionStatusPending {
		t.Fatalf("expected pending transaction after create command execution, got %s", transaction.Status)
	}
}

func TestCommandServiceProcessNextMarksTerminalFailureIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	commandService := NewCommandService(db, router, nil)
	systemAccountID, shardID, err := router.SystemAccountForUser("user_123", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}

	// Only seed the system account so the create execution fails with missing user account.
	seedCommandExecutionAccounts(t, db, systemAccountID)
	if _, err := db.ExecContext(context.Background(), `DELETE FROM ledger.account_current_balances WHERE account_id = $1`, sharding.UserAccountID("user_123")); err != nil {
		t.Fatalf("delete user balance row: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `DELETE FROM ledger.accounts WHERE account_id = $1`, sharding.UserAccountID("user_123")); err != nil {
		t.Fatalf("delete user account row: %v", err)
	}

	enqueued, _, err := commandService.EnqueueTransactionCreate(context.Background(), EnqueueCreateTransactionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_process_fail_1",
		CreateInput: CreateTransactionInput{
			Flow:          TransactionFlowAuthorizing,
			TransactionID: "tx_cmd_process_fail_1",
			PostingKey:    "pk_cmd_process_fail_1",
			Type:          "wallet_transfer_hold",
			EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
			CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
			Entries: []CreateEntryInput{
				{EntryID: "entry_cmd_process_fail_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
				{EntryID: "entry_cmd_process_fail_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
			},
		},
	})
	if err != nil {
		t.Fatalf("enqueue command: %v", err)
	}

	processed, ok, err := commandService.ProcessNext(context.Background(), shardID, time.Date(2026, 3, 22, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("process next command: %v", err)
	}
	if !ok {
		t.Fatal("expected one command to be processed")
	}
	if processed.CommandID != enqueued.CommandID || processed.Status != command.StatusFailedTerminal {
		t.Fatalf("unexpected processed command: %+v", processed)
	}
}

func TestCommandServiceProcessNextPostsTransactionIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	commandService := NewCommandService(db, router, nil)
	transactionService := NewTransactionService(db)
	systemAccountID, shardID, err := router.SystemAccountForUser("user_123", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}

	seedCommandExecutionAccounts(t, db, systemAccountID)

	if _, _, err := transactionService.Create(context.Background(), CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_cmd_post_1",
		PostingKey:    "pk_cmd_post_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 22, 8, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 22, 8, 0, 1, 0, time.UTC),
		Entries: []CreateEntryInput{
			{EntryID: "entry_cmd_post_1_debit", AccountID: sharding.UserAccountID("user_123"), Amount: 70, Currency: "USD", Direction: "debit"},
			{EntryID: "entry_cmd_post_1_credit", AccountID: systemAccountID, Amount: 70, Currency: "USD", Direction: "credit"},
		},
	}); err != nil {
		t.Fatalf("seed pending transaction: %v", err)
	}

	enqueued, _, err := commandService.EnqueueTransactionPost(context.Background(), EnqueueTransitionCommandInput{
		UserID:         "user_123",
		IdempotencyKey: "idem_cmd_post_1",
		TransactionID:  "tx_cmd_post_1",
	})
	if err != nil {
		t.Fatalf("enqueue post command: %v", err)
	}

	processed, ok, err := commandService.ProcessNext(context.Background(), shardID, time.Date(2026, 3, 22, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("process next command: %v", err)
	}
	if !ok {
		t.Fatal("expected one command to be processed")
	}
	if processed.CommandID != enqueued.CommandID || processed.Status != command.StatusSucceeded {
		t.Fatalf("unexpected processed command: %+v", processed)
	}

	transaction, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), "tx_cmd_post_1")
	if err != nil {
		t.Fatalf("load posted transaction: %v", err)
	}
	if transaction.Status != domain.TransactionStatusPosted {
		t.Fatalf("expected posted transaction after command execution, got %s", transaction.Status)
	}
}

func seedCommandExecutionAccounts(t *testing.T, db *sql.DB, systemAccountID string) {
	t.Helper()

	repos := store.NewRepositories(db)
	ctx := context.Background()
	now := time.Date(2026, 3, 22, 7, 0, 0, 0, time.UTC)

	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            sharding.UserAccountID("user_123"),
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion: 10,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  100,
				PendingDebits:  0,
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
			CurrentVersion: 0,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  0,
				PendingDebits:  0,
				PendingCredits: 0,
			},
			UpdatedAt: now,
		},
	}

	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account); err != nil {
			t.Fatalf("seed command execution account %s: %v", account.Account.ID, err)
		}
	}
}
