package service

import (
	"context"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestTransactionServiceCreateAuthorizingIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	ctx := context.Background()
	seedIntegrationAccounts(t, db)

	transactionService := NewTransactionService(db)
	accountService := NewAccountService(db)

	transaction, idempotent, err := transactionService.Create(ctx, CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_auth_create_1",
		PostingKey:    "pk_auth_create_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []CreateEntryInput{
			integrationCreateEntry("entry_auth_create_1_debit", "alice_wallet", 70, "USD", domain.DirectionDebit),
			integrationCreateEntry("entry_auth_create_1_credit", "partner_payout_holding", 70, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("expected create to succeed, got %v", err)
	}

	if idempotent {
		t.Fatal("expected first create not to be idempotent")
	}

	if transaction.Status != domain.TransactionStatusPending {
		t.Fatalf("expected pending transaction, got %s", transaction.Status)
	}

	aliceBalance, err := accountService.GetBalance(ctx, "alice_wallet")
	if err != nil {
		t.Fatalf("get alice balance: %v", err)
	}
	if aliceBalance.Posted != 100 || aliceBalance.Pending != 30 || aliceBalance.Available != 30 {
		t.Fatalf("unexpected alice balance after create: %+v", aliceBalance)
	}

	holdingBalance, err := accountService.GetBalance(ctx, "partner_payout_holding")
	if err != nil {
		t.Fatalf("get holding balance: %v", err)
	}
	if holdingBalance.Posted != 0 || holdingBalance.Pending != 70 || holdingBalance.Available != 0 {
		t.Fatalf("unexpected holding balance after create: %+v", holdingBalance)
	}
}

func TestTransactionServiceCreateDuplicatePostingKeyIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	ctx := context.Background()
	seedIntegrationAccounts(t, db)

	transactionService := NewTransactionService(db)
	input := CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_dup_1",
		PostingKey:    "pk_dup_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []CreateEntryInput{
			integrationCreateEntry("entry_dup_1_debit", "alice_wallet", 70, "USD", domain.DirectionDebit),
			integrationCreateEntry("entry_dup_1_credit", "partner_payout_holding", 70, "USD", domain.DirectionCredit),
		},
	}

	first, idempotent, err := transactionService.Create(ctx, input)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if idempotent {
		t.Fatal("expected first create not to be idempotent")
	}

	second, idempotent, err := transactionService.Create(ctx, input)
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if !idempotent {
		t.Fatal("expected second create to be idempotent")
	}
	if second.ID != first.ID || second.PostingKey != first.PostingKey {
		t.Fatalf("expected idempotent replay to return same transaction, got first=%+v second=%+v", first, second)
	}

	entries, err := store.NewRepositories(db).Transactions.ListEntriesByTransactionID(ctx, first.ID)
	if err != nil {
		t.Fatalf("list entries after idempotent replay: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected only original 2 entries, got %d", len(entries))
	}
}

func TestTransactionServicePostPendingIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	ctx := context.Background()
	seedIntegrationAccounts(t, db)

	transactionService := NewTransactionService(db)
	accountService := NewAccountService(db)

	created, _, err := transactionService.Create(ctx, CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_post_1",
		PostingKey:    "pk_post_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []CreateEntryInput{
			integrationCreateEntry("entry_post_1_debit", "alice_wallet", 70, "USD", domain.DirectionDebit),
			integrationCreateEntry("entry_post_1_credit", "partner_payout_holding", 70, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("seed pending transaction failed: %v", err)
	}

	posted, err := transactionService.Post(ctx, created.ID)
	if err != nil {
		t.Fatalf("post transaction failed: %v", err)
	}

	if posted.Status != domain.TransactionStatusPosted {
		t.Fatalf("expected posted transaction, got %s", posted.Status)
	}

	aliceBalance, err := accountService.GetBalance(ctx, "alice_wallet")
	if err != nil {
		t.Fatalf("get alice balance: %v", err)
	}
	if aliceBalance.Posted != 30 || aliceBalance.Pending != 30 || aliceBalance.Available != 30 {
		t.Fatalf("unexpected alice balance after post: %+v", aliceBalance)
	}

	holdingBalance, err := accountService.GetBalance(ctx, "partner_payout_holding")
	if err != nil {
		t.Fatalf("get holding balance: %v", err)
	}
	if holdingBalance.Posted != 70 || holdingBalance.Pending != 70 || holdingBalance.Available != 70 {
		t.Fatalf("unexpected holding balance after post: %+v", holdingBalance)
	}

	entries, err := store.NewRepositories(db).Transactions.ListEntriesByTransactionID(ctx, created.ID)
	if err != nil {
		t.Fatalf("list entries after post: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries after post transition, got %d", len(entries))
	}
}

func TestTransactionServiceArchivePendingIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	ctx := context.Background()
	seedIntegrationAccounts(t, db)

	transactionService := NewTransactionService(db)
	accountService := NewAccountService(db)

	created, _, err := transactionService.Create(ctx, CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_archive_1",
		PostingKey:    "pk_archive_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []CreateEntryInput{
			integrationCreateEntry("entry_archive_1_debit", "alice_wallet", 70, "USD", domain.DirectionDebit),
			integrationCreateEntry("entry_archive_1_credit", "partner_payout_holding", 70, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("seed pending transaction failed: %v", err)
	}

	archived, err := transactionService.Archive(ctx, created.ID)
	if err != nil {
		t.Fatalf("archive transaction failed: %v", err)
	}

	if archived.Status != domain.TransactionStatusArchived {
		t.Fatalf("expected archived transaction, got %s", archived.Status)
	}

	aliceBalance, err := accountService.GetBalance(ctx, "alice_wallet")
	if err != nil {
		t.Fatalf("get alice balance: %v", err)
	}
	if aliceBalance.Posted != 100 || aliceBalance.Pending != 100 || aliceBalance.Available != 100 {
		t.Fatalf("unexpected alice balance after archive: %+v", aliceBalance)
	}

	holdingBalance, err := accountService.GetBalance(ctx, "partner_payout_holding")
	if err != nil {
		t.Fatalf("get holding balance: %v", err)
	}
	if holdingBalance.Posted != 0 || holdingBalance.Pending != 0 || holdingBalance.Available != 0 {
		t.Fatalf("unexpected holding balance after archive: %+v", holdingBalance)
	}

	entries, err := store.NewRepositories(db).Transactions.ListEntriesByTransactionID(ctx, created.ID)
	if err != nil {
		t.Fatalf("list entries after archive: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries after archive transition, got %d", len(entries))
	}
}

func seedIntegrationAccounts(t *testing.T, db store.DBTX) {
	t.Helper()

	ctx := context.Background()
	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            "alice_wallet",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
			},
			CurrentVersion: 10,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  100,
				PendingDebits:  0,
				PendingCredits: 100,
			},
			UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
		},
		{
			Account: domain.Account{
				ID:            "partner_payout_holding",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
			},
			CurrentVersion: 3,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  0,
				PendingDebits:  0,
				PendingCredits: 0,
			},
			UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
		},
	}

	repos := store.NewRepositories(db)
	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account); err != nil {
			t.Fatalf("seed account %s: %v", account.Account.ID, err)
		}
	}
}

func integrationCreateEntry(entryID, accountID string, amount int64, currency string, direction domain.Direction) CreateEntryInput {
	return CreateEntryInput{
		EntryID:   entryID,
		AccountID: accountID,
		Amount:    amount,
		Currency:  currency,
		Direction: direction,
	}
}
