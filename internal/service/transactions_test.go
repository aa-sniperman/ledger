package service

import (
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/domain"
)

func TestBuildTransactionAuthorizingForcesPendingStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	transaction, err := buildTransaction(CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_1",
		PostingKey:    "pk_1",
		Type:          "wallet_transfer",
		EffectiveAt:   now,
		CreatedAt:     now,
		Entries: []CreateEntryInput{
			testCreateEntry("entry_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
			testCreateEntry("entry_2", "bob_wallet", 100, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("expected authorizing build to succeed, got %v", err)
	}

	if transaction.Status != domain.TransactionStatusPending {
		t.Fatalf("expected pending status, got %s", transaction.Status)
	}

	for _, entry := range transaction.Entries {
		if entry.Status != domain.EntryStatusPending {
			t.Fatalf("expected pending entry status, got %s", entry.Status)
		}
	}
}

func TestBuildTransactionAuthorizingRejectsDirectPostedStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	_, err := buildTransaction(CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: "tx_1",
		PostingKey:    "pk_1",
		Type:          "wallet_transfer",
		Status:        domain.TransactionStatusPosted,
		EffectiveAt:   now,
		CreatedAt:     now,
		Entries: []CreateEntryInput{
			testCreateEntry("entry_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
			testCreateEntry("entry_2", "bob_wallet", 100, "USD", domain.DirectionCredit),
		},
	})
	if err == nil {
		t.Fatal("expected error for direct posted authorizing transaction")
	}
}

func TestBuildTransactionRecordingAllowsPostedStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	transaction, err := buildTransaction(CreateTransactionInput{
		Flow:          TransactionFlowRecording,
		TransactionID: "tx_1",
		PostingKey:    "pk_1",
		Type:          "cash_in_settlement",
		Status:        domain.TransactionStatusPosted,
		EffectiveAt:   now,
		CreatedAt:     now,
		Entries: []CreateEntryInput{
			testCreateEntry("entry_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
			testCreateEntry("entry_2", "rail_settlement", 100, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("expected recording build to succeed, got %v", err)
	}

	if transaction.Status != domain.TransactionStatusPosted {
		t.Fatalf("expected posted status, got %s", transaction.Status)
	}

	for _, entry := range transaction.Entries {
		if entry.Status != domain.EntryStatusPosted {
			t.Fatalf("expected posted entry status, got %s", entry.Status)
		}
		if entry.TransactionID != transaction.ID {
			t.Fatalf("expected entry transaction_id %s, got %s", transaction.ID, entry.TransactionID)
		}
	}
}

func TestStatusForFlowRejectsInvalidFlow(t *testing.T) {
	t.Parallel()

	_, err := statusForFlow(TransactionFlow("bad"), domain.TransactionStatusPending)
	if err == nil {
		t.Fatal("expected invalid flow error")
	}
}

func TestStatusForFlowRecordingRequiresValidStatus(t *testing.T) {
	t.Parallel()

	_, err := statusForFlow(TransactionFlowRecording, domain.TransactionStatus("bad"))
	if err == nil {
		t.Fatal("expected invalid status error")
	}
}

func TestEntryStatusForTransactionStatusRejectsInvalidStatus(t *testing.T) {
	t.Parallel()

	_, err := entryStatusForTransactionStatus(domain.TransactionStatus("bad"))
	if err == nil {
		t.Fatal("expected invalid status error")
	}
}

func TestCollectAccountIDsDeduplicates(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ids := collectAccountIDs([]domain.Entry{
		{AccountID: "alice_wallet", EffectiveAt: now, CreatedAt: now},
		{AccountID: "alice_wallet", EffectiveAt: now, CreatedAt: now},
		{AccountID: "bob_wallet", EffectiveAt: now, CreatedAt: now},
	})

	if len(ids) != 2 {
		t.Fatalf("expected 2 unique account ids, got %d", len(ids))
	}
}

func TestBuildTransactionPreservesEffectiveAt(t *testing.T) {
	t.Parallel()

	effectiveAt := time.Date(2026, 3, 11, 8, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 3, 11, 8, 0, 2, 0, time.UTC)

	transaction, err := buildTransaction(CreateTransactionInput{
		Flow:          TransactionFlowRecording,
		TransactionID: "tx_1",
		PostingKey:    "pk_1",
		Type:          "cash_in_settlement",
		Status:        domain.TransactionStatusPosted,
		EffectiveAt:   effectiveAt,
		CreatedAt:     createdAt,
		Entries: []CreateEntryInput{
			testCreateEntry("entry_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
			testCreateEntry("entry_2", "rail_settlement", 100, "USD", domain.DirectionCredit),
		},
	})
	if err != nil {
		t.Fatalf("expected build to succeed, got %v", err)
	}

	if !transaction.EffectiveAt.Equal(effectiveAt) {
		t.Fatalf("expected effective_at %v, got %v", effectiveAt, transaction.EffectiveAt)
	}

	for _, entry := range transaction.Entries {
		if !entry.EffectiveAt.Equal(effectiveAt) {
			t.Fatalf("expected entry effective_at %v, got %v", effectiveAt, entry.EffectiveAt)
		}
		if !entry.CreatedAt.Equal(createdAt) {
			t.Fatalf("expected entry created_at %v, got %v", createdAt, entry.CreatedAt)
		}
	}
}

func TestBuildBalanceChangesPendingTransaction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	transaction, err := domain.NewTransaction(
		"tx_1",
		"pk_1",
		"wallet_transfer",
		domain.TransactionStatusPending,
		now,
		now,
		[]domain.Entry{
			{
				ID:             "entry_1",
				TransactionID:  "tx_1",
				AccountID:      "alice_wallet",
				AccountVersion: 3,
				Money:          domain.Money{Amount: 100, Currency: "USD"},
				Direction:      domain.DirectionDebit,
				Status:         domain.EntryStatusPending,
				EffectiveAt:    now,
				CreatedAt:      now,
			},
			{
				ID:             "entry_2",
				TransactionID:  "tx_1",
				AccountID:      "bob_wallet",
				AccountVersion: 7,
				Money:          domain.Money{Amount: 100, Currency: "USD"},
				Direction:      domain.DirectionCredit,
				Status:         domain.EntryStatusPending,
				EffectiveAt:    now,
				CreatedAt:      now,
			},
		},
	)
	if err != nil {
		t.Fatalf("unexpected transaction build error: %v", err)
	}

	changes, err := buildBalanceChanges(transaction)
	if err != nil {
		t.Fatalf("unexpected buildBalanceChanges error: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("expected 2 balance changes, got %d", len(changes))
	}

	if changes[0].Delta.PendingDebits != 100 {
		t.Fatalf("expected pending debit delta 100, got %+v", changes[0].Delta)
	}

	if changes[1].Delta.PendingCredits != 100 {
		t.Fatalf("expected pending credit delta 100, got %+v", changes[1].Delta)
	}
}

func TestBuildBalanceChangesPostedTransaction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	transaction, err := domain.NewTransaction(
		"tx_1",
		"pk_1",
		"cash_in_settlement",
		domain.TransactionStatusPosted,
		now,
		now,
		[]domain.Entry{
			{
				ID:             "entry_1",
				TransactionID:  "tx_1",
				AccountID:      "alice_wallet",
				AccountVersion: 3,
				Money:          domain.Money{Amount: 100, Currency: "USD"},
				Direction:      domain.DirectionDebit,
				Status:         domain.EntryStatusPosted,
				EffectiveAt:    now,
				CreatedAt:      now,
			},
			{
				ID:             "entry_2",
				TransactionID:  "tx_1",
				AccountID:      "rail_settlement",
				AccountVersion: 11,
				Money:          domain.Money{Amount: 100, Currency: "USD"},
				Direction:      domain.DirectionCredit,
				Status:         domain.EntryStatusPosted,
				EffectiveAt:    now,
				CreatedAt:      now,
			},
		},
	)
	if err != nil {
		t.Fatalf("unexpected transaction build error: %v", err)
	}

	changes, err := buildBalanceChanges(transaction)
	if err != nil {
		t.Fatalf("unexpected buildBalanceChanges error: %v", err)
	}

	if changes[0].Delta.PostedDebits != 100 || changes[0].Delta.PendingDebits != 100 {
		t.Fatalf("expected posted and pending debit delta 100, got %+v", changes[0].Delta)
	}

	if changes[1].Delta.PostedCredits != 100 || changes[1].Delta.PendingCredits != 100 {
		t.Fatalf("expected posted and pending credit delta 100, got %+v", changes[1].Delta)
	}
}

func testCreateEntry(entryID, accountID string, amount int64, currency string, direction domain.Direction) CreateEntryInput {
	return CreateEntryInput{
		EntryID:   entryID,
		AccountID: accountID,
		Amount:    amount,
		Currency:  currency,
		Direction: direction,
	}
}
