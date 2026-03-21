package service

import (
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/domain"
)

func TestBuildTransitionBalanceChangesPostedAddsOnlyPostedBuckets(t *testing.T) {
	t.Parallel()

	entries := []domain.Entry{
		testPendingEntry("entry_1", "tx_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
		testPendingEntry("entry_2", "tx_1", "bob_wallet", 100, "USD", domain.DirectionCredit),
	}

	changes, err := buildTransitionBalanceChanges(entries, domain.TransactionStatusPosted)
	if err != nil {
		t.Fatalf("expected post balance change build to succeed, got %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("expected 2 account changes, got %d", len(changes))
	}

	if changes[0].Delta.PostedDebits != 100 || changes[0].Delta.PendingDebits != 0 {
		t.Fatalf("expected posted-only debit delta, got %+v", changes[0].Delta)
	}

	if changes[1].Delta.PostedCredits != 100 || changes[1].Delta.PendingCredits != 0 {
		t.Fatalf("expected posted-only credit delta, got %+v", changes[1].Delta)
	}
}

func TestBuildTransitionBalanceChangesArchivedRemovesPendingBuckets(t *testing.T) {
	t.Parallel()

	entries := []domain.Entry{
		testPendingEntry("entry_1", "tx_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
		testPendingEntry("entry_2", "tx_1", "bob_wallet", 100, "USD", domain.DirectionCredit),
	}

	changes, err := buildTransitionBalanceChanges(entries, domain.TransactionStatusArchived)
	if err != nil {
		t.Fatalf("expected archive balance change build to succeed, got %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("expected 2 account changes, got %d", len(changes))
	}

	if changes[0].Delta.PendingDebits != -100 || changes[0].Delta.PostedDebits != 0 {
		t.Fatalf("expected pending debit release, got %+v", changes[0].Delta)
	}

	if changes[1].Delta.PendingCredits != -100 || changes[1].Delta.PostedCredits != 0 {
		t.Fatalf("expected pending credit release, got %+v", changes[1].Delta)
	}
}

func TestBuildTransitionBalanceChangesRejectsNonPendingEntry(t *testing.T) {
	t.Parallel()

	_, err := buildTransitionBalanceChanges([]domain.Entry{
		{
			ID:            "entry_1",
			TransactionID: "tx_1",
			AccountID:     "alice_wallet",
			Money:         domain.Money{Amount: 100, Currency: "USD"},
			Direction:     domain.DirectionDebit,
			Status:        domain.EntryStatusPosted,
			EffectiveAt:   time.Now().UTC(),
			CreatedAt:     time.Now().UTC(),
		},
	}, domain.TransactionStatusPosted)
	if err == nil {
		t.Fatal("expected non-pending source entry error")
	}
}

func TestBuildTransitionEntriesUsesAssignedVersions(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 3, 21, 8, 0, 0, 0, time.UTC)
	entries, err := buildTransitionEntries([]domain.Entry{
		testPendingEntry("entry_1", "tx_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
		testPendingEntry("entry_2", "tx_1", "bob_wallet", 100, "USD", domain.DirectionCredit),
	}, domain.TransactionStatusPosted, createdAt, map[string]int64{
		"alice_wallet": 13,
		"bob_wallet":   8,
	})
	if err != nil {
		t.Fatalf("expected transition entries to build, got %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 transition entries, got %d", len(entries))
	}

	if entries[0].ID != "entry_1_posted" || entries[0].Status != domain.EntryStatusPosted || entries[0].AccountVersion != 13 {
		t.Fatalf("unexpected first transition entry: %+v", entries[0])
	}

	if !entries[0].CreatedAt.Equal(createdAt) || !entries[0].EffectiveAt.Equal(testPendingEntry("entry_1", "tx_1", "alice_wallet", 100, "USD", domain.DirectionDebit).EffectiveAt) {
		t.Fatalf("expected transition timestamps to preserve effective_at and use new created_at")
	}
}

func TestBuildTransitionEntriesRequiresAssignedVersion(t *testing.T) {
	t.Parallel()

	_, err := buildTransitionEntries([]domain.Entry{
		testPendingEntry("entry_1", "tx_1", "alice_wallet", 100, "USD", domain.DirectionDebit),
	}, domain.TransactionStatusArchived, time.Now().UTC(), map[string]int64{})
	if err == nil {
		t.Fatal("expected missing version error")
	}
}

func testPendingEntry(entryID, transactionID, accountID string, amount int64, currency string, direction domain.Direction) domain.Entry {
	now := time.Date(2026, 3, 11, 8, 0, 0, 0, time.UTC)
	return domain.Entry{
		ID:            entryID,
		TransactionID: transactionID,
		AccountID:     accountID,
		Money:         domain.Money{Amount: amount, Currency: currency},
		Direction:     direction,
		Status:        domain.EntryStatusPending,
		EffectiveAt:   now,
		CreatedAt:     now,
	}
}
