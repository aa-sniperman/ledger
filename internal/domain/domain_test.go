package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewMoneyRejectsZeroAmount(t *testing.T) {
	t.Parallel()

	_, err := NewMoney(0, "USD")
	if !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("expected ErrInvalidMoney, got %v", err)
	}
}

func TestAccountValidateRejectsUnsupportedNormalBalance(t *testing.T) {
	t.Parallel()

	account := Account{
		ID:            "alice_wallet",
		Currency:      "USD",
		NormalBalance: "bad_value",
	}

	err := account.Validate()
	if !errors.Is(err, ErrInvalidAccount) {
		t.Fatalf("expected ErrInvalidAccount, got %v", err)
	}
}

func TestNewPendingTransactionRequiresAtLeastTwoEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	_, err := NewPendingTransaction(
		"tx_1",
		"pk_1",
		"wallet_transfer",
		now,
		now,
		[]Entry{testEntry("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, now)},
	)
	if !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("expected ErrInvalidTransaction, got %v", err)
	}
}

func TestNewPendingTransactionRejectsUnbalancedEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	entries := []Entry{
		testEntry("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, now),
		testEntry("entry_2", "bob_wallet", 90, "USD", 7, DirectionCredit, now),
	}

	_, err := NewPendingTransaction("tx_1", "pk_1", "wallet_transfer", now, now, entries)
	if !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("expected ErrInvalidTransaction, got %v", err)
	}
}

func TestNewPendingTransactionAcceptsBalancedEntriesAcrossCurrencies(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	entries := []Entry{
		testEntry("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, now),
		testEntry("entry_2", "settlement_wallet", 100, "USD", 11, DirectionCredit, now),
		testEntry("entry_3", "alice_wallet_vnd", 200, "VND", 5, DirectionDebit, now),
		testEntry("entry_4", "settlement_wallet_vnd", 200, "VND", 9, DirectionCredit, now),
	}

	transaction, err := NewPendingTransaction("tx_1", "pk_1", "wallet_transfer", now, now, entries)
	if err != nil {
		t.Fatalf("expected transaction to be valid, got %v", err)
	}

	if transaction.Status != TransactionStatusPending {
		t.Fatalf("expected status pending, got %s", transaction.Status)
	}
}

func TestNewTransactionAllowsDirectPostedCreate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	transaction, err := NewTransaction(
		"tx_1",
		"pk_1",
		"cash_in_settlement",
		TransactionStatusPosted,
		now,
		now,
		[]Entry{
			testEntryWithStatus("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, EntryStatusPosted, now),
			testEntryWithStatus("entry_2", "rail_settlement", 100, "USD", 11, DirectionCredit, EntryStatusPosted, now),
		},
	)
	if err != nil {
		t.Fatalf("expected direct posted transaction to be valid, got %v", err)
	}

	if transaction.Status != TransactionStatusPosted {
		t.Fatalf("expected status posted, got %s", transaction.Status)
	}
}

func TestNewTransactionRejectsEntryStatusMismatch(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	_, err := NewTransaction(
		"tx_1",
		"pk_1",
		"cash_in_settlement",
		TransactionStatusPosted,
		now,
		now,
		[]Entry{
			testEntryWithStatus("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, EntryStatusPending, now),
			testEntryWithStatus("entry_2", "rail_settlement", 100, "USD", 11, DirectionCredit, EntryStatusPosted, now),
		},
	)
	if !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("expected ErrInvalidTransaction, got %v", err)
	}
}

func TestTransactionValidateAccountsRejectsMissingAccount(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	transaction, err := NewPendingTransaction(
		"tx_1",
		"pk_1",
		"wallet_transfer",
		now,
		now,
		[]Entry{
			testEntry("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, now),
			testEntry("entry_2", "bob_wallet", 100, "USD", 7, DirectionCredit, now),
		},
	)
	if err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}

	accounts := map[string]AccountState{
		"alice_wallet": {
			Account: Account{
				ID:            "alice_wallet",
				Currency:      "USD",
				NormalBalance: NormalBalanceCredit,
			},
			CurrentVersion: 3,
		},
	}

	err = transaction.ValidateAccounts(accounts)
	if !errors.Is(err, ErrMissingAccount) {
		t.Fatalf("expected ErrMissingAccount, got %v", err)
	}
}

func TestTransactionValidateAccountsRejectsCurrencyMismatch(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	transaction, err := NewPendingTransaction(
		"tx_1",
		"pk_1",
		"wallet_transfer",
		now,
		now,
		[]Entry{
			testEntry("entry_1", "alice_wallet", 100, "USD", 3, DirectionDebit, now),
			testEntry("entry_2", "bob_wallet", 100, "USD", 7, DirectionCredit, now),
		},
	)
	if err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}

	accounts := map[string]AccountState{
		"alice_wallet": {
			Account: Account{
				ID:            "alice_wallet",
				Currency:      "USD",
				NormalBalance: NormalBalanceCredit,
			},
			CurrentVersion: 3,
		},
		"bob_wallet": {
			Account: Account{
				ID:            "bob_wallet",
				Currency:      "VND",
				NormalBalance: NormalBalanceCredit,
			},
			CurrentVersion: 7,
		},
	}

	err = transaction.ValidateAccounts(accounts)
	if !errors.Is(err, ErrAccountCurrencyMismatch) {
		t.Fatalf("expected ErrAccountCurrencyMismatch, got %v", err)
	}
}

func TestTransactionPostTransitionsPendingToPosted(t *testing.T) {
	t.Parallel()

	transaction := Transaction{
		Status: TransactionStatusPending,
	}
	if err := transaction.Post(); err != nil {
		t.Fatalf("expected post to succeed, got %v", err)
	}

	if transaction.Status != TransactionStatusPosted {
		t.Fatalf("expected status posted, got %s", transaction.Status)
	}
}

func TestTransactionArchiveRejectsPostedTransaction(t *testing.T) {
	t.Parallel()

	transaction := Transaction{
		Status: TransactionStatusPosted,
	}
	err := transaction.Archive()
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestBalanceBucketsForCreditNormalAccount(t *testing.T) {
	t.Parallel()

	buckets := BalanceBuckets{
		PostedDebits:   10,
		PostedCredits:  100,
		PendingDebits:  20,
		PendingCredits: 100,
	}

	posted, err := buckets.Posted(NormalBalanceCredit)
	if err != nil {
		t.Fatalf("unexpected posted error: %v", err)
	}

	pending, err := buckets.Pending(NormalBalanceCredit)
	if err != nil {
		t.Fatalf("unexpected pending error: %v", err)
	}

	available, err := buckets.Available(NormalBalanceCredit)
	if err != nil {
		t.Fatalf("unexpected available error: %v", err)
	}

	if posted != 90 {
		t.Fatalf("expected posted 90, got %d", posted)
	}

	if pending != 80 {
		t.Fatalf("expected pending 80, got %d", pending)
	}

	if available != 80 {
		t.Fatalf("expected available 80, got %d", available)
	}
}

func TestBalanceBucketsForDebitNormalAccount(t *testing.T) {
	t.Parallel()

	buckets := BalanceBuckets{
		PostedDebits:   100,
		PostedCredits:  10,
		PendingDebits:  100,
		PendingCredits: 30,
	}

	posted, err := buckets.Posted(NormalBalanceDebit)
	if err != nil {
		t.Fatalf("unexpected posted error: %v", err)
	}

	pending, err := buckets.Pending(NormalBalanceDebit)
	if err != nil {
		t.Fatalf("unexpected pending error: %v", err)
	}

	available, err := buckets.Available(NormalBalanceDebit)
	if err != nil {
		t.Fatalf("unexpected available error: %v", err)
	}

	if posted != 90 {
		t.Fatalf("expected posted 90, got %d", posted)
	}

	if pending != 70 {
		t.Fatalf("expected pending 70, got %d", pending)
	}

	if available != 70 {
		t.Fatalf("expected available 70, got %d", available)
	}
}

func testEntry(id, accountID string, amount int64, currency string, accountVersion int64, direction Direction, now time.Time) Entry {
	return testEntryWithStatus(id, accountID, amount, currency, accountVersion, direction, EntryStatusPending, now)
}

func testEntryWithStatus(id, accountID string, amount int64, currency string, accountVersion int64, direction Direction, status EntryStatus, now time.Time) Entry {
	return Entry{
		ID:             id,
		AccountID:      accountID,
		AccountVersion: accountVersion,
		Money:          Money{Amount: amount, Currency: currency},
		Direction:      direction,
		Status:         status,
		EffectiveAt:    now,
		CreatedAt:      now,
	}
}
