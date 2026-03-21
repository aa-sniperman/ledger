package service

import (
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/domain"
)

func TestBuildAccountBalanceViewCreditNormal(t *testing.T) {
	t.Parallel()

	view, err := buildAccountBalanceView(domain.AccountState{
		Account: domain.Account{
			ID:            "alice_wallet",
			Currency:      "USD",
			NormalBalance: domain.NormalBalanceCredit,
		},
		CurrentVersion: 12,
		CurrentBalances: domain.BalanceBuckets{
			PostedDebits:   10,
			PostedCredits:  100,
			PendingDebits:  20,
			PendingCredits: 110,
		},
		UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("expected balance view to build, got %v", err)
	}

	if view.Posted != 90 || view.Pending != 90 || view.Available != 80 {
		t.Fatalf("unexpected credit-normal balances: %+v", view)
	}
}

func TestBuildAccountBalanceViewDebitNormal(t *testing.T) {
	t.Parallel()

	view, err := buildAccountBalanceView(domain.AccountState{
		Account: domain.Account{
			ID:            "asset_pool",
			Currency:      "USD",
			NormalBalance: domain.NormalBalanceDebit,
		},
		CurrentVersion: 7,
		CurrentBalances: domain.BalanceBuckets{
			PostedDebits:   100,
			PostedCredits:  30,
			PendingDebits:  120,
			PendingCredits: 50,
		},
		UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("expected balance view to build, got %v", err)
	}

	if view.Posted != 70 || view.Pending != 70 || view.Available != 50 {
		t.Fatalf("unexpected debit-normal balances: %+v", view)
	}
}

func TestBuildAccountBalanceViewRejectsInvalidState(t *testing.T) {
	t.Parallel()

	_, err := buildAccountBalanceView(domain.AccountState{
		Account: domain.Account{
			ID:            "broken",
			Currency:      "USD",
			NormalBalance: domain.NormalBalanceCredit,
		},
		CurrentBalances: domain.BalanceBuckets{
			PostedDebits:   10,
			PostedCredits:  5,
			PendingDebits:  9,
			PendingCredits: 5,
		},
	})
	if err == nil {
		t.Fatal("expected invalid state error")
	}
}
