package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/store"
)

type AccountBalanceView struct {
	AccountID      string
	Currency       string
	NormalBalance  domain.NormalBalance
	CurrentVersion int64
	Posted         int64
	Pending        int64
	Available      int64
	UpdatedAt      time.Time
}

type AccountService struct {
	db *sql.DB
}

func NewAccountService(db *sql.DB) *AccountService {
	return &AccountService{db: db}
}

func (s *AccountService) GetBalance(ctx context.Context, accountID string) (AccountBalanceView, error) {
	state, err := store.NewRepositories(s.db).Accounts.GetByID(ctx, accountID)
	if err != nil {
		return AccountBalanceView{}, err
	}

	return buildAccountBalanceView(state)
}

func buildAccountBalanceView(state domain.AccountState) (AccountBalanceView, error) {
	if err := state.Validate(); err != nil {
		return AccountBalanceView{}, err
	}

	posted, err := state.CurrentBalances.Posted(state.Account.NormalBalance)
	if err != nil {
		return AccountBalanceView{}, fmt.Errorf("compute posted balance: %w", err)
	}

	pending, err := state.CurrentBalances.Pending(state.Account.NormalBalance)
	if err != nil {
		return AccountBalanceView{}, fmt.Errorf("compute pending balance: %w", err)
	}

	available, err := state.CurrentBalances.Available(state.Account.NormalBalance)
	if err != nil {
		return AccountBalanceView{}, fmt.Errorf("compute available balance: %w", err)
	}

	return AccountBalanceView{
		AccountID:      state.Account.ID,
		Currency:       state.Account.Currency,
		NormalBalance:  state.Account.NormalBalance,
		CurrentVersion: state.CurrentVersion,
		Posted:         posted,
		Pending:        pending,
		Available:      available,
		UpdatedAt:      state.UpdatedAt,
	}, nil
}
