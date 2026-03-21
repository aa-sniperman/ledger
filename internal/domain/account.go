package domain

import (
	"fmt"
	"strings"
	"time"
)

type NormalBalance string

const (
	NormalBalanceCredit NormalBalance = "credit_normal"
	NormalBalanceDebit  NormalBalance = "debit_normal"
)

type Account struct {
	ID            string
	Currency      string
	NormalBalance NormalBalance
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type AccountState struct {
	Account         Account
	CurrentVersion  int64
	CurrentBalances BalanceBuckets
	UpdatedAt       time.Time
}

func (n NormalBalance) Validate() error {
	switch n {
	case NormalBalanceCredit, NormalBalanceDebit:
		return nil
	default:
		return fmt.Errorf("%w: unsupported normal balance %q", ErrInvalidAccount, n)
	}
}

func (a Account) Validate() error {
	if strings.TrimSpace(a.ID) == "" {
		return fmt.Errorf("%w: account id is required", ErrInvalidAccount)
	}

	if strings.TrimSpace(a.Currency) == "" {
		return fmt.Errorf("%w: currency is required", ErrInvalidAccount)
	}

	if err := a.NormalBalance.Validate(); err != nil {
		return err
	}

	return nil
}

func (s AccountState) Validate() error {
	if err := s.Account.Validate(); err != nil {
		return err
	}

	if s.CurrentVersion < 0 {
		return fmt.Errorf("%w: current version must not be negative", ErrInvalidAccount)
	}

	if err := s.CurrentBalances.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidAccount, err)
	}

	return nil
}
