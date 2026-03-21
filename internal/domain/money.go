package domain

import (
	"fmt"
	"strings"
)

type Money struct {
	Amount   int64
	Currency string
}

func NewMoney(amount int64, currency string) (Money, error) {
	money := Money{
		Amount:   amount,
		Currency: currency,
	}

	if err := money.ValidatePositive(); err != nil {
		return Money{}, err
	}

	return money, nil
}

func (m Money) Validate() error {
	if strings.TrimSpace(m.Currency) == "" {
		return fmt.Errorf("%w: currency is required", ErrInvalidMoney)
	}

	if m.Amount < 0 {
		return fmt.Errorf("%w: amount must not be negative", ErrInvalidMoney)
	}

	return nil
}

func (m Money) ValidatePositive() error {
	if err := m.Validate(); err != nil {
		return err
	}

	if m.Amount == 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidMoney)
	}

	return nil
}
