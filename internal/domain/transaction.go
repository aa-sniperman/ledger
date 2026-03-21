package domain

import (
	"fmt"
	"strings"
	"time"
)

type TransactionStatus string

const (
	TransactionStatusPending  TransactionStatus = "pending"
	TransactionStatusPosted   TransactionStatus = "posted"
	TransactionStatusArchived TransactionStatus = "archived"
)

type EntryStatus string

const (
	EntryStatusPending  EntryStatus = "pending"
	EntryStatusPosted   EntryStatus = "posted"
	EntryStatusArchived EntryStatus = "archived"
)

type Direction string

const (
	DirectionDebit  Direction = "debit"
	DirectionCredit Direction = "credit"
)

type Entry struct {
	ID             string
	TransactionID  string
	AccountID      string
	AccountVersion int64
	Money          Money
	Direction      Direction
	Status         EntryStatus
	EffectiveAt    time.Time
	CreatedAt      time.Time
	DiscardedAt    *time.Time
}

type Transaction struct {
	ID          string
	PostingKey  string
	Type        string
	Status      TransactionStatus
	EffectiveAt time.Time
	CreatedAt   time.Time
	Entries     []Entry
}

func (s TransactionStatus) Validate() error {
	switch s {
	case TransactionStatusPending, TransactionStatusPosted, TransactionStatusArchived:
		return nil
	default:
		return fmt.Errorf("%w: unsupported transaction status %q", ErrInvalidTransaction, s)
	}
}

func (s TransactionStatus) CanTransitionTo(next TransactionStatus) bool {
	return s == TransactionStatusPending && (next == TransactionStatusPosted || next == TransactionStatusArchived)
}

func (s EntryStatus) Validate() error {
	switch s {
	case EntryStatusPending, EntryStatusPosted, EntryStatusArchived:
		return nil
	default:
		return fmt.Errorf("%w: unsupported entry status %q", ErrInvalidEntry, s)
	}
}

func (d Direction) Validate() error {
	switch d {
	case DirectionDebit, DirectionCredit:
		return nil
	default:
		return fmt.Errorf("%w: unsupported direction %q", ErrInvalidEntry, d)
	}
}

func NewTransaction(id, postingKey, transactionType string, status TransactionStatus, effectiveAt, createdAt time.Time, entries []Entry) (Transaction, error) {
	transaction := Transaction{
		ID:          id,
		PostingKey:  postingKey,
		Type:        transactionType,
		Status:      status,
		EffectiveAt: effectiveAt,
		CreatedAt:   createdAt,
		Entries:     entries,
	}

	if err := transaction.ValidateForCreate(); err != nil {
		return Transaction{}, err
	}

	return transaction, nil
}

func NewPendingTransaction(id, postingKey, transactionType string, effectiveAt, createdAt time.Time, entries []Entry) (Transaction, error) {
	return NewTransaction(id, postingKey, transactionType, TransactionStatusPending, effectiveAt, createdAt, entries)
}

func (e Entry) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("%w: entry id is required", ErrInvalidEntry)
	}

	if strings.TrimSpace(e.AccountID) == "" {
		return fmt.Errorf("%w: account id is required", ErrInvalidEntry)
	}

	if e.AccountVersion < 0 {
		return fmt.Errorf("%w: account version must not be negative", ErrInvalidEntry)
	}

	if err := e.Money.ValidatePositive(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEntry, err)
	}

	if err := e.Direction.Validate(); err != nil {
		return err
	}

	if err := e.Status.Validate(); err != nil {
		return err
	}

	if e.EffectiveAt.IsZero() {
		return fmt.Errorf("%w: effective_at is required", ErrInvalidEntry)
	}

	if e.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is required", ErrInvalidEntry)
	}

	return nil
}

func (t Transaction) ValidateForCreate() error {
	if strings.TrimSpace(t.ID) == "" {
		return fmt.Errorf("%w: transaction id is required", ErrInvalidTransaction)
	}

	if strings.TrimSpace(t.PostingKey) == "" {
		return fmt.Errorf("%w: posting key is required", ErrInvalidTransaction)
	}

	if strings.TrimSpace(t.Type) == "" {
		return fmt.Errorf("%w: transaction type is required", ErrInvalidTransaction)
	}

	if err := t.Status.Validate(); err != nil {
		return err
	}

	if t.EffectiveAt.IsZero() {
		return fmt.Errorf("%w: effective_at is required", ErrInvalidTransaction)
	}

	if t.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at is required", ErrInvalidTransaction)
	}

	if len(t.Entries) < 2 {
		return fmt.Errorf("%w: transaction must contain at least 2 entries", ErrInvalidTransaction)
	}

	perCurrency := make(map[string]struct {
		debits  int64
		credits int64
	})

	for index, entry := range t.Entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("%w: entry %d: %w", ErrInvalidTransaction, index, err)
		}

		if entry.Status != entryStatusForTransactionStatus(t.Status) {
			return fmt.Errorf("%w: entry %d status %s does not match transaction status %s", ErrInvalidTransaction, index, entry.Status, t.Status)
		}

		bucket := perCurrency[entry.Money.Currency]
		switch entry.Direction {
		case DirectionDebit:
			bucket.debits += entry.Money.Amount
		case DirectionCredit:
			bucket.credits += entry.Money.Amount
		}
		perCurrency[entry.Money.Currency] = bucket
	}

	for currency, bucket := range perCurrency {
		if bucket.debits != bucket.credits {
			return fmt.Errorf("%w: unbalanced entries for currency %s", ErrInvalidTransaction, currency)
		}
	}

	return nil
}

func entryStatusForTransactionStatus(status TransactionStatus) EntryStatus {
	switch status {
	case TransactionStatusPending:
		return EntryStatusPending
	case TransactionStatusPosted:
		return EntryStatusPosted
	case TransactionStatusArchived:
		return EntryStatusArchived
	default:
		return ""
	}
}

func (t Transaction) ValidateAccounts(accounts map[string]AccountState) error {
	for _, entry := range t.Entries {
		accountState, ok := accounts[entry.AccountID]
		if !ok {
			return fmt.Errorf("%w: account %s", ErrMissingAccount, entry.AccountID)
		}

		if err := accountState.Validate(); err != nil {
			return err
		}

		if accountState.Account.Currency != entry.Money.Currency {
			return fmt.Errorf("%w: account %s has %s but entry has %s", ErrAccountCurrencyMismatch, entry.AccountID, accountState.Account.Currency, entry.Money.Currency)
		}
	}

	return nil
}

func (t *Transaction) Post() error {
	return t.transitionTo(TransactionStatusPosted)
}

func (t *Transaction) Archive() error {
	return t.transitionTo(TransactionStatusArchived)
}

func (t *Transaction) transitionTo(next TransactionStatus) error {
	if !t.Status.CanTransitionTo(next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, next)
	}

	t.Status = next
	return nil
}
