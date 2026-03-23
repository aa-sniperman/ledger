package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/store"
)

func (s *TransactionService) Post(ctx context.Context, transactionID string) (domain.Transaction, error) {
	return s.transitionPendingTransaction(ctx, transactionID, domain.TransactionStatusPosted)
}

func (s *TransactionService) Archive(ctx context.Context, transactionID string) (domain.Transaction, error) {
	return s.transitionPendingTransaction(ctx, transactionID, domain.TransactionStatusArchived)
}

func (s *TransactionService) transitionPendingTransaction(ctx context.Context, transactionID string, nextStatus domain.TransactionStatus) (domain.Transaction, error) {
	transaction, err := store.NewRepositories(s.db).Transactions.GetByID(ctx, transactionID)
	if err != nil {
		return domain.Transaction{}, err
	}

	switch nextStatus {
	case domain.TransactionStatusPosted:
		if transaction.Status == domain.TransactionStatusPosted {
			return transaction, nil
		}
		err = transaction.Post()
	case domain.TransactionStatusArchived:
		if transaction.Status == domain.TransactionStatusArchived {
			return transaction, nil
		}
		err = transaction.Archive()
	default:
		err = fmt.Errorf("%w: unsupported transition target %s", domain.ErrInvalidTransition, nextStatus)
	}
	if err != nil {
		return domain.Transaction{}, err
	}

	pendingEntries := collectActiveEntriesByStatus(transaction.Entries, domain.EntryStatusPending)
	if len(pendingEntries) == 0 {
		return domain.Transaction{}, fmt.Errorf("%w: transaction %s has no active pending entries", domain.ErrInvalidTransaction, transactionID)
	}

	changes, err := buildTransitionBalanceChanges(pendingEntries, nextStatus)
	if err != nil {
		return domain.Transaction{}, err
	}

	err = store.InTx(ctx, s.db, func(repos store.Repositories) error {
		if err := repos.Transactions.TransitionStatus(ctx, transactionID, domain.TransactionStatusPending, nextStatus); err != nil {
			return err
		}

		transitionAt := time.Now().UTC()
		versions, err := repos.Accounts.ApplyBalanceChanges(ctx, changes, false, transitionAt)
		if err != nil {
			return err
		}

		if err := repos.Transactions.DiscardEntries(ctx, collectEntryIDs(pendingEntries), transitionAt); err != nil {
			return err
		}

		terminalEntries, err := buildTransitionEntries(pendingEntries, nextStatus, transitionAt, versions)
		if err != nil {
			return err
		}
		if err := repos.Transactions.InsertEntries(ctx, terminalEntries); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return domain.Transaction{}, err
	}

	return store.NewRepositories(s.db).Transactions.GetByID(ctx, transactionID)
}

func buildTransitionBalanceChanges(entries []domain.Entry, nextStatus domain.TransactionStatus) ([]store.BalanceChange, error) {
	changeIndexByAccount := make(map[string]int)
	changes := make([]store.BalanceChange, 0, len(entries))

	for _, entry := range entries {
		if entry.Status != domain.EntryStatusPending {
			return nil, fmt.Errorf("%w: transition source entry %s must be pending", domain.ErrInvalidEntry, entry.ID)
		}

		index, ok := changeIndexByAccount[entry.AccountID]
		if !ok {
			changeIndexByAccount[entry.AccountID] = len(changes)
			changes = append(changes, store.BalanceChange{AccountID: entry.AccountID})
			index = len(changes) - 1
		}

		change := &changes[index]
		switch nextStatus {
		case domain.TransactionStatusPosted:
			if entry.Direction == domain.DirectionDebit {
				change.Delta.PostedDebits += entry.Money.Amount
			} else {
				change.Delta.PostedCredits += entry.Money.Amount
			}
		case domain.TransactionStatusArchived:
			if entry.Direction == domain.DirectionDebit {
				change.Delta.PendingDebits -= entry.Money.Amount
			} else {
				change.Delta.PendingCredits -= entry.Money.Amount
			}
		default:
			return nil, fmt.Errorf("%w: unsupported transition target %s", domain.ErrInvalidTransition, nextStatus)
		}
	}

	return changes, nil
}

func buildTransitionEntries(entries []domain.Entry, nextStatus domain.TransactionStatus, createdAt time.Time, versions map[string]int64) ([]domain.Entry, error) {
	entryStatus, err := entryStatusForTransactionStatus(nextStatus)
	if err != nil {
		return nil, err
	}

	transitionSuffix := string(entryStatus)
	transitionEntries := make([]domain.Entry, 0, len(entries))

	for _, entry := range entries {
		version, ok := versions[entry.AccountID]
		if !ok {
			return nil, fmt.Errorf("%w: missing assigned version for account %s", domain.ErrInvalidTransaction, entry.AccountID)
		}

		transitionEntries = append(transitionEntries, domain.Entry{
			ID:             fmt.Sprintf("%s_%s", entry.ID, transitionSuffix),
			TransactionID:  entry.TransactionID,
			AccountID:      entry.AccountID,
			AccountVersion: version,
			Money:          entry.Money,
			Direction:      entry.Direction,
			Status:         entryStatus,
			EffectiveAt:    entry.EffectiveAt,
			CreatedAt:      createdAt,
		})
	}

	return transitionEntries, nil
}

func collectEntryIDs(entries []domain.Entry) []string {
	entryIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		entryIDs = append(entryIDs, entry.ID)
	}
	return entryIDs
}

func collectActiveEntriesByStatus(entries []domain.Entry, status domain.EntryStatus) []domain.Entry {
	activeEntries := make([]domain.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Status == status && entry.DiscardedAt == nil {
			activeEntries = append(activeEntries, entry)
		}
	}
	return activeEntries
}
