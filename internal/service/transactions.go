package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/store"
)

type TransactionFlow string

const (
	TransactionFlowAuthorizing TransactionFlow = "authorizing"
	TransactionFlowRecording   TransactionFlow = "recording"
)

type CreateTransactionInput struct {
	Flow          TransactionFlow
	TransactionID string
	PostingKey    string
	Type          string
	Status        domain.TransactionStatus
	EffectiveAt   time.Time
	CreatedAt     time.Time
	Entries       []CreateEntryInput
}

type CreateEntryInput struct {
	EntryID   string
	AccountID string
	Amount    int64
	Currency  string
	Direction domain.Direction
}

type TransactionService struct {
	db *sql.DB
}

func NewTransactionService(db *sql.DB) *TransactionService {
	return &TransactionService{db: db}
}

func (s *TransactionService) Create(ctx context.Context, input CreateTransactionInput) (domain.Transaction, bool, error) {
	transaction, err := buildTransaction(input)
	if err != nil {
		return domain.Transaction{}, false, err
	}

	accountIDs := collectAccountIDs(transaction.Entries)
	accountStates, err := store.NewRepositories(s.db).Accounts.GetByIDs(ctx, accountIDs)
	if err != nil {
		return domain.Transaction{}, false, err
	}

	if err := transaction.ValidateAccounts(accountStates); err != nil {
		return domain.Transaction{}, false, err
	}

	changes, err := buildBalanceChanges(transaction)
	if err != nil {
		return domain.Transaction{}, false, err
	}

	var result domain.Transaction
	var idempotent bool

	err = store.InTx(ctx, s.db, func(repos store.Repositories) error {
		versions, err := repos.Accounts.ApplyBalanceChanges(ctx, changes, input.Flow == TransactionFlowAuthorizing, transaction.CreatedAt)
		if err != nil {
			return err
		}

		applyAssignedAccountVersions(&transaction, versions)

		if err := repos.Transactions.Create(ctx, transaction); err != nil {
			return err
		}

		result = transaction
		return nil
	})
	if err != nil {
		if input.Flow == TransactionFlowAuthorizing && isPotentialIdempotentReplayError(err) {
			existing, lookupErr := store.NewRepositories(s.db).Transactions.GetByPostingKey(ctx, transaction.PostingKey)
			if lookupErr == nil {
				return existing, true, nil
			}
		}

		if store.IsUniqueViolation(err) {
			existing, lookupErr := store.NewRepositories(s.db).Transactions.GetByPostingKey(ctx, transaction.PostingKey)
			if lookupErr == nil {
				return existing, true, nil
			}
		}

		return domain.Transaction{}, false, err
	}

	return result, idempotent, nil
}

func isPotentialIdempotentReplayError(err error) bool {
	return err != nil && (store.IsUniqueViolation(err) || errors.Is(err, domain.ErrBalanceLockFailed))
}

func buildTransaction(input CreateTransactionInput) (domain.Transaction, error) {
	if err := input.Flow.Validate(); err != nil {
		return domain.Transaction{}, err
	}

	status, err := statusForFlow(input.Flow, input.Status)
	if err != nil {
		return domain.Transaction{}, err
	}

	createdAt := input.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	entries, err := buildEntries(status, input.EffectiveAt.UTC(), createdAt, input.Entries)
	if err != nil {
		return domain.Transaction{}, err
	}

	transaction, err := domain.NewTransaction(
		input.TransactionID,
		input.PostingKey,
		input.Type,
		status,
		input.EffectiveAt.UTC(),
		createdAt,
		entries,
	)
	if err != nil {
		return domain.Transaction{}, err
	}

	for index := range transaction.Entries {
		transaction.Entries[index].TransactionID = transaction.ID
	}

	return transaction, nil
}

func buildEntries(status domain.TransactionStatus, effectiveAt, createdAt time.Time, inputs []CreateEntryInput) ([]domain.Entry, error) {
	entryStatus, err := entryStatusForTransactionStatus(status)
	if err != nil {
		return nil, err
	}

	entries := make([]domain.Entry, 0, len(inputs))
	for _, input := range inputs {
		entries = append(entries, domain.Entry{
			ID:        input.EntryID,
			AccountID: input.AccountID,
			Money: domain.Money{
				Amount:   input.Amount,
				Currency: input.Currency,
			},
			Direction:   input.Direction,
			Status:      entryStatus,
			EffectiveAt: effectiveAt,
			CreatedAt:   createdAt,
		})
	}

	return entries, nil
}

func statusForFlow(flow TransactionFlow, requested domain.TransactionStatus) (domain.TransactionStatus, error) {
	switch flow {
	case TransactionFlowAuthorizing:
		if requested != "" && requested != domain.TransactionStatusPending {
			return "", fmt.Errorf("invalid create flow: authorizing transactions must be pending")
		}
		return domain.TransactionStatusPending, nil
	case TransactionFlowRecording:
		if err := requested.Validate(); err != nil {
			return "", err
		}
		return requested, nil
	default:
		return "", fmt.Errorf("invalid create flow %q", flow)
	}
}

func (f TransactionFlow) Validate() error {
	switch f {
	case TransactionFlowAuthorizing, TransactionFlowRecording:
		return nil
	default:
		return fmt.Errorf("invalid create flow %q", f)
	}
}

func entryStatusForTransactionStatus(status domain.TransactionStatus) (domain.EntryStatus, error) {
	switch status {
	case domain.TransactionStatusPending:
		return domain.EntryStatusPending, nil
	case domain.TransactionStatusPosted:
		return domain.EntryStatusPosted, nil
	case domain.TransactionStatusArchived:
		return domain.EntryStatusArchived, nil
	default:
		return "", fmt.Errorf("invalid transaction status %q", status)
	}
}

func collectAccountIDs(entries []domain.Entry) []string {
	seen := make(map[string]struct{}, len(entries))
	accountIDs := make([]string, 0, len(entries))

	for _, entry := range entries {
		if _, ok := seen[entry.AccountID]; ok {
			continue
		}

		seen[entry.AccountID] = struct{}{}
		accountIDs = append(accountIDs, entry.AccountID)
	}

	return accountIDs
}

func buildBalanceChanges(transaction domain.Transaction) ([]store.BalanceChange, error) {
	changeIndexByAccount := make(map[string]int)
	changes := make([]store.BalanceChange, 0, len(transaction.Entries))

	for _, entry := range transaction.Entries {
		index, ok := changeIndexByAccount[entry.AccountID]
		if !ok {
			changeIndexByAccount[entry.AccountID] = len(changes)
			changes = append(changes, store.BalanceChange{
				AccountID: entry.AccountID,
			})
			index = len(changes) - 1
		}

		change := &changes[index]
		switch entry.Status {
		case domain.EntryStatusPending:
			if entry.Direction == domain.DirectionDebit {
				change.Delta.PendingDebits += entry.Money.Amount
			} else {
				change.Delta.PendingCredits += entry.Money.Amount
			}
		case domain.EntryStatusPosted:
			if entry.Direction == domain.DirectionDebit {
				change.Delta.PostedDebits += entry.Money.Amount
				change.Delta.PendingDebits += entry.Money.Amount
			} else {
				change.Delta.PostedCredits += entry.Money.Amount
				change.Delta.PendingCredits += entry.Money.Amount
			}
		case domain.EntryStatusArchived:
			// Archived entries do not contribute to current balances.
		default:
			return nil, fmt.Errorf("%w: unsupported entry status %s", domain.ErrInvalidEntry, entry.Status)
		}
	}

	return changes, nil
}

func applyAssignedAccountVersions(transaction *domain.Transaction, versions map[string]int64) {
	for index := range transaction.Entries {
		if version, ok := versions[transaction.Entries[index].AccountID]; ok {
			transaction.Entries[index].AccountVersion = version
		}
	}
}
