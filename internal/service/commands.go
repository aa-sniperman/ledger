package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/commandbus"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
)

type CommandService struct {
	db        *sql.DB
	router    sharding.Router
	registry  *sharding.DBRegistry
	store     store.Repositories
	publisher commandbus.Publisher
}

const commandRetryDelay = 30 * time.Second

type EnqueueCreateTransactionCommandInput struct {
	UserID         string
	IdempotencyKey string
	CreateInput    CreateTransactionInput
}

type EnqueueTransitionCommandInput struct {
	UserID         string
	IdempotencyKey string
	TransactionID  string
}

type EnqueuePaymentCreateCommandInput struct {
	UserID         string
	IdempotencyKey string
	TransactionID  string
	PostingKey     string
	Amount         int64
	Currency       string
	EffectiveAt    time.Time
	CreatedAt      time.Time
}

type createTransactionCommandPayload struct {
	UserID string                 `json:"user_id"`
	Input  CreateTransactionInput `json:"input"`
}

func NewCommandService(db *sql.DB, router sharding.Router, registry *sharding.DBRegistry) *CommandService {
	return NewCommandServiceWithPublisher(db, router, registry, commandbus.NoopPublisher{})
}

func NewCommandServiceWithPublisher(db *sql.DB, router sharding.Router, registry *sharding.DBRegistry, publisher commandbus.Publisher) *CommandService {
	if registry == nil {
		singleRegistry, err := sharding.NewSingleDBRegistry(router.ShardIDs(), db)
		if err != nil {
			panic(err)
		}
		registry = singleRegistry
	}
	if publisher == nil {
		publisher = commandbus.NoopPublisher{}
	}

	return &CommandService{
		db:        db,
		router:    router,
		registry:  registry,
		store:     store.NewRepositories(db),
		publisher: publisher,
	}
}

func (s *CommandService) EnqueueTransactionCreate(ctx context.Context, input EnqueueCreateTransactionCommandInput) (command.Envelope, bool, error) {
	if strings.TrimSpace(input.UserID) == "" {
		return command.Envelope{}, false, fmt.Errorf("user id is required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return command.Envelope{}, false, fmt.Errorf("idempotency key is required")
	}

	transaction, err := buildTransaction(input.CreateInput)
	if err != nil {
		return command.Envelope{}, false, err
	}

	shardID, err := s.router.ShardForUser(input.UserID)
	if err != nil {
		return command.Envelope{}, false, err
	}

	if err := s.validateSingleShardUserTransaction(input.UserID, shardID, transaction); err != nil {
		return command.Envelope{}, false, err
	}

	return s.enqueue(ctx, shardID, command.TypeTransactionCreate, input.IdempotencyKey, createTransactionCommandPayload{
		UserID: input.UserID,
		Input:  input.CreateInput,
	})
}

func (s *CommandService) EnqueueWithdrawalCreate(ctx context.Context, input EnqueuePaymentCreateCommandInput) (command.Envelope, bool, error) {
	if err := validatePaymentCreateInput(input); err != nil {
		return command.Envelope{}, false, err
	}

	systemAccountID, shardID, err := s.router.SystemAccountForUser(input.UserID, input.Currency, sharding.SystemAccountRolePayoutHold)
	if err != nil {
		return command.Envelope{}, false, err
	}

	createInput := CreateTransactionInput{
		Flow:          TransactionFlowAuthorizing,
		TransactionID: input.TransactionID,
		PostingKey:    input.PostingKey,
		Type:          "withdrawal_hold",
		EffectiveAt:   input.EffectiveAt,
		CreatedAt:     input.CreatedAt,
		Entries: []CreateEntryInput{
			{EntryID: paymentEntryID(input.TransactionID, "user_debit"), AccountID: sharding.UserAccountID(input.UserID, input.Currency), Amount: input.Amount, Currency: input.Currency, Direction: domain.DirectionDebit},
			{EntryID: paymentEntryID(input.TransactionID, "system_credit"), AccountID: systemAccountID, Amount: input.Amount, Currency: input.Currency, Direction: domain.DirectionCredit},
		},
	}

	return s.enqueue(ctx, shardID, command.TypeWithdrawalCreate, input.IdempotencyKey, createTransactionCommandPayload{
		UserID: input.UserID,
		Input:  createInput,
	})
}

func (s *CommandService) EnqueueWithdrawalPost(ctx context.Context, input EnqueueTransitionCommandInput) (command.Envelope, bool, error) {
	return s.enqueueTransition(ctx, command.TypeWithdrawalPost, input)
}

func (s *CommandService) EnqueueWithdrawalArchive(ctx context.Context, input EnqueueTransitionCommandInput) (command.Envelope, bool, error) {
	return s.enqueueTransition(ctx, command.TypeWithdrawalArchive, input)
}

func (s *CommandService) EnqueueDepositRecord(ctx context.Context, input EnqueuePaymentCreateCommandInput) (command.Envelope, bool, error) {
	if err := validatePaymentCreateInput(input); err != nil {
		return command.Envelope{}, false, err
	}

	systemAccountID, shardID, err := s.router.SystemAccountForUser(input.UserID, input.Currency, sharding.SystemAccountRoleCashIn)
	if err != nil {
		return command.Envelope{}, false, err
	}

	createInput := CreateTransactionInput{
		Flow:          TransactionFlowRecording,
		TransactionID: input.TransactionID,
		PostingKey:    input.PostingKey,
		Type:          "deposit_record",
		Status:        domain.TransactionStatusPosted,
		EffectiveAt:   input.EffectiveAt,
		CreatedAt:     input.CreatedAt,
		Entries: []CreateEntryInput{
			{EntryID: paymentEntryID(input.TransactionID, "system_debit"), AccountID: systemAccountID, Amount: input.Amount, Currency: input.Currency, Direction: domain.DirectionDebit},
			{EntryID: paymentEntryID(input.TransactionID, "user_credit"), AccountID: sharding.UserAccountID(input.UserID, input.Currency), Amount: input.Amount, Currency: input.Currency, Direction: domain.DirectionCredit},
		},
	}

	return s.enqueue(ctx, shardID, command.TypeDepositRecord, input.IdempotencyKey, createTransactionCommandPayload{
		UserID: input.UserID,
		Input:  createInput,
	})
}

func (s *CommandService) EnqueueTransactionPost(ctx context.Context, input EnqueueTransitionCommandInput) (command.Envelope, bool, error) {
	return s.enqueueTransition(ctx, command.TypeTransactionPost, input)
}

func (s *CommandService) EnqueueTransactionArchive(ctx context.Context, input EnqueueTransitionCommandInput) (command.Envelope, bool, error) {
	return s.enqueueTransition(ctx, command.TypeTransactionArchive, input)
}

func (s *CommandService) ClaimNext(ctx context.Context, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error) {
	if err := shardID.Validate(); err != nil {
		return command.Envelope{}, false, err
	}

	return s.store.Commands.ClaimNext(ctx, shardID, now.UTC())
}

func (s *CommandService) ProcessNext(ctx context.Context, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error) {
	envelope, ok, err := s.ClaimNext(ctx, shardID, now)
	if err != nil || !ok {
		return envelope, ok, err
	}

	updatedEnvelope, err := s.processClaimed(ctx, envelope, now.UTC())
	if err != nil {
		return command.Envelope{}, false, err
	}

	return updatedEnvelope, true, nil
}

func (s *CommandService) ProcessByID(ctx context.Context, commandID string, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error) {
	if strings.TrimSpace(commandID) == "" {
		return command.Envelope{}, false, fmt.Errorf("command id is required")
	}
	if err := shardID.Validate(); err != nil {
		return command.Envelope{}, false, err
	}

	envelope, err := s.store.Commands.GetByID(ctx, commandID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return command.Envelope{}, false, nil
		}
		return command.Envelope{}, false, err
	}
	if envelope.ShardID != shardID {
		return command.Envelope{}, false, fmt.Errorf("command %s belongs to shard %s, not %s", commandID, envelope.ShardID, shardID)
	}

	if _, err := s.executeCommand(ctx, envelope); err != nil {
		return command.Envelope{}, false, err
	}

	envelope.Status = command.StatusSucceeded
	envelope.UpdatedAt = now.UTC()
	return envelope, true, nil
}

func (s *CommandService) ProcessEnvelope(ctx context.Context, envelope command.Envelope, now time.Time) (command.Envelope, bool, error) {
	if err := envelope.Validate(); err != nil {
		return command.Envelope{}, false, err
	}
	if _, err := s.executeCommand(ctx, envelope); err != nil {
		return command.Envelope{}, false, err
	}

	envelope.Status = command.StatusSucceeded
	envelope.UpdatedAt = now.UTC()
	return envelope, true, nil
}

func (s *CommandService) enqueueTransition(ctx context.Context, commandType command.Type, input EnqueueTransitionCommandInput) (command.Envelope, bool, error) {
	if strings.TrimSpace(input.UserID) == "" {
		return command.Envelope{}, false, fmt.Errorf("user id is required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return command.Envelope{}, false, fmt.Errorf("idempotency key is required")
	}
	if strings.TrimSpace(input.TransactionID) == "" {
		return command.Envelope{}, false, fmt.Errorf("transaction id is required")
	}

	shardID, err := s.router.ShardForUser(input.UserID)
	if err != nil {
		return command.Envelope{}, false, err
	}

	return s.enqueue(ctx, shardID, commandType, input.IdempotencyKey, input)
}

func (s *CommandService) enqueue(ctx context.Context, shardID sharding.ShardID, commandType command.Type, idempotencyKey string, payload any) (command.Envelope, bool, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return command.Envelope{}, false, fmt.Errorf("marshal command payload: %w", err)
	}

	now := time.Now().UTC()
	commandID, err := command.NewID()
	if err != nil {
		return command.Envelope{}, false, err
	}

	envelope := command.Envelope{
		CommandID:      commandID,
		IdempotencyKey: idempotencyKey,
		ShardID:        shardID,
		Type:           commandType,
		Payload:        payloadJSON,
		Status:         command.StatusAccepted,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.store.Commands.Create(ctx, envelope); err != nil {
		if store.IsUniqueViolation(err) {
			existing, lookupErr := s.store.Commands.GetByTypeAndIdempotencyKey(ctx, commandType, idempotencyKey)
			if lookupErr == nil {
				return existing, true, nil
			}
		}
		return command.Envelope{}, false, err
	}

	if err := s.publisher.PublishAccepted(ctx, envelope); err != nil {
		deleteErr := s.store.Commands.DeleteByID(ctx, envelope.CommandID)
		if deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
			return command.Envelope{}, false, fmt.Errorf("publish command %s: %w (cleanup failed: %v)", envelope.CommandID, err, deleteErr)
		}
		return command.Envelope{}, false, fmt.Errorf("publish command %s: %w", envelope.CommandID, err)
	}

	return envelope, false, nil
}

func (s *CommandService) processClaimed(ctx context.Context, envelope command.Envelope, now time.Time) (command.Envelope, error) {
	resultJSON, executionErr := s.executeCommand(ctx, envelope)
	if executionErr == nil {
		if err := s.store.Commands.MarkSucceeded(ctx, envelope.CommandID, resultJSON, now); err != nil {
			return command.Envelope{}, err
		}
		return s.store.Commands.GetByID(ctx, envelope.CommandID)
	}

	status, errorCode, errorMessage, nextAttemptAt := classifyCommandExecutionError(executionErr, now)
	if err := s.store.Commands.MarkFailed(ctx, envelope.CommandID, status, errorCode, errorMessage, nextAttemptAt, now); err != nil {
		return command.Envelope{}, err
	}

	return s.store.Commands.GetByID(ctx, envelope.CommandID)
}

func (s *CommandService) executeCommand(ctx context.Context, envelope command.Envelope) ([]byte, error) {
	switch envelope.Type {
	case command.TypeTransactionCreate, command.TypeWithdrawalCreate, command.TypeDepositRecord:
		var payload createTransactionCommandPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode create command payload: %w", err)
		}

		shardDB, err := s.registry.DBForShard(envelope.ShardID)
		if err != nil {
			return nil, err
		}

		transaction, _, err := NewTransactionService(shardDB).Create(ctx, payload.Input)
		if err != nil {
			return nil, err
		}
		if err := s.store.TransactionLocators.Create(ctx, transaction.ID, envelope.ShardID, time.Now().UTC()); err != nil {
			return nil, err
		}

		return json.Marshal(struct {
			TransactionID string `json:"transaction_id"`
			Status        string `json:"status"`
		}{
			TransactionID: transaction.ID,
			Status:        string(transaction.Status),
		})
	case command.TypeTransactionPost, command.TypeWithdrawalPost:
		var payload EnqueueTransitionCommandInput
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode post command payload: %w", err)
		}

		shardDB, err := s.registry.DBForShard(envelope.ShardID)
		if err != nil {
			return nil, err
		}

		transaction, err := NewTransactionService(shardDB).Post(ctx, payload.TransactionID)
		if err != nil {
			return nil, err
		}

		return json.Marshal(struct {
			TransactionID string `json:"transaction_id"`
			Status        string `json:"status"`
		}{
			TransactionID: transaction.ID,
			Status:        string(transaction.Status),
		})
	case command.TypeTransactionArchive, command.TypeWithdrawalArchive:
		var payload EnqueueTransitionCommandInput
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode archive command payload: %w", err)
		}

		shardDB, err := s.registry.DBForShard(envelope.ShardID)
		if err != nil {
			return nil, err
		}

		transaction, err := NewTransactionService(shardDB).Archive(ctx, payload.TransactionID)
		if err != nil {
			return nil, err
		}

		return json.Marshal(struct {
			TransactionID string `json:"transaction_id"`
			Status        string `json:"status"`
		}{
			TransactionID: transaction.ID,
			Status:        string(transaction.Status),
		})
	default:
		return nil, fmt.Errorf("unsupported command type %q", envelope.Type)
	}
}

func validatePaymentCreateInput(input EnqueuePaymentCreateCommandInput) error {
	if strings.TrimSpace(input.UserID) == "" {
		return fmt.Errorf("%w: user id is required", domain.ErrInvalidTransaction)
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return fmt.Errorf("%w: idempotency key is required", domain.ErrInvalidTransaction)
	}
	if strings.TrimSpace(input.TransactionID) == "" {
		return fmt.Errorf("%w: transaction id is required", domain.ErrInvalidTransaction)
	}
	if strings.TrimSpace(input.PostingKey) == "" {
		return fmt.Errorf("%w: posting key is required", domain.ErrInvalidTransaction)
	}
	if strings.TrimSpace(input.Currency) == "" {
		return fmt.Errorf("%w: currency is required", domain.ErrInvalidTransaction)
	}
	if input.Amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", domain.ErrInvalidMoney)
	}
	return nil
}

func paymentEntryID(transactionID, suffix string) string {
	return fmt.Sprintf("%s_%s", transactionID, suffix)
}

func classifyCommandExecutionError(err error, now time.Time) (command.Status, string, string, *time.Time) {
	if isTerminalCommandError(err) {
		return command.StatusFailedTerminal, "terminal_execution_error", err.Error(), nil
	}

	nextAttemptAt := now.Add(commandRetryDelay)
	return command.StatusFailedRetryable, "retryable_execution_error", err.Error(), &nextAttemptAt
}

func isTerminalCommandError(err error) bool {
	return errors.Is(err, domain.ErrInvalidAccount) ||
		errors.Is(err, domain.ErrInvalidMoney) ||
		errors.Is(err, domain.ErrInvalidEntry) ||
		errors.Is(err, domain.ErrInvalidTransaction) ||
		errors.Is(err, domain.ErrInvalidTransition) ||
		errors.Is(err, domain.ErrMissingAccount) ||
		errors.Is(err, domain.ErrAccountCurrencyMismatch) ||
		errors.Is(err, domain.ErrAccountVersionMismatch) ||
		errors.Is(err, domain.ErrBalanceLockFailed) ||
		errors.Is(err, store.ErrNotFound)
}

func (s *CommandService) validateSingleShardUserTransaction(userID string, shardID sharding.ShardID, transaction domain.Transaction) error {
	userAccountSeen := false

	for _, entry := range transaction.Entries {
		entryShardID, err := s.router.ShardForAccount(entry.AccountID)
		if err != nil {
			return err
		}
		if entryShardID != shardID {
			return fmt.Errorf("account %s routes to shard %s but user %s routes to shard %s", entry.AccountID, entryShardID, userID, shardID)
		}

		if strings.HasPrefix(entry.AccountID, sharding.UserWalletAccountPrefix+":") {
			expectedUserAccountID := sharding.UserAccountID(userID, entry.Money.Currency)
			if entry.AccountID != expectedUserAccountID {
				return fmt.Errorf("transaction for user %s cannot reference user account %s", userID, entry.AccountID)
			}
			userAccountSeen = true
		}
	}

	if !userAccountSeen {
		return fmt.Errorf("transaction for user %s must include a currency-scoped user wallet account", userID)
	}

	return nil
}
