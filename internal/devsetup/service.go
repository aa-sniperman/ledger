package devsetup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/domain"
	ledgerservice "github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
)

type Service struct {
	router         sharding.Router
	registry       *sharding.DBRegistry
	commandService *ledgerservice.CommandService
}

type SeedInput struct {
	UserIDs  []string
	Currency string
	Amount   int64
	SkipMint bool
	Now      time.Time
}

type SeedResult struct {
	Users    []string            `json:"users"`
	Currency string              `json:"currency"`
	SkipMint bool                `json:"skip_mint"`
	Commands []SeedCommandResult `json:"commands,omitempty"`
}

type SeedCommandResult struct {
	UserID      string           `json:"user_id"`
	CommandID   string           `json:"command_id"`
	Idempotent  bool             `json:"idempotent"`
	ShardID     sharding.ShardID `json:"shard_id"`
	CommandType command.Type     `json:"command_type"`
}

func NewService(router sharding.Router, registry *sharding.DBRegistry, commandService *ledgerservice.CommandService) *Service {
	if registry == nil {
		panic("nil shard db registry")
	}
	if commandService == nil {
		panic("nil command service")
	}

	return &Service{
		router:         router,
		registry:       registry,
		commandService: commandService,
	}
}

func (s *Service) Seed(ctx context.Context, input SeedInput) (SeedResult, error) {
	userIDs := normalizeUsers(input.UserIDs)
	if len(userIDs) == 0 {
		return SeedResult{}, fmt.Errorf("at least one user id is required")
	}
	if strings.TrimSpace(input.Currency) == "" {
		return SeedResult{}, fmt.Errorf("currency is required")
	}
	if input.Amount <= 0 && !input.SkipMint {
		return SeedResult{}, fmt.Errorf("amount must be positive")
	}
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}

	if err := s.seedAccounts(ctx, userIDs, input.Currency, input.Now); err != nil {
		return SeedResult{}, err
	}

	result := SeedResult{
		Users:    userIDs,
		Currency: input.Currency,
		SkipMint: input.SkipMint,
	}

	if input.SkipMint {
		return result, nil
	}

	for _, userID := range userIDs {
		envelope, idempotent, err := s.commandService.EnqueueDepositRecord(ctx, ledgerservice.EnqueuePaymentCreateCommandInput{
			UserID:         userID,
			IdempotencyKey: fmt.Sprintf("seed:deposit:%s:%s:%d", userID, input.Currency, input.Amount),
			TransactionID:  fmt.Sprintf("seed_deposit_%s_%s", userID, strings.ToLower(input.Currency)),
			PostingKey:     fmt.Sprintf("seed_deposit_%s_%s", userID, strings.ToLower(input.Currency)),
			Amount:         input.Amount,
			Currency:       input.Currency,
			EffectiveAt:    input.Now,
			CreatedAt:      input.Now,
		})
		if err != nil {
			return SeedResult{}, err
		}

		result.Commands = append(result.Commands, SeedCommandResult{
			UserID:      userID,
			CommandID:   envelope.CommandID,
			Idempotent:  idempotent,
			ShardID:     envelope.ShardID,
			CommandType: envelope.Type,
		})
	}

	return result, nil
}

func (s *Service) seedAccounts(ctx context.Context, userIDs []string, currency string, now time.Time) error {
	for _, shardID := range s.router.ShardIDs() {
		db, err := s.registry.DBForShard(shardID)
		if err != nil {
			return err
		}

		cashInAccounts, err := s.router.SystemAccountIDsForShard(shardID, currency, sharding.SystemAccountRoleCashIn)
		if err != nil {
			return err
		}
		for _, accountID := range cashInAccounts {
			if err := ensureAccount(ctx, db, systemAccountState(accountID, currency, domain.NormalBalanceDebit, now)); err != nil {
				return err
			}
		}

		payoutAccounts, err := s.router.SystemAccountIDsForShard(shardID, currency, sharding.SystemAccountRolePayoutHold)
		if err != nil {
			return err
		}
		for _, accountID := range payoutAccounts {
			if err := ensureAccount(ctx, db, systemAccountState(accountID, currency, domain.NormalBalanceCredit, now)); err != nil {
				return err
			}
		}
	}

	for _, userID := range userIDs {
		shardID, err := s.router.ShardForUser(userID)
		if err != nil {
			return err
		}

		db, err := s.registry.DBForShard(shardID)
		if err != nil {
			return err
		}

		if err := ensureAccount(ctx, db, userAccountState(userID, currency, now)); err != nil {
			return err
		}
	}

	return nil
}

func ensureAccount(ctx context.Context, db *sql.DB, state domain.AccountState) error {
	repos := store.NewRepositories(db)
	if _, err := repos.Accounts.GetByID(ctx, state.Account.ID); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}

	return repos.Accounts.Create(ctx, state)
}

func userAccountState(userID, currency string, now time.Time) domain.AccountState {
	return domain.AccountState{
		Account: domain.Account{
			ID:            sharding.UserAccountID(userID, currency),
			Currency:      currency,
			NormalBalance: domain.NormalBalanceCredit,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		CurrentVersion:  0,
		CurrentBalances: domain.BalanceBuckets{},
		UpdatedAt:       now,
	}
}

func systemAccountState(accountID, currency string, normalBalance domain.NormalBalance, now time.Time) domain.AccountState {
	return domain.AccountState{
		Account: domain.Account{
			ID:            accountID,
			Currency:      currency,
			NormalBalance: normalBalance,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		CurrentVersion:  0,
		CurrentBalances: domain.BalanceBuckets{},
		UpdatedAt:       now,
	}
}

func normalizeUsers(userIDs []string) []string {
	normalized := make([]string, 0, len(userIDs))
	seen := make(map[string]struct{}, len(userIDs))

	for _, userID := range userIDs {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		normalized = append(normalized, userID)
	}

	return normalized
}
