package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/worker"
)

const payoutHoldPoolSize = 4

func main() {
	var (
		usersFlag = flag.String("users", "user_123,user_456", "comma-separated user ids to seed")
		currency  = flag.String("currency", "USD", "currency to seed")
		amount    = flag.Int64("amount", 10000, "deposit amount in minor units for each user")
		skipMint  = flag.Bool("skip-mint", false, "create accounts only without minting balances")
	)
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	userIDs := parseUsers(*usersFlag)
	if len(userIDs) == 0 {
		slog.Error("parse users", "error", "at least one user id is required")
		os.Exit(1)
	}
	if strings.TrimSpace(*currency) == "" {
		slog.Error("validate currency", "error", "currency is required")
		os.Exit(1)
	}
	if *amount <= 0 && !*skipMint {
		slog.Error("validate amount", "error", "amount must be positive")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	centralDB, shardDBs, router, registry, err := openEnvironment(ctx, cfg)
	if err != nil {
		slog.Error("open environment", "error", err)
		os.Exit(1)
	}
	defer closeEnvironment(centralDB, shardDBs)

	now := time.Now().UTC()
	if err := seedAccounts(ctx, router, registry, userIDs, *currency, now); err != nil {
		slog.Error("seed accounts", "error", err)
		os.Exit(1)
	}

	slog.Info("seeded accounts", "users", userIDs, "currency", *currency)

	if *skipMint {
		slog.Info("mint skipped")
		return
	}

	commandService := service.NewCommandService(centralDB, router, registry)
	for _, userID := range userIDs {
		envelope, idempotent, err := commandService.EnqueueDepositRecord(ctx, service.EnqueuePaymentCreateCommandInput{
			UserID:         userID,
			IdempotencyKey: fmt.Sprintf("seed:deposit:%s:%s:%d", userID, *currency, *amount),
			TransactionID:  fmt.Sprintf("seed_deposit_%s_%s", userID, strings.ToLower(*currency)),
			PostingKey:     fmt.Sprintf("seed_deposit_%s_%s", userID, strings.ToLower(*currency)),
			Amount:         *amount,
			Currency:       *currency,
			EffectiveAt:    now,
			CreatedAt:      now,
		})
		if err != nil {
			slog.Error("enqueue deposit record", "user_id", userID, "error", err)
			os.Exit(1)
		}

		slog.Info("enqueued deposit seed command", "user_id", userID, "command_id", envelope.CommandID, "idempotent", idempotent, "shard_id", envelope.ShardID)
	}

	fleet, err := worker.NewFleet(commandService, router.ShardIDs(), cfg.WorkerPollInterval, slog.Default())
	if err != nil {
		slog.Error("build worker fleet", "error", err)
		os.Exit(1)
	}

	processed, err := fleet.ProcessAvailableAll(ctx)
	if err != nil {
		slog.Error("process seed commands", "error", err)
		os.Exit(1)
	}

	for shardID, count := range processed {
		slog.Info("processed seed commands", "shard_id", shardID, "count", count)
	}
}

func openEnvironment(ctx context.Context, cfg config.Config) (*sql.DB, map[sharding.ShardID]*sql.DB, sharding.Router, *sharding.DBRegistry, error) {
	centralDB, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, sharding.Router{}, nil, err
	}

	router, err := sharding.NewRouter(cfg.ShardIDs, map[sharding.SystemAccountRole]int{
		sharding.SystemAccountRolePayoutHold: payoutHoldPoolSize,
	})
	if err != nil {
		_ = centralDB.Close()
		return nil, nil, sharding.Router{}, nil, err
	}

	if len(cfg.ShardDatabaseURLs) == 0 {
		registry, err := sharding.NewSingleDBRegistry(cfg.ShardIDs, centralDB)
		if err != nil {
			_ = centralDB.Close()
			return nil, nil, sharding.Router{}, nil, err
		}
		return centralDB, nil, router, registry, nil
	}

	shardDBs := make(map[sharding.ShardID]*sql.DB, len(cfg.ShardDatabaseURLs))
	for shardID, url := range cfg.ShardDatabaseURLs {
		db, openErr := database.Open(ctx, url)
		if openErr != nil {
			closeEnvironment(centralDB, shardDBs)
			return nil, nil, sharding.Router{}, nil, fmt.Errorf("open shard db %s: %w", shardID, openErr)
		}
		shardDBs[shardID] = db
	}

	registry, err := sharding.NewDBRegistry(shardDBs)
	if err != nil {
		closeEnvironment(centralDB, shardDBs)
		return nil, nil, sharding.Router{}, nil, err
	}

	return centralDB, shardDBs, router, registry, nil
}

func closeEnvironment(centralDB *sql.DB, shardDBs map[sharding.ShardID]*sql.DB) {
	if centralDB != nil {
		_ = centralDB.Close()
	}
	for _, db := range shardDBs {
		_ = db.Close()
	}
}

func seedAccounts(ctx context.Context, router sharding.Router, registry *sharding.DBRegistry, userIDs []string, currency string, now time.Time) error {
	for _, shardID := range router.ShardIDs() {
		db, err := registry.DBForShard(shardID)
		if err != nil {
			return err
		}

		if err := ensureAccount(ctx, db, systemAccountState(cashInClearingAccountID(shardID, currency), currency, domain.NormalBalanceDebit, now)); err != nil {
			return err
		}
		if err := ensureAccount(ctx, db, systemAccountState(fmt.Sprintf("%s:%s:%s", sharding.SystemAccountRolePayoutHold, shardID, currency), currency, domain.NormalBalanceCredit, now)); err != nil {
			return err
		}

		for slot := 0; slot < payoutHoldPoolSize; slot++ {
			if err := ensureAccount(ctx, db, systemAccountState(fmt.Sprintf("%s:%s:%s:%d", sharding.SystemAccountRolePayoutHold, shardID, currency, slot), currency, domain.NormalBalanceCredit, now)); err != nil {
				return err
			}
		}
	}

	for _, userID := range userIDs {
		shardID, err := router.ShardForUser(userID)
		if err != nil {
			return err
		}

		db, err := registry.DBForShard(shardID)
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

func cashInClearingAccountID(shardID sharding.ShardID, currency string) string {
	return fmt.Sprintf("%s:%s:%s", sharding.SystemAccountRoleCashIn, shardID, currency)
}

func parseUsers(value string) []string {
	parts := strings.Split(value, ",")
	users := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		userID := strings.TrimSpace(part)
		if userID == "" {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		users = append(users, userID)
	}

	return users
}
