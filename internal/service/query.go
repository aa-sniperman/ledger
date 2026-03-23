package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
)

type QueryService struct {
	centralDB *sql.DB
	router    sharding.Router
	registry  *sharding.DBRegistry
}

func NewQueryService(centralDB *sql.DB, router sharding.Router, registry *sharding.DBRegistry) *QueryService {
	return &QueryService{
		centralDB: centralDB,
		router:    router,
		registry:  registry,
	}
}

func (s *QueryService) GetCommand(ctx context.Context, commandID string) (command.Envelope, error) {
	return store.NewRepositories(s.centralDB).Commands.GetByID(ctx, commandID)
}

func (s *QueryService) GetAccountBalance(ctx context.Context, accountID string) (AccountBalanceView, error) {
	shardID, err := s.router.ShardForAccount(accountID)
	if err != nil {
		if _, db, ok := s.registry.SingleShard(); ok {
			return NewAccountService(db).GetBalance(ctx, accountID)
		}
		return AccountBalanceView{}, fmt.Errorf("%w: %v", domain.ErrInvalidAccount, err)
	}

	db, err := s.registry.DBForShard(shardID)
	if err != nil {
		return AccountBalanceView{}, err
	}

	return NewAccountService(db).GetBalance(ctx, accountID)
}

func (s *QueryService) GetUserBalance(ctx context.Context, userID, currency string) (AccountBalanceView, error) {
	return s.GetAccountBalance(ctx, sharding.UserAccountID(userID, currency))
}

func (s *QueryService) GetTransaction(ctx context.Context, transactionID string) (domain.Transaction, error) {
	shardID, err := store.NewRepositories(s.centralDB).TransactionLocators.GetByTransactionID(ctx, transactionID)
	if err == nil {
		db, lookupErr := s.registry.DBForShard(shardID)
		if lookupErr != nil {
			return domain.Transaction{}, lookupErr
		}
		return store.NewRepositories(db).Transactions.GetByID(ctx, transactionID)
	}

	if _, db, ok := s.registry.SingleShard(); ok {
		return store.NewRepositories(db).Transactions.GetByID(ctx, transactionID)
	}

	if errors.Is(err, store.ErrNotFound) {
		return domain.Transaction{}, err
	}

	return domain.Transaction{}, err
}
