package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestShardAwareReadsHTTPIntegration(t *testing.T) {
	centralDB := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, centralDB)

	shardDBs := testutil.OpenShardTestDBs(t)
	testutil.ResetShardTestDBs(t, shardDBs)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	registry, err := sharding.NewDBRegistry(shardDBs)
	if err != nil {
		t.Fatalf("build shard registry: %v", err)
	}

	server := NewWithRegistry(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a", "shard-b"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, centralDB, router, registry)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	userID := httpTestUserIDForShard(t, router, "shard-b")
	systemAccountID, shardID, err := router.SystemAccountForUser(userID, "USD", sharding.SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("pick system account: %v", err)
	}
	seedShardHTTPAccounts(t, shardDBs[shardID], userID, systemAccountID)

	requestBody := enqueueWithdrawalCreateCommandRequest{
		UserID:         userID,
		IdempotencyKey: "idem_http_multishard_create_1",
		TransactionID:  "tx_http_multishard_1",
		PostingKey:     "pk_http_multishard_1",
		Amount:         70,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 23, 10, 0, 1, 0, time.UTC),
	}

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.create", requestBody, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on create command enqueue, got %d", statusCode)
	}
	if accepted.ShardID != string(shardID) || accepted.Status != "accepted" {
		t.Fatalf("unexpected accepted command response: %+v", accepted)
	}

	commandService := service.NewCommandService(centralDB, router, registry)
	processed, ok, err := commandService.ProcessNext(context.Background(), shardID, time.Date(2026, 3, 23, 10, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("process queued command: %v", err)
	}
	if !ok {
		t.Fatal("expected queued command to be processed")
	}
	if processed.CommandID != accepted.CommandID || processed.Status != "succeeded" {
		t.Fatalf("unexpected processed command: %+v", processed)
	}

	var loaded commandResponse
	statusCode = getJSON(t, httpServer.URL+"/commands/"+accepted.CommandID, &loaded)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on command get, got %d", statusCode)
	}
	if loaded.CommandID != accepted.CommandID || loaded.Status != "succeeded" {
		t.Fatalf("unexpected loaded command response: %+v", loaded)
	}

	var balance accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/users/"+userID+"/balances/USD", &balance)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on account balance get, got %d", statusCode)
	}
	if balance.AccountID != sharding.UserAccountID(userID, "USD") || balance.Posted != 100 || balance.Pending != 30 || balance.Available != 30 {
		t.Fatalf("unexpected balance response: %+v", balance)
	}

	var transaction transactionResponse
	statusCode = getJSON(t, httpServer.URL+"/transactions/tx_http_multishard_1", &transaction)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on transaction get, got %d", statusCode)
	}
	if transaction.TransactionID != "tx_http_multishard_1" || transaction.Status != "pending" {
		t.Fatalf("unexpected transaction response: %+v", transaction)
	}

	for otherShardID, db := range shardDBs {
		if otherShardID == shardID {
			continue
		}

		_, err := store.NewRepositories(db).Transactions.GetByID(context.Background(), "tx_http_multishard_1")
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expected transaction to be absent from shard %s, got %v", otherShardID, err)
		}
	}
}

func httpTestUserIDForShard(t *testing.T, router sharding.Router, target sharding.ShardID) string {
	t.Helper()

	for i := 1; i <= 10_000; i++ {
		userID := fmt.Sprintf("http_user_%d", i)
		shardID, err := router.ShardForUser(userID)
		if err != nil {
			t.Fatalf("route user %s: %v", userID, err)
		}
		if shardID == target {
			return userID
		}
	}

	t.Fatalf("could not find user for shard %s", target)
	return ""
}

func seedShardHTTPAccounts(t *testing.T, db store.DBTX, userID, systemAccountID string) {
	t.Helper()

	repos := store.NewRepositories(db)
	ctx := context.Background()
	now := time.Date(2026, 3, 23, 9, 0, 0, 0, time.UTC)

	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            sharding.UserAccountID(userID, "USD"),
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion: 10,
			CurrentBalances: domain.BalanceBuckets{
				PostedCredits:  100,
				PendingCredits: 100,
			},
			UpdatedAt: now,
		},
		{
			Account: domain.Account{
				ID:            systemAccountID,
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion:  0,
			CurrentBalances: domain.BalanceBuckets{},
			UpdatedAt:       now,
		},
	}

	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account); err != nil {
			t.Fatalf("seed HTTP shard account %s: %v", account.Account.ID, err)
		}
	}
}
