package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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

func TestWithdrawalCreateAndReadHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.create", enqueueWithdrawalCreateCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_create_1",
		TransactionID:  "tx_http_withdrawal_create_1",
		PostingKey:     "pk_http_withdrawal_create_1",
		Amount:         70,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on withdrawal create, got %d", statusCode)
	}

	processNextSingleShardCommand(t, db, router, "shard-a")

	var user accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/"+sharding.UserAccountID("user_123")+"/balances", &user)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for user balances, got %d", statusCode)
	}
	if user.Posted != 100 || user.Pending != 30 || user.Available != 30 {
		t.Fatalf("unexpected user balances after withdrawal create: %+v", user)
	}

	var loaded transactionResponse
	statusCode = getJSON(t, httpServer.URL+"/transactions/tx_http_withdrawal_create_1", &loaded)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for transaction get, got %d", statusCode)
	}
	if loaded.TransactionID != "tx_http_withdrawal_create_1" || loaded.Status != string(domain.TransactionStatusPending) {
		t.Fatalf("unexpected loaded transaction: %+v", loaded)
	}
}

func TestWithdrawalPostHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	createPendingWithdrawalOverHTTP(t, httpServer.URL)
	processNextSingleShardCommand(t, db, router, "shard-a")

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.post", enqueueTransitionCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_post_1",
		TransactionID:  "tx_http_withdrawal_post_1",
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on withdrawal post, got %d", statusCode)
	}
	processNextSingleShardCommand(t, db, router, "shard-a")

	var user accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/"+sharding.UserAccountID("user_123")+"/balances", &user)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for user balances, got %d", statusCode)
	}
	if user.Posted != 30 || user.Pending != 30 || user.Available != 30 {
		t.Fatalf("unexpected user balances after withdrawal post: %+v", user)
	}

	var holding accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/payout_holding:shard-a/balances", &holding)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for holding balances, got %d", statusCode)
	}
	if holding.Posted != 70 || holding.Pending != 70 || holding.Available != 70 {
		t.Fatalf("unexpected holding balances after withdrawal post: %+v", holding)
	}
}

func TestWithdrawalArchiveHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	createPendingWithdrawalOverHTTP(t, httpServer.URL)
	processNextSingleShardCommand(t, db, router, "shard-a")

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.archive", enqueueTransitionCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_archive_1",
		TransactionID:  "tx_http_withdrawal_post_1",
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on withdrawal archive, got %d", statusCode)
	}
	processNextSingleShardCommand(t, db, router, "shard-a")

	var user accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/"+sharding.UserAccountID("user_123")+"/balances", &user)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for user balances, got %d", statusCode)
	}
	if user.Posted != 100 || user.Pending != 100 || user.Available != 100 {
		t.Fatalf("unexpected user balances after withdrawal archive: %+v", user)
	}

	var holding accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/payout_holding:shard-a/balances", &holding)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for holding balances, got %d", statusCode)
	}
	if holding.Posted != 0 || holding.Pending != 0 || holding.Available != 0 {
		t.Fatalf("unexpected holding balances after withdrawal archive: %+v", holding)
	}
}

func TestDepositRecordHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	router, err := sharding.NewRouter([]sharding.ShardID{"shard-a"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.deposits.record", enqueueDepositRecordCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_deposit_record_1",
		TransactionID:  "tx_http_deposit_record_1",
		PostingKey:     "pk_http_deposit_record_1",
		Amount:         50,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 21, 11, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 21, 11, 0, 1, 0, time.UTC),
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on deposit record, got %d", statusCode)
	}
	processNextSingleShardCommand(t, db, router, "shard-a")

	var user accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/"+sharding.UserAccountID("user_123")+"/balances", &user)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for user balances, got %d", statusCode)
	}
	if user.Posted != 150 || user.Pending != 150 || user.Available != 150 {
		t.Fatalf("unexpected user balances after deposit record: %+v", user)
	}

	var clearing accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/cash_in_clearing:shard-a/balances", &clearing)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for clearing balances, got %d", statusCode)
	}
	if clearing.Posted != 50 || clearing.Pending != 50 || clearing.Available != 50 {
		t.Fatalf("unexpected clearing balances after deposit record: %+v", clearing)
	}
}

func createPendingWithdrawalOverHTTP(t *testing.T, baseURL string) {
	t.Helper()

	statusCode := postJSON(t, baseURL+"/commands/payments.withdrawals.create", enqueueWithdrawalCreateCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_create_pending",
		TransactionID:  "tx_http_withdrawal_post_1",
		PostingKey:     "pk_http_withdrawal_post_1",
		Amount:         70,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
	}, nil)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected withdrawal create enqueue to return 202, got %d", statusCode)
	}
}

func processNextSingleShardCommand(t *testing.T, db *sql.DB, router sharding.Router, shardID sharding.ShardID) {
	t.Helper()

	commandService := service.NewCommandService(db, router, nil)
	if _, ok, err := commandService.ProcessNext(context.Background(), shardID, time.Now().UTC()); err != nil {
		t.Fatalf("process next command: %v", err)
	} else if !ok {
		t.Fatal("expected queued command to be processed")
	}
}

func postJSON(t *testing.T, url string, body any, out any) int {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	response, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer response.Body.Close()

	if out != nil {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			t.Fatalf("decode POST response %s: %v", url, err)
		}
	}

	return response.StatusCode
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()

	response, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer response.Body.Close()

	if out != nil {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			t.Fatalf("decode GET response %s: %v", url, err)
		}
	}

	return response.StatusCode
}

func seedHTTPIntegrationAccounts(t *testing.T, db store.DBTX) {
	t.Helper()

	ctx := context.Background()
	repos := store.NewRepositories(db)
	now := time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC)

	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            sharding.UserAccountID("user_123"),
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
				ID:            "payout_holding:shard-a",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			CurrentVersion:  0,
			CurrentBalances: domain.BalanceBuckets{},
			UpdatedAt:       now,
		},
		{
			Account: domain.Account{
				ID:            "cash_in_clearing:shard-a",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceDebit,
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
			t.Fatalf("seed HTTP integration account %s: %v", account.Account.ID, err)
		}
	}
}
