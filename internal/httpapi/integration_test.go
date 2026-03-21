package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/store"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestCreateTransactionAndDuplicateHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	requestBody := createTransactionRequest{
		Flow:          "authorizing",
		TransactionID: "tx_http_create_1",
		PostingKey:    "pk_http_create_1",
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []createTransactionEntryRequest{
			{EntryID: "entry_http_create_1_debit", AccountID: "alice_wallet", Amount: 70, Currency: "USD", Direction: "debit"},
			{EntryID: "entry_http_create_1_credit", AccountID: "partner_payout_holding", Amount: 70, Currency: "USD", Direction: "credit"},
		},
	}

	var created transactionResponse
	statusCode := postJSON(t, httpServer.URL+"/transactions", requestBody, &created)
	if statusCode != http.StatusCreated {
		t.Fatalf("expected 201 on first create, got %d", statusCode)
	}
	if created.Status != string(domain.TransactionStatusPending) {
		t.Fatalf("expected pending transaction, got %+v", created)
	}

	var replayed transactionResponse
	statusCode = postJSON(t, httpServer.URL+"/transactions", requestBody, &replayed)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on idempotent replay, got %d", statusCode)
	}
	if replayed.TransactionID != created.TransactionID || replayed.PostingKey != created.PostingKey {
		t.Fatalf("expected replayed transaction to match original, got created=%+v replayed=%+v", created, replayed)
	}

	var alice accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/alice_wallet/balances", &alice)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for alice balances, got %d", statusCode)
	}
	if alice.Posted != 100 || alice.Pending != 30 || alice.Available != 30 {
		t.Fatalf("unexpected alice balances after create: %+v", alice)
	}
}

func TestPostTransactionHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	createPendingTransactionOverHTTP(t, httpServer.URL, "tx_http_post_1", "pk_http_post_1")

	var posted transactionResponse
	statusCode := postJSON(t, httpServer.URL+"/transactions/tx_http_post_1/post", map[string]any{}, &posted)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on post, got %d", statusCode)
	}
	if posted.Status != string(domain.TransactionStatusPosted) {
		t.Fatalf("expected posted transaction, got %+v", posted)
	}

	var alice accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/alice_wallet/balances", &alice)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for alice balances, got %d", statusCode)
	}
	if alice.Posted != 30 || alice.Pending != 30 || alice.Available != 30 {
		t.Fatalf("unexpected alice balances after post: %+v", alice)
	}

	var holding accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/partner_payout_holding/balances", &holding)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for holding balances, got %d", statusCode)
	}
	if holding.Posted != 70 || holding.Pending != 70 || holding.Available != 70 {
		t.Fatalf("unexpected holding balances after post: %+v", holding)
	}
}

func TestArchiveTransactionHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)
	seedHTTPIntegrationAccounts(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	createPendingTransactionOverHTTP(t, httpServer.URL, "tx_http_archive_1", "pk_http_archive_1")

	var archived transactionResponse
	statusCode := postJSON(t, httpServer.URL+"/transactions/tx_http_archive_1/archive", map[string]any{}, &archived)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on archive, got %d", statusCode)
	}
	if archived.Status != string(domain.TransactionStatusArchived) {
		t.Fatalf("expected archived transaction, got %+v", archived)
	}

	var alice accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/alice_wallet/balances", &alice)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for alice balances, got %d", statusCode)
	}
	if alice.Posted != 100 || alice.Pending != 100 || alice.Available != 100 {
		t.Fatalf("unexpected alice balances after archive: %+v", alice)
	}

	var holding accountBalanceResponse
	statusCode = getJSON(t, httpServer.URL+"/accounts/partner_payout_holding/balances", &holding)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 for holding balances, got %d", statusCode)
	}
	if holding.Posted != 0 || holding.Pending != 0 || holding.Available != 0 {
		t.Fatalf("unexpected holding balances after archive: %+v", holding)
	}
}

func createPendingTransactionOverHTTP(t *testing.T, baseURL, transactionID, postingKey string) {
	t.Helper()

	statusCode := postJSON(t, baseURL+"/transactions", createTransactionRequest{
		Flow:          "authorizing",
		TransactionID: transactionID,
		PostingKey:    postingKey,
		Type:          "wallet_transfer_hold",
		EffectiveAt:   time.Date(2026, 3, 21, 10, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 21, 10, 0, 1, 0, time.UTC),
		Entries: []createTransactionEntryRequest{
			{EntryID: transactionID + "_debit", AccountID: "alice_wallet", Amount: 70, Currency: "USD", Direction: "debit"},
			{EntryID: transactionID + "_credit", AccountID: "partner_payout_holding", Amount: 70, Currency: "USD", Direction: "credit"},
		},
	}, nil)
	if statusCode != http.StatusCreated {
		t.Fatalf("expected pending transaction seed create to return 201, got %d", statusCode)
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

	accounts := []domain.AccountState{
		{
			Account: domain.Account{
				ID:            "alice_wallet",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
			},
			CurrentVersion: 10,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  100,
				PendingDebits:  0,
				PendingCredits: 100,
			},
			UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
		},
		{
			Account: domain.Account{
				ID:            "partner_payout_holding",
				Currency:      "USD",
				NormalBalance: domain.NormalBalanceCredit,
				CreatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
			},
			CurrentVersion: 3,
			CurrentBalances: domain.BalanceBuckets{
				PostedDebits:   0,
				PostedCredits:  0,
				PendingDebits:  0,
				PendingCredits: 0,
			},
			UpdatedAt: time.Date(2026, 3, 21, 9, 0, 0, 0, time.UTC),
		},
	}

	for _, account := range accounts {
		if err := repos.Accounts.Create(ctx, account); err != nil {
			t.Fatalf("seed account %s: %v", account.Account.ID, err)
		}
	}
}
