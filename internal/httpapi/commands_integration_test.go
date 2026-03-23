package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestEnqueueWithdrawalCreateHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	requestBody := enqueueWithdrawalCreateCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_create_1",
		TransactionID:  "tx_http_withdrawal_create_1",
		PostingKey:     "pk_http_withdrawal_create_1",
		Amount:         70,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 22, 10, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 22, 10, 0, 1, 0, time.UTC),
	}

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.create", requestBody, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on first command enqueue, got %d", statusCode)
	}
	if accepted.Type != "payments.withdrawals.create" || accepted.Status != "accepted" || accepted.ShardID != "shard-a" {
		t.Fatalf("unexpected command response: %+v", accepted)
	}

	var loaded commandResponse
	statusCode = getJSON(t, httpServer.URL+"/commands/"+accepted.CommandID, &loaded)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on command get, got %d", statusCode)
	}
	if loaded.CommandID != accepted.CommandID || loaded.Status != "accepted" {
		t.Fatalf("unexpected loaded command response: %+v", loaded)
	}

	var replayed commandResponse
	statusCode = postJSON(t, httpServer.URL+"/commands/payments.withdrawals.create", requestBody, &replayed)
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 on idempotent command replay, got %d", statusCode)
	}
	if replayed.CommandID != accepted.CommandID {
		t.Fatalf("expected replayed command to match original, got accepted=%+v replayed=%+v", accepted, replayed)
	}
}

func TestEnqueueWithdrawalPostHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	var accepted commandResponse
	statusCode := postJSON(t, httpServer.URL+"/commands/payments.withdrawals.post", enqueueTransitionCommandRequest{
		UserID:         "user_123",
		IdempotencyKey: "idem_http_withdrawal_post_1",
		TransactionID:  "tx_http_withdrawal_post_1",
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on withdrawal post command enqueue, got %d", statusCode)
	}
	if accepted.Type != "payments.withdrawals.post" || accepted.Status != "accepted" || accepted.ShardID != "shard-a" {
		t.Fatalf("unexpected post command response: %+v", accepted)
	}
}

func TestEnqueueDepositRecordHTTPIntegration(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

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
		Amount:         55,
		Currency:       "USD",
		EffectiveAt:    time.Date(2026, 3, 22, 11, 0, 0, 0, time.UTC),
		CreatedAt:      time.Date(2026, 3, 22, 11, 0, 1, 0, time.UTC),
	}, &accepted)
	if statusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on deposit record enqueue, got %d", statusCode)
	}
	if accepted.Type != "payments.deposits.record" || accepted.Status != "accepted" || accepted.ShardID != "shard-a" {
		t.Fatalf("unexpected deposit command response: %+v", accepted)
	}
}
