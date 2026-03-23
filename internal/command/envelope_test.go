package command

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/sharding"
)

func TestEnvelopeValidateAcceptsValidEnvelope(t *testing.T) {
	t.Parallel()

	err := Envelope{
		CommandID:      "cmd_123",
		IdempotencyKey: "idem_123",
		ShardID:        "shard-a",
		Type:           TypeTransactionCreate,
		Payload:        json.RawMessage(`{"transaction_id":"tx_123"}`),
		Status:         StatusAccepted,
		CreatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
	}.Validate()
	if err != nil {
		t.Fatalf("expected valid envelope, got %v", err)
	}
}

func TestEnvelopeValidateRejectsMissingPayload(t *testing.T) {
	t.Parallel()

	err := Envelope{
		CommandID:      "cmd_123",
		IdempotencyKey: "idem_123",
		ShardID:        "shard-a",
		Type:           TypeTransactionCreate,
		Status:         StatusAccepted,
		CreatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
	}.Validate()
	if err == nil {
		t.Fatal("expected missing payload error")
	}
}

func TestEnvelopeValidateRejectsInvalidShardID(t *testing.T) {
	t.Parallel()

	err := Envelope{
		CommandID:      "cmd_123",
		IdempotencyKey: "idem_123",
		ShardID:        sharding.ShardID("Shard A"),
		Type:           TypeTransactionCreate,
		Payload:        json.RawMessage(`{"transaction_id":"tx_123"}`),
		Status:         StatusAccepted,
		CreatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
	}.Validate()
	if err == nil {
		t.Fatal("expected invalid shard id error")
	}
}

func TestEnvelopeValidateAcceptsPaymentCommandType(t *testing.T) {
	t.Parallel()

	err := Envelope{
		CommandID:      "cmd_123",
		IdempotencyKey: "idem_123",
		ShardID:        "shard-a",
		Type:           TypeWithdrawalCreate,
		Payload:        json.RawMessage(`{"transaction_id":"tx_123"}`),
		Status:         StatusAccepted,
		CreatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
	}.Validate()
	if err != nil {
		t.Fatalf("expected valid payment envelope, got %v", err)
	}
}
