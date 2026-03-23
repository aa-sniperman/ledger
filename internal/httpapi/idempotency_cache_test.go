package httpapi

import (
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/command"
)

func TestIdempotencyCacheHit(t *testing.T) {
	t.Parallel()

	cache := newIdempotencyCache(5 * time.Minute)
	now := time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }

	envelope := command.Envelope{CommandID: "cmd_1", IdempotencyKey: "idem_1"}
	cache.Put(command.TypeWithdrawalCreate, "idem_1", envelope)

	got, ok := cache.Get(command.TypeWithdrawalCreate, "idem_1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.CommandID != "cmd_1" {
		t.Fatalf("expected cached command id cmd_1, got %q", got.CommandID)
	}
}

func TestIdempotencyCacheExpires(t *testing.T) {
	t.Parallel()

	cache := newIdempotencyCache(time.Minute)
	now := time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	cache.Put(command.TypeWithdrawalCreate, "idem_1", command.Envelope{CommandID: "cmd_1"})

	cache.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, ok := cache.Get(command.TypeWithdrawalCreate, "idem_1"); ok {
		t.Fatal("expected cache miss after expiry")
	}
}

func TestIdempotencyCacheKeysByCommandType(t *testing.T) {
	t.Parallel()

	cache := newIdempotencyCache(5 * time.Minute)
	cache.Put(command.TypeWithdrawalCreate, "idem_1", command.Envelope{CommandID: "cmd_withdraw"})
	cache.Put(command.TypeDepositRecord, "idem_1", command.Envelope{CommandID: "cmd_deposit"})

	got, ok := cache.Get(command.TypeDepositRecord, "idem_1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.CommandID != "cmd_deposit" {
		t.Fatalf("expected deposit command, got %+v", got)
	}
}
