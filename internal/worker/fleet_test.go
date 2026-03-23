package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

type stubProcessor struct {
	queues map[sharding.ShardID]int
	errs   map[sharding.ShardID]error
}

func (p *stubProcessor) ProcessNext(_ context.Context, shardID sharding.ShardID, _ time.Time) (command.Envelope, bool, error) {
	if err := p.errs[shardID]; err != nil {
		return command.Envelope{}, false, err
	}
	if p.queues[shardID] == 0 {
		return command.Envelope{}, false, nil
	}

	p.queues[shardID]--
	return command.Envelope{CommandID: "cmd_" + string(shardID), ShardID: shardID, Status: command.StatusSucceeded}, true, nil
}

func TestFleetProcessAvailableAll(t *testing.T) {
	t.Parallel()

	fleet, err := NewFleet(
		&stubProcessor{queues: map[sharding.ShardID]int{"shard-a": 2, "shard-b": 1}},
		[]sharding.ShardID{"shard-a", "shard-b"},
		50*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("build worker fleet: %v", err)
	}

	processed, err := fleet.ProcessAvailableAll(context.Background())
	if err != nil {
		t.Fatalf("process available all: %v", err)
	}

	if processed["shard-a"] != 2 || processed["shard-b"] != 1 {
		t.Fatalf("unexpected processed counts: %+v", processed)
	}
}

func TestFleetProcessAvailableReturnsProcessorError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("boom")
	fleet, err := NewFleet(
		&stubProcessor{errs: map[sharding.ShardID]error{"shard-a": expectedErr}},
		[]sharding.ShardID{"shard-a"},
		50*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("build worker fleet: %v", err)
	}

	_, err = fleet.ProcessAvailable(context.Background(), "shard-a")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected processor error %v, got %v", expectedErr, err)
	}
}
