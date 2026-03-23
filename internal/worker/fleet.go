package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

type Processor interface {
	ProcessNext(ctx context.Context, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error)
}

type Fleet struct {
	processor    Processor
	shardIDs     []sharding.ShardID
	pollInterval time.Duration
	logger       *slog.Logger
	now          func() time.Time
}

func NewFleet(processor Processor, shardIDs []sharding.ShardID, pollInterval time.Duration, logger *slog.Logger) (*Fleet, error) {
	if processor == nil {
		return nil, fmt.Errorf("worker processor is required")
	}
	if len(shardIDs) == 0 {
		return nil, fmt.Errorf("at least one shard id is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("worker poll interval must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}

	normalizedShardIDs := make([]sharding.ShardID, len(shardIDs))
	copy(normalizedShardIDs, shardIDs)
	for _, shardID := range normalizedShardIDs {
		if err := shardID.Validate(); err != nil {
			return nil, err
		}
	}

	return &Fleet{
		processor:    processor,
		shardIDs:     normalizedShardIDs,
		pollInterval: pollInterval,
		logger:       logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (f *Fleet) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(len(f.shardIDs))

	for _, shardID := range f.shardIDs {
		go func(shardID sharding.ShardID) {
			defer wg.Done()
			f.runShardLoop(ctx, shardID)
		}(shardID)
	}

	wg.Wait()
}

func (f *Fleet) ProcessAvailable(ctx context.Context, shardID sharding.ShardID) (int, error) {
	if err := shardID.Validate(); err != nil {
		return 0, err
	}

	processed := 0
	for {
		select {
		case <-ctx.Done():
			return processed, ctx.Err()
		default:
		}

		_, ok, err := f.processor.ProcessNext(ctx, shardID, f.now())
		if err != nil {
			return processed, err
		}
		if !ok {
			return processed, nil
		}

		processed++
	}
}

func (f *Fleet) ProcessAvailableAll(ctx context.Context) (map[sharding.ShardID]int, error) {
	results := make(map[sharding.ShardID]int, len(f.shardIDs))

	for _, shardID := range f.shardIDs {
		processed, err := f.ProcessAvailable(ctx, shardID)
		if err != nil {
			return nil, err
		}
		results[shardID] = processed
	}

	return results, nil
}

func (f *Fleet) runShardLoop(ctx context.Context, shardID sharding.ShardID) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		envelope, ok, err := f.processor.ProcessNext(ctx, shardID, f.now())
		if err != nil {
			f.logger.Error("process shard command", "shard_id", shardID, "error", err)
			timer.Reset(f.pollInterval)
			continue
		}

		if ok {
			f.logger.Info("processed shard command", "shard_id", shardID, "command_id", envelope.CommandID, "type", envelope.Type, "status", envelope.Status)
			timer.Reset(0)
			continue
		}

		timer.Reset(f.pollInterval)
	}
}
