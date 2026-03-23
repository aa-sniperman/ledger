package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	centralDB, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("open central database", "error", err)
		os.Exit(1)
	}
	defer centralDB.Close()

	var shardDBs map[sharding.ShardID]*sql.DB
	if len(cfg.ShardDatabaseURLs) > 0 {
		shardDBs, err = database.OpenMany(ctx, cfg.ShardDatabaseURLs)
		if err != nil {
			slog.Error("open shard databases", "error", err)
			os.Exit(1)
		}
		defer database.CloseMany(shardDBs)
	}

	router, err := sharding.NewRouter(cfg.ShardIDs, map[sharding.SystemAccountRole]int{
		sharding.SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		slog.Error("build shard router", "error", err)
		os.Exit(1)
	}

	var registry *sharding.DBRegistry
	if len(shardDBs) > 0 {
		registry, err = sharding.NewDBRegistry(shardDBs)
		if err != nil {
			slog.Error("build shard registry", "error", err)
			os.Exit(1)
		}
	} else {
		registry, err = sharding.NewSingleDBRegistry(cfg.ShardIDs, centralDB)
		if err != nil {
			slog.Error("build single shard registry", "error", err)
			os.Exit(1)
		}
	}

	commandService := service.NewCommandService(centralDB, router, registry)
	fleet, err := worker.NewFleet(commandService, cfg.ShardIDs, cfg.WorkerPollInterval, slog.Default())
	if err != nil {
		slog.Error("build worker fleet", "error", err)
		os.Exit(1)
	}

	slog.Info("starting worker fleet", "shards", cfg.ShardIDs, "poll_interval", cfg.WorkerPollInterval)
	fleet.Run(ctx)
	slog.Info("worker fleet stopped")
}
