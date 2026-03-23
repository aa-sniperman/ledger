package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/httpapi"
	"github.com/sniperman/ledger/internal/kafkabus"
	"github.com/sniperman/ledger/internal/sharding"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if cfg.KafkaEnabled && cfg.KafkaAutoCreateTopic {
		if err := kafkabus.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaCommandsTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor); err != nil {
			slog.Error("ensure kafka topic", "topic", cfg.KafkaCommandsTopic, "error", err)
			os.Exit(1)
		}
	}

	var shardDBs map[sharding.ShardID]*sql.DB
	if len(cfg.ShardDatabaseURLs) > 0 {
		shardDBs, err = database.OpenMany(ctx, cfg.ShardDatabaseURLs)
		if err != nil {
			slog.Error("open shard databases", "error", err)
			os.Exit(1)
		}
		defer database.CloseMany(shardDBs)
	}

	server := httpapi.New(cfg, db)
	if len(shardDBs) > 0 {
		router, err := sharding.NewRouter(cfg.ShardIDs, nil)
		if err != nil {
			slog.Error("build shard router", "error", err)
			os.Exit(1)
		}
		registry, err := sharding.NewDBRegistry(shardDBs)
		if err != nil {
			slog.Error("build shard registry", "error", err)
			os.Exit(1)
		}
		server = httpapi.NewWithRegistry(cfg, db, router, registry)
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown server", "error", err)
		}
	}()

	slog.Info("starting api server", "addr", cfg.HTTPAddr, "env", cfg.AppEnv)

	if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serve http", "error", err)
		os.Exit(1)
	}
}
