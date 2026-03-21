package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/migrations"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	db, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	runner := migrations.NewRunner(db, migrations.Files)

	applied, err := runner.Up(ctx)
	if err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	if len(applied) == 0 {
		slog.Info("no pending migrations")
		return
	}

	for _, name := range applied {
		slog.Info("applied migration", "name", name)
	}
}
