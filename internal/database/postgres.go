package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/sniperman/ledger/internal/sharding"
)

const driverName = "pgx"

func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open(driverName, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open sql db: %w", err)
	}

	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return db, nil
}

func OpenMany(ctx context.Context, urls map[sharding.ShardID]string) (map[sharding.ShardID]*sql.DB, error) {
	dbs := make(map[sharding.ShardID]*sql.DB, len(urls))

	for shardID, url := range urls {
		db, err := Open(ctx, url)
		if err != nil {
			CloseMany(dbs)
			return nil, fmt.Errorf("open shard db %s: %w", shardID, err)
		}
		dbs[shardID] = db
	}

	return dbs, nil
}

func CloseMany(dbs map[sharding.ShardID]*sql.DB) {
	for _, db := range dbs {
		_ = db.Close()
	}
}
