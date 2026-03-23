package sharding

import (
	"database/sql"
	"fmt"
)

type DBRegistry struct {
	byShard map[ShardID]*sql.DB
}

func NewDBRegistry(byShard map[ShardID]*sql.DB) (*DBRegistry, error) {
	if len(byShard) == 0 {
		return nil, fmt.Errorf("at least one shard db is required")
	}

	registry := &DBRegistry{byShard: make(map[ShardID]*sql.DB, len(byShard))}
	for shardID, db := range byShard {
		if err := shardID.Validate(); err != nil {
			return nil, err
		}
		if db == nil {
			return nil, fmt.Errorf("nil db for shard %q", shardID)
		}
		registry.byShard[shardID] = db
	}

	return registry, nil
}

func NewSingleDBRegistry(shardIDs []ShardID, db *sql.DB) (*DBRegistry, error) {
	router, err := NewRouter(shardIDs, nil)
	if err != nil {
		return nil, err
	}

	byShard := make(map[ShardID]*sql.DB, len(router.shardIDs))
	for _, shardID := range router.shardIDs {
		byShard[shardID] = db
	}

	return NewDBRegistry(byShard)
}

func (r *DBRegistry) DBForShard(shardID ShardID) (*sql.DB, error) {
	db, ok := r.byShard[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard id %q", shardID)
	}
	return db, nil
}

func (r *DBRegistry) SingleShard() (ShardID, *sql.DB, bool) {
	if len(r.byShard) != 1 {
		return "", nil, false
	}

	for shardID, db := range r.byShard {
		return shardID, db, true
	}

	return "", nil, false
}
