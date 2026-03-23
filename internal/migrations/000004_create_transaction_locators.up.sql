CREATE TABLE IF NOT EXISTS ledger.transaction_locators (
    transaction_id TEXT PRIMARY KEY,
    shard_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transaction_locators_shard_id
    ON ledger.transaction_locators (shard_id);
