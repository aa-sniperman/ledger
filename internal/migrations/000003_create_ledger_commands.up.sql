CREATE TABLE IF NOT EXISTS ledger.ledger_commands (
    command_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    command_type TEXT NOT NULL CHECK (command_type IN ('transactions.create', 'transactions.post', 'transactions.archive')),
    payload_json JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('accepted', 'processing', 'succeeded', 'failed_retryable', 'failed_terminal')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NULL,
    result_json JSONB NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (command_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_ledger_commands_claim
    ON ledger.ledger_commands (shard_id, status, next_attempt_at, created_at);
