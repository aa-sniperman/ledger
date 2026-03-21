CREATE TABLE IF NOT EXISTS ledger.accounts (
    account_id TEXT PRIMARY KEY,
    currency TEXT NOT NULL,
    normal_balance TEXT NOT NULL CHECK (normal_balance IN ('credit_normal', 'debit_normal')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger.account_current_balances (
    account_id TEXT PRIMARY KEY REFERENCES ledger.accounts(account_id),
    posted_debits BIGINT NOT NULL DEFAULT 0 CHECK (posted_debits >= 0),
    posted_credits BIGINT NOT NULL DEFAULT 0 CHECK (posted_credits >= 0),
    pending_debits BIGINT NOT NULL DEFAULT 0 CHECK (pending_debits >= 0),
    pending_credits BIGINT NOT NULL DEFAULT 0 CHECK (pending_credits >= 0),
    current_version BIGINT NOT NULL DEFAULT 0 CHECK (current_version >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger.ledger_transactions (
    transaction_id TEXT PRIMARY KEY,
    posting_key TEXT NOT NULL UNIQUE,
    transaction_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'posted', 'archived')),
    effective_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger.ledger_entries (
    entry_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL REFERENCES ledger.ledger_transactions(transaction_id),
    account_id TEXT NOT NULL REFERENCES ledger.accounts(account_id),
    account_version BIGINT NOT NULL CHECK (account_version >= 0),
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'posted', 'archived')),
    effective_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    discarded_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_transaction_id
    ON ledger.ledger_entries (transaction_id);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id
    ON ledger.ledger_entries (account_id);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_effective_at
    ON ledger.ledger_entries (account_id, effective_at);

CREATE INDEX IF NOT EXISTS idx_account_current_balances_current_version
    ON ledger.account_current_balances (current_version);
