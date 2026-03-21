# Ledger Requirements

## Functional Requirements
- Record money movement with double-entry transactions and immutable entries.
- Keep `Transaction`, `Entry`, and `Account` as core primitives.
- Support transaction lifecycle: `pending`, `posted`, `archived`.
- Derive balances from entries, not from editable balance fields.
- Support both `recording` and `authorizing` write modes.
- Enforce atomic writes across all entries in one transaction.
- Support idempotent writes for retries and duplicate requests.
- Support balance locks and version locks for spend control.
- Support current balance reads and historical balance reconstruction.
- Support balance buckets: `posted_debits`, `posted_credits`, `pending_debits`, `pending_credits`.
- Support business views such as `available`, `reserved`, and `settling`.
- Support reconciliation, audit, and replay from ledger history.

## Non-Functional Requirements
- Correctness first: no double spend, no unbalanced transaction, no silent mutation.
- Strong auditability: every balance must be explainable from entries.
- Deterministic history: historical reads must be reproducible by `effective_at` and `account_version`.
- Concurrency safety: overlapping writes must not corrupt balances.
- Fast balance reads through safe caching and drift monitoring.
- Operational resilience: retries, backfills, reprocessing, and incident runbooks.
- Clear failure handling for pending, failed, discarded, and archived flows.
- Extensibility for new products, account types, and external integrations.
- Scalable write path with queueing where synchronous writes become a bottleneck.
- Observability for throughput, lock contention, queue lag, and cache drift.

## Research Areas
- Data model and invariants
- Transaction lifecycle
- Recording vs authorizing
- Historical balance reconstruction
- Concurrency and idempotency
- Balance caching and drift handling
- Write-ahead queueing
- Reconciliation and operational tooling
