# Phase 1 Implementation Plan

## Goal

Build the smallest production-shaped ledger service in Go that proves the core ledger model:

- double-entry transactions
- immutable entries
- transaction lifecycle: `pending`, `posted`, `archived`
- balances derived from entries
- idempotent transaction creation

Phase 1 is intentionally synchronous and single-node. It should validate the design before introducing concurrency controls, historical reconstruction, queue-based processing, or sharding.

## Scope

Phase 1 should support:

- create accounts
- create a balanced transaction in `pending`, `posted`, or `archived`
- post a pending transaction
- archive a pending transaction
- read account balances: `posted`, `pending`, `available`
- read transaction details and entries
- enforce append-only entry history
- enforce balanced writes per currency
- enforce idempotency on transaction creation

Phase 1 should not support:

- version locking
- pessimistic locking
- historical balance snapshots with `account_version`
- historical balance cache
- async workers or write-ahead queues
- sharding
- reconciliation tooling
- external rail integrations

## Architecture

Use a simple layered Go service with PostgreSQL.

Suggested layout:

- `cmd/api`: server bootstrap
- `internal/domain`: core types and invariants
- `internal/service`: use cases and transaction lifecycle logic
- `internal/store`: PostgreSQL repositories
- `internal/http`: HTTP handlers and request/response DTOs
- `internal/testutil`: fixtures and integration test helpers

Keep the first version boring:

- one binary
- one Postgres database
- standard `net/http`
- SQL migrations
- explicit SQL queries instead of ORM magic

## Data Model

### `accounts`

Fields:

- `account_id`
- `currency`
- `normal_balance`
- `created_at`
- `updated_at`

Notes:

- `normal_balance` is `credit_normal` or `debit_normal`
- no editable balance fields in this table

### `account_current_balances`

Fields:

- `account_id`
- `posted_debits`
- `posted_credits`
- `pending_debits`
- `pending_credits`
- `current_version`
- `updated_at`

Notes:

- one row per account, linked `1:1` by `account_id`
- stores the current balance buckets for fast reads and later balance locking
- `current_version` is the authoritative current account version for new entry writes
- remains a derived current-state table, not the financial source of truth

### `ledger_transactions`

Fields:

- `transaction_id`
- `posting_key`
- `type`
- `status`
- `effective_at`
- `created_at`

Notes:

- `posting_key` is the idempotency key for create requests
- transaction mode is service-layer behavior and is not persisted on the transaction record

### `ledger_entries`

Fields:

- `entry_id`
- `transaction_id`
- `account_id`
- `account_version`
- `amount`
- `currency`
- `direction`
- `status`
- `effective_at`
- `created_at`
- `discarded_at`

Notes:

- entries are append-only
- `account_version` is assigned by the write path from the current balance row version returned by the database
- pending entries can be discarded during lifecycle transition
- posting or archiving creates new terminal-state entries instead of mutating existing ones in place

## Domain Rules

Implement these rules before building the API:

1. A transaction must contain at least 2 entries.
2. Debits must equal credits per currency.
3. Entry amount must be positive.
4. All referenced accounts must exist and match entry currency.
5. Each entry `account_version` is assigned from the authoritative account version produced by the atomic balance-row update.
6. Service logic decides whether a create request is handled as an `authorizing` flow or a `recording` flow.
7. `authorizing` create flows produce `pending` transactions.
8. `recording` create flows may produce `pending`, `posted`, or `archived` transactions depending on the external event being recorded.
9. `authorizing` create flows must pass the current-balance availability check before the write commits.
10. `authorizing` create flows must update current balance buckets atomically with the ledger write.
11. Entry status must match the transaction status on creation.
12. Only `pending` transactions can transition to `posted` or `archived`.
13. Posted transactions are final.
14. Archived transactions are final.
15. Pending entries may be discarded, but posted entries are immutable.
16. Balances are always derived from ledger entries, never edited directly.

## Balance Semantics

Phase 1 keeps `ledger_entries` as the financial source of truth, but also persists `account_current_balances` as the linked current-state bucket row for each account.

Definitions:

- `posted_balance`: sum of non-discarded `posted` entries
- `pending_balance`: sum of non-discarded `posted` and `pending` entries
- `available_balance`: derived from normal balance and pending state

Recommended implementation approach:

- aggregate debit and credit sums in SQL
- interpret the result using account `normal_balance`

For a `credit_normal` account:

- `posted = posted_credits - posted_debits`
- `pending = pending_credits - pending_debits`
- `available` should follow the wallet semantics in the design docs

For a `debit_normal` account:

- invert the interpretation accordingly

In early phase 1 work, balances can still be cross-checked from `ledger_entries` to keep correctness easy to validate.

## HTTP API

Keep the API narrow.

### `POST /accounts`

Create a ledger account.

Request fields:

- `account_id`
- `currency`
- `normal_balance`

### `POST /transactions`

Create a transaction and its entries atomically.

Request fields:

- `transaction_id`
- `posting_key`
- `type`
- `status`
- `effective_at`
- `entries[]`

Each entry should contain:

- `entry_id`
- `account_id`
- `amount`
- `currency`
- `direction`

### `POST /transactions/{id}/post`

Transition a pending transaction to posted.

Behavior:

- verify current status is `pending`
- discard old pending entries
- create new posted entries with the same financial meaning
- update transaction status to `posted`

### `POST /transactions/{id}/archive`

Transition a pending transaction to archived.

Behavior:

- verify current status is `pending`
- discard old pending entries
- create new archived entries
- update transaction status to `archived`

### `GET /transactions/{id}`

Return:

- transaction metadata
- current status
- all entries, including discarded ones if needed for audit visibility

### `GET /accounts/{id}/balances`

Return:

- `posted`
- `pending`
- `available`

## Persistence Strategy

Use explicit SQL and DB transactions.

### Create Transaction

In one DB transaction:

1. verify `posting_key` uniqueness
2. validate accounts and currencies
3. validate balanced entries
4. validate status rules for the chosen service flow
5. for `authorizing` handling, apply balance checks and atomic balance-bucket updates
6. apply current balance-bucket updates for the affected accounts
7. insert `ledger_transactions`
8. insert all `ledger_entries` with status matching the transaction status
9. commit

### Post Transaction

In one DB transaction:

1. load transaction and verify status is `pending`
2. load current non-discarded pending entries
3. mark those entries with `discarded_at`
4. insert equivalent `posted` entries
5. update transaction status to `posted`
6. commit

### Archive Transaction

In one DB transaction:

1. load transaction and verify status is `pending`
2. load current non-discarded pending entries
3. mark those entries with `discarded_at`
4. insert equivalent `archived` entries
5. update transaction status to `archived`
6. commit

This keeps entry history append-only while preserving the transaction lifecycle model from the design.

## Development Milestones

### Milestone 1: Project Skeleton

- initialize Go module
- set up directory structure
- add config loading
- wire HTTP server
- add database connection and migration runner

Outcome:

- service starts and connects to Postgres

### Milestone 2: Domain Model

- define enums and value objects
- implement validation logic
- add unit tests for balancing and lifecycle rules

Outcome:

- domain rules are executable and testable in isolation

### Milestone 3: Persistence Layer

- create schema migrations
- implement account repository
- implement transaction repository
- implement DB transaction helpers

Outcome:

- repositories can create and load core entities safely

### Milestone 4: Transaction Creation

- implement `POST /transactions`
- support both service flows
- for authorizing handling, insert `pending` transaction and entries atomically
- for recording handling, insert `pending`, `posted`, or `archived` transaction and matching entries atomically based on the external event being recorded
- for authorizing handling, apply balance checks and atomic balance-bucket updates before commit
- enforce idempotency with `posting_key`

Outcome:

- clients can create ledger transactions safely through both authorizing and recording handling paths

### Milestone 5: Lifecycle Transitions

- implement `post`
- implement `archive`
- apply lifecycle transitions only to transactions currently in `pending`
- keep direct-final creates out of the pending-transition path
- ensure discarded pending entries remain auditable

Outcome:

- pending-to-final lifecycle works end to end, while direct-final create flows remain valid without a synthetic pending phase

### Milestone 6: Balance Reads

- implement SQL aggregation queries
- compute `posted`, `pending`, and `available`
- implement `GET /accounts/{id}/balances`

Outcome:

- balances can be explained entirely from ledger history

### Milestone 7: Integration Hardening

- add integration tests against Postgres
- standardize error responses
- add request validation
- add logging around transaction lifecycle operations

Outcome:

- phase 1 is stable enough for local experimentation and iteration

## Testing Plan

### Unit Tests

Cover:

- balanced vs unbalanced transaction validation
- amount validation
- invalid lifecycle transitions
- normal balance interpretation

### Repository / DB Tests

Cover:

- run against a dedicated test Postgres configured by `TEST_DATABASE_URL`
- atomic creation of transaction plus entries
- rollback on invalid insert
- post transition behavior for pending transactions
- archive transition behavior for pending transactions
- idempotency key uniqueness

### API Integration Tests

Cover:

- create account
- create pending transaction
- duplicate create request with same `posting_key`
- post pending transaction
- archive pending transaction
- fetch balances after each lifecycle step

Minimum end-to-end scenarios:

1. Balanced transaction succeeds.
2. Unbalanced transaction fails.
3. Duplicate `posting_key` is handled deterministically.
4. Posting moves a pending transaction into final posted state.
5. Archiving removes pending effect from balances for a pending transaction.
6. Re-posting or re-archiving fails.
7. Reported balances match ledger entry sums exactly.

## Suggested Working Style In Go

Since the goal is also to learn Go while building:

- keep interfaces minimal
- prefer plain structs and functions over abstraction-heavy designs
- write SQL explicitly
- write tests for invariants before adding framework code
- keep handlers thin and business logic in services

Avoid premature complexity:

- no event bus
- no CQRS split
- no generic repository framework
- no ORM-first design

## Definition Of Done

Phase 1 is done when the service can:

- create accounts
- create pending double-entry transactions
- post or archive pending transactions correctly
- return balances derived from entries
- preserve append-only audit history
- reject invalid or unbalanced writes
- behave deterministically on duplicate create requests

At that point, the core ledger model is proven and phase 2 can focus on concurrency controls, historical reconstruction, and balance caching.
