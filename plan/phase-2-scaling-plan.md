# Phase 2 Scaling Plan

## Goal

Scale the ledger from the current single synchronous service into a shard-aware, asynchronous system that can handle higher TPS without introducing cross-shard financial writes.

The target shape for phase 2 is:

- shard by `user_id`
- keep shard-local system and partner accounts
- allow only `user <-> system` and `user <-> partner` transactions inside a shard
- make API-to-ledger processing asynchronous
- prepare the ledger boundary for later durable queue integration with Kafka

This phase is about scaling the write path and operational shape, not about historical balance reconstruction yet.

## Core Constraints

Keep these rules explicit:

1. User accounts are assigned to one shard by deterministic routing.
2. Each shard owns a local pool of system and partner accounts.
3. Ledger transactions must stay single-shard.
4. Direct `user <-> user` transactions are not supported.
5. Cross-shard movement must be modeled as two local transactions coordinated at a higher layer later, not as one atomic ledger transaction.
6. The ledger remains the writer of account balances and entries for its shard.
7. Idempotency must still hold across retries, worker crashes, and duplicate queue delivery.

## Target Architecture

Split the system into three logical layers:

### 1. API / Intake Layer

Responsibilities:

- authenticate and validate requests
- resolve `user_id -> shard_id`
- assign a command id / idempotency key
- persist the command to a durable intake queue
- return an accepted response quickly

This layer should not execute ledger writes directly anymore.

### 2. Ledger Worker Layer

Responsibilities:

- consume commands for one shard
- execute the existing ledger logic against that shard database
- update command status and result
- retry safely on transient failure

Workers should process commands shard-locally so account locking stays local.

### 3. Query Layer

Responsibilities:

- serve command status
- serve balances and transaction details
- read from the shard that owns the account or transaction

For phase 2, this can still query shard primaries directly.

## Sharding Model

Use consistent hashing on `user_id`:

- use rendezvous hashing so shard additions/removals move only a bounded subset of users
- store the shard map in config first
- later move shard metadata to a registry table or service

Each shard contains:

- user accounts assigned to that shard
- shard-local system accounts
- shard-local partner accounts
- transactions and entries that only reference accounts from that shard

Recommended account strategy:

- `user_wallet:{user_id}:{currency}` lives in exactly one shard and lets one user hold multiple currencies
- `system_settlement:{shard_id}:{currency}`
- `payout_holding:{shard_id}:{currency}`
- `cash_in_clearing:{shard_id}:{currency}`
- `partner_x:{shard_id}:{currency}`

Routing rule:

- first route the end user to a shard with consistent hashing on `user_id`
- then choose system and partner accounts only from that same shard
- if a system role needs more write spread, use a shard-local pool such as `payout_holding:{shard_id}:{currency}:{slot}` where `slot` is chosen deterministically from the routed user id

This avoids one global hot system account and keeps writes local.

## Async Write Flow

### Request Path

1. Client sends ledger command to API.
2. API validates the payload and determines the shard.
3. API persists a command record with:
   - `command_id`
   - `idempotency_key`
   - `shard_id`
   - `command_type`
   - `payload`
   - `status = accepted`
   - timestamps
4. API publishes or enqueues the command for that shard.
5. API returns `202 Accepted` with `command_id`.

### Worker Path

1. Worker reads the next command for its shard.
2. Worker marks the command `processing`.
3. Worker runs the same ledger service logic now used synchronously in phase 1.
4. Worker records:
   - `status = succeeded` and result metadata, or
   - `status = failed` with retry classification
5. Worker commits offsets / queue progress only after the DB write succeeds.

### Read Path

Clients poll:

- `GET /commands/{id}`
- `GET /transactions/{id}`
- `GET /accounts/{id}/balances`

That keeps the write path fast without losing result visibility.

## Queue Strategy

Implement queueing in two stages.

### Stage A: Internal Durable Intake

Use a database-backed intake queue first.

Suggested table:

- `ledger_commands`
  - `command_id`
  - `idempotency_key`
  - `shard_id`
  - `command_type`
  - `payload_json`
  - `status`
  - `attempt_count`
  - `next_attempt_at`
  - `result_json`
  - `error_code`
  - `error_message`
  - `created_at`
  - `updated_at`

Benefits:

- simpler to implement and debug
- easy transactional durability
- good enough for first async rollout

Worker polling pattern:

- `SELECT ... FOR UPDATE SKIP LOCKED`
- small batch per shard
- bounded retries with backoff

### Stage B: Kafka Boundary

After the internal queue is stable, introduce Kafka for service-to-ledger and later ledger-to-other-service integration.

Kafka topics should be partitioned by `shard_id` so ordering is preserved within a shard.

Suggested topics:

- `ledger.commands`
- `ledger.results`
- later: `ledger.events`

Kafka rules:

- key by `shard_id`
- consumer group per ledger worker fleet
- idempotent consumers
- commit offset only after shard DB commit

Kafka is not the first step. It is the second durable transport once the command model and worker behavior are proven.

## Data Model Additions

Keep the existing per-shard ledger schema, and add command-tracking tables.

### Global Routing Metadata

Possible table:

- `account_shards`
  - `account_id`
  - `account_type`
  - `user_id`
  - `shard_id`
  - `created_at`

For phase 2, user-account routing can also be derived directly from `user_id` and naming convention.

### Command Table

Add:

- `ledger_commands`
- optional `ledger_command_attempts`

The command table is the API-facing async contract.

### Per-Shard Ledger Tables

Keep the current tables per shard:

- `accounts`
- `account_current_balances`
- `ledger_transactions`
- `ledger_entries`

No cross-shard FK relationships.

## Service Changes

### API Service

Add:

- shard router
- command intake endpoint
- command status endpoint

Change create/post/archive behavior:

- these become async commands
- API should return `202 Accepted` instead of final ledger state

Suggested endpoints:

- `POST /commands/transactions.create`
- `POST /commands/transactions.post`
- `POST /commands/transactions.archive`
- `GET /commands/{id}`

You may keep the old synchronous endpoints temporarily behind a feature flag for migration.

### Ledger Worker Service

Add:

- shard-specific worker loop
- command deserialization
- retry classification
- dead-letter handling

The worker should call the existing phase 1 service logic after routing to the shard DB.

### Query Service Behavior

Balance and transaction reads now need shard resolution:

- account reads route by `account_id -> shard_id`
- transaction reads either:
  - include shard in transaction id, or
  - maintain a lightweight `transaction_locator` table

Recommended:

- include shard in transaction id or store a locator table early

## Idempotency Model

You now need idempotency in two places:

1. API command idempotency
2. ledger transaction idempotency inside the shard

Recommended rules:

- `idempotency_key` on command intake is unique per logical request
- command intake returns the existing `command_id` on replay
- worker execution maps the command deterministically to ledger `posting_key`
- shard DB still enforces unique `posting_key`

This lets retries happen safely at both queue and DB layers.

## Failure Model

Define failures explicitly.

### Retryable

- transient DB connectivity
- queue redelivery
- shard worker crash during processing
- lock timeout or temporary contention

### Non-Retryable

- invalid payload
- missing account
- currency mismatch
- insufficient funds on authorizing command
- invalid lifecycle transition

Command status model:

- `accepted`
- `processing`
- `succeeded`
- `failed_retryable`
- `failed_terminal`

## Observability

Add metrics before traffic grows:

- commands accepted / second
- commands succeeded / failed
- queue depth by shard
- command age / queue lag
- worker retry count
- DB lock wait time
- per-shard TPS
- hot-account contention

Useful logs:

- `command_id`
- `idempotency_key`
- `shard_id`
- `transaction_id`
- `posting_key`
- worker attempt count

## Rollout Plan

Status as of 2026-03-23:

- Milestone 1: complete
- Milestone 2: complete
- Milestone 3: complete
- Milestone 4: complete
- Milestone 5: complete in code and integration tests
- Milestone 6: complete with Kafka as the command delivery path

### Milestone 1: Shard Routing Contract

- define shard id format
- add shard router in code
- define shard-local system account naming
- define shard-local system-account pool selection from routed user shard
- define command envelope schema

### Milestone 2: DB-Backed Async Intake

- add `ledger_commands`
- implement command enqueue endpoint
- implement worker poller with `SKIP LOCKED`
- return `202 Accepted`

### Milestone 3: Shard-Aware Ledger Execution

- route workers to the correct shard DB
- run create/post/archive through existing ledger logic
- store command result and failure state

### Milestone 4: Query and Status APIs

- `GET /commands/{id}`
- shard-aware `GET /accounts/{id}/balances`
- shard-aware `GET /transactions/{id}`

### Milestone 5: Multi-Shard Deployment

- run multiple shard databases
- run one worker fleet consuming shard-local work
- validate hot-account distribution across shard-local system accounts

Implemented:

- multi-shard DB registry and query routing
- real multi-shard integration tests against central + shard test DBs
- standalone `cmd/worker` process for shard-local command execution
- worker fleet abstraction that polls each shard independently
- shard-local system-account pool distribution tests

### Milestone 6: Kafka Integration

- replace DB polling delivery with Kafka
- partition by `shard_id`
- keep DB idempotency and command status semantics unchanged

Implemented:

- Kafka publisher for accepted commands keyed by `shard_id`
- Kafka consumer path that claims and processes commands by `command_id`
- DB-backed command table remains the source of truth for idempotency, status, retries, and query APIs
- worker delivery now relies on Kafka when Kafka mode is enabled; DB polling remains only for non-Kafka deployments
- Kafka config and runtime wiring in API and worker processes

## Testing Plan

Add test layers in this order:

### Unit Tests

- shard routing determinism
- command validation
- retry classification
- command status transitions

### Integration Tests

- enqueue command -> worker executes -> status succeeds
- duplicate command idempotency
- retryable worker failure then success
- non-retryable command failure
- per-shard isolation

### Load / Soak Tests

- skewed traffic to one shard
- hot-user and hot-system-account contention
- queue lag under burst
- worker crash and restart

### Chaos / Recovery Tests

- worker dies after DB commit before ack
- duplicate queue delivery
- DB unavailable temporarily
- shard routing config change

## Non-Goals For Phase 2

Do not take these on yet:

- true cross-shard atomic transactions
- direct `user <-> user` transfers
- historical balance snapshots and replay engine
- external exactly-once guarantees across Kafka and DB
- global distributed locking

## Suggested Delivery Order

Build phase 2 in this order:

1. shard router and shard-local account model
2. DB-backed async command table
3. worker loop reusing current phase 1 ledger logic
4. command status endpoint
5. shard-aware read routing
6. multi-shard deployment tests
7. Kafka integration

This keeps the system operational at each step and avoids taking on Kafka and sharding at the same time.
