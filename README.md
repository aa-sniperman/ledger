# ledger

An asynchronous, shard-aware double-entry ledger for `user <-> system` money movement.

This repo is intentionally narrow:

- supports deposits and withdrawals
- does not support direct `user <-> user` transfer
- keeps all financial writes shard-local
- uses Kafka for async command delivery
- uses PostgreSQL for durable command acceptance and shard-local ledger state

The public API is payment-shaped. Clients send `user_id`, `amount`, `currency`, and idempotency data. The server derives shard ownership and the correct shard-local system accounts.

See the visual architecture in [architecture.excalidraw.json](/Users/sniperman/ledger/ledgering/architecture.excalidraw.json).

## What Is Implemented

Current public API:

- `POST /commands/payments.withdrawals.create`
- `POST /commands/payments.withdrawals.post`
- `POST /commands/payments.withdrawals.archive`
- `POST /commands/payments.deposits.record`
- `GET /users/{id}/balances/{currency}`
- `GET /transactions/{id}`
- `GET /healthz`
- `GET /readyz`
- `GET /docs`
- `GET /openapi.json`

Development-only:

- `POST /debug/devsetup/seed`

The current design supports:

- multi-currency user wallets: `user_wallet:{user_id}:{currency}`
- shard-local multi-currency system accounts
- API-side in-memory idempotency fast path
- durable accepted-command log in central Postgres
- Kafka-first worker execution
- shard routing by rendezvous hashing on `user_id`
- shard-local system-account pools to reduce hot-row contention
- partition-aware worker concurrency
- balance locking, version bumping, and `posting_key` uniqueness in shard Postgres

## System Design

### 1. Intake

The API accepts payment intent, not raw ledger entries.

Example client intent:

- `user_id`
- `amount`
- `currency`
- `idempotency_key`
- `transaction_id`
- `posting_key`

The API then:

1. validates the request
2. checks the in-memory idempotency cache
3. derives `shard_id` from `user_id`
4. derives the shard-local user account and system account
5. writes an accepted command row to the central Postgres DB
6. publishes the full command envelope to Kafka
7. returns `202 Accepted`

### 2. Routing And Sharding

User routing:

- deterministic rendezvous hashing on `user_id`
- `user_id -> shard_id`

Account model:

- user account: `user_wallet:{user_id}:{currency}`
- system account: `{role}:{shard_id}:{currency}`
- pooled system account: `{role}:{shard_id}:{currency}:{slot}`

Important rule:

- transactions stay single-shard
- the API routes the user first, then chooses system accounts from that same shard only

### 3. Async Command Execution

Kafka is the delivery path.

The API publishes a full accepted command envelope containing:

- `command_id`
- `idempotency_key`
- `shard_id`
- command type
- payload

Workers consume from Kafka and execute directly from the Kafka message. They do not need to reload the command payload from Postgres before executing.

### 4. Worker Concurrency Model

The worker is not one-command-at-a-time anymore.

Current behavior:

- Kafka partition key is `shard_id + transaction_id`
- same transaction lifecycle stays ordered on one partition
- unrelated transactions in the same shard can land on different partitions
- one worker process runs one sequential goroutine loop per assigned Kafka partition
- different partitions execute concurrently inside the same worker process

That means the concurrency unit is:

- ordered within one partition
- parallel across partitions

### 5. Ledger Safety Model

Financial correctness is enforced in shard Postgres.

Key controls:

- conditional balance updates
- balance locking for authorizing flows
- `current_version = current_version + 1`
- DB-assigned account versions via `RETURNING`
- unique `posting_key` on ledger transactions
- idempotent transition behavior for duplicate delivery

The real source of truth for money movement is still the shard-local ledger tables:

- `accounts`
- `account_current_balances`
- `ledger_transactions`
- `ledger_entries`

### 6. Command Durability Model

`ledger_commands` is now minimal on purpose.

It is used for:

- durable intake acceptance
- durable idempotency
- later audit

It is not treated as a live product-facing command status system anymore.

## Public API Examples

Create a withdrawal hold:

```bash
curl -X POST http://localhost:8080/commands/payments.withdrawals.create \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user_123",
    "idempotency_key": "user_123:withdraw:usd:1",
    "transaction_id": "tx_withdraw_1",
    "posting_key": "pk_withdraw_1",
    "amount": 7000,
    "currency": "USD",
    "effective_at": "2026-03-23T10:00:00Z",
    "created_at": "2026-03-23T10:00:00Z"
  }'
```

Record a deposit:

```bash
curl -X POST http://localhost:8080/commands/payments.deposits.record \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user_123",
    "idempotency_key": "user_123:deposit:usd:1",
    "transaction_id": "tx_deposit_1",
    "posting_key": "pk_deposit_1",
    "amount": 10000,
    "currency": "USD",
    "effective_at": "2026-03-23T10:00:00Z",
    "created_at": "2026-03-23T10:00:00Z"
  }'
```

Read a user balance:

```bash
curl http://localhost:8080/users/user_123/balances/USD
```

Read a transaction:

```bash
curl http://localhost:8080/transactions/tx_withdraw_1
```

Open Swagger UI:

```bash
open http://localhost:8080/docs
```

## Local Setup

### Prerequisites

- Go `1.25.1`
- PostgreSQL
- Kafka

You need:

- 1 central Postgres DB
- 1 Postgres DB per shard
- 1 Kafka topic for command delivery

### 1. Configure Environment

Start from:

```bash
cp .env.example .env
```

Important variables:

- `DATABASE_URL`
- `SHARD_IDS`
- `SHARD_DATABASE_URLS`
- `KAFKA_ENABLED`
- `KAFKA_BROKERS`
- `KAFKA_COMMANDS_TOPIC`
- `KAFKA_CONSUMER_GROUP`
- `SYSTEM_ACCOUNT_POOL_SIZES`

Example shard config:

```bash
SHARD_IDS=shard-a,shard-b
SHARD_DATABASE_URLS=shard-a=postgres://postgres:postgres@localhost:5432/ledger_shard_a?sslmode=disable,shard-b=postgres://postgres:postgres@localhost:5432/ledger_shard_b?sslmode=disable
SYSTEM_ACCOUNT_POOL_SIZES=payout_holding=4,cash_in_clearing=4
```

Example Kafka config:

```bash
KAFKA_ENABLED=true
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_COMMANDS_TOPIC=ledger.commands
KAFKA_CONSUMER_GROUP=ledger-worker
KAFKA_AUTO_CREATE_TOPIC=false
KAFKA_TOPIC_PARTITIONS=2
KAFKA_REPLICATION_FACTOR=1
```

### 2. Run Migrations

```bash
make migrate
```

This migrates:

- the central DB from `DATABASE_URL`
- every shard DB in `SHARD_DATABASE_URLS`

### 3. Start The API

```bash
make api
```

### 4. Start The Worker

```bash
make worker
```

If you want a worker only for a shard subset in non-Kafka DB-polling mode:

```bash
WORKER_SHARD_IDS=shard-a make worker
```

In Kafka mode, workers are effectively global consumers and can process commands for all shards.

### 5. Seed Debug Data

Create accounts and mint balances:

```bash
make seed SEED_USERS=user_123,user_456 SEED_AMOUNT=250000 SEED_CURRENCY=USD
```

Create accounts only:

```bash
make seed-accounts SEED_USERS=user_123,user_456 SEED_CURRENCY=USD
```

Clean up old legacy single-currency seed data:

```bash
make clean-legacy-seed
```

### 6. Use The Debug Seed API

When `DEBUG_API_ENABLED=true`:

```bash
curl -X POST http://localhost:8080/debug/devsetup/seed \
  -H 'Content-Type: application/json' \
  -d '{
    "users": ["user_123", "user_456"],
    "currency": "USD",
    "amount": 250000,
    "skip_mint": false
  }'
```

## Tests

Run everything:

```bash
make test
```

Package-level shortcuts:

```bash
make test-http
make test-service
make test-worker
```

The repo includes:

- unit tests
- service integration tests
- HTTP integration tests
- multi-shard integration tests
- worker integration tests

## Realistic Capacity Targets

These are architecture estimates for the current code shape under ideal conditions:

- Kafka local / same-AZ
- shard DBs local / same-AZ
- balanced user distribution
- shard-local transactions only
- system-account pools enabled
- no severe hot-account skew

### Recommended Baseline

For a realistic production-style target of:

- `1000+` requests accepted in a burst
- `100+` true ledger writes actively executing

Start with:

- `16 shards`
- `128 Kafka partitions`
- `8 worker processes`
- workers: `2 vCPU / 2GB` each
- shard DBs: `4 vCPU / 8GB` each
- central DB: `4 vCPU / 8GB`
- API: `3 x 2 vCPU / 2GB`

Estimated behavior:

- burst intake: `1000-3000` requests accepted quickly
- true active ledger executions: about `100-150`
- sustained throughput: about `1000-2000 TPS`
- API accept latency:
  - `p95 ~ 8-20ms`
  - `p99 ~ 15-35ms`
- end-to-end completion with no major backlog:
  - `p95 ~ 40-120ms`
  - `p99 ~ 80-200ms`

Important distinction:

- `1000+ accepted requests` is realistic and desirable
- `1000+ simultaneous ledger DB writes` is usually not the right target

Kafka absorbs burst pressure. Workers and shard DBs drain it at the rate the system can safely sustain.

## Design Tradeoffs

This repo currently chooses:

- payment-shaped public API over generic ledger entry submission
- shard-local correctness over cross-shard atomicity
- Kafka-first execution over synchronous API-side posting
- durable intake idempotency in Postgres plus best-effort in-memory API cache
- narrower Kafka ordering key `shard_id + transaction_id` rather than full-shard serialization

What it does not do:

- cross-user transfer
- cross-shard atomic transaction
- historical command-status product API
- full account-set-aware partitioning
- adaptive shard-capacity-aware consumer throttling

## Repo Structure

- [cmd/api](/Users/sniperman/ledger/cmd/api) API server
- [cmd/worker](/Users/sniperman/ledger/cmd/worker) Kafka worker
- [cmd/migrate](/Users/sniperman/ledger/cmd/migrate) DB migrations
- [cmd/devsetup](/Users/sniperman/ledger/cmd/devsetup) seed tool
- [cmd/devcleanup](/Users/sniperman/ledger/cmd/devcleanup) cleanup tool
- [internal/httpapi](/Users/sniperman/ledger/internal/httpapi) HTTP handlers and OpenAPI
- [internal/kafkabus](/Users/sniperman/ledger/internal/kafkabus) Kafka publisher and consumer
- [internal/service](/Users/sniperman/ledger/internal/service) business flow orchestration
- [internal/sharding](/Users/sniperman/ledger/internal/sharding) routing and shard DB registry
- [internal/store](/Users/sniperman/ledger/internal/store) Postgres repositories
- [ledgering](/Users/sniperman/ledger/ledgering) diagrams and supporting notes
- [plan](/Users/sniperman/ledger/plan) design and scaling plans
