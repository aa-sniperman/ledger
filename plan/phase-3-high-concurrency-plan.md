# Phase 3 High-Concurrency Plan

## Goal

Increase sustained throughput and reduce p95/p99 latency under large concurrent request bursts, especially around:

- 500 concurrent transaction requests
- 1000 concurrent transaction requests
- 5000 concurrent transaction requests

Phase 3 is about execution concurrency, queue-drain speed, and reducing hot-path coordination overhead. It is not about changing the ledger invariants.

## Why The Current System Degrades At Large Concurrency

The current system is correct, but it leaves a lot of throughput on the table.

### 1. Worker execution is still too serialized

Today, one Kafka worker process runs one fetch-process-commit loop. That means one worker process executes only one command at a time even if it has CPU headroom.

Current consequence:

- adding shards does not automatically increase execution concurrency
- total execution throughput is still close to worker-process count
- p95/p99 rises quickly once accepted commands accumulate in Kafka

### 2. Same-shard Kafka traffic is intentionally ordered

Commands are keyed by `shard_id`, which is correct for shard-local ordering, but it also means one shard is effectively a single ordered stream.

Current consequence:

- commands for one shard are consumed sequentially
- hot shards build backlog even when other shards are idle
- high concurrent load becomes queueing delay instead of DB parallelism

### 3. API intake still uses central Postgres on the hot path

Even with Kafka enabled, the API still writes command state and idempotency into the central command store before returning acceptance.

Current consequence:

- central Postgres is still in the acceptance path
- duplicate-request pressure still hits the command table
- large bursts are constrained by central DB write rate and contention

### 4. Shard DB writes still hit hot system-account rows

Even with shard-local pools, the ledger write path still concentrates many writes on:

- shard-local payout holding accounts
- shard-local cash-in clearing accounts
- their currency-specific balance rows

Current consequence:

- row lock contention on `account_current_balances`
- version increments and bucket updates serialize on hot rows
- hot currencies inside a shard become latency multipliers

### 5. Command bookkeeping adds central DB round trips

Each command still updates lifecycle state:

- accepted
- processing
- succeeded / failed

Current consequence:

- extra central DB writes per command
- command state persistence is still useful, but expensive under very high TPS

## Capacity Interpretation For 500 / 1000 / 5000 Concurrent Requests

These are not exact benchmarks. They are planning targets based on the current architecture.

### 500 Concurrent Requests

Current system behavior:

- generally survivable
- API likely accepts the burst
- worker side will queue and drain rather than execute in parallel at the same rate
- latency degradation will mostly come from queueing and hot-shard skew

Primary risk:

- one or two hot shards dominate backlog while other shards stay underutilized

### 1000 Concurrent Requests

Current system behavior:

- central command-state writes become more visible
- Kafka backlog grows materially
- p95/p99 becomes dominated by wait time before execution, not by the ledger transaction itself

Primary risks:

- central Postgres command store becomes a coordination bottleneck
- single-stream-per-shard execution becomes too restrictive

### 5000 Concurrent Requests

Current system behavior:

- safe only if interpreted as queued burst, not immediate parallel execution
- command backlog and queue-drain time dominate user-visible latency
- hot-shard imbalance becomes severe

Primary risks:

- long tail latency from shard-local ordering
- hot system-account rows inside the busiest shards
- worker-process count no longer matches intended shard parallelism

## Phase 3 Target Shape

The target shape is:

- Kafka remains the delivery layer
- shard DBs remain the ledger source of truth
- command acceptance stays durable
- execution concurrency increases substantially
- hot rows are spread further
- central command-state storage is reduced or moved off the synchronous hot path

## Design Principles

1. Keep all financial writes shard-local.
2. Preserve ordering where it matters, but do not serialize more than necessary.
3. Make worker parallelism closer to actual shard count.
4. Reduce the amount of central coordination on the acceptance path.
5. Treat command state as an operational projection, not the main throughput limiter.

## Milestones

Status as of 2026-03-23:

- Milestone 1: complete in code and unit tests
- Milestone 2: complete in code and unit tests
- Milestone 3: complete in code and unit tests
- Milestone 4: complete in code and integration tests
- Milestone 5: complete in code and unit/integration tests
- Milestone 6: not started

### Milestone 1: Parallel Kafka Worker Execution

Change the Kafka worker model from one-command-at-a-time per process to partition-aware parallel execution.

Targets:

- one execution loop per assigned partition, not one per process
- allow one worker process to execute multiple shard streams concurrently
- preserve in-order processing within each partition

Expected outcome:

- worker throughput scales with assigned Kafka partitions
- adding worker CPU becomes useful again

Implemented:

- Kafka consumer now dispatches messages to one sequential worker loop per assigned partition
- one worker process can execute multiple partitions concurrently
- in-order processing is preserved within each partition
- unit tests cover both cross-partition parallelism and same-partition sequencing

### Milestone 2: Partition Strategy Beyond Raw `shard_id`

Keep shard-local correctness, but reduce over-serialization inside hot shards.

Current problem:

- today all Kafka commands for one shard are keyed the same way
- that forces the whole shard into one ordered stream
- unrelated transactions for different users in the same shard wait behind each other even when Postgres could execute them concurrently

Milestone 2 decision:

- stop ordering by raw `shard_id`
- order by a narrower conflict key: `shard_id + transaction_id`
- keep all commands for the same transaction lifecycle on one ordered stream:
  - create
  - post
  - archive
- allow unrelated transactions in the same shard to land on different Kafka partitions

Why this is safe:

- correctness does not require total shard order
- Postgres already enforces the real write safety:
  - row-level locking
  - conditional balance updates
  - `posting_key` uniqueness
  - transaction lifecycle guards
- same-transaction ordering is the command-layer dependency that must be preserved

Expected behavior after milestone 2:

- two transactions in the same shard but with different `transaction_id`s can execute concurrently
- commands for the same `transaction_id` still execute in order
- lock contention moves to the actual conflicting balance rows inside Postgres instead of being over-serialized in Kafka

Future refinement after milestone 2:

- if needed, evolve from transaction-level conflict keys to account-set-aware routing for even finer concurrency
- but transaction-level conflict keys are the first safe step because they preserve lifecycle ordering with minimal complexity

Expected outcome:

- hot shards stop being one giant serialized stream
- more of the shard DB’s write capacity becomes usable

Implemented:

- Kafka publish key is now scoped to `shard_id + transaction_id` instead of raw `shard_id`
- create, post, and archive commands for the same transaction lifecycle stay on one ordered Kafka stream
- unrelated transactions inside the same shard can now be distributed across different Kafka partitions
- unit tests cover key derivation and lifecycle-key consistency

### Milestone 3: Hot Account Distribution

Strengthen shard-local system-account spreading.

Targets:

- increase shard-local pool sizes for hot system roles
- make pool selection intentionally independent from shard selection
- add observability for per-account and per-pool contention

Expected outcome:

- less lock contention on a few balance rows
- smoother throughput under skewed currencies and withdrawal-heavy traffic

Implemented:

- system-account pool sizes are now configurable by role
- pooling support now applies to both `payout_holding` and `cash_in_clearing`
- API, worker, and dev setup all use the same configured pool-size map
- dev setup now creates all configured shard-local system accounts instead of hardcoding payout-only slot creation
- unit tests cover configured pool enumeration and spread across both payout and cash-in pools

### Milestone 4: Minimal Durable Command Log

Keep `ledger_commands` only for durable intake idempotency and later audit, not as a product-facing live status API.

Decision:

- `GET /commands/{id}` is not a required feature
- command lifecycle state does not need to be updated on every worker execution
- slower audit-style command lookups are acceptable later

Targets:

- keep API in-memory idempotency cache
- keep durable accepted-command insert for intake idempotency
- remove command-status API dependence
- stop updating command rows to `processing`, `succeeded`, and `failed_*` on the Kafka worker hot path
- let balances and transactions be the product-facing observable state instead of command rows

Resulting model:

- API writes one accepted command record
- API publishes to Kafka
- worker consumes Kafka and executes against shard Postgres
- command row remains an accepted intake/audit record, not a frequently mutated status record

Expected outcome:

- central Postgres write amplification drops
- acceptance path remains durable
- command storage stops acting like a live status subsystem

Implemented:

- removed the public `GET /commands/{id}` API and OpenAPI surface
- Kafka worker execution no longer updates command rows through `processing`, `succeeded`, or `failed_*` on the hot path
- `ledger_commands` remains the durable accepted-command log for intake idempotency and later audit
- duplicate Kafka deliveries are tolerated by relying on ledger-level idempotency and idempotent transaction transitions

### Milestone 5: Kafka-First Command Model

Make Kafka the true delivery backbone and reduce the need for central command-table coordination for throughput.

Targets:

- treat Kafka as the primary execution queue
- keep a queryable command-status projection for `GET /commands/{id}`
- preserve ledger-level deduplication with `posting_key`

Expected outcome:

- queueing scales with Kafka, not with one central table
- command status remains observable without dominating request latency

Implemented:

- Kafka messages now carry the full accepted command envelope, not only `command_id`
- the Kafka worker executes directly from the Kafka message payload instead of reloading command payload from Postgres
- `ledger_commands` remains only the durable accepted-command log for API-side idempotency and later audit
- worker execution no longer depends on command-row reads for the Kafka path

### Milestone 6: Measured Load Validation

Replace architectural estimates with real data.

Required test bands:

- 500 concurrent transaction requests
- 1000 concurrent transaction requests
- 5000 concurrent transaction requests

Measure:

- API accept latency p50 / p95 / p99
- queue wait time before worker execution
- shard-level TPS
- command success / retry / fail rates
- shard DB CPU, lock waits, and row contention
- hottest system-account rows and pool-slot distribution

Expected outcome:

- capacity numbers based on measurement, not only design reasoning
- concrete next bottleneck identified after each scale step

## Recommended Sequence

Do phase 3 in this order:

1. parallel Kafka worker execution
2. hot account distribution improvements
3. minimal durable command log
4. load testing at 500 / 1000 / 5000 concurrent requests
5. Kafka-first command model cleanup

This order is important because worker execution parallelism and hot-row spreading are likely to unlock more throughput faster than replacing the command-state store immediately.

## Success Criteria

Phase 3 is successful when:

- throughput scales materially with shard count and Kafka partition count
- one worker process can execute multiple partition streams concurrently
- 500 concurrent requests no longer create large queue-driven latency spikes
- 1000 concurrent requests remain stable without one central DB becoming the first bottleneck
- 5000 concurrent requests are absorbed as a bounded queueing problem rather than a system instability event
- hot-system-account contention is visible and controlled through shard-local pool strategy

## Non-Goals

Phase 3 does not include:

- cross-shard atomic ledger transactions
- user-to-user transfers
- historical balance reconstruction
- replacing shard-local Postgres for core ledger correctness
