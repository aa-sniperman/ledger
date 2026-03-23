# Phase 2 Performance Estimates

## Scope

These numbers are estimates for the system as currently built, not benchmark results.

They assume:

- Kafka mode enabled
- command acceptance path is:
  - API in-memory idempotency check
  - central Postgres command insert
  - Kafka publish
- worker execution path is:
  - Kafka consume
  - route to shard DB
  - run one ledger transaction
  - update command status in central Postgres
- shard DBs are on SSD and in the same region/network as API and workers
- traffic is reasonably balanced across shards
- no extreme hot-account skew

## Important Constraint

With the code as it exists today, the effective ledger execution concurrency is limited mainly by worker processes.

Why:

- Kafka messages are keyed by `shard_id`, so commands for the same shard stay ordered.
- The current Kafka consumer processes one command at a time in a single loop.
- That means one worker process effectively executes one command at a time.

So the practical concurrency ceiling is roughly:

`safe concurrent execution ~= min(worker processes, active shard streams, topic partitions)`

This means sharding Postgres helps, but only if workers and Kafka partitions scale with it.

## Definitions

`API accept latency`
- time from HTTP request arrival to `202 Accepted`
- dominated by central Postgres command insert + Kafka publish

`End-to-end latency`
- time from HTTP request arrival to command completion in ledger
- dominated by queue wait + worker execution + shard Postgres transaction

`Safe sustained TPS`
- throughput that should remain stable without queue growth under steady load

`Safe concurrent execution`
- number of ledger transactions that can be actively executing at once
- this is not the same as the number of accepted commands waiting in Kafka

## Current Estimated Throughput

| Shards | Workers | Worker Size | Shard DB Size Each | Central DB Size | Safe Concurrent Execution | Safe Sustained TPS | API Accept p95 | API Accept p99 | End-to-End p95 | End-to-End p99 |
|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|
| 2 | 1 | 1 vCPU / 512MB | 2 vCPU / 4GB | 2 vCPU / 4GB | 1 | 25-35 | 10-20ms | 20-40ms | 80-180ms | 150-350ms |
| 2 | 2 | 1 vCPU / 512MB | 2 vCPU / 4GB | 2 vCPU / 4GB | 2 | 50-70 | 10-20ms | 20-40ms | 60-140ms | 120-280ms |
| 4 | 2 | 1 vCPU / 512MB | 2 vCPU / 4GB | 2 vCPU / 4GB | 2 | 50-70 | 12-22ms | 25-45ms | 90-220ms | 180-450ms |
| 4 | 4 | 1 vCPU / 512MB | 2 vCPU / 4GB | 2 vCPU / 4GB | 4 | 100-140 | 12-25ms | 25-50ms | 70-160ms | 140-320ms |
| 8 | 4 | 1 vCPU / 1GB | 2 vCPU / 4GB | 4 vCPU / 8GB | 4 | 100-150 | 15-30ms | 30-60ms | 100-250ms | 200-500ms |
| 8 | 8 | 1 vCPU / 1GB | 2-4 vCPU / 4-8GB | 4 vCPU / 8GB | 8 | 200-280 | 15-35ms | 35-70ms | 80-180ms | 160-360ms |

## Large Concurrency Scenarios

The numbers below answer a stricter question:

"How much infrastructure would the current system need to safely execute 500, 1000, or 5000 ledger transactions concurrently?"

Because one worker process currently executes one command at a time, these scenarios scale almost linearly with worker count.

### Estimated Requirements

| Target Concurrent Executions | Minimum Practical Shards | Minimum Practical Kafka Partitions | Minimum Practical Worker Processes | Suggested Worker Size | Suggested Shard DB Size Each | Feasibility With Current Code |
|---:|---:|---:|---:|---|---|---|
| 500 | 500 | 500 | 500 | 1 vCPU / 512MB | 2-4 vCPU / 4-8GB | Technically possible, operationally poor |
| 1000 | 1000 | 1000 | 1000 | 1 vCPU / 512MB | 2-4 vCPU / 4-8GB | Technically possible, operationally very poor |
| 5000 | 5000 | 5000 | 5000 | 1 vCPU / 512MB | 2-4 vCPU / 4-8GB | Not realistic with current code shape |

### Estimated Latency If Fully Provisioned

These numbers assume the required worker/shard/partition counts above actually exist and load is balanced perfectly.

| Target Concurrent Executions | API Accept p95 | API Accept p99 | End-to-End p95 | End-to-End p99 | Notes |
|---:|---:|---:|---:|---:|---|
| 500 | 20-45ms | 40-90ms | 120-280ms | 250-600ms | Requires very large shard and worker fanout |
| 1000 | 25-60ms | 50-120ms | 150-350ms | 300-800ms | Central command DB and Kafka metadata overhead become serious |
| 5000 | 40-120ms | 80-250ms | 300-900ms | 800ms-2s+ | Current architecture is no longer economically sensible |

## More Realistic Interpretation

If `500`, `1000`, or `5000` means:

- accepted commands waiting in Kafka, rather than
- commands actively executing in shard Postgres right now

then the system can accept far more than its execution concurrency.

In that case:

- API may still return `202` quickly
- Kafka queue depth absorbs the burst
- end-to-end latency increases based on backlog

### Rough Burst Behavior

Assume a healthy `8 shard / 8 worker` deployment sustaining about `200-280 TPS`.

| Burst Size Arriving Nearly At Once | API Accept p95 | API Accept p99 | Estimated Completion p95 | Estimated Completion p99 |
|---:|---:|---:|---:|---:|
| 500 | 20-40ms | 40-80ms | 2-4s | 4-7s |
| 1000 | 20-45ms | 45-90ms | 4-8s | 8-15s |
| 5000 | 25-60ms | 50-120ms | 20-40s | 40-90s |

These burst numbers assume:

- no shard skew
- workers stay healthy
- shard DBs do not hit heavy lock contention

## Main Bottlenecks Right Now

In order:

1. Worker execution model
   - one Kafka consumer loop executes one command at a time
2. Number of worker processes
   - effective concurrency scales almost linearly with workers
3. Central Postgres command path
   - still in API hot path for insert + status store
4. Shard Postgres write latency
   - row locks, balance updates, entries, indexes
5. Kafka partitions
   - need enough partitions to expose parallelism

## Practical Conclusions

With the system as built today:

- 2 to 8 concurrent ledger executions is the realistic current range
- around 100-280 sustained TPS is plausible in the better current configurations
- 500 concurrent active ledger executions is not practical without hundreds of workers and shards
- 1000 and 5000 concurrent active ledger executions are not realistic with the current worker execution model

## What Would Improve This Most

The highest-leverage next optimization is not more RAM.

It is changing worker execution from:

- one command at a time per worker process

to something like:

- one execution loop per owned Kafka partition, or
- one execution loop per shard stream, or
- bounded parallel execution across independent shard assignments

That would let one worker process use more than one CPU effectively and reduce the near-linear worker explosion described above.

## Recommended Next Step

Before trusting any of these numbers for planning, run measured load tests for:

1. API accept latency under Kafka + central DB
2. end-to-end latency under balanced shard traffic
3. hot-shard / hot-account skew
4. worker crash and consumer group rebalance

This document should be treated as architecture guidance, not capacity proof.
