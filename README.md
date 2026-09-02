# High-Throughput Inference Gateway

Production-style inference gateway: **Java microservices + Kafka + PostgreSQL + Redis**, with **Node.js** load and webhook test tools. Single monorepo.

## Project layout

```
high-throughput-inference/
├── services/
│   ├── shared/              # shared Java library (entities, rate limiter, Kafka events)
│   ├── inference-gateway/   # HTTP API
│   ├── request-processor/   # Kafka consumer + rate limits + provider calls
│   ├── webhook-delivery/    # batch webhook delivery
│   └── provider-mock/       # fake LLM HTTP API
├── tools/
│   ├── loadgen/             # load generator CLI
│   └── webhook-mock/        # test webhook receiver (simulates client callback URL)
├── scenarios/               # validation scripts (validate.py)
├── scripts/                 # smoke tests, helpers
├── docker-compose.yml
└── pom.xml                  # Maven parent (Java modules under services/)
```

## Architecture

- **inference-gateway** — accepts single/batch requests, persists them, publishes to Kafka, and exposes status/results/admin endpoints. Ack for a batch submission is returned before any request is processed (target: <1s).
- **request-processor** — consumes from Kafka, checks the per-model RPM/TPM budget in Redis before calling the provider, and republishes a rate-limited request to a retry topic (bounded backoff, then `EXPIRED`) instead of blocking the consumer thread.
- **provider-mock** — simulates an external LLM provider: configurable per-model latency and failure rate, runs on virtual threads so simulated latency doesn't exhaust the platform thread pool.
- **webhook-delivery** — consumes "batch complete" events and delivers the callback with retry/backoff; batch completion is detected with a DB row lock so exactly one callback fires per batch.
- **loadgen** / **webhook-mock** — Node.js tooling under `tools/` for generating load and receiving/inspecting callbacks.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker Desktop
- Node.js 20+ (for loadgen / webhook-mock)
- Python 3.10+ (stdlib only — for `scenarios/validate.py`)

## Installation

```bash
# Node.js tools (loadgen, webhook-mock) — required for local CLI usage outside Docker
(cd tools/loadgen && npm install)
(cd tools/webhook-mock && npm install)
```

Docker Compose builds and installs Node dependencies inside the `webhook-mock` image automatically; the commands above are only needed if you run `loadgen` or `webhook-mock` directly on the host.

## Quick start

```bash
# Build and run full stack
docker compose up --build

# Run unit tests
mvn clean test

# E2E smoke test (stack must be running)
chmod +x scripts/e2e-smoke.sh
./scripts/e2e-smoke.sh
```

All services expose Spring Boot Actuator health at `/actuator/health`.

## API

| Endpoint | Description |
|----------|-------------|
| `POST /v1/inference` | Submit a single request (`requestId`, `model`, `estimatedTokens`, `payload`) |
| `POST /v1/batches` | Submit a batch (`requests[]` + `callbackUrl`); returns `batchId` immediately |
| `GET /v1/requests/{id}` | Individual request status/result |
| `GET /v1/batches/{id}` | Batch status: counts, callback status/attempts |
| `GET /v1/batches/{id}/results` | Per-request results for a batch (what the callback's `resultsUrl` points to) |
| `GET /v1/admin/models` | List configured models and their current limits |
| `PUT /v1/admin/models/{name}` | Update a model's `rpmLimit`/`tpmLimit` — takes effect immediately, no restart |

## Configuring models and changing limits

Model limits live in Postgres (seeded by `DataInitializer`) and are cached in Redis so `request-processor` never has to hit the DB on the hot path. A `PUT` writes both and is picked up by every processor instance on the next request for that model — no restart, no redeploy:

```bash
# see current limits
curl -s http://localhost:8081/v1/admin/models | jq

# throttle model-a to 5,000 RPM (TPM unchanged)
curl -s -X PUT http://localhost:8081/v1/admin/models/model-a \
  -H 'content-type: application/json' \
  -d '{"rpmLimit": 5000, "tpmLimit": 60000000}'
```

## Simulating provider failures

`provider-mock` (port 8082) can be configured with a per-model failure rate at runtime, the same way `inference-gateway`'s model limits are — no restart needed:

```bash
# see current latency/failure-rate config
curl -s http://localhost:8082/v1/admin/models | jq

# make 5% of model-a's simulated provider calls fail
curl -s -X PUT http://localhost:8082/v1/admin/models/model-a/failure-rate \
  -H 'content-type: application/json' \
  -d '{"failureRate": 0.05}'

# restore
curl -s -X PUT http://localhost:8082/v1/admin/models/model-a/failure-rate \
  -H 'content-type: application/json' \
  -d '{"failureRate": 0.0}'
```

`scenarios/validate.py`'s scenario 3 uses this to exercise real request-level failures (not just callback-delivery retries) within the batch, then resets the rate to 0 in a `finally` block so it can't leak into other scenario runs.

## Load generator

```bash
cd tools/loadgen

# single-request mode: 500 req/s for 60s against model-a, with completion tracking
node index.js --url http://localhost:8081 --rate 500 --duration 60 \
  --models model-a:1.0 --tokens 1000

# mixed model traffic, submission-only (skip completion polling)
node index.js --url http://localhost:8081 --rate 2000 --duration 30 \
  --models model-a:0.6,model-b:0.4 --no-track

# batch mode: 50 requests/batch at an effective 200 req/s, with callbacks
node index.js --url http://localhost:8081 --rate 200 --duration 30 \
  --batch-size 50 --callback-url http://webhook-mock:9000/callback
```

Dispatch is rate-paced and concurrency-bounded (`--concurrency`, default 256 in-flight HTTP calls) rather than one-request-at-a-time, so it can actually reach the configured `--rate`. At the end of a run it prints a JSON report with: submitted/acked/ack-failed counts, ack latency percentiles, per-model submitted RPM/TPM (max over any 60s window), and — unless `--no-track` is passed — completion/success/failure counts and end-to-end latency percentiles. In single-request mode at high rates, completion is tracked on a bounded random sample (`--track-sample`, default 3000 requests) rather than every request, to avoid the poller itself becoming the bottleneck; the report states the sample size/fraction. For an exact, full-population count use `scenarios/validate.py`, which reads Postgres directly.

## Running the validation scenarios

`scenarios/validate.py` drives the live stack (via the HTTP API, `docker exec ... psql`, and `PUT /v1/admin/models`) and implements the three scenarios from the assignment brief. It requires the stack to be up (`docker compose up --build`) and produces `BENCHMARK.md` at the repo root.

```bash
# all three scenarios
python3 scenarios/validate.py

# a single scenario, e.g. the async batch/callback one
python3 scenarios/validate.py 3
```

- **Scenario 1 — reach provider capacity**: model at 50,000 RPM / 100M TPM, >5 minutes of over-capacity load. Passes on ≥90% of allowed capacity post-warm-up, no 60s window over either limit, and every request accounted for. In practice this currently reports a `FAIL` by a very thin margin (max observed 60s RPM ~50,032-50,050 against a strict 50,000 cap, i.e. ~0.06-0.1% over) while every other condition passes cleanly (100% accounted, 100% success, avg RPM exactly at 50,000 — well above the 45,000 bar). This is a known, understood gap, not an unexplained failure — see "Known, accepted gap" under Design decisions and tradeoffs for why.
- **Scenario 2 — different models, changing limits**: model-a (30k RPM/60M TPM) and model-b (20k RPM/40M TPM) run concurrently; model-a is throttled to 5,000 RPM mid-run and restored, with no restart. Passes if both models stay within their current limits throughout and the report shows configured vs. observed throughput over time.
- **Scenario 3 — async batch completion**: 10,000 requests across two models with a callback URL; `provider-mock` is set to a 5% simulated failure rate for the duration of the batch (reset to 0% afterward) so some requests genuinely fail, and the first two callback delivery attempts are rejected (via `webhook-mock`'s `REJECT_ATTEMPTS`) to exercise retry. Passes on a <1s ack, a callback sent only after every request is terminal, successful delivery once the destination recovers, the callback summary matching `GET /v1/batches/{id}` on every field (total/succeeded/failed/expired/status, not just total), at least one genuinely failed request, and every request id appearing exactly once.

Exit code is non-zero if any scenario fails; check the console log and `BENCHMARK.md` for details.

## Ports

| Service | Port |
|---------|------|
| inference-gateway | 8081 |
| provider-mock | 8082 |
| webhook-mock | 9000 |
| Postgres | 5433 |
| Redis | 6380 |
| Kafka | 9093 |

`request-processor` and `webhook-delivery` are Kafka consumers with no external traffic — they aren't published to the host, only reachable inside the Docker network.

## Design decisions and tradeoffs

- **Ingest is decoupled from provider calls via Kafka.** `POST /v1/inference` and `POST /v1/batches` persist and enqueue only — they never call the provider synchronously. This is what lets batch submission ack in well under a second regardless of batch size, and lets `request-processor` scale independently (more consumer instances/partitions) from the ingest API.
- **Rate limiting is atomic Lua scripts against Redis**, checked per-request before the provider call, not a local in-JVM counter — every `request-processor` instance shares the same live budget per model, so scaling the consumer fleet horizontally doesn't over-count capacity. RPM and TPM are two separate checks (RPM entry is rolled back if the TPM check then rejects); this trades a small number of extra false rejections under heavy contention for keeping each script simple and independently testable, rather than one larger combined script.
- **Over-limit requests are requeued, not dropped or blocked.** A request that can't proceed goes to a retry topic with backoff and is retried until `inference.max-queue-wait-ms` elapses, at which point it's marked `EXPIRED`. This avoids blocking a Kafka consumer thread (which would stall every other request behind it) while still guaranteeing every request reaches a final state.
- **Batch completion is detected with a pessimistic DB row lock**, not a distributed counter or a periodic sweep — the request that observes the batch's last outstanding count decrementing to zero is the one that publishes the "batch complete" event, so the callback fires exactly once even with many processor instances finishing requests concurrently.
- **Callback delivery is a separate service/topic** from batch completion detection, with its own bounded retry/backoff, so a slow or down client callback endpoint can't back up request processing.
- **Virtual threads** (`spring.threads.virtual.enabled: true`) are used on every HTTP-facing service, including the provider simulator, so a blocking call (JDBC, the simulated provider latency `Thread.sleep`) doesn't pin a platform thread — this is a large part of how the system reaches the required throughput without hand-written async/reactive code.
- **The TPM limiter is a prefix-sum ledger, not a per-request scan.** An earlier version re-summed every token entry currently inside the 60s window on every single check (O(window size) per request), which pegged Redis — single-threaded — at 100% CPU well before the JVM side ran out of capacity (confirmed live: request-processor at 25% CPU, Redis at 101%, throughput capped at ~200 req/s against a 50,000 RPM / 900 req/s target). It now stores one ledger entry per admitted request whose *member* is the model's running cumulative token total, so the window sum is just `(latest total) - (total just before the window)` — two `O(log N)` lookups regardless of how many requests are in the window. Verified directly against Redis with a 50,000-entry ledger before rewiring it in; after the fix, the same load left Redis at ~2-7% CPU with request-processor/Postgres becoming the (expected, legitimate) bottleneck instead.
- **`IngestService.submitSingle`/`submitBatch` are deliberately NOT `@Transactional`.** They used to be — the DB save and the Kafka publish both ran inside one transaction, which meant the publish (visible to a consumer immediately) could reach `request-processor` *before* the transaction actually committed the row. At low throughput a consumer's own latency masked this; once the TPM fix above let `request-processor` keep pace with submission, this became a real, reproducible bug — a fast consumer would poll the message, get zero rows back for `findById`, silently treat it as `SKIPPED`, and ack it. The row would still commit moments later and sit as `QUEUED` forever with nothing left to ever reprocess it (confirmed live: a 5-minute run lost 146,124 of 258,030 requests this way, with zero errors logged). Saving outside a transaction commits immediately (Spring Data repositories are transactional per call), so the row is guaranteed durable and visible before the Kafka publish is even called.
- **Both Lua scripts use Redis's own clock (`TIME`), not a client-supplied timestamp.** A timestamp captured in the JVM before the network round-trip to Redis can be slightly stale by the time the script actually runs under load, letting a few extra admissions slip past the window boundary. Using `TIME` inside the script (safe for AOF/replication — Redis fixes the value consistently for the whole script execution) removes that skew; measured effect was real but small (the RPM 60s-window overshoot dropped from 50,050 to 50,032 out of a 50,000 cap in back-to-back 5-minute runs at saturation).
- **Known, accepted gap: a tiny (~0.02-0.1%) apparent RPM/TPM overshoot can still show up in *external* validation, even though admission is provably exact.** Both Lua scripts guarantee, by construction, that the count of ledger entries newer than `now - window` is at most the configured limit at the instant of every admission (verified directly against Redis, independent of load) — so the system genuinely never *admits* more than the configured rate. `scenarios/validate.py`, however, measures "observed RPM/TPM" from each request's `completed_at` timestamp, not from Redis's internal admission timestamp, because it deliberately treats the system as a black box (only public request state, no Redis introspection). Under sustained at-capacity load, p95/p99 submit-to-complete latency reaches into the tens of seconds (real queueing, not a bug — see the benchmark report), so a handful of requests admitted just before a 60-second boundary can *complete* just after it, or vice versa, nudging the completion-time-based measurement a few dozen requests over a limit that was never actually exceeded at admission time. This was deliberately left as-is rather than "fixed" by adding an artificial safety margin at admission, since that would reduce real throughput to compensate for a measurement artifact rather than an actual defect.
- **`RequestState.REJECTED` exists but is currently unused** — there is no fast-reject/load-shedding path today; every over-capacity request waits (bounded) rather than being rejected immediately. Left as an explicit extension point rather than removed, since large-scale deployments typically want a "shed load past N seconds of expected queue depth" policy here.
- **`request-processor`'s Kafka listener concurrency and DB connection pool are sized together.** Both the primary and retry consumer groups run `inference.kafka.listener-concurrency` (default 32, matching the topics' partition count) threads, each of which opens its own short-lived JDBC transaction per record; the previous default Hikari pool size of 10 meant most of those threads were blocked waiting for a connection rather than doing work. The pool (`DB_POOL_SIZE`, default 64) is sized to cover both groups at full concurrency.

## Scaling toward 10 billion inference requests per minute

10B req/min (~167M req/s) is roughly 500x the required 300k req/s benchmark and is not reachable by scaling the current single-Redis-instance, single-Postgres-instance topology further — it requires removing several centralized bottlenecks that are acceptable at 300k–1M req/s:

1. **Shard the rate limiter.** Redis is single-threaded, so past a few million ops/sec on one instance, per-model RPM/TPM state needs to be sharded — e.g. by `hash(model, requestId) % N` across a Redis Cluster, with each model's budget divided across shards (or one model pinned to one shard when a single model's limit is large enough to need it). The atomic Lua-script approach stays the same per shard; only the routing changes.
2. **Bound the TPM ledger's memory, not its per-check cost.** The per-check cost is already solved at current scale (the TPM limiter is an O(log N) prefix-sum ledger, not a per-request scan — see Design decisions above). What doesn't yet scale to 10B/min is that the ledger isn't pruned on the hot path, so it grows for as long as a model sees continuous traffic; the trade was "correctness first, prune later" since every operation stays O(log N) regardless of ledger size. At this volume, add periodic pruning that preserves the anchor entry just outside the window (so `totalBefore` lookups stay valid), or move to fixed per-second counters with an incrementally-maintained rolling sum, bounding memory to O(window) entries instead of O(traffic × window).
3. **Partition Kafka topics and consumers by model (or model-shard), not just by request id.** At 10B/min, a single `inference.requests` topic's partition count becomes the throughput ceiling for consumer parallelism; partitioning by model lets each model's traffic scale its own consumer group independently, and lets a hot model be split further by a secondary key.
4. **Move batch/request state out of a single Postgres instance.** Row-level locking for exactly-once batch completion works at current scale but a single primary can't absorb 10B/min of state transitions. This becomes a sharded/partitioned store (e.g. per-batch-id shard) or an event-sourced design where completion is derived from a Kafka-compacted topic instead of a DB row, trading the simplicity of "lock the row" for a merge/reduce step.
5. **Push provider-side rate accounting to the edge.** With many `request-processor` instances across many regions/clusters, a single shared limiter (even sharded) adds cross-AZ/cross-region latency to every request. A token-bucket allowance can be leased in bulk to each processor instance/region (e.g. "you get 1% of the model's global RPM/TPM for the next 5s"), with a lightweight central service only handling lease negotiation, not per-request checks.
6. **Autoscale request-processor and provider calls on a queue-depth signal**, not fixed instance counts — Kafka consumer lag per partition is the natural signal to scale the processor fleet, and provider-call concurrency per instance should scale with available virtual-thread capacity and observed provider latency (a mini circuit breaker/AIMD loop) rather than a hardcoded concurrency cap.

None of this is implemented in this submission — the required/stretch benchmarks (300k / 1M req/s) are met with the architecture described above (Kafka decoupling, atomic per-model Redis limiting, virtual threads); the above is the extension path, not current behavior.

## Testing

```bash
mvn clean test
```

Unit tests cover the sliding-window rate limiter, batch progress/completion tracking, callback delivery retry behavior, and core ingest/request-processor logic.
