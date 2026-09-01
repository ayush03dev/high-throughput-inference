# High-Throughput Inference Gateway

Production-style inference gateway: **Java microservices + Kafka + PostgreSQL + Redis**, with **Node.js** load and callback tools. Single monorepo.

## Project layout

```
high-throughput-inference/
├── services/
│   ├── common/              # shared Java library (entities, rate limiter, Kafka events)
│   ├── ingest-api/          # HTTP API
│   ├── scheduler-worker/    # Kafka consumer + rate limits + provider calls
│   ├── callback-worker/     # batch webhook delivery
│   └── provider-simulator/  # fake LLM HTTP API
├── tools/
│   ├── loadgen/             # load generator CLI
│   └── callback-mock/       # webhook receiver for tests
├── scenarios/               # validation scripts
├── scripts/                 # smoke tests, helpers
├── docker-compose.yml
└── pom.xml                  # Maven parent (Java modules under services/)
```

## Architecture

- **ingest-api** — accept requests/batches, persist, publish to Kafka
- **scheduler-worker** — consume Kafka, enforce RPM/TPM (Redis), call provider
- **provider-simulator** — fake LLM HTTP API
- **callback-worker** — deliver batch webhooks with retries
- **loadgen** / **callback-mock** — Node.js tooling (under `tools/`)

See [DESIGN.md](DESIGN.md) for full architecture.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker Desktop
- Node.js 20+ (for loadgen / callback-mock)

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

## API

| Endpoint | Description |
|----------|-------------|
| `POST /v1/inference` | Submit single request |
| `POST /v1/batches` | Submit batch with `callbackUrl` |
| `GET /v1/requests/{id}` | Request status |
| `GET /v1/batches/{id}` | Batch status |
| `PUT /v1/admin/models/{name}` | Update RPM/TPM limits |

## Load generator

```bash
cd tools/loadgen && npm install
node index.js --url http://localhost:8080 --rate 100 --duration 10 --models model-a:1.0 --tokens 1000
```

## Ports

| Service | Port |
|---------|------|
| ingest-api | 8080 |
| scheduler-worker | 8081 |
| provider-simulator | 8082 |
| callback-worker | 8083 |
| callback-mock | 9000 |
| Postgres | 5432 |
| Redis | 6379 |
| Kafka | 9092 |

## Validation scenarios

See [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) and `scripts/e2e-smoke.sh` for reproducible tests.
