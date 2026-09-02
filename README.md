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
├── scenarios/               # validation scripts
├── scripts/                 # smoke tests, helpers
├── docker-compose.yml
└── pom.xml                  # Maven parent (Java modules under services/)
```

## Architecture

- **inference-gateway** — accept requests/batches, persist, publish to Kafka
- **request-processor** — consume Kafka, enforce RPM/TPM (Redis), call provider
- **provider-mock** — fake LLM HTTP API
- **webhook-delivery** — deliver batch webhooks with retries
- **loadgen** / **webhook-mock** — Node.js tooling (under `tools/`)

See [DESIGN.md](DESIGN.md) for full architecture.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker Desktop
- Node.js 20+ (for loadgen / webhook-mock)

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
node index.js --url http://localhost:8081 --rate 100 --duration 10 --models model-a:1.0 --tokens 1000
```

## Ports

| Service | Port |
|---------|------|
| inference-gateway | 8081 |
| provider-mock | 8082 |
| webhook-mock | 9000 |
| Postgres | 5433 |
| Redis | 6380 |
| Kafka | 9093 |

## Validation scenarios

See [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) and `scripts/e2e-smoke.sh` for reproducible tests.
