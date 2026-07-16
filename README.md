# Food Delivery Order Processing Service

An event-driven Go service that consumes food-order events from Kafka, keeps the current order state in PostgreSQL, and exposes a paginated List Orders API.

The included Go generator continuously produces validated create, status, and item-update events. Kafka is the transport because it preserves keyed ordering, supports independent consumer groups, and lets the producer and API scale separately.

## Quick start

Prerequisite: Docker Desktop with its Linux engine running.

```bash
docker-compose up --build -d
docker-compose --profile generator up --build -d generator
curl http://localhost:8080/v1/orders
```

The first command starts PostgreSQL, Kafka, topic initialization, the database migration, and the API. The generator is opt-in so the API can also be inspected against a quiet database.

Useful commands:

```bash
docker-compose logs -f api generator
curl "http://localhost:8080/v1/orders?status=Preparing&limit=20&offset=0"
curl http://localhost:8080/readyz
docker-compose down
```

Use `docker-compose down -v` only when you intentionally want to remove all local PostgreSQL data.

## Hybrid mode: dependencies in Docker, Go services on WSL

Start dependencies and the migration in Docker:

```bash
docker-compose up -d postgres kafka kafka-init migrate
```

If the full Docker mode is already running, stop only the app containers before starting WSL processes:

```bash
docker-compose stop api generator
```

Run the API from the repository root:

```bash
go run ./cmd/api
```

In another shell from the repository root, run the generator:

```bash
go run ./cmd/generator
```

The local defaults use `localhost:5432` and `localhost:9092`. The Docker Compose services use their internal hostnames automatically.

This mode runs only PostgreSQL, Kafka, topic initialization, and the migration in Docker. The API and generator run as normal WSL processes, which makes their JSON logs visible directly in each terminal.

## Logs

Docker mode:

```bash
docker-compose logs -f api
docker-compose logs -f generator
docker-compose logs -f postgres kafka
```

The API logs HTTP requests, successfully processed event IDs, database/Kafka retries, and outbox publications. The generator logs each published event and Kafka retries. Payloads are not logged to avoid noisy or sensitive output.

WSL process mode:

```bash
go run ./cmd/api 2>&1 | tee api.log
go run ./cmd/generator 2>&1 | tee generator.log
```

## Live reload with Air

Air watches the Go API entrypoint under `cmd/api`; it does not try to build the repository root.

```bash
docker-compose up -d postgres kafka kafka-init migrate
air
```

Run the generator separately in another WSL terminal:

```bash
go run ./cmd/generator
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | API listen address |
| `DATABASE_URL` | local `orders` PostgreSQL URL | PostgreSQL connection string |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_EVENTS_TOPIC` | `order-events` | Source event topic |
| `KAFKA_ACKS_TOPIC` | `order-created` | Server-created order ID acknowledgements |
| `KAFKA_DLQ_TOPIC` | `order-events-dlq` | Invalid-event dead-letter topic |
| `KAFKA_GROUP_ID` | `order-service` | Kafka consumer group |
| `OUTBOX_POLL_INTERVAL` | `1s` | Database outbox publishing interval |
| `EVENTS_PER_SECOND` | `2` | Generator rate, from 1 to 100 |

## Repository layout

```text
cmd/api/                 Go process entry point
internal/domain/         Event and order validation rules
internal/messaging/      Kafka consumer, DLQ, and transactional-outbox publisher
internal/repository/     PostgreSQL state, idempotency, and queries
internal/httpapi/        HTTP handlers
migrations/              PostgreSQL schema
cmd/generator/           Go continuous event generator
```

## Verification

```bash
go test ./...
docker-compose config
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl "http://localhost:8080/v1/orders?limit=5"
```

See [API.md](API.md) for the HTTP contract and [DESIGN.md](DESIGN.md) for the event flow and trade-offs.
