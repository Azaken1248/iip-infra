# IIP — Infrastructure

Local Docker Compose orchestration for the Intern Integration Platform's Release 1 walking skeleton — see [`docs`](https://github.com/Azaken1248/iip-docs) for architecture, use cases, data model, and the implementation plan.

This repo expects to sit alongside its sibling repos, checked out at the same level:

```
InternIntegrationPlatform/
├── docs/
├── infra/            <- this repo
├── ui/
├── source-service/
├── db-adapter/
└── file-adapter/
```

## What's wired up

| Service | Image / build | Port |
| --- | --- | --- |
| `kafka` | `apache/kafka:4.3.1` (KRaft, single node) | 9092 (host), `kafka:19092` (containers) |
| `kafka-topics-init` | same, one-shot job creating `intern.created` / `intern.dlq` | — |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui` | 8090 |
| `postgres` | `postgres:17-alpine`, schema from `postgres/init.sql` | 5433 (host) → 5432 (container) |
| `source-service` | built from `../source-service` | 8080 |
| `db-adapter` | built from `../db-adapter` | 8081 |
| `file-adapter` | built from `../file-adapter` | 8082 |
| `ui` | built from `../ui` (nginx) | 3000 |

Containers talk to Kafka via `kafka:19092`, **not** `kafka:9092` — port 9092 is only advertised as `localhost`, for host-machine tools connecting from outside Docker (see the [Kafka listener config](docker-compose.yml)). This is the classic Kafka advertised-listener trap: get it backwards and every in-cluster client will bootstrap successfully, then fail on the follow-up metadata-driven reconnect.

Postgres's host-side port defaults to **5433**, not 5432 — chosen because 5432 commonly collides with a locally-installed Postgres. Containers always reach it at `postgres:5432` regardless; only host-machine tools (e.g. `psql`) need the 5433 mapping, and it's overridable via `POSTGRES_HOST_PORT`.

## Run

```bash
cp .env.example .env   # override POSTGRES_PASSWORD / POSTGRES_HOST_PORT if you want
docker compose up --build
```

- UI: http://localhost:3000
- Source Service: http://localhost:8080 (`/actuator/health`)
- Kafka UI: http://localhost:8090

## Notes

- `kafka-topics-init` is a stand-in for proper topic provisioning; once the services declare their own `NewTopic` beans (Spring Kafka), this can be trimmed down or removed.
- The file adapter's dedup store and CSV output live on the named `file-adapter-data` volume — see [Architecture §4.5](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md) for why this adapter must stay single-instance.
