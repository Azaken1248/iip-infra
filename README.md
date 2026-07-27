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
| `kafka-topics-init` | same, one-shot job creating `interns.created` / `iip.dlq` | — |
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

## Manual smoke test (Phase 1.21)

The automated version of this lives in `e2e-tests/` (below); this is the same walkthrough by hand, useful for eyeballing the UI itself rather than just asserting on it:

```bash
cp .env.example .env
docker compose up -d --build kafka kafka-topics-init postgres source-service db-adapter file-adapter
```

1. Open http://localhost:3000 (or `cd ../ui && npm run dev` for hot-reload instead), submit an intern.
2. Confirm it in Postgres: `docker exec iip-postgres psql -U iip -d iip -c "SELECT * FROM interns;"`
3. Confirm it in the CSV: `docker exec iip-file-adapter cat /data/interns.csv`
4. Open the **Targets** tab in the UI — both targets should show *Running*, and clicking either should show the same record you just confirmed by hand above.
5. Click **Pause** on the Database target, submit a second intern, and confirm (via the `psql` command above) it does *not* yet appear. Click **Resume** and confirm it appears within a few seconds — this is Original Specification §9's zero-data-loss guarantee, demonstrated interactively rather than just asserted in a test.

## Full-pipeline proof test (`e2e-tests/`)

A separate, standalone Maven project (no Spring Boot — it only orchestrates other services' containers, it doesn't run application code of its own) proving Original Specification §9's guarantee: one HTTP submission through the real Source Service lands in both real targets. It builds the actual Docker images from each service's own `Dockerfile` via Testcontainers, wires them together on one network, and asserts a Postgres row and a CSV line both appear. It lives here rather than in any single service's repo because this is the only place that's ever known how to wire all three together.

```bash
cd e2e-tests
./mvnw test
```

## Notes

- `kafka-topics-init` is a stand-in for proper topic provisioning; once the services declare their own `NewTopic` beans (Spring Kafka), this can be trimmed down or removed.
- The file adapter's dedup store and CSV output live on the named `file-adapter-data` volume — see [Architecture §4.5](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md) for why this adapter must stay single-instance.
