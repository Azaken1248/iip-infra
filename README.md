# IIP — Infrastructure

Local Docker Compose orchestration for the Intern Integration Platform's Release 1 walking skeleton — see [`docs`](https://github.com/Azaken1248/iip-docs) for architecture, use cases, data model, and the implementation plan.

This repo expects to sit alongside its sibling repos, checked out at the same level:

```
InternIntegrationPlatform/
├── docs/
├── infra/            <- this repo
├── ui/
├── source-service/
├── contract-registry/
├── db-adapter/
└── file-adapter/
```

## What's wired up

| Service | Image / build | Port |
| --- | --- | --- |
| `kafka` | `apache/kafka:4.3.1` (KRaft, single node) | 9092 (host), `kafka:19092` (containers) |
| `kafka-topics-init` | same, one-shot job creating `interns.created` / `iip.dlq` | — |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui` | 8090 |
| `postgres` | `postgres:17-alpine`, schema from `postgres/*.sql` (applied in lexical order) | 5433 (host) → 5432 (container) |
| `schema-registry` | `confluentinc/cp-schema-registry`, holds the envelope schema | 8085 (host) → 8081 (container) |
| `schema-registry-init` | one-shot job registering `schemas/envelope.json` as `iip.envelope-value` | — |
| `contract-registry` | built from `../contract-registry` | 8083 |
| `contract-registry-init` | one-shot job POSTing `contracts/*.json` into the registry | — |
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

If `docker compose` reports `unknown command`, the Compose v2 CLI plugin isn't
installed and the standalone binary is what you have — substitute
`docker-compose` for `docker compose` in every command in this file. The two are
interchangeable here; nothing in this compose file depends on which one runs it.

The ports below are the defaults. `.env` overrides them, so check there before
concluding a service is down — a health check against the default port may be
answering from something else entirely.

- UI: http://localhost:3000
- Source Service: http://localhost:8080 (`/actuator/health`)
- Contract Registry: http://localhost:8083 (`/contracts`)
- Schema Registry: http://localhost:8085 (`/subjects`)
- Kafka UI: http://localhost:8090

### Upgrading a deployment that already has data

`./postgres` is mounted at `/docker-entrypoint-initdb.d`, and Postgres runs
those scripts **only when the data directory is empty**. On a volume created
before a migration was added, that migration never runs — so a stack that works
perfectly from scratch fails on an existing volume, and the symptom is not a
missing-table error you can see. `contract-registry` runs `ddl-auto: validate`,
so it exits during startup; `contract-registry-init` then polls a corpse.

After pulling changes that add a file under `./postgres`, apply it by hand:

```bash
docker exec -i iip-postgres psql -U iip -d iip < postgres/02-registry.sql
```

Every script here is written with `CREATE TABLE IF NOT EXISTS`, so re-applying
one is a no-op and this is always safe. `docker compose down -v` also works but
destroys the interns rows and the file-adapter dedup store.

Note that this is untested territory: `e2e-tests` builds a fresh Postgres on
every run, so it exercises the from-scratch path and never this one.

`./seed` is deliberately **not** mounted there. It holds control-plane data
rather than schema — currently the adapter attachments — and it is applied on
every `up` by the `attachment-init` job, so it needs no manual step and works
on an existing volume. Anything that must run after the contracts exist belongs
in `./seed`, not `./postgres`: an attachment references a contract by foreign
key, and as an init script it would fail Postgres' own startup on a fresh
volume.

### Where contracts come from

From Release 4 the source-service ships **no contract files**. It fetches
them from the Contract Registry at startup and re-fetches every
`CONTRACT_REFRESH_INTERVAL_MS`, so a contract registered while the stack is
running goes live without a restart.

`contracts/*.json` in this repo is the deployment's starting state, seeded by
`contract-registry-init` through the same public `POST /contracts` the
control-plane UI will use — there is no privileged back door. Adding a
contract is therefore either dropping a file here and re-running that job, or
POSTing it yourself:

```bash
curl -X POST http://localhost:8083/contracts \
  -H 'Content-Type: application/json' --data-binary @contracts/interns.json
```

If `source-service` exits at startup complaining that no contracts are
registered, the registry is up but empty — re-run `contract-registry-init`.

### Where the envelope schema comes from

The **contract** describes one schema's payload and lives in the Contract
Registry. The **envelope** is the same for every contract (Data Model §1a) and
lives in the Schema Registry, as `schemas/envelope.json` registered under
subject `iip.envelope-value` with BACKWARD compatibility. Two registries,
because they answer different questions and change at different rates: a
contract is edited from a UI several times a day, an envelope change is a
platform-wide event that goes through CI and a deploy.

From Release 4 none of the three service images ships the envelope schema
either. All three fetch it at startup and **refuse to start without it** —
the source service validates every envelope before the producer batches it,
and both adapters validate every message before touching a database, a file,
or a dedup store.

What is on the wire is still plain canonical JSON. The registry owns the
schema, not the byte format: no magic byte, no schema id, no Confluent client
needed to read a topic. That is deliberate — see the decision note under
Release 4 in the [rollout plan](https://github.com/Azaken1248/iip-docs/blob/main/05-phased-rollout.md).

```bash
curl -s http://localhost:8085/subjects/iip.envelope-value/versions/latest | jq -r .schema | jq .
```

### The compatibility gate

`scripts/compatibility-gate.sh` is what CI runs, and it needs both registries
up:

```bash
sh scripts/compatibility-gate.sh
```

It checks `schemas/envelope.json` against the registered envelope subject,
replays the per-contract BACKWARD check over every file in `contracts/`, and
confirms each service's `src/test/resources/schemas/envelope.json` fixture
still matches this repo's copy. An incompatible change fails the build rather
than production, which is the whole point — and the gate is itself proven to
fail, by `e2e-tests`, against a deliberately-breaking contract.

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
docker pull confluentinc/cp-schema-registry:7.7.1   # first run only, see below
cd e2e-tests
./mvnw test
```

**Pre-pull that image on a first run.** Testcontainers abandons an image pull
after two minutes and the Schema Registry image is around 1.5 GB, so on a slow
connection the suite fails with `ContainerFetchException: Can't get Docker
image` before a single assertion runs. That limit is not configurable from the
test, and it is a pull problem rather than a pipeline problem — pulling once
by hand is the whole fix. Every other image the suite uses is either small or
built locally from a sibling repo.

From Release 4 the suite proves more than Release 1's exit criterion. It also
runs `schemas/register-envelope.sh` and `scripts/compatibility-gate.sh` as
scripts, in a container on the pipeline's own network, so what is tested is
what a deployment and CI actually execute rather than a Java reimplementation
of it — including that the gate *fails* when handed a deliberately-breaking
contract, and that a contract evolved through the API goes live with nothing
redeployed (Phases 4.10 and 4.11).

## Notes

- `kafka-topics-init` is a stand-in for proper topic provisioning; once the services declare their own `NewTopic` beans (Spring Kafka), this can be trimmed down or removed.
- The file adapter's dedup store and CSV output live on the named `file-adapter-data` volume — see [Architecture §4.5](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md) for why this adapter must stay single-instance.
