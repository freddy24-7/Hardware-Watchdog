# Hardware Watchdog

A production-grade hardware metrics pipeline built as an engineering portfolio piece.
A Rust agent collects CPU, memory, and disk I/O every 5 seconds, publishes to Kafka,
and a Rust consumer persists batches to PostgreSQL. Grafana visualises the data in real time.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Docker Compose Network                                         │
│                                                                 │
│  ┌──────────┐   JSON/5s   ┌───────────┐   batch    ┌────────┐  │
│  │  agent   │ ──────────► │   Kafka   │ ─────────► │consumer│  │
│  │  (Rust)  │             │ hw.metrics│             │ (Rust) │  │
│  └──────────┘             └───────────┘             └───┬────┘  │
│       │                         │                       │       │
│  sysinfo crate             Zookeeper               upsert│       │
│  CPU / mem / disk                                       ▼       │
│                                                   ┌──────────┐  │
│                                                   │ Postgres │  │
│                                                   │hw_metrics│  │
│                                                   └────┬─────┘  │
│                                                        │        │
│                                               ┌────────▼──────┐ │
│                                               │    Grafana    │ │
│                                               │ :3000         │ │
│                                               └───────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Why Kafka?

The agent measures hardware metrics in tight 5-second loops. Writing directly to Postgres
would couple collection availability to database availability — if Postgres is slow or down,
the agent either blocks (losing timing accuracy) or drops data. Kafka acts as a durable,
ordered buffer: the agent publishes and moves on immediately, while the consumer can lag,
catch up, batch efficiently, and retry writes without any feedback loop to the collector.

### Why batch upserts?

Individual `INSERT` per row would send one round-trip per message. With 12 messages/minute
per host that is negligible now, but the pattern does not scale. Batching via `UNNEST` sends
one round-trip per flush regardless of batch size. The `ON CONFLICT DO UPDATE` makes every
write idempotent — Kafka's at-least-once delivery cannot create duplicate rows.

### Why offset commits after DB writes?

If the consumer committed offsets first and then crashed before writing to Postgres, those
messages would be permanently lost. Committing only after a successful upsert means the
worst case is re-processing the same batch on restart — which the upsert handles safely.

---

## Quickstart

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- No Rust, Java, or Postgres installation required — everything runs in containers

### Run

```bash
git clone <repo-url>
cd hardware-watchdog
docker compose up --build -d
```

First build takes 5–10 minutes (compiling rdkafka from source). Subsequent builds use the
cached dependency layer and complete in under 30 seconds.

### Open Grafana

Navigate to [http://localhost:3000](http://localhost:3000) and log in with:

- **Username:** `admin`
- **Password:** `admin` (or the value of `GF_SECURITY_ADMIN_PASSWORD` in your `.env`)

The **Hardware Watchdog** dashboard loads automatically — no manual datasource or panel
configuration required.

### Tear down

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers and delete all data
```

---

## Environment Variables

Copy `.env.example` to `.env` to override defaults.

| Variable | Service | Default | Description |
|---|---|---|---|
| `KAFKA_BROKERS` | agent, consumer | `kafka:9092` | Kafka bootstrap broker address |
| `KAFKA_TOPIC` | agent, consumer | `hw.metrics` | Topic for metric events |
| `KAFKA_GROUP_ID` | consumer | `hw-watchdog` | Consumer group ID |
| `DATABASE_URL` | consumer | `postgres://watchdog:watchdog@postgres:5432/watchdog` | Postgres connection URL |
| `METRICS_INTERVAL_SECS` | agent | `5` | Collection interval in seconds |
| `CONSUMER_BATCH_SIZE` | consumer | `50` | Max rows per DB flush |
| `CONSUMER_BATCH_TIMEOUT_MS` | consumer | `2000` | Max wait before flushing partial batch |
| `POSTGRES_USER` | postgres | `watchdog` | Postgres username |
| `POSTGRES_PASSWORD` | postgres | `watchdog` | Postgres password |
| `POSTGRES_DB` | postgres | `watchdog` | Postgres database name |
| `GF_SECURITY_ADMIN_PASSWORD` | grafana | `admin` | Grafana admin password |
| `RUST_LOG` | agent, consumer | `info` | Log level filter |

---

## Running Tests

### Static analysis (requires Rust toolchain)

```bash
cargo clippy -- -D warnings
cargo fmt --check
```

### Per-crate tests

```bash
cargo test -p agent
cargo test -p consumer
```

### Integration test (full pipeline)

```bash
docker compose up -d
sleep 30

# Confirm rows are landing
docker compose exec postgres psql -U watchdog -d watchdog \
  -c "SELECT COUNT(*), MIN(collected_at), MAX(collected_at) FROM hw_metrics;"

# Confirm Grafana dashboard is provisioned
curl -sf http://admin:admin@localhost:3000/api/search | python3 -m json.tool | grep Hardware
```

---

## Project Structure

```
hardware-watchdog/
├── Cargo.toml                          workspace root
├── docker-compose.yml                  all services with health checks
├── .env.example                        environment variable reference
├── migrations/
│   ├── 001_create_metrics.sql          hw_metrics table
│   └── 002_create_indexes.sql          time-range indexes for Grafana
├── agent/                              metrics collector + Kafka producer
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                     Tokio entrypoint, graceful shutdown
│       ├── config.rs                   typed config from env vars
│       ├── metrics.rs                  MetricsSnapshot + sysinfo collection
│       └── producer.rs                 rdkafka producer with retry logic
├── consumer/                           Kafka consumer + PostgreSQL writer
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                     Tokio entrypoint, graceful shutdown
│       ├── config.rs                   typed config from env vars
│       ├── consumer.rs                 rdkafka consumer loop with batching
│       └── db.rs                       sqlx pool + batch upsert
└── grafana/
    └── provisioning/
        ├── datasources/postgres.yml    auto-provisioned datasource
        └── dashboards/
            ├── provider.yml            dashboard provider config
            └── hw_metrics.json         Hardware Watchdog dashboard
```

---

## Design Decisions

### Crate choices

| Concern | Crate | Rationale |
|---|---|---|
| Async runtime | `tokio` | Industry standard; `StreamConsumer` from rdkafka requires it |
| Kafka client | `rdkafka` | Thin bindings over battle-tested librdkafka C library |
| Database | `sqlx` | Async-native; compile-time query checking against live schema |
| Metrics | `sysinfo` | Cross-platform; `System` handle must be reused to compute CPU deltas |
| Errors | `thiserror` + `anyhow` | `thiserror` in libraries for typed errors; `anyhow` in binaries for context |
| Logging | `tracing` | Structured, async-aware; JSON format suits log aggregation pipelines |
| Config | `envy` | Deserialises env vars directly into typed structs; no scattered `env::var` calls |

### Primary key design

`PRIMARY KEY (collected_at, host)` rather than a surrogate integer:
- Upserts are naturally idempotent — no separate unique constraint needed
- Rows cluster by time on disk, matching Grafana's access pattern
- Multi-host deployments work without schema changes

### Docker image size

Multi-stage builds compile in `rust:slim` and copy only the final binary into
`ubuntu:24.04`. The runtime image contains no Rust toolchain, no build tools, and
no source code.

---

## Deployment Notes (Railway / Render / Fly.io)

The agent and consumer are stateless and read all configuration from environment variables,
making them straightforward to deploy on any container platform.

**Environment variables to set:**

| Variable | Value |
|---|---|
| `KAFKA_BROKERS` | Your managed Kafka broker URL (e.g. Upstash, Confluent Cloud) |
| `DATABASE_URL` | Your managed Postgres connection string |
| `KAFKA_TOPIC` | `hw.metrics` |
| `KAFKA_GROUP_ID` | `hw-watchdog` |
| `RUST_LOG` | `info` |

**Managed Kafka options:**
- [Upstash Kafka](https://upstash.com/kafka) — serverless, free tier available
- [Confluent Cloud](https://confluent.io) — free tier, same API as the local Confluent image
- [Redpanda Cloud](https://redpanda.com) — Kafka-compatible, generous free tier

**Note:** The Grafana provisioning in this repo targets a local Postgres datasource.
For a cloud deployment, update `grafana/provisioning/datasources/postgres.yml` with your
managed Postgres host and credentials, or use Grafana Cloud with a remote datasource plugin.
