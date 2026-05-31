# Hardware Watchdog

A production-grade hardware metrics pipeline. A Rust agent collects CPU, memory, and disk I/O every 5 seconds and sends it to a
cloud-hosted Rust ingest service, which persists rows to PostgreSQL. Grafana visualises
the data in real time with a per-machine dashboard.

---

## Try it — one command

```bash
curl -fsSL https://raw.githubusercontent.com/freddy24-7/Hardware-Watchdog/main/install.sh | bash
```

This detects your platform, downloads the agent binary, and prints two things:

1. **A start command** — run it in your terminal to begin sending metrics
2. **Your personal Grafana URL** — open it to see your own CPU, memory, and disk data live

Log in to Grafana with `admin` / `watchdog2024`. Keep the agent running while you watch the dashboard update every 5 seconds.

> Supports macOS (Apple Silicon and Intel) and Linux (x86_64).

---

## Architecture

```
  Your machine                         Railway (cloud)
  ┌────────────────────┐               ┌─────────────────────────────────┐
  │  agent (Rust)      │   POST /ingest│  ingest service (Rust / Axum)   │
  │  sysinfo → JSON    │ ────────────► │  validates + upserts to Postgres│
  │  ~/.hw-watchdog-id │               └──────────────┬──────────────────┘
  └────────────────────┘                              │
                                                      ▼
                                        ┌─────────────────────────┐
                                        │  PostgreSQL             │
                                        │  hw_metrics table       │
                                        │  (machine_id, host,     │
                                        │   cpu_pct, mem_pct, …)  │
                                        └──────────┬──────────────┘
                                                   │
                                        ┌──────────▼──────────────┐
                                        │  Grafana                │
                                        │  dashboard filtered     │
                                        │  by ?var-machine_id=…   │
                                        └─────────────────────────┘
```

### Design decisions

**Why HTTP instead of Kafka?**
For a single-agent demo, Kafka adds infrastructure complexity with no benefit. The agent
POSTs directly to the ingest service — simpler, cheaper, and the end user needs no broker.

**Why a stable `machine_id`?**
Multiple users share one Postgres instance. Each machine generates a UUID on first run
(persisted to `~/.hw-watchdog-id`) so Grafana can filter to exactly one user's data
without accounts or authentication.

**Why upserts?**
`ON CONFLICT (collected_at, host, machine_id) DO UPDATE` makes every write idempotent.
If the agent retries a failed POST, the row is overwritten in place — no duplicates.

**Why `sqlx` with raw `UNNEST` queries?**
`sqlx::query!` provides compile-time SQL checking against a live schema. The batch upsert
uses `UNNEST` to send one round-trip per flush regardless of batch size.

---

## Local development

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- No Rust or Postgres installation required

### Run

```bash
git clone https://github.com/freddy24-7/Hardware-Watchdog.git
cd hardware-watchdog
docker compose up --build -d
```

Open Grafana at [http://localhost:3000](http://localhost:3000) — login `admin` / `admin`.

The agent runs inside Docker and posts to the local ingest service. To test with your
own `machine_id`, run the agent binary directly:

```bash
INGEST_URL=http://localhost:8080/ingest ./hw-watchdog-agent
```

### Tear down

```bash
docker compose down      # stop containers, keep data
docker compose down -v   # stop containers and delete all data
```

---

## Environment variables

Copy `.env.example` to `.env` to override defaults.

| Variable | Service | Default | Description |
|---|---|---|---|
| `INGEST_URL` | agent | `http://ingest:8080/ingest` | HTTP endpoint to POST metrics to |
| `METRICS_INTERVAL_SECS` | agent | `5` | Collection interval in seconds |
| `DATABASE_URL` | ingest | `postgres://watchdog:watchdog@postgres:5432/watchdog` | Postgres connection URL |
| `PORT` | ingest | `8080` | TCP port for the HTTP server |
| `POSTGRES_USER` | postgres | `watchdog` | Postgres username |
| `POSTGRES_PASSWORD` | postgres | `watchdog` | Postgres password |
| `POSTGRES_DB` | postgres | `watchdog` | Postgres database name |
| `GF_SECURITY_ADMIN_PASSWORD` | grafana | `admin` | Grafana admin password |
| `RUST_LOG` | agent, ingest | `info` | Log level filter |

---

## Project structure

```
hardware-watchdog/
├── Cargo.toml                          workspace root
├── docker-compose.yml                  all services with health checks
├── .env.example                        environment variable reference
├── install.sh                          one-line installer for end users
├── migrations/
│   ├── 001_create_metrics.sql          hw_metrics table
│   ├── 002_create_indexes.sql          time-range indexes for Grafana
│   └── 003_add_machine_id.sql          multi-user machine_id column
├── agent/                              metrics collector → HTTP producer
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                     Tokio entrypoint, graceful shutdown
│       ├── config.rs                   typed config from env vars
│       ├── metrics.rs                  MetricsSnapshot + sysinfo + machine_id
│       └── producer.rs                 reqwest HTTP POST with retry
├── consumer/                           Axum HTTP ingest service
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                     Tokio entrypoint, graceful shutdown
│       ├── config.rs                   typed config from env vars
│       ├── handlers.rs                 POST /ingest and GET /health
│       └── db.rs                       sqlx pool + upsert
└── grafana/
    ├── Dockerfile                      extends grafana/grafana with provisioning
    ├── railway.toml                    Railway build config for Grafana service
    └── provisioning/
        ├── datasources/postgres.yml    auto-provisioned datasource
        └── dashboards/
            ├── provider.yml            dashboard provider config
            └── hw_metrics.json         Hardware Watchdog dashboard (machine_id filter)
```

---

## Crate choices

| Concern | Crate | Rationale |
|---|---|---|
| Async runtime | `tokio` | Industry standard for async Rust services |
| HTTP server | `axum` | Ergonomic, tower-compatible; minimal boilerplate |
| HTTP client | `reqwest` | Async-native; rustls avoids OpenSSL build dep |
| Database | `sqlx` | Async-native; compile-time query checking |
| Metrics | `sysinfo` | Cross-platform; `System` handle reused across ticks for correct CPU delta |
| Errors | `thiserror` + `anyhow` | `thiserror` for typed library errors; `anyhow` for binary entrypoints |
| Logging | `tracing` | Structured, async-aware; JSON format in containers |
| Config | `envy` | Deserialises env vars directly into typed structs |
