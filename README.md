# Alert deduplication pipeline

Kafka → **deduplicate** noisy error logs into incidents → **PostgreSQL** (system of record) → **fan-out** to Zenduty, Microsoft Teams, webhooks, and more.

Includes an **operator UI** (ack / resolve, TTA/TTR, demo fire, **shared label widgets**) backed by a **Redis read cache** so multiple UI instances stay consistent.

Built for a **Python** team with **pluggable stream runtimes**: **Quix Streams** (default) or **Apache Flink (PyFlink)**.

Repository: [github.com/ribhav-pahuja/log-aggregator](https://github.com/ribhav-pahuja/log-aggregator)

---

## Architecture

```
                    ┌─────────────────────────────────┐
  App logs ───────► │  Kafka topic: logs              │
                    └───────────────┬─────────────────┘
                                    │
              ┌─────────────────────┴─────────────────────┐
              │  Stream runtime (choose one)                │
              │  PIPELINE_RUNTIME=quix | flink              │
              │  Quix Streams  OR  PyFlink (Dockerfile.flink)│
              └─────────────────────┬─────────────────────┘
                                    │ handle_payload()
                                    ▼
              ┌─────────────────────────────────────────┐
              │  AlertProcessor (runtime-agnostic core)   │
              │  • YAML dedup_fields + window/refire        │
              │  • DedupEngine (fingerprint)              │
              │  • Postgres upsert (alerts)               │
              │  • Multi-API dispatch + dispatch_log      │
              └─────────────────────┬─────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
    PostgreSQL              Zenduty / Teams              Operator UI
    alerts                  generic webhook              FastAPI :8000
    dispatch_log            (pluggable)                    │
    dashboard_widgets                                      │ reads
                                                           ▼
                                                    Redis snapshot cache
                                                    TTL 10s + stampede lock
```

**Core idea:** business logic lives in `AlertProcessor` (`src/alert_pipeline/processing/`). Runtimes only move bytes from Kafka into that processor — so you can swap Quix ↔ Flink without rewriting dedup, DB, or dispatch.

---

## Quix vs Flink

| | **Quix Streams** (default) | **Apache Flink (PyFlink)** |
| --- | --- | --- |
| Role | Python Kafka stream app | Distributed stream engine (embedded mini-cluster in our image) |
| Ops | One container + Kafka + DB | Heavier image (JDK + Flink); prefer `Dockerfile.flink` |
| Dedup state | In-process (+ DB as SoR) | Same processor; use **`FLINK_PARALLELISM=1`** unless you add shared state |
| When to use | Day-to-day Python ownership | You already run Flink / need that runtime for policy reasons |

Set the engine with:

```bash
PIPELINE_RUNTIME=quix    # default
PIPELINE_RUNTIME=flink   # requires Flink image (see below)
```

Details: [`src/alert_pipeline/runtime/README.md`](src/alert_pipeline/runtime/README.md).

---

## Quick start (Docker)

```bash
cp .env.example .env          # optional: Zenduty/Teams keys
docker compose up --build -d  # kafka, postgres, redis, pipeline (Quix), UI

# Dashboard
open http://localhost:8000

# Pipeline logs
docker compose logs -f alert-pipeline

# Optional synthetic Kafka traffic
docker compose --profile demo up -d
```

| Service | Role | Ports |
| --- | --- | --- |
| `kafka` | Log ingress | `9092` host / `kafka:29092` in-network |
| `kafka-init` | Creates `logs` topic | — |
| `postgres` | Alerts, dispatch audit, **shared widgets** | `5432` |
| `redis` | **UI read cache** (multi-instance) | `6379` |
| `alert-pipeline` | Dedup + DB + dispatch (`PIPELINE_RUNTIME`) | — |
| `alert-ui` | Operator UI + APIs | **8000** |
| `webhook-debug` | Echo sink for dry-run dispatches | `8080` |
| `log-producer` | Demo traffic (`--profile demo`) | — |

### Run pipeline on Flink

PyFlink is finicky on some host Python versions; use the dedicated image:

```bash
PIPELINE_DOCKERFILE=Dockerfile.flink \
PIPELINE_IMAGE=alert-pipeline:flink \
PIPELINE_RUNTIME=flink \
FLINK_PARALLELISM=1 \
docker compose up -d --build alert-pipeline
```

Switch back to Quix:

```bash
PIPELINE_DOCKERFILE=Dockerfile \
PIPELINE_IMAGE=alert-pipeline:local \
PIPELINE_RUNTIME=quix \
docker compose up -d --build alert-pipeline
```

### Tear down

```bash
docker compose --profile demo down -v
```

---

## Deduplication

### Behaviour

1. Only logs at or above the configured **min level** (default `ERROR`, from YAML / env) become incidents.
2. Events sharing the same **fingerprint** within **`dedup_window_seconds`** collapse into one active incident (`occurrence_count` increases).
3. Optional **refire** every **`refire_interval_seconds`** can emit an `updated` notification (and dispatch), unless suppressed while **acknowledged**.
4. After the window with no events, in-process state expires; the next match can open a **new** incident (DB row was resolved or none active).
5. **Host is not in the default fingerprint** (fleet-wide collapse). Add `host` in `dedup_fields` if you need per-node incidents.

### Configurable fingerprint fields (`config/alerts.yaml`)

```yaml
defaults:
  min_level: ERROR
  dedup_window_seconds: 300
  refire_interval_seconds: 60
  suppress_dispatch_while_acknowledged: true
  dedup_fields:
    - service
    - level
    - labels      # all labels
    - message

# Optional overrides (merged: defaults ← service ← error_code)
services:
  payments-api:
    dedup_window_seconds: 600
    # dedup_fields: [service, error_code]
error_codes:
  DB_CONN:
    dedup_window_seconds: 900
```

Supported field names: `service`, `level` / `severity`, `message`, `labels`, `error_code`, `host`, `trace_id`, `label:<name>` (single label key).

Mount path in containers: `/config/alerts.yaml` (`ALERT_CONFIG_PATH`).

---

## Operator UI

**URL:** [http://localhost:8000](http://localhost:8000)

| Feature | Notes |
| --- | --- |
| Stats bar | Totals by status, dispatch ok/fail |
| Incident list | Paginated (**default page size 10**), filters, search |
| Detail pane | Labels, sample message, TTA/TTR, dispatch history |
| **Acknowledge** / **Resolve** / **Reopen** | Updates Postgres; computes **TTA** / **TTR** |
| Demo controls | Fire alerts (DB write + optional Kafka), clear all |
| **Shared label widgets** | Stored in Postgres; multi-label **AND** filters; all UI instances agree |
| Auto-refresh | ~5s (toggle in header) |

### Alert sort order

Incidents are ordered by **`last_seen` descending** (most recently active first), then paginated.

### Shared widgets (multi-label)

Widgets are **not** localStorage-only: they live in **`dashboard_widgets`** so every server and operator sees the same boards.

- Each widget has a **title**, **status filter**, and a **list of labels**.
- An alert must match **all** label rules (AND). Empty value = key must exist with any value.
- UI: textarea with one `key=value` (or `key`) per line.
- APIs: see [HTTP API](#http-api) below.

### UI read cache (Redis)

The UI **does not query Postgres on every poll**. It reads a **shared snapshot** from Redis:

| Setting | Default | Meaning |
| --- | --- | --- |
| `REDIS_URL` | `redis://redis:6379/0` (Compose) | Shared cache |
| `UI_CACHE_TTL_SECONDS` | **10** | Logical snapshot TTL |
| `UI_CACHE_LOCK_TTL_SECONDS` | 5 | Stampede lock TTL |

- **Stampede protection:** Redis `SET NX` lock so only one instance reloads from DB when the snapshot expires; probabilistic early expiry spreads load.
- **Writes** (ack/resolve/demo/widgets that change alerts) update Postgres, then **invalidate** the snapshot and rebuild under the lock.
- If Redis is unavailable, the process falls back to **in-memory** cache (not multi-instance safe) and keeps serving (no 500 on Redis MISCONF).
- **Every DB snapshot load** logs a **WARNING** you can alert on:

  ```text
  ALERT_DB_FETCH event=alert_ui_db_fetch reason=... fetch_count=N ...
  ```

  Inspect: `GET /api/cache` → `db_fetch_count`, `source` (`redis` | `memory`).

---

## Status lifecycle & TTA / TTR

| Status | Meaning |
| --- | --- |
| `open` | New incident |
| `updated` | More matching logs while active (pipeline) |
| `acknowledged` | Operator owns it; counts can still rise |
| `resolved` | Closed; new matching errors can open a new row |

Stored on each alert row:

- **`tta_seconds`** — time to acknowledge (`acknowledged_at − first_seen`)
- **`ttr_seconds`** — time to resolve (`resolved_at − first_seen`)
- Resolve without prior ack sets TTA = TTR (implicit ack)

---

## HTTP API (UI service)

Base: `http://localhost:8000`

### Alerts (paginated, cache-backed reads)

```http
GET /api/alerts?page=1&page_size=10
GET /api/alerts?status=open,updated&service=payments-api&q=timeout
GET /api/alerts?label_key=env&label_value=prod
GET /api/alerts?labels=[{"key":"env","value":"local"},{"key":"source","value":"ui-demo"}]
```

Response shape:

```json
{
  "items": [ /* AlertOut */ ],
  "page": 1,
  "page_size": 10,
  "total": 42,
  "pages": 5,
  "has_next": true,
  "has_prev": false
}
```

Default **`page_size` for alerts is 10** (max 200). Legacy `limit` / `offset` still accepted.

```http
GET  /api/alerts/{id}
GET  /api/alerts/{id}/dispatches?page=1&page_size=50
POST /api/alerts/{id}/ack
POST /api/alerts/{id}/resolve
POST /api/alerts/{id}/reopen
POST /api/alerts/{id}/status   {"status":"acknowledged"|"resolved"|"open"|...}
GET  /api/stats
GET  /api/services
GET  /api/cache
GET  /api/dispatches/recent?page=1&page_size=30
```

### Shared widgets

```http
GET    /api/widgets
POST   /api/widgets
       {"title":"Prod platform","labels":[{"key":"env","value":"prod"},{"key":"team","value":"platform"}],
        "status_filter":"open,updated,acknowledged","sort_order":0}
PUT    /api/widgets/{id}
DELETE /api/widgets/{id}
```

### Demo

```http
POST /api/demo/reset
POST /api/demo/fire
     {"service":"payments-api","message":"...","severity":"ERROR","error_code":"DEMO",
      "count":1,"also_publish_kafka":false}
```

Demo fire **always writes Postgres** (so the UI updates immediately); optional Kafka publish exercises the stream path.

---

## Database tables

| Table | Purpose |
| --- | --- |
| `alerts` | Incidents: fingerprint, status, counts, labels, **TTA/TTR**, timestamps |
| `dispatch_log` | Outbound notification audit |
| `dashboard_widgets` | **Shared** UI widgets (multi-label filters) |

---

## Dispatch destinations

Pluggable `AlertDispatcher` implementations:

- **Zenduty** — `DISPATCH_ZENDUTY_ENABLED`, `ZENDUTY_INTEGRATION_KEY`
- **Microsoft Teams** — `DISPATCH_TEAMS_ENABLED`, `TEAMS_WEBHOOK_URL`
- **Generic webhook** — `DISPATCH_WEBHOOK_ENABLED`, `WEBHOOK_URL` (Compose defaults to `webhook-debug:8080`)

Add more: subclass in `src/alert_pipeline/dispatchers/`, register in `build_dispatchers()`.

---

## Configuration (env)

See [`.env.example`](.env.example). Important variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PIPELINE_RUNTIME` | `quix` | `quix` or `flink` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers |
| `KAFKA_INPUT_TOPIC` | `logs` | Source topic |
| `DATABASE_URL` | SQLite tmp / Compose Postgres | SQLAlchemy URL |
| `REDIS_URL` | `redis://localhost:6379/0` | UI shared cache |
| `UI_CACHE_TTL_SECONDS` | `10` | Snapshot TTL |
| `ALERT_CONFIG_PATH` | `config/alerts.yaml` | Dedup / refire YAML |
| `DEDUP_WINDOW_SECONDS` | `300` | Fallback if YAML missing |
| `FLINK_PARALLELISM` | `1` | PyFlink tasks (keep 1 without external dedup state) |
| `DISPATCH_*` | — | Channel toggles and secrets |

Behavioural tuning prefers **`config/alerts.yaml`** over env when both apply.

---

## Project layout

```
src/alert_pipeline/
  processing/handler.py   # AlertProcessor — portable core
  runtime/                # quix_runtime, flink_runtime, factory
  dedup/                  # fingerprint + DedupEngine
  alert_config.py         # YAML load/merge
  db/                     # models, repository
  cache/alert_cache.py    # Redis (+ memory fallback) UI cache
  dispatchers/            # Zenduty, Teams, webhook
  ui/                     # FastAPI + static dashboard
  metrics.py              # TTA/TTR on status change
config/alerts.yaml
Dockerfile                # Quix / default app image
Dockerfile.flink          # PyFlink worker image
docker-compose.yml
tests/
```

---

## Development & tests

```bash
python -m venv .venv && source .venv/bin/activate   # 3.11+; Flink optional extra needs 3.11 often
pip install -e ".[dev]"
# Optional Flink on host (prefer Docker image on macOS ARM / Python 3.12):
# pip install -e ".[flink]"

pytest -q
# Cache / stampede / pagination:
pytest -q tests/test_alert_cache.py
```

Host pipeline against Compose infra:

```bash
docker compose up -d kafka postgres redis webhook-debug
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DATABASE_URL=postgresql+psycopg://alerts:alerts@localhost:5432/alerts
export REDIS_URL=redis://localhost:6379/0
export DISPATCH_WEBHOOK_ENABLED=true
export WEBHOOK_URL=http://localhost:8080/alerts
alert-pipeline   # or PIPELINE_RUNTIME=flink with Flink deps/image
alert-ui
```

---

## Production notes

- Prefer **PostgreSQL** and **Redis** for multi-UI deployments.
- Pipeline dedup memory is **per process** — one consumer instance (or Flink parallelism 1) unless you add shared dedup state (e.g. Redis) inside `DedupEngine`.
- Alert on log marker **`ALERT_DB_FETCH`** if UI cache miss rate is too high.
- Tenacity retries (3× backoff) on HTTP dispatchers; failures are recorded in `dispatch_log`.
- Do not commit real integration secrets; use `.env` locally only.

---

## License / contributing

Open an issue or PR on the GitHub repo. Keep new stream engines behind `StreamRuntime` and route all event handling through `AlertProcessor`.
