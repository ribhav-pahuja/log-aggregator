# Alert deduplication pipeline

Kafka → **deduplicate** noisy error logs into incidents → **PostgreSQL** (system of record) → **fan-out** to Zenduty, Microsoft Teams, webhooks, and more.

Includes an **operator UI** (ack / resolve, TTA/TTR, demo fire, **shared label widgets**) backed by a **Redis read cache** so multiple UI instances stay consistent.

Built for a **Python** team with **Quix Streams** (Kafka consumer + keyed-state dedup).

Repository: [github.com/ribhav-pahuja/log-aggregator](https://github.com/ribhav-pahuja/log-aggregator)

---

## Architecture

```
                    ┌─────────────────────────────────┐
  App logs ───────► │  Kafka: logs  (+ logs-dlq)      │
                    └───────────────┬─────────────────┘
                                    │
              ┌─────────────────────┴─────────────────────┐
              │  Quix Streams (Dockerfile.pipeline)         │
              │  1. parse / min-level                       │
              │  2. group_by(fingerprint)                   │
              │  3. keyed-state dedup (event-time window)   │
              │  4. emit → Postgres + outbox enqueue        │
              └─────────────────────┬─────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
    PostgreSQL              dispatch worker              Operator UI
    alerts (+ Alembic)      (outbox → Zenduty/           Dockerfile.ui
    dispatch_outbox         Teams / webhook)             FastAPI :8000
    dispatch_log                                           │ /metrics
    dashboard_widgets                                      ▼
                                                    Redis: UI snapshot cache only
```

**Core idea:** **Dedup** runs in **Quix keyed state** (`group_by` + per-fingerprint state, **event-time** windows). `AlertProcessor` **persists** incidents and **enqueues** notifications to `dispatch_outbox`. A separate **`alert-dispatch-worker`** drains the outbox (idempotent per channel). **Redis is only the UI read cache** (not used for dedup).

**HLD:** [`docs/HLD.md`](docs/HLD.md)

---

## Quick start (Docker)

```bash
cp .env.example .env          # required: POSTGRES_* and DATABASE_URL
docker compose up --build -d  # kafka, postgres, redis, pipeline (Quix), UI

# Dashboard
open http://localhost:8000

# Pipeline logs
docker compose logs -f alert-pipeline

# Optional synthetic Kafka traffic
docker compose --profile demo up -d

# Optional: enable webhook echo dispatches
# DISPATCH_WEBHOOK_ENABLED=true in .env, then recreate alert-pipeline
```

| Service | Role | Ports |
| --- | --- | --- |
| `kafka` | Log ingress (auto-create topics **disabled**) | `9092` host / `kafka:29092` in-network |
| `kafka-init` | Creates `logs` + `logs-dlq` | — |
| `postgres` | Alerts, outbox, dispatch audit, **shared widgets** (Alembic on boot) | `5432` |
| `redis` | **UI read cache only** (not pipeline dedup) | `6379` |
| `alert-pipeline` | Dedup + DB + outbox enqueue (`Dockerfile.pipeline`) | — |
| `alert-dispatch-worker` | Drains `dispatch_outbox` → Zenduty/Teams/webhook | — |
| `alert-ui` | Operator UI + APIs + `/metrics` (`Dockerfile.ui`) | **8000** |
| `webhook-debug` | Echo sink (opt-in via `DISPATCH_WEBHOOK_ENABLED`) | `8080` |
| `log-producer` | Demo traffic (`--profile demo`) | — |


### Tear down

```bash
docker compose --profile demo down -v
```

---

## Deduplication

### Behaviour

1. Only logs at or above the configured **min level** (default `ERROR`, from YAML / env) become incidents.
2. Events sharing the same **fingerprint** within **`dedup_window_seconds`** (measured in **event time**, not wall-clock) collapse into one active incident.
3. Optional **refire** every **`refire_interval_seconds`** (event-time) can emit an `updated` notification, unless suppressed while **acknowledged**.
4. After the window with no events, Quix per-key state is treated as expired; the next match can open a **new** incident (unless `allow_reopen_after_resolve: false` and a resolved row still exists).
5. **Host is not in the default fingerprint** (fleet-wide collapse). Choose fields per service / error_code in `config/alerts.yaml`.
6. **Quix co-locates fingerprints** via `group_by`. **Redis is not used for dedup** — only for the operator UI snapshot cache.
7. On emit: Postgres upsert + **outbox enqueue**; `alert-dispatch-worker` sends notifications with idempotency keys `{alert_id}:{channel}:{occurrence_count}`.

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

![Operator UI — shared widgets, incident list, and detail pane](docs/images/operator-ui.jpg)

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
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers |
| `KAFKA_INPUT_TOPIC` | `logs` | Source topic |
| `DATABASE_URL` | SQLite tmp / Compose Postgres | SQLAlchemy URL |
| `REDIS_URL` | `redis://localhost:6379/0` | UI shared cache |
| `UI_CACHE_TTL_SECONDS` | `10` | Snapshot TTL |
| `ALERT_CONFIG_PATH` | `config/alerts.yaml` | Dedup / refire YAML |
| `DEDUP_WINDOW_SECONDS` | `300` | Fallback if YAML missing |
| `DISPATCH_*` | — | Channel toggles and secrets |

Behavioural tuning prefers **`config/alerts.yaml`** over env when both apply.

---

## Project layout

```
src/alert_pipeline/
  runtime/quix_runtime.py # Kafka → group_by → state dedup → emit
  processing/handler.py   # AlertProcessor — persist + dispatch
  dedup/
    quix_state.py         # window/refire transitions (Quix State)
    fingerprint.py        # hash + title
    engine.py             # in-process engine (unit tests only)
  alert_config.py         # YAML load/merge
  db/                     # models, repository, Alembic-friendly schema
  cache/alert_cache.py    # Redis UI snapshot cache
  dispatchers/            # Zenduty, Teams, webhook
  ui/                     # FastAPI + static dashboard
  metrics.py              # TTA/TTR on status change
config/alerts.yaml
alembic/                  # schema migrations
Dockerfile.pipeline       # Quix pipeline image
Dockerfile.ui             # UI image
Dockerfile                # full/compat image
docker-compose.yml
tests/
```

---

## Development & tests

```bash
python -m venv .venv && source .venv/bin/activate   # 3.11+
pip install -e ".[dev,pipeline,ui]"

pytest -q
# Cache / stampede / pagination:
pytest -q tests/test_alert_cache.py
# Quix-state dedup unit tests (no Kafka):
pytest -q tests/test_quix_dedup_state.py
```

Host pipeline against Compose infra:

```bash
docker compose up -d kafka postgres redis webhook-debug
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DATABASE_URL=postgresql+psycopg://alerts:alerts@localhost:5432/alerts
export REDIS_URL=redis://localhost:6379/0   # UI cache invalidation only
export DEDUP_BACKEND=quix
export DISPATCH_WEBHOOK_ENABLED=true
export WEBHOOK_URL=http://localhost:8080/alerts
alert-pipeline
alert-ui
```

---

## Production notes

- **Stream runtime:** Quix only (Apache Flink support was removed).
- **Dedup:** Quix per-key state after `group_by(fingerprint)` — not Redis, not in-process across workers.
- Prefer **PostgreSQL** as system of record; run **Alembic** on deploy (`entrypoint` does `upgrade head`).
- **Redis** is optional for multi-UI read caching; pipeline correctness does not depend on it for dedup.
- Alert on log marker **`ALERT_DB_FETCH`** if UI cache miss rate is too high.
- Tenacity retries (3× backoff) on HTTP dispatchers; failures are recorded in `dispatch_log`.
- Do not commit real integration secrets; use `.env` locally only.

---

## License / contributing

Open an issue or PR on the GitHub repo. Keep business rules in `dedup/` + `AlertProcessor`; keep the Quix runtime as a thin Kafka/state adapter.
