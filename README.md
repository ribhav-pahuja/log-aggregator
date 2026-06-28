# Alert deduplication pipeline

Kafka → **deduplicate** noisy error logs into incidents → **persist** in PostgreSQL → **fan-out** to Zenduty, Microsoft Teams, and any webhook.

Built for a **Python** team using open-source components only.

## Why not Flink (by default)?

| Option | Fit for this use case | Python DX | Ops burden | When to choose it |
| --- | --- | --- | --- | --- |
| **Quix Streams** (this repo) | Excellent — Kafka-native stream app | First-class Python | Low (just your app + Kafka) | Default for Python teams; alert dedup + sinks |
| **Apache Flink (PyFlink)** | Excellent at massive scale / complex CEP | Usable, but Java ecosystem is richer | High (JobManager, TaskManagers, checkpoints, savepoints) | Multi-TB streams, exactly-once across many jobs, advanced windowing/CEP |
| **Faust** | Similar idea to Quix | Python | Medium | Legacy; prefer Quix Streams for new work |
| **Spark Structured Streaming** | Good for micro-batch analytics | PySpark is mature | High (cluster) | You already run Spark and want SQL windows |
| **Plain consumer + Redis** | Fine for small load | Python | Lowest | Single service, no stream framework needed |

**Recommendation:** start with **Quix Streams + PostgreSQL + pluggable HTTP dispatchers**. Revisit **Flink** if you outgrow a single (or small set of) Python processes, need event-time windows with watermarks at huge volume, or must share Flink infra with a Java streaming platform team.

Architecture with Quix still mirrors Flink’s mental model (source → keyed state / window → sinks), so a later port is straightforward: fingerprint becomes the key, dedup window becomes a session/tumbling window, DB + HTTP become sinks.

```
┌─────────────┐     ┌──────────────────────────────┐     ┌────────────┐
│  App logs   │────▶│  Kafka topic: logs           │────▶│  Quix app  │
└─────────────┘     └──────────────────────────────┘     │  (Python)  │
                                                         │            │
                         fingerprint + time window       │  Dedup     │
                         (in-process; partition-keyed)   │  Engine    │
                                                         └─────┬──────┘
                                                               │
                                              ┌────────────────┼────────────────┐
                                              ▼                ▼                ▼
                                        PostgreSQL      Zenduty API     Teams webhook
                                        (alerts +       (optional)      + generic
                                         dispatch log)                  webhooks
```

## Quick start (Docker — recommended)

Everything runs in Compose: Kafka, Postgres, the pipeline image, an HTTP echo sink, and (optionally) a sample log producer.

```bash
cp .env.example .env          # optional: set Zenduty/Teams keys
docker compose up --build -d  # core stack + alert-pipeline

# Watch pipeline logs
docker compose logs -f alert-pipeline

# Also emit synthetic ERROR logs so dedup is visible
docker compose --profile demo up --build -d
docker compose logs -f log-producer

# Alert dashboard (all incidents on one screen)
open http://localhost:8000
# API: http://localhost:8000/api/alerts  ·  http://localhost:8000/api/stats

# Inspect dispatched webhooks (default sink)
curl -s http://localhost:8080/ | head
# or open http://localhost:8080 in a browser while alerts fire
```

### Alert UI

The `alert-ui` service (FastAPI + single-page dashboard) reads the same Postgres `alerts` / `dispatch_log` tables the pipeline writes to.

- **URL:** [http://localhost:8000](http://localhost:8000)
- Live stats (open / updated / resolved, dispatch success/fail)
- Filter by status, severity, service; full-text search
- Incident detail: fingerprint, occurrences, labels, sample message
- Dispatch history per alert; mark **resolved** / **reopen** from the UI
- Auto-refresh every 5s (toggle in the header)

Local (without container): `alert-ui` or `python -m alert_pipeline.ui.app` with `DATABASE_URL` set.

Tear down:

```bash
docker compose --profile demo down -v
```

| Service | Role | Ports |
| --- | --- | --- |
| `kafka` | Log ingress | `9092` (host), `29092` (in-network as `kafka:29092`) |
| `kafka-init` | Creates `logs` topic once | — |
| `postgres` | Alert + dispatch audit store | `5432` |
| `alert-pipeline` | Dedup + DB + multi-API dispatch | — |
| `webhook-debug` | Echo HTTP sink for dry-run | `8080` |
| `log-producer` | Demo traffic (`--profile demo`) | — |

Image build is multi-stage (`Dockerfile`); entrypoint waits for Kafka/Postgres TCP before starting. Override secrets via `.env` or `docker compose` `environment`.

**Enable real on-call tools** in `.env` (compose interpolates them into `alert-pipeline`):

```bash
DISPATCH_ZENDUTY_ENABLED=true
ZENDUTY_INTEGRATION_KEY=your-integration-key

DISPATCH_TEAMS_ENABLED=true
TEAMS_WEBHOOK_URL=https://outlook.office.com/webhook/...

# Optional: turn off the echo sink once real channels are wired
DISPATCH_WEBHOOK_ENABLED=false
```

Rebuild/restart after env changes:

```bash
docker compose up -d --force-recreate alert-pipeline
```

### Run on the host (without the app container)

```bash
docker compose up -d kafka postgres webhook-debug
./scripts/create_topic.sh logs   # if kafka-init already ran, this is a no-op

python -m venv .venv && source .venv/bin/activate   # or: uv venv --python 3.12 .venv
pip install -e ".[dev]"

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export DATABASE_URL=postgresql+psycopg://alerts:alerts@localhost:5432/alerts
export WEBHOOK_URL=http://localhost:8080/alerts
export DISPATCH_WEBHOOK_ENABLED=true

alert-pipeline    # terminal 1
log-producer      # terminal 2
```

Repeated ERROR lines with the same service/message (or `error_code`) collapse into **one incident** for `DEDUP_WINDOW_SECONDS` (default 5 minutes). Occasional **update** notifications fire every `DEDUP_UPDATE_INTERVAL_SECONDS` with the rolling occurrence count.

## Deduplication rules

1. Only logs at or above `ALERT_MIN_LEVEL` (default `ERROR`) create alerts.
2. **Fingerprint** = SHA-256 of either:
   - `service + error_code + level` when `error_code` is set, else
   - `service + normalized message + level` (UUIDs, long hex, and integers stripped so IDs don’t fragment incidents).
3. Host is **not** part of the key (multi-replica failures → one incident).
4. First event opens an incident (`is_new=True`) and is dispatched.
5. Duplicates inside the window increment `occurrence_count` and are suppressed unless the update interval has elapsed (then an `updated` alert is sent).
6. After the window elapses with no events, state is dropped; the next matching log opens a **new** incident.

For multi-replica pipeline workers, either:

- run a **single** consumer group member for this app (simplest), or
- key Kafka messages by fingerprint / service and use **external state** (Redis `SET key NX EX window`) so all workers share dedup — the `DedupEngine` interface is intentionally small so you can swap the store.

## Database schema

- **`alerts`** — one row per incident (`id`, `fingerprint`, severity, counts, timestamps, status).
- **`dispatch_log`** — audit of every outbound HTTP call (channel, success, status code, error).

## Adding a new destination (PagerDuty, Slack, Opsgenie, …)

1. Subclass `AlertDispatcher` in `src/alert_pipeline/dispatchers/`.
2. Implement `send(alert) -> DispatchResult`.
3. Register it in `build_dispatchers()` behind a settings flag.

## Configuration

All settings are env vars (see `.env.example`). Notable ones:

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_INPUT_TOPIC` | `logs` | Source topic |
| `DEDUP_WINDOW_SECONDS` | `300` | Incident collapse window |
| `DEDUP_UPDATE_INTERVAL_SECONDS` | `60` | Min time between “still firing” updates |
| `ALERT_MIN_LEVEL` | `ERROR` | Threshold for alert generation |
| `DATABASE_URL` | SQLite tmp | SQLAlchemy URL |

## Tests

```bash
pytest -q
```

## Production notes

- Prefer **PostgreSQL** over SQLite.
- Put a **DLQ / retry** in front of flaky HTTP APIs (Tenacity already retries 3× with backoff per dispatcher).
- Consider publishing deduped alerts to a second Kafka topic (`alerts.deduped`) for analytics — easy extension on the Quix dataframe with `.to_topic(...)`.
- If you later adopt Flink, implement the same fingerprint as the key and the same window semantics in a `ProcessFunction` / session window; keep the Python dispatchers as a small sidecar consumer on `alerts.deduped` so on-call tools stay in Python.
