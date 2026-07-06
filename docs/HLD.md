# High-Level Design: Alert Deduplication Pipeline

| | |
| --- | --- |
| **Status** | Living document |
| **Version** | 0.1.0 (matches package) |
| **Audience** | Engineers owning ingress, reliability, or on-call tooling |
| **Related** | [README](../README.md), [Runtime notes](../src/alert_pipeline/runtime/README.md), [alerts.yaml](../config/alerts.yaml) |

---

## 1. Purpose

Turn a high-volume stream of application **error logs** into a small number of durable **incidents**, then:

1. **Persist** them as the system of record (PostgreSQL).
2. **Notify** humans and systems (Zenduty, Teams, webhooks, …).
3. **Operate** them from a shared dashboard (ack / resolve, TTA/TTR, label widgets).

The product is intentionally **runtime-pluggable** (Quix Streams) so the same business logic can run under different stream engines without rewrite.

### 1.1 Goals

| Goal | Design implication |
| --- | --- |
| Collapse noisy duplicates into one incident | Fingerprint + time window + optional refire |
| One source of truth for operators | Postgres for incidents, dispatch audit, widgets |
| Multi-instance UI consistency | Redis snapshot cache with stampede lock |
| Swap stream engines cheaply | `AlertProcessor` owns all domain logic; runtimes only move bytes |
| Configurable without redeploy of logic | YAML defaults + per-service / error_code overrides |

### 1.2 Non-goals (current scope)

- Full observability stack (metrics backends, tracing collectors, log storage).
- Long-term log retention / search (this system stores **incidents**, not every log line).
- Multi-tenant SaaS control plane.
- Exactly-once notification guarantees across external APIs (best-effort with retries + audit).

---

## 2. System context

```
                    ┌──────────────────────────────────────────────────┐
                    │                  This system                      │
  Producers         │                                                  │
  (apps, agents,    │   Kafka          Pipeline           Postgres     │
   log shippers) ──►│   topic:logs ──► Quix ──►  alerts        │
                    │        │              │             dispatch_log │
                    │        │              │             widgets      │
                    │        │              ▼                          │
                    │        │         Dispatchers ──► Zenduty / Teams │
                    │        │                         / webhooks      │
                    │        │              │                          │
                    │        │              ▼                          │
                    │        │         Operator UI ◄── Redis cache     │
                    │        │         (FastAPI :8000)                 │
                    └────────┼──────────────────────────────────────────┘
                             │
                    optional demo producer / webhook echo sink
```

**External actors**

| Actor | Interaction |
| --- | --- |
| Application / platform logging | Publishes structured log events to Kafka `logs` |
| On-call / SRE | Uses UI to acknowledge, resolve, filter by labels |
| Incident tools (Zenduty, Teams, custom webhooks) | Receive JSON payloads on new / refired incidents |
| Operators | Configure windows/fields via `config/alerts.yaml` and env |

---

## 3. Architecture overview

### 3.1 Layered view

```
┌─────────────────────────────────────────────────────────────────┐
│  Presentation                                                     │
│  UI static assets + FastAPI REST (list, stats, ack/resolve,      │
│  widgets, demo fire)                                              │
└───────────────────────────────┬─────────────────────────────────┘
                                │ reads via AlertReadCache
┌───────────────────────────────▼─────────────────────────────────┐
│  Application services                                             │
│  • AlertProcessor (handle_payload / handle_event)                 │
│  • DispatchFanout + pluggable AlertDispatcher implementations     │
│  • AlertRepository (CRUD + upsert + audit)                        │
│  • AlertReadCache (Redis snapshot + lock)                         │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Domain                                                           │
│  • Fingerprint (configurable fields)                              │
│  • DedupEngine (active window, refire, expire)                    │
│  • AlertEvent / LogEvent schemas, severity ranking                │
│  • YAML alert config resolution (defaults ← service ← error_code) │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Infrastructure adapters                                          │
│  • QuixStreamRuntime                         │
│  • SQLAlchemy + Postgres (or sqlite for tests)                    │
│  • Redis (UI cache only today)                                    │
│  • httpx + tenacity (outbound dispatch)                           │
│  • Kafka (ingress)                                                │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Runtime abstraction (core design choice)

```
                    PIPELINE_RUNTIME=quix
                              │
              │
              ▼
     QuixStreamRuntime
     (Application + SDF + keyed state)
                              ▼
                     AlertProcessor.handle_payload()
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         DedupEngine    AlertRepository   DispatchFanout
```

**Invariant:** runtimes must not implement dedup, DB, or notification rules. They only:

1. Subscribe to Kafka.
2. Decode message values into a structure `parse_log_payload` accepts.
3. Call `AlertProcessor`.
4. Optionally surface results for debug (debug sinks).

This keeps engine choice an ops/policy decision, not a product rewrite.

### 3.3 Process topology (Compose reference deployment)

| Service | Role | Stateful? |
| --- | --- | --- |
| `kafka` | Log ingress (KRaft single-node in compose) | Offsets, topics |
| `kafka-init` | Creates `logs` (3 partitions, RF=1) | No |
| `postgres` | SoR for alerts, dispatch_log, dashboard_widgets | Yes (volume) |
| `redis` | UI read cache (not pipeline dedup today) | Ephemeral OK (cache) |
| `alert-pipeline` | Consumer + processor + dispatch | Dedup in **Redis** (default) or memory |
| `alert-ui` | FastAPI + static UI | Stateless (state in PG/Redis) |
| `webhook-debug` | Echo sink for local dispatch verification | No |
| `log-producer` (profile `demo`) | Synthetic traffic | No |

---

## 4. Major components

### 4.1 Ingress contract (Kafka)

- **Topic:** `logs` (configurable via `KAFKA_INPUT_TOPIC`).
- **Value:** JSON object preferred; heterogeneous values are normalized in `parse_log_payload`.
- **Consumer group:** `alert-pipeline` (Quix).
- **Semantic model:** at-least-once delivery from Kafka; pipeline must tolerate reprocessing.

**Canonical log fields** (see `LogEvent`):

| Field | Role |
| --- | --- |
| `timestamp` | Event time (ISO / epoch; defaults to now) |
| `level` / severity | Gate for min-level filtering |
| `service` | Primary grouping dimension |
| `host` | Optional in fingerprint |
| `message` | Human text; normalized (UUID/hex stripped) when fingerprinted |
| `error_code` | Optional override key in YAML |
| `trace_id` | Correlation; optional in fingerprint |
| `labels` | Key/value map; full map or single `label:<k>` in fingerprint |

### 4.2 Fingerprinting & configuration

**Fingerprint** = first 32 hex chars of SHA-256 over an ordered, normalized field list from YAML.

Default fields: `service`, `level`, `labels`, `message` (host **excluded** so the same error across a fleet collapses to one incident).

**Config merge order** (`config/alerts.yaml`):

```
defaults  ←  services.<name>  ←  error_codes.<code>
```

Resolved settings per event include: `min_level`, `dedup_window_seconds`, `refire_interval_seconds`, `suppress_dispatch_while_acknowledged`, `dedup_fields`.

### 4.3 DedupEngine (domain)

In-memory map: `fingerprint → IncidentState`.

| Case | Behaviour |
| --- | --- |
| No active state for fingerprint | Open **new** incident (`is_new=True`), store state |
| Within window, before refire interval | Increment count; **suppress** emit |
| Within window, refire due | Emit **updated** with same `alert_id`, bumped count |
| Window elapsed (no recent events) | Drop state; next event can open a **new** incident |

**Shared state (production path):** Dedup uses **Quix keyed state** after `group_by(fingerprint)`. Redis is UI cache only. In-process memory remains for unit tests. See [§8 Hardening status](#8-hardening-status--remaining-work).

### 4.4 AlertProcessor (application core)

Pipeline for one payload:

```
parse → min-level check → DedupEngine.process
       → if alert: upsert Postgres → maybe suppress dispatch if acked
       → DispatchFanout (if not suppressed)
       → ProcessResult (metrics / optional sink material)
```

Skipped reasons: `unparseable`, `below_min_level`, `dedup_suppressed`.

### 4.5 Persistence (AlertRepository)

| Table | Purpose |
| --- | --- |
| `alerts` | One row per incident lifecycle; open rows matched by fingerprint + active status |
| `dispatch_log` | Per-channel attempt audit (success, status_code, body/error) |
| `dashboard_widgets` | Shared UI filters (multi-label AND rules) |

**Upsert rule:** if an active row (`open` / `updated` / `acknowledged`) exists for the fingerprint, update counts / last_seen / sample; else insert. Operator transitions (`acknowledged`, `resolved`, `reopen`) set TTA/TTR via `apply_status_timestamps`.

Schema bootstrap today: SQLAlchemy `create_all()` plus a small additive column migrator for TTA/TTR fields — **not** a full migration framework.

### 4.6 Dispatch subsystem

```
AlertDispatcher (ABC)
    ├── ZendutyDispatcher
    ├── TeamsDispatcher
    └── WebhookDispatcher
         build_dispatchers(Settings) → list
         DispatchFanout.dispatch(alert) → each channel + log_dispatch
```

- Global kill switch: `DISPATCH_ENABLED`.
- Per-channel enable flags and secrets from env.
- HTTP calls use **tenacity** (e.g. 3 attempts, exponential backoff).
- Failures are logged and audited; they do **not** currently roll back the incident row.

### 4.7 Operator UI & read path

- **Writes** (status change, widgets, demo fire): Postgres, then cache invalidate.
- **Reads** (list, stats, detail aggregates): `AlertReadCache` snapshot in Redis.

Cache design highlights:

| Mechanism | Why |
| --- | --- |
| Snapshot key + TTL (default 10s) | Bound DB load under 5s UI polling |
| `SET NX` lock | Single refresher under multi-UI stampede |
| Soft / early expiry | Reduce thundering herd at exact TTL boundary |
| In-memory fallback | Degrade if Redis unavailable (instance-local) |

**Consistency model:** UI may lag up to ~TTL after an ack/resolve that occurred on another path without invalidation; local mutations invalidate. Pipeline writes do not currently push invalidation to Redis — operators rely on TTL.

### 4.8 Stream runtimes

| State | In-process `DedupEngine` | Same — **must not** scale >1 without shared dedup |

---

## 5. Key sequences

### 5.1 Happy path: new incident

```
Producer → Kafka(logs)
        → Runtime.decode
        → AlertProcessor
            → fingerprint F
            → DedupEngine: no state → AlertEvent(new)
            → repo.upsert: INSERT alerts
            → fanout: Zenduty/Teams/Webhook
            → repo.log_dispatch × N
        → (async) UI poll → Redis miss/refresh → show open incident
```

### 5.2 Duplicate within window (suppressed)

```
… → DedupEngine: state exists, refire not due
  → return None (dedup_suppressed)
  → no DB write, no dispatch
```

### 5.3 Refire while open

```
… → DedupEngine: emit updated (same alert_id, count++)
  → repo.upsert: UPDATE occurrence_count, last_seen, status=updated
  → if status==acknowledged and suppress_dispatch_while_acknowledged: skip notify
  → else fanout again
```

### 5.4 Operator acknowledge

```
UI POST /alerts/{id}/status {acknowledged}
  → Postgres timestamps + tta_seconds
  → cache.invalidate()
  → subsequent refires may suppress dispatch (YAML)
```

---

## 6. Data model (logical)

```
Alert (incident)
  id, fingerprint, title, description, severity, service, host,
  status ∈ {open, updated, acknowledged, resolved},
  occurrence_count, first_seen, last_seen,
  error_code?, trace_id?, labels, sample_message,
  acknowledged_at?, resolved_at?, tta_seconds?, ttr_seconds?

DispatchAttempt
  id, alert_id → Alert, channel, success, status_code?,
  response_body?, error_message?, created_at

DashboardWidget
  id, title, labels[], status_filter, sort_order
```

**Active incident uniqueness (logical):** at most one non-resolved row per fingerprint *as enforced by application query*, not by a DB unique constraint today.

---

## 7. Cross-cutting concerns

### 7.1 Configuration sources

| Layer | Examples |
| --- | --- |
| Env / `.env` / Compose | Kafka, DB, Redis, dispatch flags, runtime |
| YAML | Dedup fields, windows, min level, per-service overrides |
| Code defaults | Settings pydantic defaults (incl. sqlite fallback for local/dev) |

### 7.2 Security & secrets (target posture)

- DB credentials, integration keys, webhook URLs must not be production defaults in VCS.
- Compose/demo may use known-local passwords; production should inject secrets from a vault / runtime secret store.
- UI currently has **no auth** — assume network perimeter (VPN / mesh / reverse proxy) until auth is added.

### 7.3 Observability (current)

- Structured-ish process logs (incident open, suppress, dispatch ok/fail).
- `ALERT_DB_FETCH` style markers on cache miss path (UI).
- `ProcessResult` for potential metrics hooks; no Prometheus exporter wired by default.

### 7.4 Failure modes (design intent)

| Failure | Expected behaviour |
| --- | --- |
| Malformed Kafka message | Log + skip (no DLQ today) |
| Postgres down | Processor/UI fail operations; pipeline should surface errors / restart |
| Redis down | UI falls back toward direct DB / local cache; **pipeline dedup fails open/closed depending on Redis errors at startup** (fail fast preferred) |
| Downstream webhook 5xx | Retries then audit failure; incident remains |
| Pipeline restart | With Redis dedup, active windows survive process death for the key TTL; DB upsert never decreases `occurrence_count` |

---

## 8. Hardening status & remaining work

### Implemented (post design-review hardening)

| Item | Status |
| --- | --- |
| Shared Redis dedup (`DEDUP_BACKEND=redis`, keys `alert_dedup:fp:{hash}`) | Done — Compose default |
| Partial unique index on active fingerprint | Done — repo bootstrap + Alembic |
| Upsert never resets `occurrence_count` on stale `is_new` | Done |
| Kafka broker + client auto-create topics disabled | Done |
| `logs-dlq` topic + unparseable routing | Done |
| `DATABASE_URL` / `POSTGRES_*` required from `.env` (no compose secret defaults) | Done |
| Alembic initial migration + entrypoint `upgrade head` | Done |
| Split `Dockerfile.pipeline` / `Dockerfile.ui` | Done |
| Webhook dispatch opt-in (`DISPATCH_WEBHOOK_ENABLED` default false) | Done |
| Pipeline invalidates UI Redis snapshot on write | Done |
| Tighter dep floors + `requirements.lock` | Done |
| Non-JSON strings no longer coerced into incidents | Done |

### Still open (lower priority / product choices)

| Issue | Risk | Direction |
| --- | --- | --- |
| Sync dispatch on consume path | Slow/hung webhook stalls consumer throughput | Async outbox or side queue for notifications |
| UI unauthenticated | Exposure if port public | Authn/z at gateway or app |
| Exactly-once notifications | Duplicate pages under at-least-once Kafka | Idempotency keys per channel / outbox |

---

## 9. Target architecture (post-hardening)

```
  Kafka logs ──► N pipeline workers (Quix)
                      │
                      ├─► Redis dedup keys (shared window state)
                      ├─► Postgres (SoR, unique active fingerprint)
                      ├─► Outbox / notify workers (optional)
                      └─► Dispatchers + dispatch_log

  UI × M ──► Redis snapshot ◄── invalidate on write paths
         └──► Postgres
```

**Acceptance criteria for “production multi-instance”**

1. Two pipeline processes, same consumer group, no duplicate *new* incidents for one fingerprint under steady load.
2. Process kill + restart does not reset `occurrence_count` or storm refires beyond configured policy.
3. Schema change is an Alembic revision, not a wipe.
4. Topics are explicitly provisioned; auto-create disabled.

---

## 10. Deployment views

### 10.1 Local / demo (current Compose)

Single Kafka, single Postgres, single pipeline, single UI, optional producer. Optimized for developer experience and demos, **not** HA.

### 10.2 Suggested production sketch

| Concern | Recommendation |
| --- | --- |
| Kafka | Managed cluster; explicit topic config (partitions, retention, RF≥3) |
| Pipeline | ≥2 replicas **only after** Redis (or equivalent) shared dedup |
| Postgres | Managed; migrations via CI; no default passwords |
| Redis | Managed; used for dedup **and** UI cache (separate key prefixes / DBs) |
| UI | Behind auth proxy; horizontal scale OK with shared Redis |
| Secrets | Injected env / secret store; rotate integration keys |
| Images | Split pipeline vs UI; pin digests |

---

## 11. Module map (code)

```
src/alert_pipeline/
  main.py                 # pipeline entry
  config.py               # env Settings
  alert_config.py         # YAML load/merge
  schemas.py              # LogEvent, AlertEvent, levels
  metrics.py              # TTA/TTR helpers
  sample_producer.py      # demo traffic
  dedup/
    fingerprint.py        # hash + title
    engine.py             # in-process window state
  processing/
    handler.py            # AlertProcessor (portable core)
  runtime/
    base.py, factory.py
    quix_runtime.py
  db/
    models.py, repository.py
  dispatchers/
    base.py, registry.py, zenduty.py, teams.py, webhook.py
  cache/
    alert_cache.py        # UI Redis snapshot
  ui/
    app.py, static/       # FastAPI + dashboard
config/alerts.yaml
tests/                    # unit coverage for dedup, cache, dispatch, repo, processor
```

---

## 12. Design principles (summary)

1. **Business logic is runtime-agnostic** — engines are adapters.
2. **Postgres is the operator system of record** — UI and audit trust it.
3. **Dedup is a policy layer** — YAML fields/windows, not hard-coded service names.
4. **Dispatch is pluggable and audited** — add a channel by subclass + registry flag.
5. **UI scale ≠ pipeline scale** — Redis for read consistency; pipeline scale requires shared **write-path** state (not implemented yet).
6. **Demo defaults ≠ production defaults** — harden credentials, auto-create, and dispatch footguns before multi-tenant or public exposure.

---

## 13. Open decisions

| Decision | Options | Notes |
| --- | --- | --- |
| Shared dedup store | Postgres advisory locks vs Quix keyed state | Redis already a dependency; natural fit for TTL windows |
| Notification reliability | Sync (today) vs outbox table vs dedicated topic | Outbox pairs well with multi-worker |
| Auth for UI | None / reverse proxy / OIDC | Required before non-private networks |
| Partition key for Kafka | None (default) vs fingerprint/service | Affects co-location of related events; does **not** replace shared dedup if multiple consumers |

---

*Document generated from the repository structure and source as of the design review. Update this HLD when P0 shared-dedup or Alembic land so the “current” vs “target” sections stay honest.*
