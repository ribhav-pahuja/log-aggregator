# High-Level Design: Alert Deduplication Pipeline

| | |
| --- | --- |
| **Status** | Living document |
| **Version** | 0.2.0 |
| **Audience** | Engineers owning ingress, reliability, or on-call tooling |
| **Related** | [README](../README.md), [Runtime notes](../src/alert_pipeline/runtime/README.md), [alerts.yaml](../config/alerts.yaml) |

---

## 1. Purpose

Turn a high-volume stream of application **error logs** into a small number of durable **incidents**, then:

1. **Persist** them as the system of record (PostgreSQL).
2. **Notify** humans and systems (Zenduty, Teams, webhooks, …).
3. **Operate** them from a shared dashboard (ack / resolve, TTA/TTR, label widgets).

The pipeline is a **Python-native Quix Streams** application: Kafka consume → fingerprint co-location → keyed-state dedup → Postgres + dispatch. The operator UI is a separate FastAPI service with an optional **Redis read cache**.

### 1.1 Goals

| Goal | Design implication |
| --- | --- |
| Collapse noisy duplicates into one incident | Fingerprint + time window + optional refire in **Quix keyed state** |
| One source of truth for operators | **Postgres** for incidents, dispatch audit, widgets |
| Multi-instance UI consistency | **Redis snapshot cache** (read path only) with stampede lock |
| Python-team ownership | Quix library, no JVM stream cluster |
| Configurable without code changes | YAML defaults + per-service / error_code overrides |

### 1.2 Non-goals (current scope)

- Full observability stack (metrics backends, tracing collectors, log storage).
- Long-term log retention / search (this system stores **incidents**, not every log line).
- Multi-tenant SaaS control plane.
- Exactly-once notification guarantees across external APIs (best-effort with retries + audit).
- Multi-engine portability (Flink / Spark / etc.) — **Quix only**.

---

## 2. Key architecture decisions

These choices were made explicitly after comparing alternatives. They are the “why” behind the current code shape.

### 2.1 Decision: Quix Streams as the only pipeline runtime (not Flink)

| | |
| --- | --- |
| **Status** | **Accepted** |
| **Decision** | Use **Quix Streams** as the sole Kafka stream runtime. **Apache Flink / PyFlink support was removed.** |
| **Context** | Early designs treated Quix and Flink as swappable “message movers” around a portable `AlertProcessor`. Flink added a second image (JDK + connector JARs), optional extras, and parallelism/state footguns. |

#### Options considered

| Option | Summary |
| --- | --- |
| **A. Quix only (chosen)** | Python library, consumer-group deploy, `group_by` + per-key state for co-located fingerprints |
| **B. PyFlink only** | True `keyBy` + distributed state; requires TaskManager/JobManager-style ops and a heavy image |
| **C. Dual Quix + Flink** | Same business logic behind adapters; two ops models, two state stories, continuous drift risk |
| **D. Plain `confluent-kafka` consumer** | Least framework; no built-in `group_by` / state store / changelog |

#### Why Quix over Flink

| Criterion | Quix | Flink (PyFlink) | Winner for this product |
| --- | --- | --- | --- |
| **Team skill / language** | Pure Python app mental model | Python API on a **JVM** engine | **Quix** — Python-native ownership |
| **Ops surface** | Process + Kafka consumer group | Cluster or mini-cluster, checkpoints, savepoints, JARs | **Quix** — no JobManager/TaskManager |
| **Local / Compose DX** | One Dockerfile, fast rebuild | Separate Flink image, version pins, ARM/Python pain | **Quix** |
| **Keyed co-location** | `group_by(fingerprint)` + stateful apply + repartition/changelog topics | First-class `keyBy` + ValueState | **Flink stronger in theory**; **Quix enough** for alert windows |
| **Fit to problem** | “Smart Kafka consumer with state” | General-purpose distributed stream processor | **Quix** — we need collapse-and-notify, not CEP/SQL at scale |
| **Image size / supply chain** | Python deps only | JDK + Flink + connectors | **Quix** |
| **Risk of half-supported dual path** | Single path to maintain | Dual runtime always lags | **Quix-only** avoids “works on Quix, broken on Flink” |

#### Why not “keep Flink optional forever”

- Optional engines that are not the default **rot**: docs, CI, and mental models diverge.
- Flink’s value (massive parallelism, event-time, large state backends) is **unused** by this product’s requirements.
- Supporting `FLINK_PARALLELISM>1` without engine-native keyed state caused **silent duplicate incidents** — a correctness trap we no longer want in the codebase.

#### Consequences

- `PIPELINE_RUNTIME=flink` is rejected at config/runtime selection.
- `Dockerfile.flink` and `flink_runtime.py` are gone.
- Scaling model is **Kafka partitions + Quix consumer group + `group_by` co-location**, not Flink task slots.

---

### 2.2 Decision: Dedup lives in Quix keyed state (not Redis)

| | |
| --- | --- |
| **Status** | **Accepted** |
| **Decision** | Active-window dedup (fingerprint → open incident, suppress, refire) runs in **Quix per-key state** after `group_by(fingerprint)`. **Redis is not used for dedup.** |
| **Context** | Multi-worker safety requires co-located state for a given fingerprint. Redis was evaluated as a shared external window store. |

#### Options considered

| Option | Summary |
| --- | --- |
| **A. Quix keyed state (chosen)** | `group_by` co-locates by fingerprint; `stateful=True` apply holds window/refire fields; changelog topics for recovery |
| **B. Redis keys `dedup:fp:{hash}` + TTL** | Any worker can process any partition; shared suppress/emit decisions via Redis locks |
| **C. Postgres-only decision** | Every event (or every emit candidate) uses a transaction + active-fingerprint unique index |
| **D. In-process memory only** | Simple and wrong under multi-consumer / restart without co-location |

#### Why Quix state over Redis for dedup

| Criterion | Quix keyed state | Redis dedup | Winner |
| --- | --- | --- | --- |
| **Co-location with stream keys** | Natural: same key as `group_by` | External map; must still get events to workers | **Quix** — one co-location story |
| **Moving parts** | State store + repartition/changelog topics (owned by Quix/Kafka) | Extra dependency on critical write path | **Quix** for pipeline critical path |
| **Separation of concerns** | Stream runtime owns stream windows | Cache product owns incident windows | **Quix** — Redis stays a **read-cache** tool |
| **Failure coupling** | Pipeline fails/restarts with Kafka/state | Redis outage could block or corrupt emit policy if used for dedup | **Quix** — Redis outage should **not** stop correct dedup |
| **Semantics** | Window/refire next to consume path | TTL keys good for “forget after N seconds” but easy to diverge from DB `alert_id` | **Quix** keeps window next to processing |
| **Multi-instance correctness** | Same fingerprint → same key → same state partition | Works if all workers use Redis correctly | Both viable; **Quix chosen for co-location + fewer systems on the hot path** |

#### Why Redis is still in the architecture

Redis **is** retained for a **different** job:

| Use | Technology |
| --- | --- |
| Active incident window / suppress / refire | **Quix state** |
| Operator UI list/stats under multi-instance poll | **Redis snapshot cache** (TTL, stampede lock, invalidate on write) |
| Durable incidents, ack/resolve, TTA/TTR, audit | **Postgres** |

This avoids a dual-brain problem: Redis must not disagree with Quix about “is this fingerprint active?” while the UI still benefits from a shared read cache.

#### Trade-offs we accept

- Quix creates internal **`repartition__*`** and **changelog** topics (Admin API; broker auto-create can stay off).
- State recovery follows Quix/Kafka-streams-style semantics, not “any process + shared Redis GET.”
- Horizontal scale still depends on **partition count** and key distribution of fingerprints after `group_by`.
- Postgres remains the **operator SoR**; Quix state is the **live processing window**, not the dashboard of record.

#### Alternatives we may revisit later

| If this becomes true… | Consider… |
| --- | --- |
| Extreme ERROR flood makes state+DB emit too costly | Postgres-only or hybrid “count in PG” for accuracy under load |
| Org standardizes on Flink platform | Reintroduce Flink **as the only** engine with keyed state (not dual) |
| Need multi-runtime portable processor again | External store (Postgres preferred over Redis for truth) |

---

### 2.3 Decision summary (one view)

```
                    ┌──────────────────────────────────────┐
                    │           Decision stack               │
                    ├──────────────────────────────────────┤
                    │  Kafka transport     →  Quix           │
                    │  Fingerprint co-loc  →  group_by       │
                    │  Window / refire     →  Quix State     │
                    │  Incident truth      →  Postgres       │
                    │  UI poll performance →  Redis cache    │
                    │  Notifications      →  pluggable HTTP │
                    │  Flink               →  removed        │
                    │  Redis for dedup     →  rejected       │
                    └──────────────────────────────────────┘
```

---

## 3. System context

```
                    ┌──────────────────────────────────────────────────┐
                    │                  This system                      │
  Producers         │                                                  │
  (apps, agents,    │   Kafka          Quix pipeline      Postgres     │
   log shippers) ──►│   logs ────────► group_by+state ──► alerts       │
                    │   logs-dlq         │                 dispatch_log │
                    │                    │                 widgets      │
                    │                    ▼                              │
                    │              Dispatchers ──► Zenduty / Teams /    │
                    │                              webhooks             │
                    │                    │                              │
                    │                    ▼                              │
                    │              Operator UI ◄── Redis (UI cache)     │
                    │              (FastAPI :8000)                       │
                    └──────────────────────────────────────────────────┘
```

**External actors**

| Actor | Interaction |
| --- | --- |
| Application / platform logging | Publishes structured log events to Kafka `logs` |
| On-call / SRE | Uses UI to acknowledge, resolve, filter by labels |
| Incident tools (Zenduty, Teams, custom webhooks) | Receive JSON payloads on new / refired incidents |
| Operators | Configure windows/fields via `config/alerts.yaml` and env |

---

## 4. Architecture overview

### 4.1 Layered view

```
┌─────────────────────────────────────────────────────────────────┐
│  Presentation                                                     │
│  UI static assets + FastAPI REST (list, stats, ack/resolve,      │
│  widgets, demo fire)                                              │
└───────────────────────────────┬─────────────────────────────────┘
                                │ reads via AlertReadCache (Redis)
┌───────────────────────────────▼─────────────────────────────────┐
│  Application services                                             │
│  • Quix runtime: enrich → group_by → state dedup → emit           │
│  • AlertProcessor.emit_alert (Postgres + dispatch + UI invalidate)│
│  • DispatchFanout + pluggable AlertDispatcher implementations     │
│  • AlertRepository (CRUD + upsert + audit)                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Domain                                                           │
│  • Fingerprint (configurable fields)                              │
│  • quix_state window/refire transitions                           │
│  • AlertEvent / LogEvent schemas, severity ranking                │
│  • YAML alert config (defaults ← service ← error_code)            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Infrastructure                                                   │
│  • Quix Streams + Kafka (ingress, repartition, changelog, DLQ)    │
│  • PostgreSQL (+ Alembic)                                         │
│  • Redis (UI snapshot only)                                       │
│  • httpx + tenacity (outbound dispatch)                           │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 End-to-end processing path

```
Kafka(logs)
  → safe JSON deserialize (bad → logs-dlq)
  → min-level filter (YAML / env)
  → build fingerprint + enrichment row
  → group_by(fingerprint)          # co-locate same incident key
  → stateful process_enriched_with_state()
        new | suppress | refire update
  → on emit: AlertProcessor.emit_alert()
        upsert Postgres
        invalidate UI Redis snapshot (optional)
        fan-out dispatchers (+ dispatch_log)
```

### 4.3 Process topology (Compose reference)

| Service | Role | Stateful? |
| --- | --- | --- |
| `kafka` | Log ingress | Offsets, topics |
| `kafka-init` | Creates `logs` + `logs-dlq` | No |
| `postgres` | SoR for alerts, dispatch_log, widgets | Yes (volume) |
| `redis` | **UI read cache only** | Ephemeral OK |
| `alert-pipeline` | Quix app (state dir + Kafka state topics) | Yes (Quix state / changelog) |
| `alert-ui` | FastAPI + static UI | Stateless (PG/Redis) |
| `webhook-debug` | Optional echo sink | No |
| `log-producer` (profile `demo`) | Synthetic traffic | No |

---

## 5. Major components

### 5.1 Ingress contract (Kafka)

- **Topic:** `logs` (configurable via `KAFKA_INPUT_TOPIC`).
- **DLQ:** `logs-dlq` for unparseable payloads (when enabled).
- **Internal (Quix):** `repartition__*` and `changelog__*` topics created by the app for `group_by` / state.
- **Consumer group:** `alert-pipeline`.
- **Semantic model:** at-least-once from Kafka; pipeline must tolerate reprocessing.

**Canonical log fields** (see `LogEvent`): `timestamp`, `level`, `service`, `host`, `message`, `error_code`, `trace_id`, `labels`.

### 5.2 Fingerprinting & configuration

**Fingerprint** = first 32 hex chars of SHA-256 over an ordered, normalized field list from YAML.

Default fields: `service`, `level`, `labels`, `message` (host **excluded** for fleet-wide collapse).

**Config merge:** `defaults ← services.<name> ← error_codes.<code>`.

### 5.3 Quix keyed-state dedup (`dedup/quix_state.py`)

After `group_by(fingerprint)`, each key holds an incident blob:

- `alert_id`, counts, `last_emitted_at`, severity, sample message, window, etc.

| Case | Behaviour |
| --- | --- |
| No state / window expired | Open **new** incident (`is_new=True`), store state |
| Within window, before refire | Increment count; **suppress** emit |
| Within window, refire due | Emit **updated** with same `alert_id` |
| After idle beyond window | Next event treated as new |

Unit tests cover transitions without Kafka (`tests/test_quix_dedup_state.py`). In-process `DedupEngine` remains for **unit tests only**, not multi-worker production.

### 5.4 AlertProcessor (persist + dispatch)

- **`emit_alert`**: path used by Quix after state decides to emit.
- **`handle_event`**: in-process engine for tests.
- Upsert never decreases `occurrence_count` when DB already has a higher active count.
- Partial unique index on active fingerprint reduces duplicate open rows under races.
- Optional UI cache invalidation after write (`ui_cache_invalidate.py`).

### 5.5 Persistence

| Table | Purpose |
| --- | --- |
| `alerts` | Incidents; active rows matched by fingerprint + status |
| `dispatch_log` | Per-channel attempt audit |
| `dashboard_widgets` | Shared UI filters |

Schema: **Alembic** (`alembic upgrade head` on entrypoint) + idempotent `create_all` fallback for tests/sqlite.

### 5.6 Dispatch

`AlertDispatcher` ABC → Zenduty / Teams / Webhook via `build_dispatchers()` + `DispatchFanout` + tenacity retries + `dispatch_log`.

### 5.7 Operator UI & Redis (read path only)

- Writes: Postgres, then cache invalidate.
- Reads: shared Redis snapshot (TTL, `SET NX` lock, memory fallback).
- **Does not implement pipeline dedup.**

---

## 6. Key sequences

### 6.1 New incident

```
Producer → Kafka(logs) → Quix enrich → group_by(fp)
  → state: empty → emit new AlertEvent
  → repo.upsert INSERT → dispatch fan-out → UI cache invalidate
```

### 6.2 Duplicate within window (suppressed)

```
… → state: active, refire not due → return None
  → no DB write, no dispatch
```

### 6.3 Refire

```
… → state: emit updated (same alert_id, count++)
  → repo.upsert UPDATE → maybe suppress if acknowledged → else dispatch
```

### 6.4 Operator acknowledge

```
UI POST status=acknowledged → Postgres TTA → cache.invalidate
  → later refires may skip notify (YAML suppress_dispatch_while_acknowledged)
```

---

## 7. Cross-cutting concerns

### 7.1 Configuration

| Layer | Examples |
| --- | --- |
| Env / `.env` / Compose | Kafka, DB, Redis (UI), dispatch flags |
| YAML | Dedup fields, windows, min level, overrides |
| Code defaults | Settings pydantic (sqlite for unit tests only) |

### 7.2 Security

- Compose requires `POSTGRES_*` / `DATABASE_URL` from `.env` (no silent secret defaults in compose for app DB URL).
- UI has **no auth** — assume network perimeter until auth is added.

### 7.3 Observability

- Process logs (new incident, suppress, dispatch ok/fail, `ALERT_DB_FETCH` on UI cache miss).
- No Prometheus exporter by default.

### 7.4 Failure modes

| Failure | Expected behaviour |
| --- | --- |
| Malformed Kafka message | Route to **DLQ** when enabled; do not invent incidents |
| Postgres down | Emit path fails; restart / alert on pipeline |
| Redis down | UI degrades (DB / local fallback); **pipeline dedup continues** (Quix state) |
| Downstream webhook 5xx | Retries then audit failure; incident remains |
| Pipeline restart | Quix state recovery via changelog/state dir; DB upsert protects counts |

---

## 8. Hardening status

### Implemented

| Item | Status |
| --- | --- |
| Quix-only runtime; Flink removed | Done |
| Quix `group_by` + keyed-state dedup | Done |
| Redis reserved for UI cache (not dedup) | Done |
| Partial unique index on active fingerprint | Done |
| Upsert never resets `occurrence_count` on stale `is_new` | Done |
| Broker auto-create topics disabled; Quix Admin creates repartition/changelog | Done |
| `logs-dlq` + unparseable routing | Done |
| Required DB env in compose; Alembic on boot | Done |
| Split `Dockerfile.pipeline` / `Dockerfile.ui` | Done |
| Webhook dispatch opt-in by default in compose | Done |
| Pipeline invalidates UI Redis snapshot on write | Done |

### Still open

| Issue | Direction |
| --- | --- |
| Sync dispatch on consume path | Outbox / async notify workers |
| UI unauthenticated | Gateway auth / OIDC |
| Exactly-once notifications | Idempotency keys per channel |

---

## 9. Deployment

### 9.1 Local / demo (Compose)

```bash
cp .env.example .env
docker compose up --build -d
# optional: docker compose --profile demo up -d
```

Single Kafka, Postgres, Redis (UI), pipeline, UI. Not HA.

### 9.2 Production sketch

| Concern | Recommendation |
| --- | --- |
| Kafka | Managed; explicit `logs` / `logs-dlq` + allow Quix internal topics |
| Pipeline | Replicas sized to partitions; Quix state/changelogs healthy |
| Postgres | Managed; Alembic in CI/CD |
| Redis | Managed **for UI cache only**; optional if single UI + acceptable DB load |
| UI | Auth proxy; scale horizontally with shared Redis |
| Images | `Dockerfile.pipeline` / `Dockerfile.ui`; pin digests |

---

## 10. Module map

```
src/alert_pipeline/
  main.py
  config.py
  alert_config.py
  schemas.py
  metrics.py
  sample_producer.py
  ui_cache_invalidate.py
  dedup/
    fingerprint.py
    quix_state.py       # production window/refire (Quix State)
    engine.py           # in-process (tests)
    store.py            # memory store helper (tests; redis-dedup deprecated)
  processing/
    handler.py          # emit_alert / test handle_event
  runtime/
    quix_runtime.py     # only stream runtime
    factory.py
  db/
    models.py, repository.py
  dispatchers/
  cache/
    alert_cache.py      # UI Redis
  ui/
config/alerts.yaml
alembic/
docs/HLD.md             # this document
tests/
```

---

## 11. Design principles

1. **One stream runtime (Quix)** — no dual Flink path to maintain.
2. **Dedup co-located with stream keys** — `group_by(fingerprint)` + Quix state, not an external cache.
3. **Postgres is operator truth** — ack/resolve, TTA/TTR, widgets, audit.
4. **Redis is a UI performance tool** — never required for emit/suppress correctness.
5. **Policy in YAML** — windows and fingerprint fields without code changes.
6. **Dispatch is pluggable and audited** — subclass + registry + `dispatch_log`.
7. **Demo defaults ≠ production secrets** — required env for DB credentials.

---

## 12. Open decisions (remaining)

| Topic | Options | Notes |
| --- | --- | --- |
| Notification reliability | Sync (today) vs outbox | Outbox reduces consumer stall |
| Auth for UI | None / reverse proxy / OIDC | Required off private networks |
| Ingress Kafka key | None vs pre-set fingerprint | `group_by` already re-keys; optional optimization |
| Postgres-assisted counts | Emit-only counts vs update DB every event | Accuracy vs write load |

**Closed decisions (see §2):** Quix-only runtime; no Redis dedup; no Flink.

---

*Last updated for Quix keyed-state dedup, Flink removal, and Redis-as-UI-cache-only. Keep this document aligned with `runtime/quix_runtime.py` and `dedup/quix_state.py`.*
