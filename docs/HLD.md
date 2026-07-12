# High-Level Design: Alert Deduplication Pipeline

| | |
| --- | --- |
| **Status** | Living document |
| **Version** | 0.4.1 |
| **Last updated** | 2026-07-12 |
| **Audience** | Engineers owning ingress, reliability, or on-call tooling |
| **Related** | [README](../README.md), [Runtime notes](../src/alert_pipeline/runtime/README.md), [alerts.yaml](../config/alerts.yaml) |

**Reading guide**

| Role | Start with |
| --- | --- |
| New engineer / on-call | §1–3, §5–6, §13 open items |
| Product / PM | §1, §3, §4 lifecycle |
| Platform / ops | §6 guarantees, §9–12 |
| Why Quix / not Redis for dedup | [Appendix A — ADRs](#appendix-a--architecture-decision-records) |

### Document changelog

| Version | Date | What changed |
| --- | --- | --- |
| 0.4.1 | 2026-07-12 | Align architecture diagrams with outbox worker; atomic upsert+enqueue; shared dedup transition |
| 0.4.0 | 2026-07-12 | Async outbox dispatch + worker; event-time windows; allow_reopen DB check; Prometheus `/metrics`; dual CI |
| 0.3.0 | 2026-07-08 | Restructure: current system first; data model, guarantees, scaling, security, ops signals; ADRs demoted to appendix |
| 0.2.0 | — | Quix keyed-state dedup; Flink removal; Redis as UI cache only |

**Keep aligned with:** `runtime/quix_runtime.py`, `dedup/quix_state.py`, `dedup/transition.py`, `processing/handler.py`, `dispatchers/outbox_worker.py`, `config/alerts.yaml`.

---

## 1. Purpose

Turn a high-volume stream of application **error logs** into a small number of durable **incidents**, then:

1. **Persist** them as the system of record (PostgreSQL).
2. **Notify** humans and systems (Zenduty, Teams, webhooks, …).
3. **Operate** them from a shared dashboard (ack / resolve, TTA/TTR, label widgets).

The pipeline is a **Python-native Quix Streams** application: Kafka consume → fingerprint co-location → keyed-state dedup → **Postgres upsert + outbox enqueue** (one transaction). A separate **`alert-dispatch-worker`** drains the outbox to Zenduty / Teams / webhooks. The operator UI is another FastAPI process with an optional **Redis read cache**.

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

### 1.3 Success metrics (product)

| Metric | Intent |
| --- | --- |
| Alert volume reduction | Many ERROR logs → few open incidents per fingerprint window |
| False-merge rate | Over-broad fingerprints hide distinct failures (tune `dedup_fields`) |
| MTTA / TTA, TTR | Operator speed; stored on incident rows when acked/resolved |
| Dispatch reliability | Successful channel attempts vs audited failures under load |
| Pipeline health | Consumer lag, DLQ growth, outbox backlog, emit-path DB errors |

---

## 2. Architecture at a glance

**Three** app processes share Postgres (and optionally Redis for the UI):

```
  Producers (apps, agents, log shippers)
           │
           ▼
     ┌───────────┐     ┌──────────────────────────────────────────┐
     │  Kafka    │     │  alert-pipeline (Quix Streams)             │
     │  logs     │────►│  parse → min-level → fingerprint           │
     │  logs-dlq │     │  group_by(fp) → keyed-state dedup          │
     └───────────┘     │  emit → Postgres upsert + outbox (1 txn)   │
                       │       → UI cache invalidate (best-effort)  │
                       └──────────────────┬───────────────────────┘
                                          │ write SoR + dispatch_outbox
                                          ▼
                       ┌──────────────────────────────────────────┐
                       │  PostgreSQL (system of record)             │
                       │  alerts · dispatch_outbox · dispatch_log   │
                       │  dashboard_widgets                         │
                       └─────┬──────────────────────────▲─────────┘
                             │ claim / mark sent        │ read / write
                             ▼                          │ (ack, resolve)
              ┌──────────────────────────┐   ┌──────────┴───────────────┐
              │  alert-dispatch-worker   │   │  alert-ui (FastAPI :8000)  │
              │  drain outbox → HTTP     │   │  list, stats, widgets      │
              └────────────┬─────────────┘   │  Redis: UI snapshot only   │
                           │                 └────────────────────────────┘
                           ▼
              ┌──────────────────────────┐
              │  Zenduty / Teams /       │
              │  webhooks (+ audit log)  │
              └──────────────────────────┘
```

**HTTP to notification channels never runs on the Quix sink path** (default `DISPATCH_MODE=outbox`). The pipeline only enqueues work; the worker performs side-effects.

### 2.1 Decision stack (one view)

```
  Kafka transport     →  Quix Streams
  Fingerprint co-loc  →  group_by(fingerprint)
  Window / refire     →  Quix per-key state (rules in transition.py)
  Incident truth      →  Postgres (alerts)
  Notify reliability  →  dispatch_outbox + alert-dispatch-worker
  Notify audit        →  dispatch_log + idempotency keys
  UI poll performance →  Redis snapshot cache
  Flink / Redis-dedup →  rejected (see Appendix A)
```

| Concern | Owner |
| --- | --- |
| Active window / suppress / refire | **Quix state** |
| Incidents, ack/resolve, TTA/TTR, widgets | **Postgres** |
| Pending notifications | **`dispatch_outbox`** (filled by pipeline, drained by worker) |
| Channel HTTP + retries | **`alert-dispatch-worker`** |
| Multi-instance UI list/stats | **Redis** (optional; not on emit correctness path) |

**Core idea:** Dedup co-locates with stream keys. Redis must never be the second brain for “is this fingerprint active?” Notifications are asynchronous and audited, not inline on consume.

---

## 3. System context

### 3.1 External actors

| Actor | Interaction |
| --- | --- |
| Application / platform logging | Publishes structured log events to Kafka `logs` |
| On-call / SRE | Uses UI to acknowledge, resolve, filter by labels |
| Incident tools (Zenduty, Teams, custom webhooks) | Receive JSON payloads on new / refired incidents |
| Operators | Configure windows/fields via `config/alerts.yaml` and env |

### 3.2 Process topology (Compose reference)

| Service | Role | Stateful? |
| --- | --- | --- |
| `kafka` | Log ingress | Offsets, topics |
| `kafka-init` | Creates `logs` + `logs-dlq` | No |
| `postgres` | SoR for alerts, dispatch_outbox, dispatch_log, widgets | Yes (volume) |
| `redis` | **UI read cache only** | Ephemeral OK |
| `alert-pipeline` | Quix app (state dir + Kafka state topics); enqueue only | Yes (Quix state / changelog) |
| `alert-dispatch-worker` | Drains `dispatch_outbox` → HTTP channels | Stateless (PG) |
| `alert-ui` | FastAPI + static UI + `/metrics` | Stateless (PG/Redis) |
| `webhook-debug` | Optional echo sink | No |
| `log-producer` (profile `demo`) | Synthetic traffic | No |

---

## 4. Data model and incident lifecycle

### 4.1 LogEvent (ingress) vs Alert / incident (SoR)

| Concept | Role | Durability |
| --- | --- | --- |
| **LogEvent** | One Kafka message after parse/normalize | Not retained by this system (except bad payloads on DLQ) |
| **Incident (`alerts` row)** | Deduplicated open/updated/acked/resolved case | **Postgres system of record** |
| **Quix state blob** | Live window for one fingerprint | Stream state + changelog; not the operator dashboard |

**What we do not store:** every suppressed log line, full message history, or a search index over raw logs.

### 4.2 Canonical LogEvent fields

See `LogEvent` in `schemas.py`. Typical Kafka JSON:

```json
{
  "timestamp": "2026-07-08T12:00:00Z",
  "level": "ERROR",
  "service": "payments-api",
  "host": "pod-7",
  "message": "connection refused to db primary",
  "error_code": "DB_CONN",
  "trace_id": "abc123",
  "labels": { "env": "prod", "region": "us-east" }
}
```

Aliases accepted at parse time include `severity`/`log_level`, `msg`/`error`, `app`/`application`, `@timestamp`/`time`, `hostname`/`pod`, etc.

### 4.3 Incident row (`alerts`)

| Field group | Examples |
| --- | --- |
| Identity | `id` (UUID), `fingerprint` |
| Content | `title`, `description`, `sample_message`, `severity`, `service`, `host` |
| Grouping context | `error_code`, `trace_id`, `labels_json` |
| Counts / time | `occurrence_count`, `first_seen`, `last_seen` |
| Operator timeline | `status`, `acknowledged_at`, `resolved_at`, `tta_seconds`, `ttr_seconds` |

Related tables:

| Table | Purpose |
| --- | --- |
| `dispatch_outbox` | Pending notification work (async worker); unique `idempotency_key` |
| `dispatch_log` | Per-channel attempt audit (success, status_code, error, optional idempotency_key) |
| `dashboard_widgets` | Shared UI label filters |

Schema: **Alembic** (`alembic upgrade head` on entrypoint) + idempotent `create_all` fallback for tests/sqlite.

### 4.4 Status lifecycle

**Active** statuses (dedup merges / upserts into these rows): `open`, `updated`, `acknowledged`.

```
                    ┌─────────────┐
         new emit → │    open     │
                    └──────┬──────┘
                           │ refire emit (same alert_id)
                           ▼
                    ┌─────────────┐
                    │  updated    │◄── further refires
                    └──────┬──────┘
                           │
              UI ack       │        UI resolve (may skip ack)
                           ▼
                    ┌─────────────┐         ┌─────────────┐
                    │acknowledged │────────►│  resolved   │
                    └─────────────┘  resolve└─────────────┘
                           │
                           │ optional: un-ack → open/updated path in UI
                           ▼
                    (status rewrite; TTA may be recomputed)
```

| Transition | Who | Notes |
| --- | --- | --- |
| → `open` | Pipeline | New fingerprint window / expired window |
| → `updated` | Pipeline | Refire within window (same `alert_id`) |
| → `acknowledged` | Operator UI | Sets `acknowledged_at`, computes **TTA** |
| → `resolved` | Operator UI | Sets `resolved_at`, computes **TTR** |
| Reopen after resolve | Policy | YAML `allow_reopen_after_resolve`; when **false**, emit path skips if only a **resolved** row exists for the fingerprint (no new incident). When **true**, a new row may be opened (new UUID if Quix reuses a resolved PK). |

Partial unique index on **active** fingerprint reduces duplicate open rows under races. Upsert **never decreases** `occurrence_count` when the DB already holds a higher active count.

### 4.5 Fingerprint

**Fingerprint** = first 32 hex chars of SHA-256 over an ordered, normalized field list from YAML.

Default fields: `service`, `level`, `labels`, `message` (**host excluded** for fleet-wide collapse).

**User control** is field selection only (`dedup_fields` under defaults / services / error_codes). Built-in message normalization (not YAML-configurable) strips UUIDs, long hex, ISO timestamps, and request/trace id tokens. Changing `dedup_fields` produces **new** fingerprint strings → new incident groups (existing open rows are not rewritten).

---

## 5. Components and processing path

### 5.1 Layered view (three processes)

```
┌─────────────────────────────────────────────────────────────────┐
│  Presentation (alert-ui)                                          │
│  Static assets + FastAPI REST (list, stats, ack/resolve,         │
│  widgets, demo fire) · GET /metrics                               │
└───────────────────────────────┬─────────────────────────────────┘
                                │ reads via AlertReadCache (Redis)
┌───────────────────────────────▼─────────────────────────────────┐
│  Application services (three deployables)                         │
│  • alert-pipeline (Quix): enrich → group_by → state → emit        │
│  • AlertProcessor.emit_alert: reopen policy → upsert+outbox txn   │
│    → UI cache invalidate (after commit)                           │
│  • alert-dispatch-worker: claim outbox → DispatchFanout → HTTP    │
│  • AlertRepository (CRUD + upsert + outbox + audit)               │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Domain                                                           │
│  • Fingerprint (configurable fields)                              │
│  • transition.apply_dedup_transition (shared rules)               │
│  • quix_state / DedupEngine adapters                              │
│  • AlertEvent / LogEvent schemas, severity ranking                │
│  • YAML alert config (defaults ← service ← error_code)            │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│  Infrastructure                                                   │
│  • Quix Streams + Kafka (ingress, repartition, changelog, DLQ)    │
│  • PostgreSQL (+ Alembic): alerts, dispatch_outbox, dispatch_log  │
│  • Redis (UI snapshot only)                                       │
│  • httpx + tenacity (outbound dispatch on worker only)            │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 End-to-end processing path

```
Kafka(logs)
  → safe JSON deserialize (bad → logs-dlq)
  → min-level filter (YAML / env)
  → build fingerprint + enrichment row
  → group_by(fingerprint)          # co-locate same incident key
  → stateful process_enriched_with_state()
        (apply_dedup_transition → new | suppress | update)
  → on emit: AlertProcessor.emit_alert()
        1. allow_reopen policy check (DB status)
        2. ONE transaction: upsert alerts + enqueue dispatch_outbox
           (skip enqueue if refire + acked + suppress_dispatch_while_acknowledged)
        3. invalidate UI Redis snapshot (optional, after commit)
  → alert-dispatch-worker
        claim outbox (SKIP LOCKED + CAS) → HTTP channel → dispatch_log
        → sent | failed (backoff) | dead
```

### 5.3 Ingress contract (Kafka)

| Item | Value |
| --- | --- |
| Input topic | `logs` (`KAFKA_INPUT_TOPIC`) |
| DLQ | `logs-dlq` for unparseable payloads (when enabled) |
| Internal (Quix) | `repartition__*` and `changelog__*` for `group_by` / state |
| Consumer group | `alert-pipeline` |
| Delivery from Kafka | **At-least-once**; pipeline must tolerate reprocessing |

Broker auto-create can stay **off**; Quix uses the Admin API for internal topics (`auto_create_topics` for those).

**DLQ policy (current):** unparseable messages are routed when enabled; **no built-in redrive consumer**. Operators should alert on DLQ depth/rate and inspect payloads manually or with a separate redrive tool.

### 5.4 Quix keyed-state dedup

After `group_by(fingerprint)`, each key holds an incident blob: `alert_id`, counts, `last_emitted_at`, severity, sample message, window, etc.

| Case | Behaviour |
| --- | --- |
| No state / window expired | Open **new** incident (`is_new=True`), store state |
| Within window, before refire | Increment count; **suppress** emit |
| Within window, refire due | Emit **updated** with same `alert_id` |
| After idle beyond window | Next event treated as new |

**Time model:** window and refire use **event time** (`event.timestamp`). Far-future timestamps are clamped to wall-clock (±5 min skew). Injectable `now` remains for unit tests. There is still **no full watermark / late-data side output** — very late events can reopen windows relative to their own timestamps.

Unit tests: `tests/test_quix_dedup_state.py`, `tests/test_event_time.py`, `tests/test_dedup_parity.py`.  
**Single rule core:** `dedup/transition.py` (`apply_dedup_transition`) is shared by Quix (`quix_state.py`) and in-process `DedupEngine` so window/refire semantics cannot drift. Memory store remains for **unit tests only**, not multi-worker production.

### 5.5 AlertProcessor (persist + outbox)

- **`emit_alert`**: production path after Quix state decides to emit.
- **`handle_event`**: in-process engine for tests only.
- Order on emit (outbox mode):
  1. Reopen policy (optional skip).
  2. **`upsert_and_maybe_enqueue`** — one Postgres transaction (incident + outbox).
  3. UI cache invalidate (best-effort, **after** commit).
- `DISPATCH_MODE=inline` (tests / simple demos only): upsert alone, then sync fan-out HTTP on the emit path (not recommended for production).

### 5.6 Dispatch (outbox + worker)

Default **`DISPATCH_MODE=outbox`**:

```
  alert-pipeline                         alert-dispatch-worker
  ─────────────                          ─────────────────────
  emit_alert                             loop:
    └─ txn:                              1. claim due rows
         INSERT/UPDATE alerts              (FOR UPDATE SKIP LOCKED + CAS)
         INSERT dispatch_outbox          2. HTTP via DispatchFanout
           (per channel)                 3. write dispatch_log
    └─ commit                            4. mark sent | failed | dead
    └─ invalidate Redis (optional)
```

1. Emit path inserts one `dispatch_outbox` row per enabled channel with  
   `idempotency_key = {alert_id}:{channel}:{occurrence_count}` (unique), **in the same transaction as the alert upsert**.
2. **`alert-dispatch-worker`** claims due rows (`pending`/`failed`) multi-worker-safely (`FOR UPDATE SKIP LOCKED` on Postgres + compare-and-swap status transition), calls the channel, writes `dispatch_log`, marks `sent` / retries with backoff / `dead` after max attempts.
3. Reprocessing that re-enqueues the same key is a no-op; worker also skips if audit already has a successful row for that key.

Channel HTTP uses **tenacity** inside each dispatcher. Payload: `AlertEvent.to_dispatch_dict()`.

### 5.7 Operator UI and Redis (read path only)

| Path | Behaviour |
| --- | --- |
| Writes (ack/resolve/widgets) | Postgres, then cache invalidate |
| Pipeline emit | Postgres, then optional invalidate |
| Reads | Shared Redis snapshot (TTL, `SET NX` stampede lock, memory/DB fallback) |

**Does not implement pipeline dedup.** Multi-instance UI: after invalidate, instances converge on the next cache miss (eventual consistency for list/stats; operators should treat SoR as Postgres).

---

## 6. Correctness, delivery, and failure modes

### 6.1 Guarantees

| Property | Guarantee | Mechanism | Known holes |
| --- | --- | --- | --- |
| Kafka consume | At-least-once | Consumer offsets / Quix commit semantics | Reprocess can re-enter state machine |
| Per-fingerprint co-location | Same fp → same state partition after `group_by` | Quix repartition | Hot keys pin one worker |
| Global event order | **None** | — | Only per-key ordering after co-location |
| Suppress within window | Best-effort in live state | Quix keyed state | Restart/reprocess edge cases; not a global lock service |
| Incident durability | Strong once upsert commits | Postgres | State may advance before upsert if process dies mid-pipeline (engine-dependent) |
| `occurrence_count` | Non-decreasing on active upsert | Repository logic | Suppressed events never hit DB → counts lag true log volume |
| Active fingerprint uniqueness | Soft guarantee | Partial unique index + upsert retry | Races resolved to one active row |
| Notifications | **At-least-once / best-effort** with per-channel idempotency keys | Outbox unique key + audit skip | Worker crash mid-send can still duplicate if channel is not idempotent |
| Exactly-once notify | **Not provided** end-to-end | — | External APIs may not honor our keys |
| UI cache freshness | Best-effort | Invalidate on write + TTL | Brief staleness multi-instance |

### 6.2 Emit-path ordering and partial failure

Intended order in `emit_alert` (outbox mode):

1. **Reopen policy** (optional skip).
2. **One Postgres transaction:** upsert incident **and** enqueue outbox rows
   (`AlertRepository.upsert_and_maybe_enqueue`). Ack-suppress is decided after
   upsert inside the same txn so refires on acknowledged incidents skip enqueue.
3. **Invalidate UI cache** (best-effort; only after commit; failure should not block).

| Crash / failure point | Likely outcome |
| --- | --- |
| After state emit decision, before commit | May reprocess; state may re-emit; outbox key still unique after successful commit |
| Mid-transaction (upsert+enqueue) | Full rollback — no orphan incident without outbox rows |
| Worker fails HTTP | Row retries with backoff then `dead`; check outbox + `dispatch_log` |
| Redis down on invalidate | UI may show stale list until TTL/miss; **dedup unaffected** |

**Capacity:** HTTP is **off** the Quix sink path in outbox mode. Scale workers horizontally; watch `alert_pipeline_outbox_pending`.

### 6.3 Key sequences

#### New incident

```
Producer → Kafka(logs) → Quix enrich → group_by(fp)
  → state: empty → emit new AlertEvent
  → repo.upsert+outbox (one txn) → UI cache invalidate
  → alert-dispatch-worker → channels
```

#### Duplicate within window (suppressed)

```
… → state: active, refire not due → return None
  → no DB write, no dispatch
```

#### Refire

```
… → state: emit updated (same alert_id, count++)
  → repo.upsert+outbox (one txn); skip enqueue if acknowledged + suppress
  → UI cache invalidate
  → worker → channels (if enqueued)
```

#### Operator acknowledge

```
UI POST status=acknowledged → Postgres TTA → cache.invalidate
  → later refires may skip notify (YAML suppress_dispatch_while_acknowledged)
```

#### Resolve (and reopen policy)

```
UI POST status=resolved → Postgres TTR → cache.invalidate
  → active unique index no longer covers row
  → further matching logs: new open incident if Quix window expired / new state;
    YAML allow_reopen_after_resolve governs product expectation — verify in ops runbooks
```

#### Pipeline restart

```
Quix recovers per-key state via changelog / state dir
  → DB upsert protects counts on re-emit
  → downstream may see duplicate notifications (at-least-once)
```

#### UI multi-instance after write

```
Instance A writes Postgres + invalidate
  → Instance B cache miss (or TTL) → rebuild snapshot from DB
  → brief window where B may serve pre-invalidate snapshot
```

### 6.4 Failure modes summary

| Failure | Expected behaviour |
| --- | --- |
| Malformed Kafka message | Route to **DLQ** when enabled; do not invent incidents |
| Postgres down | Emit path fails; page on pipeline health / lag |
| Redis down | UI degrades (DB / local fallback); **pipeline dedup continues** |
| Downstream webhook 5xx | Retries then audit failure; incident remains |
| Pipeline restart | State recovery; upsert protects counts; possible re-notify |
| Extreme ERROR flood | Outbox backlog + DB emit pressure; lag grows (see §11). Scale workers; HTTP is off the Quix path |

---

## 7. Configuration and policy

### 7.1 Layers

| Layer | Examples | Notes |
| --- | --- | --- |
| Env / `.env` / Compose | Kafka, `DATABASE_URL`, Redis UI URL, dispatch enable flags, secrets | Process restart typically required |
| YAML (`config/alerts.yaml`) | Dedup fields, windows, min level, per-service / error_code overrides | Path via `ALERT_CONFIG_PATH`; treat reload as restart-safe unless explicitly hot-reloaded |
| Code defaults | Pydantic `Settings` (sqlite only for unit tests) | Never rely on demo defaults for prod secrets |

### 7.2 YAML merge order

```
defaults  ←  services.<name>  ←  error_codes.<code>
```

Later layers override earlier for the same keys.

### 7.3 `dedup_fields`

Supported values (see comments in `alerts.yaml` / `KNOWN_FIELDS`):

| Field | Meaning |
| --- | --- |
| `service`, `level` / `severity`, `message`, `labels` | Common defaults |
| `error_code`, `host`, `trace_id` | Optional tightening / splitting |
| `label:<name>` or `labels.<name>` | Single label key (e.g. `label:env`) |

Only listed fields participate. **Host is not in the default set** (fleet-wide collapse). Add `host` when per-node incidents are required.

**Changing fingerprint policy in production:** expect a burst of **new** incidents (new hashes). Prefer deliberate rollouts; do not assume old open rows merge into new fingerprints.

### 7.4 Behavioural defaults (illustrative)

From `config/alerts.yaml` (may change — treat file as source of truth):

| Setting | Typical default |
| --- | --- |
| `min_level` | `ERROR` |
| `dedup_window_seconds` | `300` |
| `refire_interval_seconds` | `60` |
| `suppress_dispatch_while_acknowledged` | `true` |
| `allow_reopen_after_resolve` | `true` |

Env can also influence min-level / window settings via `Settings` for tests and overrides — when both env and YAML apply, **document the effective precedence in deploy config** and prefer a single source in production.

---

## 8. Security and trust assumptions

| Area | Current stance |
| --- | --- |
| UI authentication | **None** — assume private network / gateway until OIDC (e.g. Keycloak) or reverse-proxy auth is added |
| Threat model (today) | Trusted operators on a perimeter-controlled network; not multi-tenant internet SaaS |
| Demo endpoints | Process-local rate limits on `/api/demo/*` (`DEMO_RATE_LIMIT_PER_MINUTE`) |
| DB credentials | Compose requires `POSTGRES_*` / `DATABASE_URL` from `.env` (no silent production secrets in compose) |
| Dispatch secrets | Zenduty/Teams/webhook URLs and tokens via env; rotate by redeploying worker with new env |
| Network trust | Pipeline can reach Kafka, Postgres, Redis (UI); **worker** needs egress to notification endpoints |
| Multi-instance UI | Shared Redis + Postgres; no per-user isolation |

**Before public or shared-network exposure:** add auth at the gateway (or app), restrict webhook egress, and audit who can resolve/ack.

---

## 9. Observability and operational signals

### 9.1 Two kinds of “metrics”

| Kind | What | Where |
| --- | --- | --- |
| **Operator SLIs** | TTA / TTR, open counts, severity mix | Postgres fields + UI stats |
| **Pipeline SRE signals** | Emits, skips, outbox enqueue/process, dispatch success/fail | Prometheus text on UI **`GET /metrics`** (`observability.py`) |

### 9.2 Logging (current)

Process logs for: new incident, suppress (debug), outbox enqueue, dispatch ok/fail, UI cache miss markers (e.g. `ALERT_DB_FETCH`).

### 9.3 Recommended alerts on the pipeline itself

| Signal | Why |
| --- | --- |
| Consumer lag / stall | Postgres outage or Quix state issues (HTTP no longer on hot path in outbox mode) |
| `alert_pipeline_outbox_pending` growth | Worker down or channel outage |
| DLQ message rate / depth | Bad producers or schema drift |
| Emit / upsert errors | SoR unavailable |
| Dispatch failure / dead outbox rate | On-call noise or silent miss depending on channel |
| Pipeline / worker restart loops | State/config/image issues |

### 9.4 Testing strategy (design contracts)

| Layer | What it locks |
| --- | --- |
| Unit: `tests/test_quix_dedup_state.py`, `test_event_time.py` | Window / refire / event-time |
| Unit: `tests/test_outbox.py` | Enqueue, worker drain, idempotency, reopen policy |
| Unit: fingerprint / config / dispatchers / repository | Policy and persistence edges |
| CI | GitHub Actions + GitLab CI (lint, pytest, alembic, docker build) |
| Compose demo | End-to-end smoke (not full HA) |
| Not fully automated | Chaos restart mid-dispatch, multi-replica partition rebalance under flood |

---

## 10. Scaling and capacity

| Factor | Guidance |
| --- | --- |
| Horizontal scale | Pipeline **replicas should not exceed** co-location topic partition count after `group_by` (plus Kafka input partitions for the pre-`group_by` stage) |
| Hot keys | One noisy fingerprint co-locates on one partition/worker — refire and DB emit concentrate there |
| Key distribution | Fingerprint cardinality drives balance; very low-cardinality fields (e.g. only `service`) worsen skew |
| Emit cost | Each emit = DB write + N outbox rows; HTTP cost moves to workers |
| UI scale | Horizontal UI instances + shared Redis; single UI can run without Redis at higher DB load |
| When to redesign | Extreme ERROR floods where Quix state + DB upsert cannot keep lag bounded → coarser emit policy / more partitions |

**Local Compose is not HA.** Production should size partitions for expected fingerprint parallelism and watch lag under load tests.

### Quix state / changelog ops (runbook notes)

| Topic | Guidance |
| --- | --- |
| Changelog / repartition topics | Created by Quix Admin API; set **compaction** on changelogs and retention aligned with max recovery needs |
| State size | Grows with active fingerprints in-window; expired keys drop on next event for that key — watch partition disk |
| Recovery | Rebuild state from changelog + replay; **Postgres remains SoR** for operators |
| Multi-replica | Correctness relies on Kafka partitioning after `group_by`; do not run more pipeline replicas than useful partitions |
| Postgres | Prefer connection pooling (managed PG or pgbouncer); archive/resolve history for large tables over time |

---

## 11. Deployment

### 11.1 Local / demo (Compose)

```bash
cp .env.example .env
docker compose up --build -d
# optional: docker compose --profile demo up -d
```

Single Kafka, Postgres, Redis (UI), pipeline, UI. Not HA.

### 11.2 Production sketch

| Concern | Recommendation |
| --- | --- |
| Kafka | Managed; explicit `logs` / `logs-dlq`; allow Quix internal topics; set retention/compaction for DLQ/changelogs intentionally |
| Pipeline | Replicas sized to partitions; healthy Quix state/changelogs; pin image digests (`Dockerfile.pipeline`) |
| Dispatch worker | One or more `alert-dispatch-worker` processes; scale with outbox backlog |
| Postgres | Managed SoR; Alembic in CI/CD; **backup/restore is mandatory** (incidents are not fully rebuildable from short log retention alone) |
| Quix state | Rebuildable in principle from replay + policy, but **not** a substitute for Postgres backups |
| Redis | Managed **for UI cache only**; optional if single UI + acceptable DB load |
| UI | Auth proxy (Keycloak/OIDC deferred); scale horizontally with shared Redis (`Dockerfile.ui`) |
| HA expectation | Multi-replica pipeline only with correct partition/state story; do not assume active-active without load testing |

---

## 12. Hardening status

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
| Async `dispatch_outbox` + `alert-dispatch-worker` | Done |
| Per-channel idempotency keys | Done (external APIs may still duplicate) |
| Event-time windows / refire | Done |
| `allow_reopen_after_resolve` consults DB | Done |
| Prometheus `/metrics` on UI | Done |
| GitHub Actions + GitLab CI | Done |

### Still open / roadmap

| Issue | Direction |
| --- | --- |
| UI unauthenticated | Gateway auth / Keycloak OIDC (deferred) |
| Exactly-once notify end-to-end | Channel-side idempotency support |
| Full watermark / late data side outputs | Advanced stream time semantics |
| DLQ redrive | Tooling + ownership |
| Incremental UI cache / no hard 2000 cap | Cache redesign |

---

## 13. Open decisions

| Topic | Options | Notes |
| --- | --- | --- |
| Auth for UI | None (today) / reverse proxy / Keycloak OIDC | Required off private networks; Keycloak deferred |
| Ingress Kafka key | None vs pre-set fingerprint | `group_by` already re-keys; optional optimization |
| Postgres-assisted counts | Emit-only counts vs update DB every event | Accuracy vs write load |

**Closed decisions (see Appendix A):** Quix-only runtime; no Redis dedup; no Flink dual path; **outbox dispatch**; **event-time windows**.

---

## 14. Design principles

1. **One stream runtime (Quix)** — no dual Flink path to maintain.
2. **Dedup co-located with stream keys** — `group_by(fingerprint)` + Quix state, not an external cache.
3. **Postgres is operator truth** — ack/resolve, TTA/TTR, widgets, audit.
4. **Redis is a UI performance tool** — never required for emit/suppress correctness.
5. **Policy in YAML** — windows and fingerprint fields without code changes.
6. **Dispatch is pluggable, async, and audited** — outbox + worker + `dispatch_log` + idempotency keys.
7. **Demo defaults ≠ production secrets** — required env for DB credentials.
8. **At-least-once is honest** — design for idempotent upserts and audited notify, not false exactly-once claims.
9. **Event-time for windows** — log timestamps drive suppress/refire; clamp far-future skew.

---

## Appendix A — Architecture decision records

These choices explain the “why” behind the current shape. They are **accepted** historical decisions, not the primary onboarding path.

### A.1 Decision: Quix Streams as the only pipeline runtime (not Flink)

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
| **Fit to problem** | “Smart Kafka consumer with state” | General-purpose distributed stream processor | **Quix** — collapse-and-notify, not CEP/SQL at scale |
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

### A.2 Decision: Dedup lives in Quix keyed state (not Redis)

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

## Appendix B — Module map

```
src/alert_pipeline/
  main.py
  config.py                 # env / Settings
  alert_config.py           # YAML load + merge
  schemas.py                # LogEvent, AlertEvent, statuses
  metrics.py                # TTA/TTR helpers (not Prometheus)
  sample_producer.py
  ui_cache_invalidate.py
  dedup/
    fingerprint.py
    quix_state.py           # production window/refire (Quix State)
    engine.py               # in-process DedupEngine — tests only
    store.py                # memory store helper — tests only
                            # (Redis-as-dedup path removed / rejected)
  processing/
    handler.py              # emit_alert / test handle_event
  runtime/
    quix_runtime.py         # only stream runtime
    factory.py
  db/
    models.py, repository.py
  dispatchers/              # Zenduty, Teams, webhook + registry
  cache/
    alert_cache.py          # UI Redis snapshot
  ui/                       # FastAPI app + static assets
config/alerts.yaml
alembic/
docs/HLD.md                 # this document
docs/images/                # e.g. operator-ui screenshot assets
tests/                      # includes test_quix_dedup_state.py as state contract
```

---

## Appendix C — Glossary

| Term | Meaning |
| --- | --- |
| **Fingerprint** | Stable hash key grouping “same” errors for one window |
| **Incident / alert row** | Operator-visible SoR entity in Postgres |
| **Window** | Idle timeout after which state treats the next event as new |
| **Refire** | Periodic re-emit/update while the window stays active |
| **SoR** | System of record (Postgres for operators) |
| **DLQ** | Dead-letter queue for unparseable ingress |
| **TTA / TTR** | Time to acknowledge / time to resolve (operator SLIs) |

---

*Version 0.3.0 — current-system-first HLD with guarantees, lifecycle, scaling, and ADRs in the appendix. Update this document when runtime, state, or SoR semantics change.*
