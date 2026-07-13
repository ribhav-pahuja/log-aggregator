"""FastAPI dashboard: reads from shared Redis TTL cache; writes go to Postgres."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.alert_config import get_alert_config
from alert_pipeline.cache.alert_cache import AlertReadCache
from alert_pipeline.config import get_settings
from alert_pipeline.db.models import AlertRecord, WidgetRecord
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.schemas import (
    AlertEvent,
    AlertOut,
    AlertStatus,
    AlertView,
    DispatchOut,
    LogEvent,
    LogLevel,
    StatsOut,
    StatsView,
)

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"

AlertStatusLiteral = Literal["open", "updated", "acknowledged", "resolved"]


class StatusBody(BaseModel):
    status: AlertStatusLiteral


class DemoFireBody(BaseModel):
    service: str = "demo-service"
    message: str = "demo failure from UI"
    severity: str = "ERROR"
    host: str = "ui-demo"
    error_code: str | None = "DEMO"
    count: int = Field(default=1, ge=1, le=20)
    also_publish_kafka: bool = False


class DemoResetOut(BaseModel):
    alerts_deleted: int
    dispatch_log_deleted: int
    outbox_deleted: int = 0


class DemoFireOut(BaseModel):
    mode: str
    events_sent: int
    alert_id: str | None = None
    alerts: list[AlertOut] = Field(default_factory=list)
    note: str = ""


class DemoSeedDeadBody(BaseModel):
    """Create synthetic dead outbox rows so the dead-letter UI can be exercised."""

    count: int = Field(default=3, ge=1, le=20)
    channel: str = "webhook"
    # Also flip any pending/failed outbox rows to dead (e.g. after Fire alert)
    mark_open_as_dead: bool = True


class DemoSeedDeadOut(BaseModel):
    created: int
    marked_open: int
    dead_total: int
    outbox_ids: list[int] = Field(default_factory=list)
    note: str = ""


class LabelSpec(BaseModel):
    key: str
    value: str = ""


class WidgetIn(BaseModel):
    title: str
    labels: list[LabelSpec] = Field(default_factory=list)
    status_filter: str = "open,updated,acknowledged"
    sort_order: int = 0


class WidgetOut(BaseModel):
    id: str
    title: str
    labels: list[LabelSpec]
    status_filter: str
    sort_order: int


class PageMeta(BaseModel):
    page: int
    page_size: int
    total: int
    pages: int
    has_next: bool
    has_prev: bool


class AlertPageOut(BaseModel):
    items: list[AlertOut]
    page: int
    page_size: int
    total: int
    pages: int
    has_next: bool
    has_prev: bool


class DispatchPageOut(BaseModel):
    items: list[DispatchOut]
    page: int
    page_size: int
    total: int
    pages: int
    has_next: bool
    has_prev: bool


class OutboxRowOut(BaseModel):
    id: int
    idempotency_key: str
    alert_id: str
    channel: str
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class OutboxPageOut(BaseModel):
    items: list[OutboxRowOut]
    page: int
    page_size: int
    total: int
    pages: int
    has_next: bool
    has_prev: bool


class OutboxSummaryOut(BaseModel):
    counts: dict[str, int]
    open: int
    dead: int


class OutboxIdsBody(BaseModel):
    """Select specific outbox ids, or all rows matching status."""

    ids: list[int] = Field(default_factory=list)
    all: bool = False
    status: str = "dead"


class OutboxActionOut(BaseModel):
    affected: int
    action: str


def _page_meta(page, page_size, total) -> PageMeta:
    pages = max(1, (total + page_size - 1) // page_size) if total else 0
    return PageMeta(
        page=page,
        page_size=page_size,
        total=total,
        pages=pages,
        has_next=page * page_size < total,
        has_prev=page > 1,
    )


def _rate_limit_ok(bucket: list[float], *, limit: int, window_seconds: float = 60.0) -> bool:
    """Simple process-local sliding window. Returns True if request is allowed."""
    import time as _time

    now = _time.time()
    cutoff = now - window_seconds
    # prune in place
    while bucket and bucket[0] < cutoff:
        bucket.pop(0)
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True


def create_app() -> FastAPI:
    settings = get_settings()
    repo = AlertRepository(settings.database_url)
    session_factory: sessionmaker[Session] = repo._session_factory  # noqa: SLF001
    cache = AlertReadCache(
        session_factory,
        redis_url=settings.redis_url,
        ttl_seconds=settings.ui_cache_ttl_seconds,
        lock_ttl_seconds=settings.ui_cache_lock_ttl_seconds,
        max_alerts=settings.ui_cache_max_alerts,
        key_prefix=settings.ui_cache_key_prefix,
    )
    _demo_fire_hits: list[float] = []
    _demo_reset_hits: list[float] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # Schema: Alembic (entrypoint RUN_MIGRATIONS=1). Do not create_all here.
        cache.start()
        logger.info(
            "UI Redis cache started ttl=%ss url=%s",
            settings.ui_cache_ttl_seconds,
            settings.redis_url,
        )
        yield
        cache.stop()

    app = FastAPI(title="Alert Pipeline UI", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def no_cache_static(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static") or request.url.path == "/":
            response.headers["Cache-Control"] = "no-store, max-age=0"
        return response

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    def _touch_cache() -> None:
        """After a write: force reload so next UI poll sees DB state."""
        cache.invalidate()
        cache.refresh(force=True)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics():
        from fastapi.responses import Response

        from alert_pipeline.observability import metrics_payload

        body, content_type = metrics_payload()
        return Response(content=body, media_type=content_type)

    @app.get("/api/cache")
    def cache_meta() -> dict:
        """Diagnostics: confirm UI is serving from the read cache."""
        return cache.meta()

    @app.get("/", response_class=HTMLResponse)
    def index() -> FileResponse:
        index_path = STATIC_DIR / "index.html"
        if not index_path.is_file():
            raise HTTPException(status_code=500, detail="UI assets missing")
        return FileResponse(index_path)

    @app.get("/api/stats", response_model=StatsOut)
    def stats() -> StatsView:
        return cache.stats()

    @app.get("/api/alerts", response_model=AlertPageOut)
    def list_alerts(
        status: str | None = Query(None),
        severity: str | None = Query(None),
        service: str | None = Query(None),
        q: str | None = Query(None),
        label_key: str | None = Query(None, description="Filter by label key (e.g. env)"),
        label_value: str | None = Query(
            None, description="Optional exact label value; omit to match any value for key"
        ),
        labels: str | None = Query(
            None,
            description='JSON array of {"key","value"} — all must match (AND). '
            'Example: [{"key":"env","value":"prod"},{"key":"team","value":"platform"}]',
        ),
        page: int = Query(1, ge=1),
        page_size: int = Query(10, ge=1, le=200),
        # legacy aliases
        limit: int | None = Query(None, ge=1, le=200),
        offset: int | None = Query(None, ge=0),
    ) -> AlertPageOut:
        if limit is not None:
            page_size = limit
            if offset is not None:
                page = (offset // page_size) + 1
        multi_labels = None
        if labels:
            import json as _json

            try:
                parsed = _json.loads(labels)
                if isinstance(parsed, list):
                    multi_labels = [
                        {"key": str(x.get("key", "")), "value": str(x.get("value", ""))}
                        for x in parsed
                        if isinstance(x, dict)
                    ]
            except _json.JSONDecodeError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid labels JSON: {exc}") from exc
        pg = cache.list_alerts_page(
            status=status,
            severity=severity,
            service=service,
            q=q,
            label_key=label_key,
            label_value=label_value,
            labels=multi_labels,
            page=page,
            page_size=page_size,
        )
        meta = _page_meta(pg.page, pg.page_size, pg.total)
        return AlertPageOut(
            items=list(pg.items),
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    @app.get("/api/alerts/{alert_id}", response_model=AlertOut)
    def get_alert(alert_id: str) -> AlertView:
        row = cache.get_alert(alert_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        return row

    @app.get("/api/alerts/{alert_id}/dispatches", response_model=DispatchPageOut)
    def alert_dispatches(
        alert_id: str,
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=200),
        limit: int | None = Query(None, ge=1, le=200),
    ) -> DispatchPageOut:
        if limit is not None:
            page_size = limit
        pg = cache.alert_dispatches_page(alert_id, page=page, page_size=page_size)
        meta = _page_meta(pg.page, pg.page_size, pg.total)
        return DispatchPageOut(
            items=list(pg.items),
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    def _apply_status(alert_id: str, status: AlertStatusLiteral) -> AlertView:
        with session_factory() as session:
            row = session.get(AlertRecord, alert_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Alert not found")
            apply_status_timestamps(row, status, now=datetime.now(timezone.utc))
            session.commit()
        _touch_cache()
        cached = cache.get_alert(alert_id)
        if cached is None:
            raise HTTPException(status_code=404, detail="Alert not found after update")
        logger.info("Alert %s set to %s (cache refreshed)", alert_id, status)
        return cached

    @app.post("/api/alerts/{alert_id}/status", response_model=AlertOut)
    def set_status(alert_id: str, body: StatusBody) -> AlertView:
        return _apply_status(alert_id, body.status)

    @app.post("/api/alerts/{alert_id}/ack", response_model=AlertOut)
    def acknowledge(alert_id: str) -> AlertView:
        return _apply_status(alert_id, "acknowledged")

    @app.post("/api/alerts/{alert_id}/resolve", response_model=AlertOut)
    def resolve(alert_id: str) -> AlertView:
        return _apply_status(alert_id, "resolved")

    @app.post("/api/alerts/{alert_id}/reopen", response_model=AlertOut)
    def reopen(alert_id: str) -> AlertView:
        return _apply_status(alert_id, "open")

    @app.get("/api/services", response_model=list[str])
    def services() -> list[str]:
        return cache.list_services()

    def _widget_out(row: WidgetRecord) -> WidgetOut:
        import json as _json

        try:
            raw = _json.loads(row.labels_json or "[]")
        except _json.JSONDecodeError:
            raw = []
        specs = [
            LabelSpec(key=str(x.get("key", "")), value=str(x.get("value", "")))
            for x in raw
            if isinstance(x, dict) and x.get("key")
        ]
        return WidgetOut(
            id=row.id,
            title=row.title,
            labels=specs,
            status_filter=row.status_filter or "",
            sort_order=row.sort_order,
        )

    @app.get("/api/widgets", response_model=list[WidgetOut])
    def list_widgets() -> list[WidgetOut]:
        """Shared widgets (all UI servers / operators)."""
        return [_widget_out(w) for w in repo.list_widgets()]

    @app.post("/api/widgets", response_model=WidgetOut)
    def create_widget(body: WidgetIn) -> WidgetOut:
        if not body.labels:
            raise HTTPException(status_code=400, detail="At least one label filter is required")
        row = repo.upsert_widget(
            widget_id=None,
            title=body.title.strip() or "Widget",
            labels=[x.model_dump() for x in body.labels],
            status_filter=body.status_filter or "",
            sort_order=body.sort_order,
        )
        return _widget_out(row)

    @app.put("/api/widgets/{widget_id}", response_model=WidgetOut)
    def update_widget(widget_id: str, body: WidgetIn) -> WidgetOut:
        if not body.labels:
            raise HTTPException(status_code=400, detail="At least one label filter is required")
        existing = repo.get_widget(widget_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Widget not found")
        row = repo.upsert_widget(
            widget_id=widget_id,
            title=body.title.strip() or existing.title,
            labels=[x.model_dump() for x in body.labels],
            status_filter=body.status_filter or "",
            sort_order=body.sort_order,
        )
        return _widget_out(row)

    @app.delete("/api/widgets/{widget_id}")
    def delete_widget(widget_id: str) -> dict[str, bool]:
        ok = repo.delete_widget(widget_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Widget not found")
        return {"deleted": True}

    @app.get("/api/outbox/summary", response_model=OutboxSummaryOut)
    def outbox_summary() -> OutboxSummaryOut:
        """Outbox status counts for ops (alert on ``dead``)."""
        counts = repo.outbox_status_counts()
        return OutboxSummaryOut(
            counts=counts,
            open=repo.count_outbox_open(),
            dead=int(counts.get("dead") or 0),
        )

    @app.get("/api/outbox", response_model=OutboxPageOut)
    def list_outbox(
        status: str | None = Query(
            "dead",
            description="Filter by status; default dead. Use 'all' for every status.",
        ),
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=200),
    ) -> OutboxPageOut:
        """List dispatch_outbox rows (default: dead-letter queue)."""
        st = None if not status or status.lower() in ("all", "*") else status
        total = repo.count_outbox(st)
        offset = (page - 1) * page_size
        rows = repo.list_outbox(status=st, limit=page_size, offset=offset)
        meta = _page_meta(page, page_size, total)
        items = [
            OutboxRowOut(
                id=r.id,
                idempotency_key=r.idempotency_key,
                alert_id=r.alert_id,
                channel=r.channel,
                status=r.status,
                attempts=r.attempts,
                next_attempt_at=r.next_attempt_at,
                last_error=r.last_error,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in rows
        ]
        return OutboxPageOut(
            items=items,
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    @app.post("/api/outbox/redrive", response_model=OutboxActionOut)
    def redrive_outbox(body: OutboxIdsBody) -> OutboxActionOut:
        """Reset dead/failed outbox rows to pending for another worker attempt."""
        if body.all:
            n = repo.redrive_outbox(status=body.status or "dead", all_matching=True)
        else:
            if not body.ids:
                raise HTTPException(status_code=400, detail="Provide ids or set all=true")
            n = repo.redrive_outbox(ids=body.ids, all_matching=False)
        logger.info("Outbox redrive affected=%s all=%s status=%s", n, body.all, body.status)
        return OutboxActionOut(affected=n, action="redrive")

    @app.post("/api/outbox/clear", response_model=OutboxActionOut)
    def clear_outbox(body: OutboxIdsBody) -> OutboxActionOut:
        """Permanently delete outbox rows (default scope: dead)."""
        if body.all:
            n = repo.delete_outbox(status=body.status or "dead", all_matching=True)
        else:
            if not body.ids:
                raise HTTPException(status_code=400, detail="Provide ids or set all=true")
            n = repo.delete_outbox(ids=body.ids, all_matching=False)
        logger.info("Outbox clear affected=%s all=%s status=%s", n, body.all, body.status)
        return OutboxActionOut(affected=n, action="clear")

    @app.get("/api/dispatches/recent", response_model=DispatchPageOut)
    def recent_dispatches(
        page: int = Query(1, ge=1),
        page_size: int = Query(30, ge=1, le=200),
        limit: int | None = Query(None, ge=1, le=200),
    ) -> DispatchPageOut:
        if limit is not None:
            page_size = limit
        pg = cache.recent_dispatches_page(page=page, page_size=page_size)
        meta = _page_meta(pg.page, pg.page_size, pg.total)
        return DispatchPageOut(
            items=list(pg.items),
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    @app.post("/api/demo/reset", response_model=DemoResetOut)
    def demo_reset() -> DemoResetOut:
        if not _rate_limit_ok(
            _demo_reset_hits, limit=max(5, settings.demo_rate_limit_per_minute // 3)
        ):
            raise HTTPException(status_code=429, detail="Demo reset rate limit exceeded")
        result = repo.clear_all()
        _touch_cache()
        logger.info("Demo reset: %s", result)
        return DemoResetOut(**result)

    @app.post("/api/demo/seed-dead-outbox", response_model=DemoSeedDeadOut)
    def demo_seed_dead_outbox(body: DemoSeedDeadBody | None = None) -> DemoSeedDeadOut:
        """Create dead outbox rows for operator UI demos (no real HTTP required).

        1. Inserts ``count`` synthetic incidents + outbox rows already marked ``dead``.
        2. Optionally marks existing pending/failed outbox rows as ``dead`` too.
        """
        from datetime import datetime, timezone
        from uuid import uuid4

        from sqlalchemy import select, update

        from alert_pipeline.db.models import DispatchOutbox
        from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel

        if not _rate_limit_ok(_demo_fire_hits, limit=settings.demo_rate_limit_per_minute):
            raise HTTPException(status_code=429, detail="Demo rate limit exceeded")

        body = body or DemoSeedDeadBody()
        channel = (body.channel or "webhook").strip() or "webhook"
        now = datetime.now(timezone.utc)
        created_ids: list[int] = []

        for i in range(body.count):
            aid = str(uuid4())
            alert = AlertEvent(
                id=aid,
                fingerprint=f"demo-dead-{aid[:8]}",
                title=f"[demo] dead outbox sample #{i + 1}",
                description="Synthetic dead notification for UI demo",
                severity=LogLevel.ERROR,
                service="demo-dead-outbox",
                host="ui-demo",
                status=AlertStatus.OPEN,
                occurrence_count=1,
                first_seen=now,
                last_seen=now,
                sample_message="demo: max attempts exhausted",
                error_code="DEMO_DEAD",
                labels={"source": "ui-demo", "purpose": "dead-outbox"},
                is_new=True,
            )
            repo.upsert_alert(alert)
            keys = repo.enqueue_dispatch(alert, [channel])
            if not keys:
                # Rare: same key already present — bump occurrence for unique key
                alert.occurrence_count = i + 100
                keys = repo.enqueue_dispatch(alert, [channel])
            with repo.session() as session:
                row = session.scalar(
                    select(DispatchOutbox).where(
                        DispatchOutbox.idempotency_key
                        == repo.make_idempotency_key(alert.id, channel, alert.occurrence_count)
                    )
                )
                if row is not None:
                    row.status = "dead"
                    row.attempts = max(1, settings.dispatch_outbox_max_attempts)
                    row.last_error = (
                        f"demo: simulated channel failure after max attempts (channel={channel})"
                    )
                    row.updated_at = now
                    created_ids.append(row.id)

        marked_open = 0
        if body.mark_open_as_dead:
            with repo.session() as session:
                result = session.execute(
                    update(DispatchOutbox)
                    .where(DispatchOutbox.status.in_(("pending", "failed", "processing")))
                    .values(
                        status="dead",
                        last_error="demo: marked open outbox as dead for UI exercise",
                        updated_at=now,
                    )
                )
                marked_open = int(result.rowcount or 0)

        dead_total = repo.count_outbox("dead")
        _touch_cache()
        logger.info(
            "Demo seed-dead-outbox created=%s marked_open=%s dead_total=%s",
            len(created_ids),
            marked_open,
            dead_total,
        )
        return DemoSeedDeadOut(
            created=len(created_ids),
            marked_open=marked_open,
            dead_total=dead_total,
            outbox_ids=created_ids,
            note=(
                f"Created {len(created_ids)} synthetic dead row(s)"
                + (f"; marked {marked_open} open row(s) dead" if marked_open else "")
                + f". Total dead now: {dead_total}. Use Redrive/Clear in the panel below."
            ),
        )

    @app.post("/api/demo/fire", response_model=DemoFireOut)
    def demo_fire(body: DemoFireBody) -> DemoFireOut:
        from alert_pipeline.dispatchers.registry import (
            DispatchFanout,
            build_dispatchers,
            enabled_channel_names,
        )
        from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES

        if not _rate_limit_ok(_demo_fire_hits, limit=settings.demo_rate_limit_per_minute):
            raise HTTPException(status_code=429, detail="Demo fire rate limit exceeded")

        level = LogLevel.normalize(body.severity)
        now = datetime.now(timezone.utc)
        base_log = LogEvent(
            timestamp=now,
            level=level,
            service=body.service.strip() or "demo-service",
            host=body.host or "ui-demo",
            message=(body.message or "demo failure").strip(),
            error_code=(body.error_code or None) or None,
            labels={"source": "ui-demo", "env": "local"},
        )
        cfg = get_alert_config().resolve_for(base_log)
        fp = compute_fingerprint(base_log, cfg.dedup_fields)

        existing_count = 0
        existing_id: str | None = None
        existing_first = now
        with session_factory() as session:
            row = session.scalar(
                select(AlertRecord).where(
                    AlertRecord.fingerprint == fp,
                    AlertRecord.status.in_(tuple(ACTIVE_ALERT_STATUSES)),
                )
            )
            if row is not None:
                existing_count = row.occurrence_count
                existing_id = row.id
                existing_first = row.first_seen

        created_alert: AlertView | None = None
        alert_id: str | None = existing_id
        for i in range(body.count):
            ts = datetime.now(timezone.utc)
            occ = existing_count + i + 1
            is_first_create = existing_id is None and i == 0
            alert_kwargs: dict = {
                "fingerprint": fp,
                "title": build_title(base_log),
                "description": base_log.message,
                "severity": level,
                "service": base_log.service,
                "host": base_log.host,
                "status": AlertStatus.OPEN if is_first_create else AlertStatus.UPDATED,
                "occurrence_count": occ,
                "first_seen": existing_first if existing_id else ts,
                "last_seen": ts,
                "error_code": base_log.error_code,
                "labels": base_log.labels,
                "sample_message": base_log.message,
                "is_new": is_first_create,
            }
            if existing_id:
                alert_kwargs["id"] = existing_id
            alert = AlertEvent(**alert_kwargs)
            channels = enabled_channel_names(settings)
            mode = (settings.dispatch_mode or "outbox").lower()
            if is_first_create and i == 0 and mode == "outbox" and channels:
                # Atomic upsert + outbox (same as pipeline emit path)
                record, _keys = repo.upsert_and_maybe_enqueue(alert, channels)
            else:
                record = repo.upsert_alert(alert)
                if is_first_create and i == 0 and mode == "inline" and channels:
                    fanout = DispatchFanout(build_dispatchers(settings), repo=repo)
                    fanout.dispatch(alert)
            existing_id = record.id
            alert_id = record.id

        _touch_cache()
        if alert_id:
            created_alert = cache.get_alert(alert_id)

        kafka_note = ""
        if body.also_publish_kafka:
            try:
                import json as _json

                from confluent_kafka import Producer

                producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
                topic = settings.kafka_input_topic
                for i in range(body.count):
                    event = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "level": level.value,
                        "service": base_log.service,
                        "host": base_log.host,
                        "message": base_log.message,
                        "error_code": base_log.error_code,
                        "trace_id": f"demo-{datetime.now(timezone.utc).timestamp()}-{i}",
                        "labels": {"source": "ui-demo", "env": "local"},
                    }
                    producer.produce(
                        topic,
                        key=base_log.service.encode("utf-8"),
                        value=_json.dumps(event).encode("utf-8"),
                    )
                producer.flush(5)
                kafka_note = f" Also published {body.count} message(s) to Kafka topic '{topic}'."
            except Exception as exc:  # noqa: BLE001
                kafka_note = f" Kafka publish skipped: {exc}"

        return DemoFireOut(
            mode="direct",
            events_sent=body.count,
            alert_id=alert_id,
            alerts=[created_alert] if created_alert else [],
            note=f"Saved to database (id={alert_id}); UI cache refreshed.{kafka_note}",
        )

    return app


def main() -> None:
    import os

    import uvicorn

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    settings = get_settings()
    uvicorn.run(
        "alert_pipeline.ui.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.environ.get("UI_PORT", "8000")),
        log_level=settings.log_level.lower(),
    )


def __getattr__(name: str):
    """Lazy ASGI app for ``uvicorn alert_pipeline.ui.app:app`` without import-time DB connect."""
    if name == "app":
        return create_app()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
