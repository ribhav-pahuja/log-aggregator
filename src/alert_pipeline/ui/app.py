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
from alert_pipeline.cache.alert_cache import AlertReadCache, CachedAlert, CachedDispatch
from alert_pipeline.config import get_settings
from alert_pipeline.db.models import AlertRecord, Base
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogEvent, LogLevel

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"

AlertStatusLiteral = Literal["open", "updated", "acknowledged", "resolved"]


class AlertOut(BaseModel):
    id: str
    fingerprint: str
    title: str
    description: str
    severity: str
    service: str
    host: str
    status: str
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime
    error_code: str | None
    trace_id: str | None
    labels: dict[str, str] = Field(default_factory=dict)
    sample_message: str
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    tta_seconds: int | None = None
    ttr_seconds: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    dispatch_success: int = 0
    dispatch_failed: int = 0


class DispatchOut(BaseModel):
    id: int
    alert_id: str
    channel: str
    success: bool
    status_code: int | None
    error_message: str | None
    created_at: datetime


class StatsOut(BaseModel):
    total: int
    open: int
    updated: int
    acknowledged: int
    resolved: int
    critical_or_error: int
    services: int
    dispatches_ok: int
    dispatches_fail: int
    last_alert_at: datetime | None


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


class DemoFireOut(BaseModel):
    mode: str
    events_sent: int
    alert_id: str | None = None
    alerts: list[AlertOut] = Field(default_factory=list)
    note: str = ""


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


def _alert_out(c: CachedAlert) -> AlertOut:
    return AlertOut.model_validate(c.as_dict())


def _dispatch_out(d: CachedDispatch) -> DispatchOut:
    return DispatchOut(
        id=d.id,
        alert_id=d.alert_id,
        channel=d.channel,
        success=d.success,
        status_code=d.status_code,
        error_message=d.error_message,
        created_at=d.created_at,
    )


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

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        Base.metadata.create_all(repo._engine)  # noqa: SLF001
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
    def stats() -> StatsOut:
        s = cache.stats()
        return StatsOut(
            total=s.total,
            open=s.open,
            updated=s.updated,
            acknowledged=s.acknowledged,
            resolved=s.resolved,
            critical_or_error=s.critical_or_error,
            services=s.services,
            dispatches_ok=s.dispatches_ok,
            dispatches_fail=s.dispatches_fail,
            last_alert_at=s.last_alert_at,
        )

    @app.get("/api/alerts", response_model=AlertPageOut)
    def list_alerts(
        status: str | None = Query(None),
        severity: str | None = Query(None),
        service: str | None = Query(None),
        q: str | None = Query(None),
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
        pg = cache.list_alerts_page(
            status=status,
            severity=severity,
            service=service,
            q=q,
            page=page,
            page_size=page_size,
        )
        meta = _page_meta(pg.page, pg.page_size, pg.total)
        return AlertPageOut(
            items=[_alert_out(a) for a in pg.items],
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    @app.get("/api/alerts/{alert_id}", response_model=AlertOut)
    def get_alert(alert_id: str) -> AlertOut:
        row = cache.get_alert(alert_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        return _alert_out(row)

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
            items=[_dispatch_out(d) for d in pg.items],
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    def _apply_status(alert_id: str, status: AlertStatusLiteral) -> AlertOut:
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
        return _alert_out(cached)

    @app.post("/api/alerts/{alert_id}/status", response_model=AlertOut)
    def set_status(alert_id: str, body: StatusBody) -> AlertOut:
        return _apply_status(alert_id, body.status)

    @app.post("/api/alerts/{alert_id}/ack", response_model=AlertOut)
    def acknowledge(alert_id: str) -> AlertOut:
        return _apply_status(alert_id, "acknowledged")

    @app.post("/api/alerts/{alert_id}/resolve", response_model=AlertOut)
    def resolve(alert_id: str) -> AlertOut:
        return _apply_status(alert_id, "resolved")

    @app.post("/api/alerts/{alert_id}/reopen", response_model=AlertOut)
    def reopen(alert_id: str) -> AlertOut:
        return _apply_status(alert_id, "open")

    @app.get("/api/services", response_model=list[str])
    def services() -> list[str]:
        return cache.list_services()

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
            items=[_dispatch_out(d) for d in pg.items],
            page=meta.page,
            page_size=meta.page_size,
            total=meta.total,
            pages=meta.pages,
            has_next=meta.has_next,
            has_prev=meta.has_prev,
        )

    @app.post("/api/demo/reset", response_model=DemoResetOut)
    def demo_reset() -> DemoResetOut:
        result = repo.clear_all()
        _touch_cache()
        logger.info("Demo reset: %s", result)
        return DemoResetOut(**result)

    @app.post("/api/demo/fire", response_model=DemoFireOut)
    def demo_fire(body: DemoFireBody) -> DemoFireOut:
        from alert_pipeline.dispatchers.registry import DispatchFanout, build_dispatchers
        from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES

        level = LogLevel.normalize(body.severity)
        fanout = DispatchFanout(build_dispatchers(settings), repo=repo)
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

        created_alert: AlertOut | None = None
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
            record = repo.upsert_alert(alert)
            existing_id = record.id
            alert_id = record.id
            if is_first_create and i == 0:
                fanout.dispatch(alert)

        _touch_cache()
        if alert_id:
            cached = cache.get_alert(alert_id)
            if cached:
                created_alert = _alert_out(cached)

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

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    settings = get_settings()
    uvicorn.run(
        "alert_pipeline.ui.app:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.environ.get("UI_PORT", "8000")),
        log_level=settings.log_level.lower(),
    )


app = create_app()
