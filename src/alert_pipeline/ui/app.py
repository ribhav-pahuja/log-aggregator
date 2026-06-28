"""FastAPI dashboard: one screen for all deduplicated alerts + dispatch audit."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.config import get_settings
from alert_pipeline.db.models import AlertRecord, Base, DispatchLog
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.alert_config import get_alert_config
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
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
    # Also publish to Kafka (pipeline may suppress duplicates in-memory after a DB clear).
    # Direct DB write always happens so the UI is never empty after Fire.
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


def _row_to_alert(row: AlertRecord, dispatch_ok: int = 0, dispatch_fail: int = 0) -> AlertOut:
    try:
        labels = json.loads(row.labels_json or "{}")
        if not isinstance(labels, dict):
            labels = {}
        labels = {str(k): str(v) for k, v in labels.items()}
    except json.JSONDecodeError:
        labels = {}
    return AlertOut(
        id=row.id,
        fingerprint=row.fingerprint,
        title=row.title,
        description=row.description,
        severity=row.severity,
        service=row.service,
        host=row.host,
        status=row.status,
        occurrence_count=row.occurrence_count,
        first_seen=row.first_seen,
        last_seen=row.last_seen,
        error_code=row.error_code,
        trace_id=row.trace_id,
        labels=labels,
        sample_message=row.sample_message,
        acknowledged_at=getattr(row, "acknowledged_at", None),
        resolved_at=getattr(row, "resolved_at", None),
        tta_seconds=getattr(row, "tta_seconds", None),
        ttr_seconds=getattr(row, "ttr_seconds", None),
        created_at=row.created_at,
        updated_at=row.updated_at,
        dispatch_success=dispatch_ok,
        dispatch_failed=dispatch_fail,
    )


def _dispatch_counts(session: Session, alert_ids: list[str]) -> tuple[dict[str, int], dict[str, int]]:
    if not alert_ids:
        return {}, {}
    dispatch_rows = session.execute(
        select(
            DispatchLog.alert_id,
            DispatchLog.success,
            func.count().label("cnt"),
        )
        .where(DispatchLog.alert_id.in_(alert_ids))
        .group_by(DispatchLog.alert_id, DispatchLog.success)
    ).all()
    ok_map: dict[str, int] = {}
    fail_map: dict[str, int] = {}
    for alert_id, success, cnt in dispatch_rows:
        if success:
            ok_map[alert_id] = int(cnt)
        else:
            fail_map[alert_id] = int(cnt)
    return ok_map, fail_map


def create_app() -> FastAPI:
    settings = get_settings()
    repo = AlertRepository(settings.database_url)
    session_factory: sessionmaker[Session] = repo._session_factory  # noqa: SLF001

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        Base.metadata.create_all(repo._engine)  # noqa: SLF001
        logger.info("UI connected to database")
        yield

    app = FastAPI(title="Alert Pipeline UI", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def no_cache_static(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static") or request.url.path == "/":
            response.headers["Cache-Control"] = "no-store, max-age=0"
        return response

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_class=HTMLResponse)
    def index() -> FileResponse:
        index_path = STATIC_DIR / "index.html"
        if not index_path.is_file():
            raise HTTPException(status_code=500, detail="UI assets missing")
        return FileResponse(index_path)

    @app.get("/api/stats", response_model=StatsOut)
    def stats() -> StatsOut:
        with session_factory() as session:
            total = session.scalar(select(func.count()).select_from(AlertRecord)) or 0

            def _count(status: str) -> int:
                return (
                    session.scalar(
                        select(func.count())
                        .select_from(AlertRecord)
                        .where(AlertRecord.status == status)
                    )
                    or 0
                )

            open_n = _count("open")
            updated_n = _count("updated")
            ack_n = _count("acknowledged")
            resolved_n = _count("resolved")
            crit = (
                session.scalar(
                    select(func.count())
                    .select_from(AlertRecord)
                    .where(AlertRecord.severity.in_(("ERROR", "CRITICAL", "FATAL")))
                )
                or 0
            )
            services = (
                session.scalar(
                    select(func.count(func.distinct(AlertRecord.service))).select_from(AlertRecord)
                )
                or 0
            )
            ok = (
                session.scalar(
                    select(func.count()).select_from(DispatchLog).where(DispatchLog.success == 1)
                )
                or 0
            )
            fail = (
                session.scalar(
                    select(func.count()).select_from(DispatchLog).where(DispatchLog.success == 0)
                )
                or 0
            )
            last_at = session.scalar(select(func.max(AlertRecord.last_seen)))
        return StatsOut(
            total=total,
            open=open_n,
            updated=updated_n,
            acknowledged=ack_n,
            resolved=resolved_n,
            critical_or_error=crit,
            services=services,
            dispatches_ok=ok,
            dispatches_fail=fail,
            last_alert_at=last_at,
        )

    @app.get("/api/alerts", response_model=list[AlertOut])
    def list_alerts(
        status: str | None = Query(
            None, description="open|updated|acknowledged|resolved or comma-separated"
        ),
        severity: str | None = Query(None),
        service: str | None = Query(None),
        q: str | None = Query(None, description="Search title, message, fingerprint"),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> list[AlertOut]:
        with session_factory() as session:
            stmt = select(AlertRecord).order_by(desc(AlertRecord.last_seen)).offset(offset).limit(limit)
            if status:
                statuses = [s.strip() for s in status.split(",") if s.strip()]
                if statuses:
                    stmt = stmt.where(AlertRecord.status.in_(statuses))
            if severity:
                sevs = [s.strip().upper() for s in severity.split(",") if s.strip()]
                if sevs:
                    stmt = stmt.where(AlertRecord.severity.in_(sevs))
            if service:
                stmt = stmt.where(AlertRecord.service == service)
            if q:
                like = f"%{q}%"
                stmt = stmt.where(
                    or_(
                        AlertRecord.title.ilike(like),
                        AlertRecord.sample_message.ilike(like),
                        AlertRecord.fingerprint.ilike(like),
                        AlertRecord.error_code.ilike(like),
                        AlertRecord.service.ilike(like),
                    )
                )
            rows = session.scalars(stmt).all()
            ok_map, fail_map = _dispatch_counts(session, [r.id for r in rows])
            return [
                _row_to_alert(r, dispatch_ok=ok_map.get(r.id, 0), dispatch_fail=fail_map.get(r.id, 0))
                for r in rows
            ]

    @app.get("/api/alerts/{alert_id}", response_model=AlertOut)
    def get_alert(alert_id: str) -> AlertOut:
        with session_factory() as session:
            row = session.get(AlertRecord, alert_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Alert not found")
            ok_map, fail_map = _dispatch_counts(session, [alert_id])
            return _row_to_alert(
                row, dispatch_ok=ok_map.get(alert_id, 0), dispatch_fail=fail_map.get(alert_id, 0)
            )

    @app.get("/api/alerts/{alert_id}/dispatches", response_model=list[DispatchOut])
    def alert_dispatches(alert_id: str, limit: int = Query(50, ge=1, le=200)) -> list[DispatchOut]:
        with session_factory() as session:
            rows = session.scalars(
                select(DispatchLog)
                .where(DispatchLog.alert_id == alert_id)
                .order_by(desc(DispatchLog.created_at))
                .limit(limit)
            ).all()
            return [
                DispatchOut(
                    id=r.id,
                    alert_id=r.alert_id,
                    channel=r.channel,
                    success=bool(r.success),
                    status_code=r.status_code,
                    error_message=r.error_message,
                    created_at=r.created_at,
                )
                for r in rows
            ]

    def _apply_status(alert_id: str, status: AlertStatusLiteral) -> AlertOut:
        with session_factory() as session:
            row = session.get(AlertRecord, alert_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Alert not found")
            apply_status_timestamps(row, status, now=datetime.now(timezone.utc))
            session.commit()
            session.refresh(row)
            ok_map, fail_map = _dispatch_counts(session, [alert_id])
            logger.info("Alert %s set to %s", alert_id, status)
            return _row_to_alert(
                row, dispatch_ok=ok_map.get(alert_id, 0), dispatch_fail=fail_map.get(alert_id, 0)
            )

    @app.post("/api/alerts/{alert_id}/status", response_model=AlertOut)
    def set_status(alert_id: str, body: StatusBody) -> AlertOut:
        return _apply_status(alert_id, body.status)

    @app.post("/api/alerts/{alert_id}/ack", response_model=AlertOut)
    def acknowledge(alert_id: str) -> AlertOut:
        """Operator acknowledges the incident (still active; pipeline keeps updating counts)."""
        return _apply_status(alert_id, "acknowledged")

    @app.post("/api/alerts/{alert_id}/resolve", response_model=AlertOut)
    def resolve(alert_id: str) -> AlertOut:
        """Close the incident. New matching errors open a fresh alert."""
        return _apply_status(alert_id, "resolved")

    @app.post("/api/alerts/{alert_id}/reopen", response_model=AlertOut)
    def reopen(alert_id: str) -> AlertOut:
        return _apply_status(alert_id, "open")

    @app.get("/api/services", response_model=list[str])
    def services() -> list[str]:
        with session_factory() as session:
            rows = session.scalars(
                select(AlertRecord.service).distinct().order_by(AlertRecord.service)
            ).all()
            return list(rows)

    @app.get("/api/dispatches/recent", response_model=list[DispatchOut])
    def recent_dispatches(limit: int = Query(30, ge=1, le=100)) -> list[DispatchOut]:
        with session_factory() as session:
            rows = session.scalars(
                select(DispatchLog).order_by(desc(DispatchLog.created_at)).limit(limit)
            ).all()
            return [
                DispatchOut(
                    id=r.id,
                    alert_id=r.alert_id,
                    channel=r.channel,
                    success=bool(r.success),
                    status_code=r.status_code,
                    error_message=r.error_message,
                    created_at=r.created_at,
                )
                for r in rows
            ]

    @app.post("/api/demo/reset", response_model=DemoResetOut)
    def demo_reset() -> DemoResetOut:
        """Empty slate: delete all alerts and dispatch history."""
        result = repo.clear_all()
        logger.info("Demo reset: %s", result)
        return DemoResetOut(**result)

    @app.post("/api/demo/fire", response_model=DemoFireOut)
    def demo_fire(body: DemoFireBody) -> DemoFireOut:
        """
        Always persist via the DB (so Fire is reliable even if Kafka dedup state
        still thinks the incident is open after a Clear). Optionally also publish
        to Kafka for end-to-end pipeline demos.
        """
        settings = get_settings()
        level = LogLevel.normalize(body.severity)
        from alert_pipeline.dispatchers.registry import DispatchFanout, build_dispatchers
        from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES

        fanout = DispatchFanout(build_dispatchers(settings), repo=repo)
        now = datetime.now(timezone.utc)
        # Stable demo fingerprint (same service + error_code/message → one incident)
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

        # Load active row if any so we can increment occurrence_count correctly
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
            # Dispatch only on brand-new incident (first event of a new fingerprint)
            if is_first_create and i == 0:
                fanout.dispatch(alert)
            with session_factory() as session:
                row = session.get(AlertRecord, record.id)
                if row is not None:
                    created_alert = _row_to_alert(row)

        kafka_note = ""
        if body.also_publish_kafka:
            try:
                import json
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
                        value=json.dumps(event).encode("utf-8"),
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
            note=f"Saved to database (id={alert_id}).{kafka_note}",
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
