"""Dead outbox list / redrive / clear operator path."""

from datetime import datetime, timezone

from sqlalchemy import select

from alert_pipeline.config import Settings, get_settings
from alert_pipeline.db.models import DispatchOutbox
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _alert(**kwargs) -> AlertEvent:
    now = datetime.now(timezone.utc)
    base = dict(
        fingerprint="fp-ops",
        title="t",
        description="d",
        severity=LogLevel.ERROR,
        service="svc",
        host="h",
        status=AlertStatus.OPEN,
        occurrence_count=1,
        first_seen=now,
        last_seen=now,
        sample_message="boom",
        is_new=True,
    )
    base.update(kwargs)
    return AlertEvent(**base)


def _seed_dead(repo: AlertRepository, n: int = 2) -> list[int]:
    ids = []
    for i in range(n):
        a = _alert(id=f"aid-{i}", occurrence_count=i + 1, fingerprint=f"fp-{i}")
        repo.enqueue_dispatch(a, ["webhook"])
    with repo.session() as session:
        for row in session.scalars(select(DispatchOutbox)).all():
            row.status = "dead"
            row.attempts = 8
            row.last_error = "max attempts"
            ids.append(row.id)
    return ids


def test_list_and_count_dead(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/ops.db")
    _seed_dead(repo, 3)
    assert repo.count_outbox("dead") == 3
    rows = repo.list_outbox(status="dead", limit=10)
    assert len(rows) == 3
    assert all(r.status == "dead" for r in rows)
    counts = repo.outbox_status_counts()
    assert counts.get("dead") == 3


def test_redrive_dead_to_pending(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/rd.db")
    ids = _seed_dead(repo, 2)
    n = repo.redrive_outbox(ids=ids[:1])
    assert n == 1
    with repo.session() as session:
        r = session.get(DispatchOutbox, ids[0])
        assert r is not None
        assert r.status == "pending"
        assert r.attempts == 0
        assert r.last_error is None
        r2 = session.get(DispatchOutbox, ids[1])
        assert r2 is not None and r2.status == "dead"


def test_redrive_all_dead(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/rda.db")
    _seed_dead(repo, 3)
    n = repo.redrive_outbox(all_matching=True, status="dead")
    assert n == 3
    assert repo.count_outbox("pending") == 3
    assert repo.count_outbox("dead") == 0


def test_delete_dead(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/del.db")
    ids = _seed_dead(repo, 2)
    n = repo.delete_outbox(ids=[ids[0]])
    assert n == 1
    assert repo.count_outbox("dead") == 1
    n2 = repo.delete_outbox(all_matching=True, status="dead")
    assert n2 == 1
    assert repo.count_outbox("dead") == 0


def test_api_outbox_summary_and_redrive(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from alert_pipeline.ui import app as ui_app

    db = f"sqlite+pysqlite:///{tmp_path}/api.db"
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        database_url=db,
        redis_url="redis://127.0.0.1:1/0",
        ui_cache_invalidate_on_write=False,
    )
    monkeypatch.setattr(ui_app, "get_settings", lambda: settings)
    get_settings.cache_clear()

    repo = AlertRepository(db)
    _seed_dead(repo, 2)

    client = TestClient(ui_app.create_app())
    s = client.get("/api/outbox/summary")
    assert s.status_code == 200
    body = s.json()
    assert body["dead"] == 2

    listed = client.get("/api/outbox?status=dead")
    assert listed.status_code == 200
    assert listed.json()["total"] == 2

    r = client.post("/api/outbox/redrive", json={"all": True, "status": "dead"})
    assert r.status_code == 200
    assert r.json()["affected"] == 2
    assert client.get("/api/outbox/summary").json()["dead"] == 0

    # Mark remaining pending rows dead again, then clear
    with repo.session() as session:
        for row in session.scalars(select(DispatchOutbox)).all():
            row.status = "dead"
    c = client.post("/api/outbox/clear", json={"all": True, "status": "dead"})
    assert c.status_code == 200
    assert c.json()["affected"] == 2
    assert client.get("/api/outbox/summary").json()["dead"] == 0
