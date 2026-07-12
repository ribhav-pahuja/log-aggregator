"""Postgres-only test fixtures.

Requires a running PostgreSQL (Compose ``postgres`` or CI service)::

    export TEST_DATABASE_URL=postgresql+psycopg://alerts:alerts@localhost:5432/alerts
    # or DATABASE_URL with the same shape

Pytest truncates app tables between tests so cases stay isolated.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# Project root (for alembic.ini)
_ROOT = Path(__file__).resolve().parents[1]

# Host-side default matches docker-compose port mapping + .env.example credentials.
_DEFAULT_PG = "postgresql+psycopg://alerts:alerts@localhost:5432/alerts"

_TABLES = (
    "dispatch_outbox",
    "dispatch_log",
    "alerts",
    "dashboard_widgets",
)


def _resolve_database_url() -> str:
    url = (
        os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL") or _DEFAULT_PG
    ).strip()
    if "sqlite" in url.lower():
        raise RuntimeError(
            "SQLite is not supported. Set TEST_DATABASE_URL or DATABASE_URL to "
            f"PostgreSQL, e.g. {_DEFAULT_PG}"
        )
    if not url.startswith("postgresql"):
        raise RuntimeError(
            f"DATABASE_URL must be PostgreSQL (postgresql+psycopg://...), got: {url!r}"
        )
    # Host processes use localhost; compose internal hostname is unusable from the host.
    url = url.replace("@postgres:", "@localhost:")
    return url


@pytest.fixture(scope="session")
def database_url() -> str:
    return _resolve_database_url()


@pytest.fixture(scope="session", autouse=True)
def _migrate_postgres(database_url: str) -> None:
    """Apply Alembic migrations once per test session."""
    os.environ["DATABASE_URL"] = database_url
    from alembic.config import Config

    from alembic import command

    cfg = Config(str(_ROOT / "alembic.ini"))
    command.upgrade(cfg, "head")


def _truncate(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE " + ", ".join(_TABLES) + " RESTART IDENTITY CASCADE"))
    finally:
        engine.dispose()


@pytest.fixture
def clean_db(database_url: str) -> str:
    """Yield a Postgres URL with empty app tables; truncate again after the test."""
    _truncate(database_url)
    yield database_url
    _truncate(database_url)


@pytest.fixture
def repo(clean_db: str):
    from alert_pipeline.db.repository import AlertRepository

    return AlertRepository(clean_db)
