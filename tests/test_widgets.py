"""Shared dashboard widgets (Postgres-backed)."""

from __future__ import annotations

from alert_pipeline.db.repository import AlertRepository


def test_widget_crud(repo: AlertRepository):
    w = repo.upsert_widget(
        widget_id=None,
        title="Prod platform",
        labels=[
            {"key": "env", "value": "prod"},
            {"key": "team", "value": "platform"},
        ],
        status_filter="open,updated,acknowledged",
        sort_order=1,
    )
    assert w.id
    assert w.title == "Prod platform"

    listed = repo.list_widgets()
    assert len(listed) == 1
    assert listed[0].id == w.id

    got = repo.get_widget(w.id)
    assert got is not None
    assert "env" in got.labels_json

    updated = repo.upsert_widget(
        widget_id=w.id,
        title="Prod only",
        labels=[{"key": "env", "value": "prod"}],
        status_filter="",
        sort_order=0,
    )
    assert updated.title == "Prod only"
    assert repo.delete_widget(w.id) is True
    assert repo.get_widget(w.id) is None
    assert repo.delete_widget("missing") is False
