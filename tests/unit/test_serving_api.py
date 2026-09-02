"""FastAPI serving layer: health is local; ETA reads eta-current.

Spec for the lookup path: GET /eta/{order_id} returns the DynamoDB item
or 404. AWS is stubbed so this does not need a live MiniStack. Endpoints
are called as functions (same handlers FastAPI wires) to avoid pulling
in starlette's TestClient extra.
"""

import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from serving import api as serving_api  # noqa: E402


def test_health_ok():
    assert serving_api.health() == {"status": "ok"}


def test_eta_404_when_order_missing(monkeypatch):
    class _EmptyDdb:
        def get_item(self, **kwargs):
            return {}

    monkeypatch.setattr(serving_api.aws, "client", lambda *_a, **_k: _EmptyDdb())
    with pytest.raises(HTTPException) as exc:
        serving_api.get_eta("missing-order")
    assert exc.value.status_code == 404


def test_eta_returns_recorded_minutes(monkeypatch):
    class _HitDdb:
        def get_item(self, **kwargs):
            return {
                "Item": {
                    "order_id": {"S": "ord-1"},
                    "eta_minutes": {"N": "18.5"},
                    "scored_at": {"S": "2026-01-01T00:00:00Z"},
                }
            }

    monkeypatch.setattr(serving_api.aws, "client", lambda *_a, **_k: _HitDdb())
    body = serving_api.get_eta("ord-1")
    assert body["order_id"] == "ord-1"
    assert body["eta_minutes"] == 18.5
    assert body["scored_at"] == "2026-01-01T00:00:00Z"
