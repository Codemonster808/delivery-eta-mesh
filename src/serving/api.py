#!/usr/bin/env python3
"""FastAPI serving layer: live ETA lookup, daily accuracy."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from accuracy import compute_mae  # noqa: E402 (scripts/accuracy.py)
from fastapi import FastAPI, HTTPException  # noqa: E402

from utils import aws  # noqa: E402

app = FastAPI(title="delivery-eta-mesh")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/eta/{order_id}")
def get_eta(order_id: str):
    ddb = aws.client("dynamodb")
    resp = ddb.get_item(TableName="eta-current", Key={"order_id": {"S": order_id}})
    item = resp.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="no ETA recorded for this order")
    return {
        "order_id": order_id,
        "eta_minutes": float(item["eta_minutes"]["N"]),
        "scored_at": item["scored_at"]["S"],
    }


@app.get("/accuracy/daily")
def accuracy_daily(events_path: str = "data/events.jsonl"):
    if not Path(events_path).exists():
        raise HTTPException(
            status_code=404, detail=f"{events_path} not found — run data_gen.py first"
        )
    return compute_mae(events_path)
