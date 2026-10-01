"""HTTP API for fraud analysts.

    uv run uvicorn banking_cdc.alerts_service.api:create_app_from_env --factory --port 8080

Set KAFKA_BOOTSTRAP (and KAFKA_API_KEY / KAFKA_API_SECRET for Confluent Cloud) to consume
alerts; ALERTS_DB sets the SQLite path. OpenAPI docs are served at /docs.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from banking_cdc.alerts_service.consumer import AlertConsumer
from banking_cdc.alerts_service.store import AlertStore

ALERTS_TOPIC = "banking.fraud.alerts"


class Alert(BaseModel):
    alert_id: str
    rule: str
    account_id: int
    evidence_keys: list[str]
    first_evidence_ts: str | None = None
    last_source_commit_ts: str | None = None
    alert_created_at: str | None = None
    latency_seconds: float | None = None
    status: str
    note: str | None = None
    decided_at: str | None = None
    received_at: str


class Decision(BaseModel):
    status: Literal["OPEN", "CONFIRMED", "DISMISSED"]
    note: str | None = None


def create_app(store: AlertStore, consumer: AlertConsumer | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if consumer:
            consumer.start()
        yield
        if consumer:
            consumer.stop()

    app = FastAPI(
        title="Fraud alerts",
        description="Analyst queue for alerts published by the CDC pipeline's Gold layer.",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "consumer_running": bool(consumer and consumer.running),
            "alerts": store.stats()["total"],
        }

    @app.get("/alerts", response_model=list[Alert])
    def list_alerts(
        status: Literal["OPEN", "CONFIRMED", "DISMISSED"] | None = None,
        rule: str | None = None,
        account_id: int | None = None,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        return store.list(status, rule, account_id, limit, offset)

    @app.get("/alerts/{alert_id}", response_model=Alert)
    def get_alert(alert_id: str):
        alert = store.get(alert_id)
        if alert is None:
            raise HTTPException(404, "alert not found")
        return alert

    @app.patch("/alerts/{alert_id}", response_model=Alert)
    def decide(alert_id: str, decision: Decision):
        alert = store.set_status(alert_id, decision.status, decision.note)
        if alert is None:
            raise HTTPException(404, "alert not found")
        return alert

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        out = store.stats()
        if consumer:
            out["consumer"] = consumer.counts
        return out

    return app


def create_app_from_env() -> FastAPI:
    store = AlertStore(os.environ.get("ALERTS_DB", "alerts.db"))
    consumer = None
    if os.environ.get("KAFKA_BOOTSTRAP"):
        from banking_cdc.pipeline.config import kafka_conf

        def secret(scope_key: str, field: str) -> str:
            value = os.environ.get(f"{scope_key}_{field}".upper())
            if value is None:
                raise KeyError(field)
            return value

        consumer = AlertConsumer(
            kafka_conf(secret), os.environ.get("ALERTS_TOPIC", ALERTS_TOPIC), store
        )
    return create_app(store, consumer)
