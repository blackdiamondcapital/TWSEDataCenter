#!/usr/bin/env python3
"""Render background worker for long-running stock-data jobs."""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Any
from urllib.parse import urlencode

from cloud_jobs import claim_next_job, complete_job, fail_job, update_job

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("cloud-worker")

POLL_SECONDS = max(1.0, float(os.environ.get("WORKER_POLL_SECONDS", "5")))
WORKER_ID = os.environ.get("RENDER_INSTANCE_ID") or f"{socket.gethostname()}:{os.getpid()}"


def _compact_result(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list):
        # Financial statements are useful to the UI, but cap persisted JSON size.
        return {"count": len(payload), "data": payload[:200], "truncated": len(payload) > 200}
    if not isinstance(payload, dict):
        return {"value": str(payload)[:2000]}
    compact: dict[str, Any] = {}
    for key, value in payload.items():
        if key == "data" and isinstance(value, list):
            compact["count"] = payload.get("count", len(value))
            compact["data"] = value[:100]
            compact["truncated"] = len(value) > 100
        elif key in {"errors", "daily_stats", "monthly_stats"} and isinstance(value, list):
            compact[key] = value[:100]
        elif key not in {"raw", "html"}:
            compact[key] = value
    return compact


class Heartbeat:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=2)

    def _run(self):
        elapsed = 0
        while not self.stop_event.wait(20):
            elapsed += 20
            try:
                update_job(
                    self.job_id,
                    message=f"Worker running ({elapsed}s)",
                )
            except Exception:
                logger.exception("Unable to update heartbeat for %s", self.job_id)


def _request_for_job(job_type: str, params: dict[str, Any]):
    # Importing here keeps queue startup lightweight and gives Render clearer errors.
    from server import app, stock_api

    query = dict(params)
    query.pop("dry_run", None)
    query["use_local_db"] = "false"
    query["persist"] = "true"

    with app.test_client() as client:
        if job_type == "stock_prices":
            body = dict(params)
            body.pop("dry_run", None)
            body["use_local_db"] = False
            body.setdefault("update_prices", True)
            body.setdefault("update_returns", True)
            if not body.get("symbols"):
                symbols = [
                    str(item.get("symbol") or "")
                    for item in stock_api.get_all_symbols()
                    if item.get("symbol")
                ]
                scope = str(body.pop("stock_scope", "count"))
                stock_count = max(1, int(body.pop("stock_count", 50) or 50))
                range_from = str(body.pop("range_from", "") or "")
                range_to = str(body.pop("range_to", "") or "")
                if scope == "listed":
                    symbols = [symbol for symbol in symbols if symbol.endswith(".TW")]
                elif scope == "otc":
                    symbols = [symbol for symbol in symbols if symbol.endswith(".TWO")]
                elif scope == "range" and (range_from or range_to):
                    symbols = [
                        symbol for symbol in symbols
                        if (not range_from or symbol.split(".")[0] >= range_from)
                        and (not range_to or symbol.split(".")[0] <= range_to)
                    ]
                elif scope != "all":
                    symbols = symbols[:stock_count]
                body["symbols"] = symbols
            return client.post("/api/update", json=body)
        if job_type == "returns":
            body = dict(params)
            body.pop("dry_run", None)
            body["use_neon"] = True
            body["upload_to_neon"] = False
            body.setdefault("all", not bool(body.get("symbol") or body.get("symbols")))
            return client.post("/api/returns/compute", json=body)
        if job_type == "t86":
            return client.get(f"/api/t86/fetch?{urlencode(query)}")
        if job_type == "margin":
            return client.get(f"/api/margin/fetch?{urlencode(query)}")
        if job_type == "revenue":
            return client.get(f"/api/revenue/fetch_range?{urlencode(query)}")
        if job_type == "income_statement":
            query["write_to_db"] = "1"
            query.setdefault("retry_on_block", "1")
            return client.get(f"/api/income-statement?{urlencode(query)}")
        if job_type == "balance_sheet":
            query["write_to_db"] = "1"
            query.setdefault("retry_on_block", "1")
            return client.get(f"/api/balance-sheet?{urlencode(query)}")
        if job_type == "cash_flow":
            query["write_to_db"] = "1"
            query.setdefault("retry_on_block", "1")
            return client.get(f"/api/cash-flow-statement?{urlencode(query)}")
    raise ValueError(f"Unsupported job type: {job_type}")


def execute_job(job: dict[str, Any]) -> dict[str, Any]:
    job_id = str(job["id"])
    job_type = str(job["job_type"])
    params = dict(job.get("params") or {})
    if params.get("dry_run"):
        update_job(job_id, progress=90, message="Dry run validated")
        return {"dry_run": True, "job_type": job_type, "params": params}

    update_job(job_id, progress=5, message=f"Starting {job_type}")
    heartbeat = Heartbeat(job_id)
    heartbeat.start()
    try:
        response = _request_for_job(job_type, params)
        payload = response.get_json(silent=True)
        if response.status_code >= 400:
            detail = payload.get("error") if isinstance(payload, dict) else response.get_data(as_text=True)
            raise RuntimeError(f"HTTP {response.status_code}: {detail}")
        update_job(job_id, progress=95, message="Finalizing result")
        result = _compact_result(payload)
        result["http_status"] = response.status_code
        return result
    finally:
        heartbeat.stop()


def run_once() -> bool:
    job = claim_next_job(WORKER_ID)
    if not job:
        return False
    job_id = str(job["id"])
    logger.info("Claimed job %s type=%s attempt=%s", job_id, job["job_type"], job["attempts"])
    try:
        result = execute_job(job)
        complete_job(job_id, result)
        logger.info("Completed job %s", job_id)
    except Exception as exc:
        logger.exception("Job %s failed", job_id)
        fail_job(job_id, exc)
    return True


def main():
    logger.info("Worker %s started", WORKER_ID)
    run_once_mode = os.environ.get("WORKER_RUN_ONCE", "").lower() in {"1", "true", "yes"}
    while True:
        worked = run_once()
        if run_once_mode:
            return
        if not worked:
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
