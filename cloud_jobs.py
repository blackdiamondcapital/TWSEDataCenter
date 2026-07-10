"""Persistent Neon-backed job queue shared by the API and Render worker."""

from __future__ import annotations

import os
import socket
import uuid
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor

ALLOWED_JOB_TYPES = {
    "stock_prices",
    "returns",
    "t86",
    "margin",
    "revenue",
    "income_statement",
    "balance_sheet",
    "cash_flow",
}


def database_url() -> str:
    value = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL or NEON_DATABASE_URL is required")
    return value


def connect():
    return psycopg2.connect(database_url(), cursor_factory=RealDictCursor)


def ensure_job_table(conn=None) -> None:
    own_connection = conn is None
    db = conn or connect()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tw_job_runs (
                    id UUID PRIMARY KEY,
                    job_type VARCHAR(64) NOT NULL,
                    params JSONB NOT NULL DEFAULT '{}'::jsonb,
                    status VARCHAR(24) NOT NULL DEFAULT 'queued',
                    progress INTEGER NOT NULL DEFAULT 0,
                    current_item VARCHAR(128),
                    message TEXT,
                    result JSONB,
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 2,
                    locked_by VARCHAR(128),
                    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    heartbeat_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT tw_job_runs_status_check
                        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
                    CONSTRAINT tw_job_runs_progress_check
                        CHECK (progress >= 0 AND progress <= 100)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS tw_job_runs_queue_idx
                ON tw_job_runs(status, queued_at)
                """
            )
        db.commit()
    finally:
        if own_connection:
            db.close()


def enqueue_job(
    job_type: str,
    params: Optional[dict[str, Any]] = None,
    *,
    max_attempts: int = 2,
) -> dict[str, Any]:
    if job_type not in ALLOWED_JOB_TYPES:
        raise ValueError(f"Unsupported job_type: {job_type}")
    job_id = str(uuid.uuid4())
    with closing(connect()) as db, db:
        ensure_job_table(db)
        with db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tw_job_runs (id, job_type, params, max_attempts)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (job_id, job_type, Json(params or {}), max(1, min(int(max_attempts), 5))),
            )
            return dict(cursor.fetchone())


def get_job(job_id: str) -> Optional[dict[str, Any]]:
    with closing(connect()) as db, db:
        ensure_job_table(db)
        with db.cursor() as cursor:
            cursor.execute("SELECT * FROM tw_job_runs WHERE id = %s", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None


def list_jobs(limit: int = 20) -> list[dict[str, Any]]:
    with closing(connect()) as db, db:
        ensure_job_table(db)
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM tw_job_runs
                ORDER BY queued_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit), 100)),),
            )
            return [dict(row) for row in cursor.fetchall()]


def claim_next_job(worker_id: Optional[str] = None) -> Optional[dict[str, Any]]:
    worker = worker_id or f"{socket.gethostname()}:{os.getpid()}"
    with closing(connect()) as db, db:
        ensure_job_table(db)
        with db.cursor() as cursor:
            # A worker killed during deployment relinquishes its job after 15 minutes.
            cursor.execute(
                """
                UPDATE tw_job_runs
                SET status = 'queued', locked_by = NULL, started_at = NULL,
                    message = 'Recovered after stale worker', updated_at = NOW()
                WHERE status = 'running'
                  AND heartbeat_at < NOW() - INTERVAL '15 minutes'
                  AND attempts < max_attempts
                """
            )
            cursor.execute(
                """
                WITH next_job AS (
                    SELECT id
                    FROM tw_job_runs
                    WHERE status = 'queued'
                    ORDER BY queued_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE tw_job_runs AS jobs
                SET status = 'running',
                    attempts = attempts + 1,
                    locked_by = %s,
                    started_at = COALESCE(started_at, NOW()),
                    heartbeat_at = NOW(),
                    updated_at = NOW(),
                    message = 'Worker started'
                FROM next_job
                WHERE jobs.id = next_job.id
                RETURNING jobs.*
                """,
                (worker,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def update_job(
    job_id: str,
    *,
    progress: Optional[int] = None,
    current_item: Optional[str] = None,
    message: Optional[str] = None,
    result: Optional[dict[str, Any]] = None,
) -> None:
    assignments = ["heartbeat_at = NOW()", "updated_at = NOW()"]
    values: list[Any] = []
    if progress is not None:
        assignments.append("progress = %s")
        values.append(max(0, min(int(progress), 99)))
    if current_item is not None:
        assignments.append("current_item = %s")
        values.append(str(current_item)[:128])
    if message is not None:
        assignments.append("message = %s")
        values.append(str(message))
    if result is not None:
        assignments.append("result = %s")
        values.append(Json(result))
    values.append(job_id)
    with closing(connect()) as db, db:
        with db.cursor() as cursor:
            cursor.execute(
                f"UPDATE tw_job_runs SET {', '.join(assignments)} WHERE id = %s",
                values,
            )


def complete_job(job_id: str, result: Optional[dict[str, Any]] = None) -> None:
    with closing(connect()) as db, db:
        with db.cursor() as cursor:
            cursor.execute(
                """
                UPDATE tw_job_runs
                SET status = 'succeeded', progress = 100, result = %s,
                    message = 'Completed', finished_at = NOW(),
                    heartbeat_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (Json(result or {}), job_id),
            )


def fail_job(job_id: str, error: Exception | str) -> None:
    with closing(connect()) as db, db:
        with db.cursor() as cursor:
            cursor.execute(
                """
                UPDATE tw_job_runs
                SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
                    error = %s,
                    message = CASE WHEN attempts < max_attempts
                        THEN 'Retry queued' ELSE 'Failed' END,
                    locked_by = NULL,
                    heartbeat_at = NOW(),
                    finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE NOW() END,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (str(error)[:8000], job_id),
            )


def serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    serialized = dict(job)
    for key, value in list(serialized.items()):
        if isinstance(value, datetime):
            serialized[key] = value.astimezone(timezone.utc).isoformat()
        elif isinstance(value, uuid.UUID):
            serialized[key] = str(value)
    return serialized
