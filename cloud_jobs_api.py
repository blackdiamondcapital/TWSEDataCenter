"""Flask blueprint exposing the persistent cloud job queue."""

from __future__ import annotations

import os
import secrets
from functools import wraps

from flask import Blueprint, jsonify, request

from cloud_jobs import (
    ALLOWED_JOB_TYPES,
    enqueue_job,
    get_job,
    list_jobs,
    serialize_job,
)

cloud_jobs_blueprint = Blueprint("cloud_jobs", __name__)


def _provided_token() -> str:
    header = request.headers.get("X-Admin-Token", "")
    if header:
        return header
    authorization = request.headers.get("Authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def require_admin(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        expected = os.environ.get("ADMIN_API_TOKEN", "")
        if not expected:
            return jsonify({"success": False, "error": "Cloud job API is not configured"}), 503
        if not secrets.compare_digest(_provided_token(), expected):
            return jsonify({"success": False, "error": "Unauthorized"}), 401
        return view(*args, **kwargs)

    return wrapped


@cloud_jobs_blueprint.route("/api/jobs", methods=["POST"])
@require_admin
def create_job():
    body = request.get_json(silent=True) or {}
    job_type = str(body.get("job_type") or "").strip()
    params = body.get("params") or {}
    if job_type not in ALLOWED_JOB_TYPES:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Unsupported job_type",
                    "allowed": sorted(ALLOWED_JOB_TYPES),
                }
            ),
            400,
        )
    if not isinstance(params, dict):
        return jsonify({"success": False, "error": "params must be an object"}), 400
    try:
        job = enqueue_job(
            job_type,
            params,
            max_attempts=body.get("max_attempts", 2),
        )
        return jsonify({"success": True, "job": serialize_job(job)}), 202
    except Exception:
        return jsonify({"success": False, "error": "Unable to create job"}), 500


@cloud_jobs_blueprint.route("/api/jobs", methods=["GET"])
@require_admin
def jobs_index():
    try:
        limit = int(request.args.get("limit", 20))
        return jsonify(
            {
                "success": True,
                "jobs": [serialize_job(job) for job in list_jobs(limit)],
            }
        )
    except Exception:
        return jsonify({"success": False, "error": "Unable to list jobs"}), 500


@cloud_jobs_blueprint.route("/api/jobs/<job_id>", methods=["GET"])
@require_admin
def job_detail(job_id: str):
    try:
        job = get_job(job_id)
        if not job:
            return jsonify({"success": False, "error": "Job not found"}), 404
        return jsonify({"success": True, "job": serialize_job(job)})
    except Exception:
        return jsonify({"success": False, "error": "Unable to load job"}), 500


@cloud_jobs_blueprint.route("/api/cloud/health", methods=["GET"])
def cloud_health():
    return jsonify(
        {
            "success": True,
            "service": "twse-data-center-api",
            "jobs_configured": bool(os.environ.get("ADMIN_API_TOKEN")),
            "database_configured": bool(
                os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
            ),
        }
    )
