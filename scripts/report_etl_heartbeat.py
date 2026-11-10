#!/usr/bin/env python3
"""Best-effort ETL heartbeat reporter for scheduled GitHub Actions."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", required=True, choices=("started", "success", "failure", "cancelled", "skipped"))
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--slot", default="")
    parser.add_argument("--data-as-of", default="")
    parser.add_argument("--rows", default="")
    return parser.parse_args()


def warning(message: str) -> None:
    print(f"::warning title=ETL heartbeat::{message}", file=sys.stderr)


def main() -> int:
    args = parse_args()
    base_url = os.environ.get("MONITOR_API_URL", "").strip()
    secret = os.environ.get("ETL_REPORT_SECRET", "").strip()
    if not base_url or not secret:
        warning("MONITOR_API_URL 或 ETL_REPORT_SECRET 未設定，略過回報")
        return 0

    job_keys = {
        "stock_price_fetch": "dc-tw-stock",
        "TWII": "dc-twii",
        "etf-daily-price-fetch": "dc-etf",
        "t86-daily": "dc-t86",
        "bwibbu-margin-daily": "dc-bwibbu-margin",
        "board-financials-fetch": "dc-board-financials",
        "monthly-revenue-fetch": "dc-monthly-revenue",
        "quarterly-statements-fetch": "dc-quarterly-statements",
    }
    normalized_workflow = args.workflow.removesuffix(".yml")
    payload = {
        "job_key": job_keys.get(normalized_workflow, normalized_workflow),
        "status": args.status,
        "repo": os.environ.get("GITHUB_REPOSITORY", ""),
        "workflow": args.workflow,
        "slot": args.slot,
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "run_url": (
            f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
            f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/"
            f"{os.environ.get('GITHUB_RUN_ID', '')}"
        ),
        "data_as_of": args.data_as_of,
        "rows": args.rows,
    }
    url = f"{base_url.rstrip('/')}/api/heartbeat"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {secret}",
            "X-ETL-Report-Secret": secret,
            "Content-Type": "application/json",
            "User-Agent": "TWSEDataCenter-ETL/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            if not 200 <= response.status < 300:
                warning(f"回報失敗：HTTP {response.status}")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        warning(f"回報失敗：{exc}")
    except Exception as exc:  # Monitoring must never break the ETL.
        warning(f"未預期回報錯誤：{exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
