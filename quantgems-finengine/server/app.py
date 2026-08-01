# -*- coding: utf-8 -*-
"""QuantGems 財報引擎 — 讀取 Neon 季報資料的 API。"""

from __future__ import annotations

import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import psycopg2
from flask import Flask, jsonify, request
from flask_cors import CORS
from psycopg2.extras import RealDictCursor

from labels import (
    EMPHASIS_KEYS,
    EPS_KEYS,
    LABELS_ZH,
    RATIO_KEYS,
    groups_for,
)

ROOT = Path(__file__).resolve().parents[1]
PARENT_ENV = ROOT.parent / ".env"
LOCAL_ENV = ROOT / ".env"


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    if PARENT_ENV.exists():
        load_dotenv(PARENT_ENV)
    if LOCAL_ENV.exists():
        load_dotenv(LOCAL_ENV, override=True)


_load_env()

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

TABLES = {
    "income": os.getenv("NEON_INCOME_STATEMENT_TABLE")
    or os.getenv("INCOME_STATEMENT_TABLE")
    or "tw_income_statement",
    "balance": os.getenv("NEON_BALANCE_SHEET_TABLE")
    or os.getenv("BALANCE_SHEET_TABLE")
    or "tw_balance_sheet",
    "cashflow": os.getenv("NEON_CASH_FLOW_TABLE")
    or os.getenv("CASH_FLOW_TABLE")
    or "tw_cash_flow_statement",
    "ratios": os.getenv("NEON_FINANCIAL_RATIOS_TABLE")
    or os.getenv("FINANCIAL_RATIOS_TABLE")
    or "tw_financial_ratios",
    "symbols": os.getenv("SYMBOLS_TABLE") or "tw_stock_symbols",
}


def get_conn():
    url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if url:
        return psycopg2.connect(url, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=os.getenv("DB_NAME", "postgres"),
        cursor_factory=RealDictCursor,
    )


def normalize_code(raw: str) -> str:
    s = (raw or "").strip().upper()
    s = s.replace(".TW", "").replace(".TWO", "")
    s = re.sub(r"[^0-9A-Z]", "", s)
    return s


def ratio_symbol(code: str) -> str:
    return f"{code}.TW"


def to_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def period_label(period: str) -> str:
    p = str(period or "")
    if len(p) >= 6 and p[:4].isdigit() and p[4:6].isdigit():
        y, s = p[:4], int(p[4:6])
        if 1 <= s <= 4:
            return f"{y} Q{s}"
    return p


def serialize_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if k in ("created_at", "updated_at"):
            out[k] = v.isoformat() if v else None
        elif isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


@app.get("/api/health")
def health():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()
        return jsonify({"ok": True, "service": "quantgems-finengine"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/meta")
def meta():
    return jsonify(
        {
            "brand": "QuantGems",
            "product": "財報引擎",
            "labels": LABELS_ZH,
            "groups": {
                "income": groups_for("income"),
                "balance": groups_for("balance"),
                "cashflow": groups_for("cashflow"),
            },
            "emphasis": sorted(EMPHASIS_KEYS),
            "epsKeys": sorted(EPS_KEYS),
            "ratioKeys": RATIO_KEYS,
        }
    )


@app.get("/api/symbols/search")
def search_symbols():
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify({"items": []})
    limit = min(int(request.args.get("limit") or 20), 50)
    code = normalize_code(q)
    like = f"%{q}%"
    table = TABLES["symbols"]
    sql = f"""
        SELECT symbol, name, short_name, market, industry
        FROM {table}
        WHERE symbol ILIKE %s
           OR short_name ILIKE %s
           OR name ILIKE %s
           OR REPLACE(symbol, '.TW', '') = %s
           OR REPLACE(symbol, '.TWO', '') = %s
        ORDER BY
          CASE
            WHEN REPLACE(symbol, '.TW', '') = %s OR REPLACE(symbol, '.TWO', '') = %s THEN 0
            WHEN symbol ILIKE %s THEN 1
            WHEN short_name ILIKE %s THEN 2
            ELSE 3
          END,
          symbol
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (like, like, like, code, code, code, code, f"{code}%", f"{q}%", limit),
            )
            rows = [serialize_row(r) for r in cur.fetchall()]
    for r in rows:
        r["code"] = normalize_code(r.get("symbol") or "")
        r["label"] = r.get("short_name") or r.get("name") or r["code"]
    return jsonify({"items": rows, "q": q})


@app.get("/api/reports/<code>/overview")
def report_overview(code: str):
    code = normalize_code(code)
    if not code:
        return jsonify({"error": "invalid code"}), 400

    income_t = TABLES["income"]
    balance_t = TABLES["balance"]
    cash_t = TABLES["cashflow"]
    ratios_t = TABLES["ratios"]
    symbols_t = TABLES["symbols"]
    rsym = ratio_symbol(code)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, name, short_name, market, industry
                FROM {symbols_t}
                WHERE REPLACE(symbol, '.TW', '') = %s
                   OR REPLACE(symbol, '.TWO', '') = %s
                LIMIT 1
                """,
                (code, code),
            )
            meta_row = cur.fetchone()

            cur.execute(
                f"""
                SELECT period FROM {income_t}
                WHERE "股票代號" = %s
                ORDER BY period DESC
                """,
                (code,),
            )
            periods = [r["period"] for r in cur.fetchall()]

            latest = periods[0] if periods else None
            highlight = None
            if latest:
                cur.execute(
                    f"""
                    SELECT
                      i.period,
                      i."Revenue" AS revenue,
                      i."GrossProfitFromOperations" AS gross_profit,
                      i."ProfitLossFromOperatingActivities" AS op_profit,
                      i."ProfitLoss" AS net_profit,
                      i."BasicEarningsLossPerShareTotal" AS eps,
                      b."Assets" AS assets,
                      b."Equity" AS equity,
                      b."Liabilities" AS liabilities,
                      c."NetCashFlowsFromUsedInOperatingActivities" AS cfo,
                      c."NetCashFlowsFromUsedInInvestingActivities" AS cfi,
                      c."NetCashFlowsFromUsedInFinancingActivities" AS cff
                    FROM {income_t} i
                    LEFT JOIN {balance_t} b
                      ON i."股票代號" = b."股票代號" AND i.period = b.period
                    LEFT JOIN {cash_t} c
                      ON i."股票代號" = c."股票代號" AND i.period = c.period
                    WHERE i."股票代號" = %s AND i.period = %s
                    """,
                    (code, latest),
                )
                highlight = cur.fetchone()

            cur.execute(
                f"""
                SELECT period, gross_margin, op_margin, net_margin, roa, roe,
                       debt_ratio, current_ratio, quick_ratio,
                       revenue, gross_profit, op_profit, net_profit, assets, equity
                FROM {ratios_t}
                WHERE symbol = %s OR symbol = %s OR REPLACE(symbol, '.TW', '') = %s
                ORDER BY period DESC
                LIMIT 16
                """,
                (rsym, code, code),
            )
            ratios = [serialize_row(r) for r in cur.fetchall()]

            # Trend series from income (last 12 periods)
            cur.execute(
                f"""
                SELECT period,
                       "Revenue" AS revenue,
                       "GrossProfitFromOperations" AS gross_profit,
                       "ProfitLossFromOperatingActivities" AS op_profit,
                       "ProfitLoss" AS net_profit,
                       "BasicEarningsLossPerShareTotal" AS eps
                FROM {income_t}
                WHERE "股票代號" = %s
                ORDER BY period DESC
                LIMIT 12
                """,
                (code,),
            )
            trend = list(reversed([serialize_row(r) for r in cur.fetchall()]))

    symbol = serialize_row(meta_row) if meta_row else {
        "symbol": f"{code}.TW",
        "name": None,
        "short_name": None,
        "market": None,
        "industry": None,
    }
    symbol["code"] = code
    symbol["label"] = symbol.get("short_name") or symbol.get("name") or code

    hl = serialize_row(highlight) if highlight else None
    if hl:
        hl["periodLabel"] = period_label(hl.get("period"))

    return jsonify(
        {
            "symbol": symbol,
            "periods": [{"value": p, "label": period_label(p)} for p in periods],
            "latestPeriod": latest,
            "highlight": hl,
            "ratios": [
                {**r, "periodLabel": period_label(r.get("period"))} for r in ratios
            ],
            "trend": [
                {**r, "periodLabel": period_label(r.get("period"))} for r in trend
            ],
        }
    )


@app.get("/api/reports/<code>/<kind>")
def report_statement(code: str, kind: str):
    code = normalize_code(code)
    kind = (kind or "").strip().lower()
    if kind not in TABLES or kind in ("ratios", "symbols"):
        return jsonify({"error": "kind must be income|balance|cashflow"}), 400
    if not code:
        return jsonify({"error": "invalid code"}), 400

    limit = min(int(request.args.get("limit") or 8), 20)
    periods_arg = (request.args.get("periods") or "").strip()
    table = TABLES[kind]

    with get_conn() as conn:
        with conn.cursor() as cur:
            if periods_arg:
                wanted = [p.strip() for p in periods_arg.split(",") if p.strip()]
                cur.execute(
                    f"""
                    SELECT * FROM {table}
                    WHERE "股票代號" = %s AND period = ANY(%s)
                    ORDER BY period DESC
                    """,
                    (code, wanted),
                )
            else:
                cur.execute(
                    f"""
                    SELECT * FROM {table}
                    WHERE "股票代號" = %s
                    ORDER BY period DESC
                    LIMIT %s
                    """,
                    (code, limit),
                )
            rows = [serialize_row(r) for r in cur.fetchall()]

    # Keep newest → oldest for table columns (left = latest)
    rows = rows[:limit]
    periods = [r.get("period") for r in rows]
    groups = groups_for(kind)

    lines = []
    for g in groups:
        section_lines = []
        for key in g["keys"]:
            values = {}
            any_val = False
            for r in rows:
                p = r.get("period")
                val = to_number(r.get(key))
                values[p] = val
                if val is not None:
                    any_val = True
            if not any_val:
                continue
            section_lines.append(
                {
                    "key": key,
                    "label": LABELS_ZH.get(key, key),
                    "emphasis": key in EMPHASIS_KEYS,
                    "isEps": key in EPS_KEYS,
                    "values": values,
                }
            )
        if section_lines:
            lines.append({"section": g["title"], "id": g["id"], "items": section_lines})

    return jsonify(
        {
            "code": code,
            "kind": kind,
            "periods": [{"value": p, "label": period_label(p)} for p in periods],
            "sections": lines,
            "raw": rows,
        }
    )


@app.get("/api/stats")
def stats():
    with get_conn() as conn:
        with conn.cursor() as cur:
            out = {}
            for key, table in TABLES.items():
                if key == "symbols":
                    continue
                cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
                out[key] = cur.fetchone()["n"]
            cur.execute(
                f'SELECT COUNT(DISTINCT "股票代號") AS n FROM {TABLES["income"]}'
            )
            out["companies"] = cur.fetchone()["n"]
            cur.execute(f"SELECT MAX(period) AS p FROM {TABLES['income']}")
            out["latestPeriod"] = cur.fetchone()["p"]
    return jsonify(out)


if __name__ == "__main__":
    port = int(os.getenv("FINENGINE_PORT", "8787"))
    app.run(host="0.0.0.0", port=port, debug=True)
