#!/usr/bin/env python3
"""本機回補上櫃權證日線到 Neon／設定中的資料庫（不依賴完整 server.py）。

用法：
  python3 scripts/backfill_tpex_warrant_daily.py
  python3 scripts/backfill_tpex_warrant_daily.py --start 2025-11-01 --end 2026-07-31
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import requests
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv():
    env_path = ROOT / '.env'
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding='utf-8', errors='ignore').splitlines():
        s = line.strip()
        if not s or s.startswith('#') or '=' not in s:
            continue
        key, val = s.split('=', 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _parse_roc_date_text(text):
    if not text:
        return None
    s = str(text).strip().replace('/', '').replace('-', '')
    if not s.isdigit():
        return None
    try:
        if len(s) == 7:
            return date(int(s[:3]) + 1911, int(s[3:5]), int(s[5:7]))
        if len(s) == 8:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None
    return None


def _to_decimal_or_none(val):
    if val is None:
        return None
    s = str(val).replace(',', '').strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_int_or_none(val):
    num = _to_decimal_or_none(val)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


def _db_url():
    for key in ('DATABASE_URL', 'NEON_DATABASE_URL', 'POSTGRES_URL'):
        v = (os.environ.get(key) or '').strip()
        if v:
            return v
    return ''


def _connect():
    import psycopg2

    url = _db_url()
    if not url:
        raise RuntimeError('缺少 DATABASE_URL / NEON_DATABASE_URL')
    parsed = urlparse(url)
    qs = parse_qs(parsed.query or '')
    sslmode = (qs.get('sslmode') or ['require'])[0]
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=(parsed.path or '/').lstrip('/') or None,
        user=unquote(parsed.username or ''),
        password=unquote(parsed.password or ''),
        sslmode=sslmode,
    )
    return conn


def fetch_day(trade_date_obj: date, retries: int = 4) -> list[dict]:
    d_param = trade_date_obj.strftime('%Y/%m/%d')
    url = (
        'https://www.tpex.org.tw/web/extend/warrant/dailyQ/wntQuts_result.php'
        f'?l=zh-tw&t=D&o=data&d={d_param}'
    )
    last_err = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            resp = requests.get(url, timeout=90, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code != 200:
                raise RuntimeError(f'HTTP {resp.status_code} ({d_param})')
            text = resp.content.decode('utf-8-sig', errors='replace').strip()
            if not text:
                return []
            out = []
            for row in csv.DictReader(io.StringIO(text)):
                raw_date = (row.get('資料日期') or '').strip()
                code = (row.get('代號') or '').strip()
                if not raw_date or not code:
                    continue
                parsed = _parse_roc_date_text(raw_date)
                if parsed is not None and parsed != trade_date_obj:
                    continue
                out.append({
                    'Date': raw_date,
                    'Code': code,
                    'Name': (row.get('名稱') or '').strip(),
                    'Open': row.get('開市價'),
                    'High': row.get('最高價'),
                    'Low': row.get('最低價'),
                    'Close': row.get('收市價'),
                    'Change': row.get('漲跌'),
                    'TradeVol.': row.get('成交量'),
                    'No.OfTransactions': row.get('筆數'),
                    'TradeValue': row.get('成交金額'),
                    'UnderlyingStockCode': (row.get('標的代號') or '').strip(),
                    'UnderlyingStock': (row.get('證券') or '').strip(),
                    'UnderlyingStockClosePrice': row.get('標的或指數收盤'),
                    'UnderlyingStock PriceChange': row.get('標的或指數漲跌'),
                })
            return out
        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(1.2 * attempt)
    raise RuntimeError(f'fetch failed {d_param}: {last_err}')


SQL = """
    INSERT INTO tpex_warrant_daily_quotes (
        trade_date, warrant_code, warrant_name, open_price, high_price, low_price,
        close_price, price_change, trade_volume, transaction_count, trade_value,
        underlying_code, underlying_name, underlying_close_price,
        underlying_price_change, raw_trade_date_text, updated_at
    ) VALUES %s
    ON CONFLICT (trade_date, warrant_code) DO UPDATE SET
        warrant_name = EXCLUDED.warrant_name,
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        price_change = EXCLUDED.price_change,
        trade_volume = EXCLUDED.trade_volume,
        transaction_count = EXCLUDED.transaction_count,
        trade_value = EXCLUDED.trade_value,
        underlying_code = EXCLUDED.underlying_code,
        underlying_name = EXCLUDED.underlying_name,
        underlying_close_price = EXCLUDED.underlying_close_price,
        underlying_price_change = EXCLUDED.underlying_price_change,
        raw_trade_date_text = EXCLUDED.raw_trade_date_text,
        updated_at = NOW()
"""


def import_rows(cursor, data: list[dict]) -> int:
    rows = []
    for item in data:
        trade_date = _parse_roc_date_text(item.get('Date'))
        code = str(item.get('Code') or '').strip()
        if trade_date is None or not code:
            continue
        rows.append((
            trade_date,
            code,
            str(item.get('Name') or '').strip() or None,
            _to_decimal_or_none(item.get('Open')),
            _to_decimal_or_none(item.get('High')),
            _to_decimal_or_none(item.get('Low')),
            _to_decimal_or_none(item.get('Close')),
            _to_decimal_or_none(item.get('Change')),
            _to_int_or_none(item.get('TradeVol.')),
            _to_int_or_none(item.get('No.OfTransactions')),
            _to_decimal_or_none(item.get('TradeValue')),
            str(item.get('UnderlyingStockCode') or '').strip() or None,
            str(item.get('UnderlyingStock') or '').strip() or None,
            _to_decimal_or_none(item.get('UnderlyingStockClosePrice')),
            _to_decimal_or_none(item.get('UnderlyingStock PriceChange')),
            item.get('Date'),
        ))
    if not rows:
        return 0
    execute_values(
        cursor,
        SQL,
        rows,
        template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())',
        page_size=1000,
    )
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description='Backfill TPEX warrant daily quotes')
    parser.add_argument('--start', default='', help='YYYY-MM-DD（預設 end 往前 270 天）')
    parser.add_argument('--end', default='', help='YYYY-MM-DD（預設今天）')
    parser.add_argument('--sleep', type=float, default=0.25, help='每日間隔秒數')
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='單日失敗時略過並繼續（預設遇錯中止）',
    )
    args = parser.parse_args()

    _load_dotenv()
    end_date = date.fromisoformat(args.end) if args.end else date.today()
    start_date = (
        date.fromisoformat(args.start)
        if args.start
        else (end_date - timedelta(days=270))
    )

    print(
        f'[{datetime.now().isoformat(timespec="seconds")}] '
        f'backfill TPEX warrant daily {start_date} -> {end_date}',
        flush=True,
    )

    conn = _connect()
    imported_days = skipped_days = imported_count = failed_days = 0
    try:
        cur = conn.cursor()
        # 已有資料的日期跳過，方便斷點續跑
        cur.execute(
            """
            SELECT DISTINCT trade_date
            FROM tpex_warrant_daily_quotes
            WHERE trade_date BETWEEN %s AND %s
            """,
            (start_date, end_date),
        )
        existing = {r[0] for r in (cur.fetchall() or []) if r and r[0]}
        if existing:
            print(f'  existing days in range: {len(existing)}', flush=True)

        d = start_date
        while d <= end_date:
            if d in existing:
                skipped_days += 1
                print(f'  have {d.isoformat()} (skip)', flush=True)
                d += timedelta(days=1)
                continue
            try:
                data = fetch_day(d)
                if not data:
                    skipped_days += 1
                    print(f'  skip {d.isoformat()} (no data)', flush=True)
                else:
                    n = import_rows(cur, data)
                    conn.commit()
                    imported_days += 1
                    imported_count += n
                    existing.add(d)
                    print(f'  ok   {d.isoformat()} rows={n}', flush=True)
            except Exception as e:
                conn.rollback()
                print(f'  FAIL {d.isoformat()}: {e}', file=sys.stderr, flush=True)
                if args.continue_on_error:
                    failed_days += 1
                else:
                    raise
            if args.sleep > 0 and d < end_date:
                time.sleep(args.sleep)
            d += timedelta(days=1)
    finally:
        conn.close()

    print(
        'OK',
        {
            'start': start_date.isoformat(),
            'end': end_date.isoformat(),
            'importedDays': imported_days,
            'skippedDays': skipped_days,
            'failedDays': failed_days,
            'importedCount': imported_count,
        },
        flush=True,
    )
    return 1 if failed_days else 0


if __name__ == '__main__':
    raise SystemExit(main())
