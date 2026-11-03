#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""從公開資訊觀測站「歷史重大訊息」抓董事會通過財務報告（第31款）。

頁面：https://mopsov.twse.com.tw/mops/web/t05st01
流程與網站相同：年度／月份／日期（可加公司代號）→ 清單 → 詳細資料。
解析說明欄 1～14（董事會日期、期間、營收、毛利、EPS、資產負債權益等）。
"""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import date, datetime, timedelta
from html import unescape

import psycopg2
import requests
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

TABLE_NAME = os.environ.get("BOARD_FINANCIALS_TABLE", "tw_board_financials")
MOPS_HOME = "https://mopsov.twse.com.tw/mops/web/t05st01"
MOPS_AJAX = "https://mopsov.twse.com.tw/mops/web/ajax_t05st01"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}
NA_TEXT = {"", "-", "--", "---", "n/a", "na", "none", "null", "無", "不適用", "不适用"}
TITLE_KEEP = re.compile(r"(董事會通過|董事會決議通過|業經董事會決議通過).{0,40}(財務報告|自結財務)")
TITLE_SKIP = re.compile(r"預計提報|預計召開|召集通知")
LIST_ONCLICK = re.compile(
    r"seq_no\.value='(\d+)';"
    r"document\.t05st01_fm\.spoke_time\.value='(\d+)';"
    r"document\.t05st01_fm\.spoke_date\.value='(\d+)';"
    r"document\.t05st01_fm\.co_id\.value='([^']+)';"
    r"document\.t05st01_fm\.TYPEK\.value='([^']+)'",
    re.I,
)


def log(msg: str) -> None:
    print(msg, flush=True)


def clean_text(raw) -> str:
    text = unescape(str(raw or ""))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ").replace("&nbsp;", " ")).strip()


def parse_roc_date(raw) -> date | None:
    text = str(raw or "").strip()
    if not text or text.lower() in NA_TEXT:
        return None
    compact = re.sub(r"\D", "", text)
    try:
        if len(compact) == 7:
            return date(int(compact[:3]) + 1911, int(compact[3:5]), int(compact[5:7]))
        if len(compact) == 8:
            year = int(compact[:4])
            if year < 1912:
                year += 1911
            return date(year, int(compact[4:6]), int(compact[6:8]))
    except ValueError:
        return None
    m = re.search(r"(\d{2,4})[./\-年](\d{1,2})[./\-月](\d{1,2})", text)
    if not m:
        return None
    year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 1912:
        year += 1911
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_amount(raw):
    text = str(raw or "").strip()
    if not text or text.lower() in NA_TEXT:
        return None
    neg = False
    if (text.startswith("(") and text.endswith(")")) or (text.startswith("（") and text.endswith("）")):
        neg = True
        text = text[1:-1]
    text = text.replace(",", "").replace("，", "").replace(" ", "")
    if text.startswith("-"):
        neg = True
        text = text[1:]
    if not text or text.lower() in NA_TEXT:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return -number if neg else number


def parse_int_amount(raw):
    number = parse_amount(raw)
    return None if number is None else int(round(number))


def season_from_end(end: date | None) -> int | None:
    return None if not end else {3: 1, 6: 2, 9: 3, 12: 4}.get(end.month)


def is_board_fs_title(title: str) -> bool:
    text = str(title or "")
    if TITLE_SKIP.search(text):
        return False
    if TITLE_KEEP.search(text):
        return True
    return "財務報告" in text and "董事會" in text and "通過" in text


def is_board_fs_body(body: str) -> bool:
    text = str(body or "")
    return bool(
        "基本每股盈餘" in text
        and "營業收入" in text
        and re.search(r"1\.提報董事會", text)
    )


def _field(body: str, pattern: str) -> str:
    m = re.search(pattern, body, re.S)
    if not m:
        return ""
    return re.split(r"\s+\d+\.", m.group(1).strip(), maxsplit=1)[0].strip()


def parse_explanation(body: str) -> dict:
    text = re.sub(r"[ \t]+", " ", str(body or "").replace("\r\n", "\n").replace("\r", "\n"))
    period_blob = _field(
        text,
        r"3\.財務報告或年度自結財務資訊報導期間[\s\S]{0,80}?"
        r"起訖日期(?:\(XXX/XX/XX~XXX/XX/XX\))?[:：]\s*([^\n]+)",
    )
    period_start = period_end = None
    pm = re.search(
        r"(\d{2,4}/\d{1,2}/\d{1,2})\s*[~～\-至]+\s*(\d{2,4}/\d{1,2}/\d{1,2})",
        period_blob or text,
    )
    if pm:
        period_start = parse_roc_date(pm.group(1))
        period_end = parse_roc_date(pm.group(2))
    other = _field(text, r"14\.其他應敘明事項[:：]\s*([\s\S]+)$")
    other = re.sub(r"\n?以上資料均由各公司[\s\S]*$", "", other).strip()
    return {
        "board_date": parse_roc_date(_field(text, r"1\.提報董事會或經董事會決議日期[:：]\s*([^\n]+)")),
        "audit_committee_date": parse_roc_date(_field(text, r"2\.審計委員會通過日期[:：]\s*([^\n]+)")),
        "period_start": period_start,
        "period_end": period_end,
        "revenue": parse_int_amount(_field(text, r"4\.1月1日累計至本期止營業收入\(仟元\)[:：]\s*([^\n]+)")),
        "gross_profit": parse_int_amount(_field(text, r"5\.1月1日累計至本期止營業毛利\(毛損\) \(仟元\)[:：]\s*([^\n]+)")),
        "operating_profit": parse_int_amount(_field(text, r"6\.1月1日累計至本期止營業利益\(損失\) \(仟元\)[:：]\s*([^\n]+)")),
        "pretax_profit": parse_int_amount(_field(text, r"7\.1月1日累計至本期止稅前淨利\(淨損\) \(仟元\)[:：]\s*([^\n]+)")),
        "net_profit": parse_int_amount(_field(text, r"8\.1月1日累計至本期止本期淨利\(淨損\) \(仟元\)[:：]\s*([^\n]+)")),
        "parent_net_profit": parse_int_amount(
            _field(text, r"9\.1月1日累計至本期止歸屬於母公司業主淨利\(損\) \(仟元\)[:：]\s*([^\n]+)")
        ),
        "eps": parse_amount(_field(text, r"10\.1月1日累計至本期止基本每股盈餘\(損失\) \(元\)[:：]\s*([^\n]+)")),
        "total_assets": parse_int_amount(_field(text, r"11\.期末總資產\(仟元\)[:：]\s*([^\n]+)")),
        "total_liabilities": parse_int_amount(_field(text, r"12\.期末總負債\(仟元\)[:：]\s*([^\n]+)")),
        "parent_equity": parse_int_amount(_field(text, r"13\.期末歸屬於母公司業主之權益\(仟元\)[:：]\s*([^\n]+)")),
        "other_notes": (other[:2000] if other else None),
    }


def html_to_text(html: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|tr)>", "\n", text)
    text = re.sub(r"(?i)</td>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\n+", "\n", re.sub(r"[ \t]+", " ", text)).strip()


def new_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    return sess


def list_mops(
    sess: requests.Session,
    day_from: date,
    day_to: date,
    typek: str,
    co_id: str = "",
) -> list[dict]:
    if day_from.year != day_to.year or day_from.month != day_to.month:
        raise ValueError("list_mops 同一請求須在同一年月")
    payload = {
        "encodeURIComponent": "1",
        "step": "1",
        "firstin": "1",
        "off": "1",
        "keyword4": "",
        "code1": "",
        "TYPEK2": "",
        "checkbtn": "",
        "queryName": "co_id",
        "TYPEK": typek,
        "isnew": "false",
        "co_id": co_id,
        "year": str(day_from.year - 1911),
        "month": f"{day_from.month:02d}",
        "b_date": f"{day_from.day:02d}",
        "e_date": f"{day_to.day:02d}",
    }
    resp = sess.post(MOPS_AJAX, data=payload, timeout=45)
    resp.raise_for_status()
    html = resp.text
    rows = []
    for m in LIST_ONCLICK.finditer(html):
        window = html[max(0, m.start() - 700): m.start()]
        title_m = re.search(r"<font[^>]*>(.*?)</font>", window, re.S | re.I)
        title = clean_text(re.sub(r"<[^>]+>", "", title_m.group(1) if title_m else ""))
        if not is_board_fs_title(title):
            continue
        rows.append(
            {
                "seq_no": m.group(1),
                "spoke_time": m.group(2),
                "spoke_date": m.group(3),
                "co_id": m.group(4),
                "TYPEK": m.group(5),
                "title": title,
            }
        )
    return rows


def fetch_detail(sess: requests.Session, row: dict) -> dict | None:
    payload = {
        "encodeURIComponent": "1",
        "firstin": "true",
        "off": "1",
        "step": "2",
        "seq_no": row["seq_no"],
        "spoke_time": row["spoke_time"],
        "spoke_date": row["spoke_date"],
        "co_id": row["co_id"],
        "TYPEK": row["TYPEK"],
    }
    resp = sess.post(MOPS_AJAX, data=payload, timeout=40)
    resp.raise_for_status()
    body = html_to_text(resp.text)
    if not is_board_fs_body(body):
        return None
    parsed = parse_explanation(body)
    if parsed["period_end"] is None and parsed["eps"] is None and parsed["revenue"] is None:
        return None
    typek = str(row.get("TYPEK") or "").lower()
    market = "TWSE" if typek == "sii" else "TPEX" if typek == "otc" else typek.upper()
    subject_m = re.search(r"主旨\s+(.+?)\n符合條款", body, re.S)
    name_m = re.search(r"本資料由[^\n]*?\)\s*\d+\s*([^\n]+?)\s+公司提供", body)
    return {
        "stock_no": str(row["co_id"]).strip(),
        "stock_name": (clean_text(name_m.group(1)) if name_m else None),
        "market": market,
        "speak_date": parse_roc_date(row["spoke_date"]),
        "speak_time": str(row["spoke_time"]).strip(),
        "seq_no": int(row["seq_no"]),
        "subject": clean_text(subject_m.group(1) if subject_m else row.get("title")),
        "clause": "第31款",
        "fact_date": parse_roc_date(_field(body, r"事實發生日\s+([^\n]+)")),
        **parsed,
        "season": season_from_end(parsed["period_end"]),
        "source_text": body[:8000],
    }


def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks = []
    cur = start
    while cur <= end:
        month_end = date(cur.year + (cur.month == 12), 1 if cur.month == 12 else cur.month + 1, 1) - timedelta(days=1)
        chunk_end = min(end, month_end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def fetch_t05st01(start: date, end: date, stock: str = "", sleep_s: float = 0.85) -> list[dict]:
    sess = new_session()
    try:
        sess.get(MOPS_HOME, timeout=20)
    except Exception as exc:
        log(f"⚠️ t05st01 預熱失敗：{exc}")
    markets = ("all",) if stock else ("sii", "otc")
    records = []
    seen = set()
    for chunk_from, chunk_to in month_chunks(start, end):
        for typek in markets:
            label = stock or typek
            try:
                rows = list_mops(sess, chunk_from, chunk_to, typek, co_id=stock)
            except Exception as exc:
                log(f"⚠️ 清單 {chunk_from}~{chunk_to} {label} 失敗：{exc}")
                time.sleep(sleep_s)
                continue
            log(f"… t05st01 {chunk_from}~{chunk_to} {label} 董事會財報 {len(rows)} 則")
            for row in rows:
                key = (row["co_id"], row["spoke_date"], row["spoke_time"], row["seq_no"])
                if key in seen:
                    continue
                seen.add(key)
                try:
                    rec = fetch_detail(sess, row)
                except Exception as exc:
                    log(f"⚠️ 詳細資料 {row.get('co_id')} 失敗：{exc}")
                    rec = None
                if rec:
                    records.append(rec)
                time.sleep(sleep_s)
            time.sleep(0.4)
    return records


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                stock_no VARCHAR(20) NOT NULL,
                stock_name VARCHAR(100),
                market VARCHAR(10),
                speak_date DATE NOT NULL,
                speak_time VARCHAR(16) NOT NULL DEFAULT '',
                seq_no INTEGER NOT NULL DEFAULT 0,
                subject TEXT,
                clause VARCHAR(40),
                fact_date DATE,
                board_date DATE,
                audit_committee_date DATE,
                period_start DATE,
                period_end DATE,
                season SMALLINT,
                revenue BIGINT,
                gross_profit BIGINT,
                operating_profit BIGINT,
                pretax_profit BIGINT,
                net_profit BIGINT,
                parent_net_profit BIGINT,
                eps NUMERIC(18,4),
                total_assets BIGINT,
                total_liabilities BIGINT,
                parent_equity BIGINT,
                other_notes TEXT,
                source_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stock_no, speak_date, speak_time, seq_no)
            );
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS {TABLE_NAME}_period_idx ON {TABLE_NAME}(period_end, stock_no);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {TABLE_NAME}_stock_idx ON {TABLE_NAME}(stock_no, period_end DESC);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS {TABLE_NAME}_speak_idx ON {TABLE_NAME}(speak_date);")
    conn.commit()


def upsert_records(records: list[dict], db_url: str) -> int:
    if not records:
        return 0
    conn = psycopg2.connect(
        db_url, connect_timeout=30, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5
    )
    try:
        ensure_table(conn)
        values, seen = [], set()
        for rec in records:
            stock_no = (rec.get("stock_no") or "").strip()
            speak_date = rec.get("speak_date")
            if not stock_no or not speak_date:
                continue
            speak_time = str(rec.get("speak_time") or "").strip()
            try:
                seq_no = int(rec.get("seq_no") or 0)
            except (TypeError, ValueError):
                seq_no = 0
            key = (stock_no, speak_date, speak_time, seq_no)
            if key in seen:
                continue
            seen.add(key)
            values.append(
                (
                    stock_no, rec.get("stock_name"), rec.get("market"), speak_date, speak_time, seq_no,
                    rec.get("subject"), rec.get("clause"), rec.get("fact_date"), rec.get("board_date"),
                    rec.get("audit_committee_date"), rec.get("period_start"), rec.get("period_end"), rec.get("season"),
                    rec.get("revenue"), rec.get("gross_profit"), rec.get("operating_profit"), rec.get("pretax_profit"),
                    rec.get("net_profit"), rec.get("parent_net_profit"), rec.get("eps"),
                    rec.get("total_assets"), rec.get("total_liabilities"), rec.get("parent_equity"),
                    rec.get("other_notes"), rec.get("source_text"),
                )
            )
        if not values:
            return 0
        sql = f"""
            INSERT INTO {TABLE_NAME} (
                stock_no, stock_name, market, speak_date, speak_time, seq_no,
                subject, clause, fact_date, board_date, audit_committee_date,
                period_start, period_end, season,
                revenue, gross_profit, operating_profit, pretax_profit,
                net_profit, parent_net_profit, eps,
                total_assets, total_liabilities, parent_equity,
                other_notes, source_text
            )
            VALUES %s
            ON CONFLICT (stock_no, speak_date, speak_time, seq_no) DO UPDATE SET
                stock_name = COALESCE(EXCLUDED.stock_name, {TABLE_NAME}.stock_name),
                market = COALESCE(EXCLUDED.market, {TABLE_NAME}.market),
                subject = COALESCE(EXCLUDED.subject, {TABLE_NAME}.subject),
                clause = COALESCE(EXCLUDED.clause, {TABLE_NAME}.clause),
                fact_date = COALESCE(EXCLUDED.fact_date, {TABLE_NAME}.fact_date),
                board_date = COALESCE(EXCLUDED.board_date, {TABLE_NAME}.board_date),
                audit_committee_date = COALESCE(EXCLUDED.audit_committee_date, {TABLE_NAME}.audit_committee_date),
                period_start = COALESCE(EXCLUDED.period_start, {TABLE_NAME}.period_start),
                period_end = COALESCE(EXCLUDED.period_end, {TABLE_NAME}.period_end),
                season = COALESCE(EXCLUDED.season, {TABLE_NAME}.season),
                revenue = COALESCE(EXCLUDED.revenue, {TABLE_NAME}.revenue),
                gross_profit = COALESCE(EXCLUDED.gross_profit, {TABLE_NAME}.gross_profit),
                operating_profit = COALESCE(EXCLUDED.operating_profit, {TABLE_NAME}.operating_profit),
                pretax_profit = COALESCE(EXCLUDED.pretax_profit, {TABLE_NAME}.pretax_profit),
                net_profit = COALESCE(EXCLUDED.net_profit, {TABLE_NAME}.net_profit),
                parent_net_profit = COALESCE(EXCLUDED.parent_net_profit, {TABLE_NAME}.parent_net_profit),
                eps = COALESCE(EXCLUDED.eps, {TABLE_NAME}.eps),
                total_assets = COALESCE(EXCLUDED.total_assets, {TABLE_NAME}.total_assets),
                total_liabilities = COALESCE(EXCLUDED.total_liabilities, {TABLE_NAME}.total_liabilities),
                parent_equity = COALESCE(EXCLUDED.parent_equity, {TABLE_NAME}.parent_equity),
                other_notes = COALESCE(EXCLUDED.other_notes, {TABLE_NAME}.other_notes),
                source_text = COALESCE(EXCLUDED.source_text, {TABLE_NAME}.source_text),
                updated_at = CURRENT_TIMESTAMP
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
        conn.commit()
        return len(values)
    finally:
        conn.close()


SAMPLE = (
    "1.提報董事會或經董事會決議日期:113/11/12 2.審計委員會通過日期:113/11/12 "
    "3.財務報告或年度自結財務資訊報導期間 起訖日期(XXX/XX/XX~XXX/XX/XX):113/01/01~113/09/30 "
    "4.1月1日累計至本期止營業收入(仟元):17,716,766 5.1月1日累計至本期止營業毛利(毛損) (仟元):6,573,862 "
    "6.1月1日累計至本期止營業利益(損失) (仟元):4,230,161 7.1月1日累計至本期止稅前淨利(淨損) (仟元):4,786,922 "
    "8.1月1日累計至本期止本期淨利(淨損) (仟元):3,477,729 9.1月1日累計至本期止歸屬於母公司業主淨利(損) (仟元):3,465,087 "
    "10.1月1日累計至本期止基本每股盈餘(損失) (元):30.73 11.期末總資產(仟元):42,682,670 "
    "12.期末總負債(仟元):18,652,355 13.期末歸屬於母公司業主之權益(仟元):24,059,117 14.其他應敘明事項:無"
)


def self_test() -> int:
    parsed = parse_explanation(SAMPLE)
    assert parsed["board_date"] == date(2024, 11, 12), parsed
    assert parsed["period_start"] == date(2024, 1, 1), parsed
    assert parsed["period_end"] == date(2024, 9, 30), parsed
    assert parsed["revenue"] == 17716766, parsed
    assert parsed["eps"] == 30.73, parsed
    assert parsed["parent_equity"] == 24059117, parsed
    assert parsed["other_notes"] == "無", parsed
    neg = parse_explanation("6.1月1日累計至本期止營業利益(損失) (仟元):(2,467) 10.1月1日累計至本期止基本每股盈餘(損失) (元):(0.07)")
    assert neg["operating_profit"] == -2467 and neg["eps"] == -0.07, neg
    log("✅ 欄位解析 self-test 通過")
    return 0


def parse_iso(raw: str | None) -> date | None:
    return datetime.strptime(raw, "%Y-%m-%d").date() if raw else None


def filing_deadline(year: int, season: int) -> date:
    if season == 1:
        return date(year, 5, 15)
    if season == 2:
        return date(year, 8, 14)
    if season == 3:
        return date(year, 11, 14)
    if season == 4:
        return date(year + 1, 3, 31)
    raise ValueError("season 必須為 1-4")


def period_speak_window(year: int, season: int) -> tuple[date, date]:
    """董事會通過財報的發言日期窗口（法定截止日前約 45 日至截止日後 1 日）。"""
    deadline = filing_deadline(year, season)
    if season == 4:
        start = date(year + 1, 2, 1)
    elif season == 1:
        start = date(year, 4, 1)
    elif season == 2:
        start = date(year, 7, 1)
    else:
        start = date(year, 10, 1)
    return start, deadline + timedelta(days=1)


def parse_periods(raw: str) -> list[tuple[int, int]]:
    out = []
    for part in re.split(r"[,\s]+", str(raw or "")):
        m = re.match(r"^(\d{4})[Qq]([1-4])$", part.strip())
        if m:
            out.append((int(m.group(1)), int(m.group(2))))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="觀測站 t05st01 第31款董事會財務報告")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--stock", help="公司代號，對應頁面「公司代號或簡稱」")
    parser.add_argument("--days", type=int, default=2, help="抓最近 N 日（預設 2）")
    parser.add_argument("--from-date", dest="from_date", help="YYYY-MM-DD")
    parser.add_argument("--to-date", dest="to_date", help="YYYY-MM-DD")
    parser.add_argument("--periods", help="例如 2026Q1,2026Q2（用法定申報窗口當發言日期）")
    parser.add_argument("--sleep", type=float, default=0.7, help="每則詳細資料間隔秒數")
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    windows: list[tuple[date, date]] = []
    periods = parse_periods(args.periods or "")
    if periods:
        for year, season in periods:
            windows.append(period_speak_window(year, season))
    else:
        end = parse_iso(args.to_date) or date.today()
        start = parse_iso(args.from_date) or (end - timedelta(days=max(0, int(args.days) - 1)))
        windows.append((start, end))
    for start, end in windows:
        if start > end:
            log("❌ 日期起迄錯誤")
            return 1
    stock = re.sub(r"\D", "", str(args.stock or ""))
    db_url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not args.dry_run and not db_url:
        log("❌ 未設定 NEON_DATABASE_URL（或 DATABASE_URL）")
        return 1

    log("=" * 72)
    log("🚀 歷史重大訊息 t05st01 → 董事會通過財務報告")
    log(f"🔗 {MOPS_HOME}")
    for start, end in windows:
        log(f"📅 {start.isoformat()} ～ {end.isoformat()}" + (f"  公司 {stock}" if stock else "  上市+上櫃"))
    log(f"📦 {TABLE_NAME}")
    log("=" * 72)

    records: list[dict] = []
    try:
        for start, end in windows:
            records.extend(fetch_t05st01(start, end, stock=stock, sleep_s=max(0.2, float(args.sleep))))
    except Exception as exc:
        log(f"❌ 抓取失敗：{exc}")
        import traceback
        traceback.print_exc()
        return 1

    log(f"抓取完成：{len(records)} 筆")
    if not records:
        log("⚠️ 本次無第31款財報數字（可能尚未公告）")
        return 0 if args.allow_empty else 2
    if args.dry_run:
        rec = records[0]
        log(
            f"dry-run 例：{rec.get('stock_no')} {rec.get('subject')} "
            f"營收={rec.get('revenue')} EPS={rec.get('eps')}"
        )
        return 0
    try:
        n = upsert_records(records, db_url)
    except Exception as exc:
        log(f"❌ 寫入失敗：{exc}")
        import traceback
        traceback.print_exc()
        return 1
    log(f"✅ 已 upsert {TABLE_NAME}：{n} 筆")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
