import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

from .db import (
    ensure_tables,
    fetch_symbols as _fetch_symbols,
    batch_fetch_prices,
    fetch_prices,
    batch_fetch_existing_return_dates,
    fetch_existing_return_dates,
    resolve_symbols_in_prices,
    upsert_returns,
    upsert_returns_neon,
)
from .returns import normalize_prices, compute_returns_from_close, build_return_records

logger = logging.getLogger(__name__)


def compute_returns(
    symbol: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    all: bool = False,
    limit: Optional[int] = None,
    fill_missing: bool = False,
    use_neon: bool = False,
    upload_to_neon: bool = False,
    progress_callback: Optional[callable] = None,
    batch_size: Optional[int] = None,
    max_workers: Optional[int] = None,
) -> dict:
    """Compute returns and upsert into tw_stock_returns.

    Args:
        symbol: 指定股票代碼
        start: 開始日期
        end: 結束日期
        all: 計算所有股票
        limit: 限制股票數量
        fill_missing: 僅填補缺失的報酬率
        use_neon: 從 Neon 雲端資料庫讀取股價並寫入報酬率
        upload_to_neon: 從本地讀取股價，但同時上傳報酬率到 Neon 雲端資料庫

    Returns a report dict containing processed symbols and rows written.
    """
    ensure_tables(use_neon=use_neon)
    if upload_to_neon:
        # 確保 Neon 雲端資料庫也有表格
        ensure_tables(use_neon=True)

    t0_total = time.perf_counter()

    # resolve symbol list
    resolved_symbols: List[str]
    if symbols:
        resolved_symbols = [s for s in symbols if isinstance(s, str) and s.strip()]
    elif all or (not symbol):
        resolved_symbols = _fetch_symbols(limit=limit, use_neon=use_neon)
    else:
        resolved_symbols = [symbol]

    # Map user-provided symbols (e.g. numeric codes) to actual symbols existing in tw_stock_prices
    requested_to_actual = resolve_symbols_in_prices(resolved_symbols, use_neon=use_neon)
    actual_symbols = [requested_to_actual.get(s, s) for s in resolved_symbols]

    total_symbols = len(actual_symbols)
    if progress_callback:
        try:
            progress_callback({
                "event": "start",
                "total": total_symbols,
                "fill_missing": fill_missing,
                "use_neon": use_neon,
                "upload_to_neon": upload_to_neon,
            })
        except Exception:
            logger.exception("progress_callback start event failed")

    total_written = 0
    total_written_neon = 0
    per_symbol: List[dict] = [None] * total_symbols if total_symbols else []

    # batch size: 默認 10，可由參數或環境變數覆寫
    def _clamp(val, lo, hi):
        try:
            v = int(val)
            return max(lo, min(hi, v))
        except Exception:
            return None

    batch_size_override = _clamp(batch_size, 1, 500)
    if batch_size_override is None:
        batch_size_override = _clamp(os.getenv("RETURNS_BATCH_SIZE", "10"), 1, 500) or 10
    batch_size = max(1, min(total_symbols or 1, batch_size_override))

    max_workers_override = _clamp(max_workers, 1, 64)
    if max_workers_override is None:
        max_workers_override = _clamp(os.getenv("RETURNS_MAX_WORKERS", "4"), 1, 64) or 4

    def process_symbol(sym: str, index: int, price_rows, existing_dates, requested_symbol: str | None = None):
        try:
            t0 = time.perf_counter()
            if not price_rows:
                result = {"symbol": sym, "written": 0, "reason": "no_prices"}
                if requested_symbol and requested_symbol != sym:
                    result["requested_symbol"] = requested_symbol
                return index, result, 0, 0

            price_df = normalize_prices(price_rows)
            ret_df = compute_returns_from_close(price_df)
            if ret_df.empty:
                result = {"symbol": sym, "written": 0, "reason": "empty_returns"}
                return index, result, 0, 0

            filtered_reason = None
            if fill_missing:
                if existing_dates is None:
                    existing_dates = fetch_existing_return_dates(sym, start, end, use_neon=use_neon)
                if existing_dates:
                    before_count = len(ret_df)
                    # ensure index is datetime and filter using set membership efficiently
                    import pandas as pd
                    ret_df.index = pd.to_datetime(ret_df.index)
                    existing_set = set(existing_dates)
                    keep_mask = pd.Series(ret_df.index.date).apply(lambda d: d not in existing_set).to_numpy()
                    ret_df = ret_df.loc[keep_mask]
                    if before_count > 0 and len(ret_df) == 0:
                        filtered_reason = 'already_up_to_date'

            records = build_return_records(sym, ret_df)
            if not records:
                result = {"symbol": sym, "written": 0, "reason": filtered_reason or "no_new_records"}
                return index, result, 0, 0

            written = upsert_returns(records, use_neon=use_neon)
            result = {"symbol": sym, "written": written, "reason": filtered_reason}
            if requested_symbol and requested_symbol != sym:
                result["requested_symbol"] = requested_symbol

            result["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 2)

            written_neon = 0
            if upload_to_neon and not use_neon:
                try:
                    written_neon = upsert_returns_neon(records)
                    if written_neon:
                        result["written_neon"] = written_neon
                    logger.info(f"☁️ {sym} 報酬率已上傳到 Neon: {written_neon} 筆")
                except Exception as e:
                    logger.error(f"上傳 {sym} 報酬率到 Neon 失敗: {e}")
                    result["neon_error"] = str(e)

            return index, result, written, written_neon
        except Exception as e:
            logger.exception("compute_returns error for %s", sym)
            return index, {"symbol": sym, "written": 0, "error": str(e)}, 0, 0

    def symbol_batches(seq: List[str], size: int):
        for start_idx in range(0, len(seq), size):
            yield start_idx, seq[start_idx:start_idx + size]

    # Keep requested symbols aligned with actual symbols for reporting
    requested_symbols_seq = list(resolved_symbols)
    actual_symbols_seq = list(actual_symbols)

    for batch_start, batch in symbol_batches(actual_symbols_seq, batch_size):
        if not batch:
            continue

        t0_batch = time.perf_counter()
        price_map = batch_fetch_prices(batch, start, end, use_neon=use_neon)
        t_price = time.perf_counter()
        existing_map = batch_fetch_existing_return_dates(batch, start, end, use_neon=use_neon) if fill_missing else {}
        t_existing = time.perf_counter()
        logger.info(
            "returns_calc batch: size=%s fetch_prices=%.2fms fetch_existing=%.2fms use_neon=%s fill_missing=%s",
            len(batch),
            (t_price - t0_batch) * 1000,
            (t_existing - t_price) * 1000,
            use_neon,
            fill_missing,
        )

        workers = min(len(batch), max_workers_override)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index = {}
            for offset, sym in enumerate(batch):
                index = batch_start + offset + 1
                requested_sym = requested_symbols_seq[batch_start + offset] if (batch_start + offset) < len(requested_symbols_seq) else None
                price_rows = price_map.get(sym)
                existing_dates = existing_map.get(sym)
                future = executor.submit(process_symbol, sym, index, price_rows, existing_dates, requested_sym)
                future_to_index[future] = index

            for future in as_completed(future_to_index):
                index, result, written, written_neon = future.result()
                position = index - 1
                if 0 <= position < len(per_symbol):
                    per_symbol[position] = result
                else:
                    per_symbol.append(result)

                total_written += written
                if upload_to_neon:
                    total_written_neon += written_neon

                if progress_callback:
                    try:
                        progress_callback({
                            "event": "progress",
                            "symbol": result.get("symbol"),
                            "index": index,
                            "total": total_symbols,
                            "written": result.get("written", 0),
                            "written_neon": result.get("written_neon"),
                            "reason": result.get("reason"),
                            "error": result.get("error"),
                            "neon_error": result.get("neon_error"),
                            "use_neon": use_neon,
                        })
                    except Exception:
                        logger.exception("progress_callback progress event failed")

    per_symbol = [item for item in per_symbol if item is not None]

    result_dict = {"total_written": total_written, "symbols": per_symbol}
    if upload_to_neon:
        result_dict["total_written_neon"] = total_written_neon

    if progress_callback:
        try:
            progress_callback({
                "event": "summary",
                "summary": result_dict,
            })
        except Exception:
            logger.exception("progress_callback summary event failed")
    result_dict["elapsed_ms"] = round((time.perf_counter() - t0_total) * 1000, 2)
    return result_dict
