import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

from .db import (
    ensure_tables,
    fetch_symbols as _fetch_symbols,
    batch_fetch_prices,
    fetch_prices,
    batch_fetch_existing_return_dates,
    fetch_existing_return_dates,
    upsert_returns,
    upsert_returns_neon,
)
from .returns import normalize_prices, compute_returns_from_close, build_return_records

logger = logging.getLogger(__name__)


def compute_returns(
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    all: bool = False,
    limit: Optional[int] = None,
    fill_missing: bool = False,
    use_neon: bool = False,
    upload_to_neon: bool = False,
    progress_callback: Optional[callable] = None,
) -> dict:
    """Compute returns and upsert into stock_returns.

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

    # resolve symbol list
    symbols: List[str]
    if all or (not symbol):
        symbols = _fetch_symbols(limit=limit, use_neon=use_neon)
    else:
        symbols = [symbol]

    total_symbols = len(symbols)
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

    batch_size = max(1, min(total_symbols or 1, int(os.getenv("RETURNS_BATCH_SIZE", "10"))))
    max_workers_env = max(1, int(os.getenv("RETURNS_MAX_WORKERS", "4")))

    def process_symbol(sym: str, index: int, price_rows, existing_dates):
        try:
            if not price_rows:
                result = {"symbol": sym, "written": 0, "reason": "no_prices"}
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
                    ret_df = ret_df.loc[[d for d in ret_df.index.date if d not in existing_dates]]
                    import pandas as pd
                    ret_df.index = pd.to_datetime(ret_df.index)
                    if before_count > 0 and len(ret_df) == 0:
                        filtered_reason = 'already_up_to_date'

            records = build_return_records(sym, ret_df)
            if not records:
                result = {"symbol": sym, "written": 0, "reason": filtered_reason or "no_new_records"}
                return index, result, 0, 0

            written = upsert_returns(records, use_neon=use_neon)
            result = {"symbol": sym, "written": written, "reason": filtered_reason}

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

    for batch_start, batch in symbol_batches(symbols, batch_size):
        if not batch:
            continue

        price_map = batch_fetch_prices(batch, start, end, use_neon=use_neon)
        existing_map = batch_fetch_existing_return_dates(batch, start, end, use_neon=use_neon) if fill_missing else {}

        workers = min(len(batch), max_workers_env)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_index = {}
            for offset, sym in enumerate(batch):
                index = batch_start + offset + 1
                price_rows = price_map.get(sym)
                existing_dates = existing_map.get(sym)
                future = executor.submit(process_symbol, sym, index, price_rows, existing_dates)
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
    return result_dict
