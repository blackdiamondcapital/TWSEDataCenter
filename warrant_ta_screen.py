"""Batch warrant technical screening (server-side).

Mirrors frontend src/lib/taScreenRules.js so portal can scan the full market
without N timeseries HTTP round-trips.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable


DEFAULT_GOLDEN_WAVE_PARAMS = {
    'fastMa': 30,
    'slowMa': 100,
    'fastMa2': 130,
    'slowMa2': 140,
}

WARRANT_GOLDEN_WAVE_PARAMS = {
    'fastMa': 12,
    'slowMa': 40,
    'fastMa2': 50,
    'slowMa2': 55,
}


def resolve_golden_wave_params(bar_count: int) -> dict[str, int]:
    n = int(bar_count or 0)
    if n >= 160:
        return DEFAULT_GOLDEN_WAVE_PARAMS
    return WARRANT_GOLDEN_WAVE_PARAMS


def calc_sma(values: list[float | None], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    p = max(1, int(period))
    for i in range(len(values)):
        if i < p - 1:
            continue
        chunk = values[i - p + 1 : i + 1]
        if any(v is None for v in chunk):
            continue
        out[i] = round(sum(chunk) / p, 4)  # type: ignore[arg-type]
    return out


def calc_ema(values: list[float | None], period: int) -> list[float | None]:
    n = len(values)
    result: list[float | None] = [None] * n
    k = 2 / (period + 1)
    ema = None
    for i, raw in enumerate(values):
        if raw is None:
            continue
        val = float(raw)
        ema = val if ema is None else val * k + ema * (1 - k)
        result[i] = ema
    return result


def calc_golden_wave(closes: list[float], params: dict[str, int]) -> dict[str, list[float | None]]:
    n = len(closes)
    dif: list[float | None] = [None] * n
    dif_sub: list[float | None] = [None] * n
    if not n:
        return {'dif': dif, 'difSub': dif_sub}

    ema_fast = calc_ema(closes, params['fastMa'])  # type: ignore[arg-type]
    ema_slow = calc_ema(closes, params['slowMa'])  # type: ignore[arg-type]
    ema_fast2 = calc_ema(closes, params['fastMa2'])  # type: ignore[arg-type]
    ema_slow2 = calc_ema(closes, params['slowMa2'])  # type: ignore[arg-type]

    for i in range(n):
        f, s = ema_fast[i], ema_slow[i]
        if f is not None and s is not None:
            dif[i] = f - s

    dif_slow: list[float | None] = [None] * n
    for i in range(n):
        f2, s2 = ema_fast2[i], ema_slow2[i]
        if f2 is not None and s2 is not None:
            dif_slow[i] = f2 - s2

    for i in range(n):
        d, ds = dif[i], dif_slow[i]
        if d is not None and ds is not None:
            dif_sub[i] = abs(d - ds)

    return {'dif': dif, 'difSub': dif_sub}


def is_golden_wave_bar_red(dif_val, dif_sub_val, dif_sub_prev3) -> bool:
    try:
        dif = float(dif_val)
    except (TypeError, ValueError):
        return False
    if dif < 0:
        return False
    try:
        sub_now = float(dif_sub_val)
    except (TypeError, ValueError):
        return False
    if dif_sub_prev3 is None:
        return True
    try:
        sub_prev3 = float(dif_sub_prev3)
    except (TypeError, ValueError):
        return True
    return sub_now > sub_prev3


def is_golden_wave_first_red(closes: list[float], params: dict[str, int]) -> bool:
    if not closes:
        return False
    gw = calc_golden_wave(closes, params)
    i = len(gw['dif']) - 1
    if i < 0:
        return False
    sub_prev3 = gw['difSub'][i - 3] if i >= 3 else None
    if not is_golden_wave_bar_red(gw['dif'][i], gw['difSub'][i], sub_prev3):
        return False
    if i < 1:
        return True
    sub_prev3_prev = gw['difSub'][i - 1 - 3] if i - 1 >= 3 else None
    return not is_golden_wave_bar_red(gw['dif'][i - 1], gw['difSub'][i - 1], sub_prev3_prev)


def build_heikin_ashi(bars: list[dict[str, Any]]) -> list[dict[str, float]]:
    ha: list[dict[str, float]] = []
    for i, bar in enumerate(bars):
        try:
            o = float(bar['open'])
            h = float(bar['high'])
            low = float(bar['low'])
            c = float(bar['close'])
        except (TypeError, ValueError, KeyError):
            continue
        ha_close = (o + h + low + c) / 4
        if i == 0 or not ha:
            prev_open, prev_close = o, c
        else:
            prev_open, prev_close = ha[-1]['open'], ha[-1]['close']
        ha_open = (prev_open + prev_close) / 2
        ha.append({
            'open': ha_open,
            'close': ha_close,
            'high': max(h, ha_open, ha_close),
            'low': min(low, ha_open, ha_close),
        })
    return ha


def is_heikin_first_red(bars: list[dict[str, Any]]) -> bool:
    ha = build_heikin_ashi(bars)
    if len(ha) < 2:
        return False
    last, prev = ha[-1], ha[-2]
    return last['close'] >= last['open'] and not (prev['close'] >= prev['open'])


def is_ma5_above_ma10(closes: list[float]) -> bool:
    if not closes:
        return False
    ma5 = calc_sma(closes, 5)  # type: ignore[arg-type]
    ma10 = calc_sma(closes, 10)  # type: ignore[arg-type]
    i = len(closes) - 1
    return ma5[i] is not None and ma10[i] is not None and ma5[i] > ma10[i]  # type: ignore[operator]


def evaluate_ta_signals(bars_asc: list[dict[str, Any]]) -> dict[str, bool]:
    """bars_asc: chronological OHLC dicts with open/high/low/close."""
    closes: list[float] = []
    ohlc: list[dict[str, Any]] = []
    for b in bars_asc:
        try:
            c = float(b.get('close'))
        except (TypeError, ValueError):
            continue
        closes.append(c)
        try:
            o = float(b.get('open'))
            h = float(b.get('high'))
            low = float(b.get('low'))
        except (TypeError, ValueError):
            continue
        ohlc.append({'open': o, 'high': h, 'low': low, 'close': c})
    params = resolve_golden_wave_params(len(closes))
    return {
        'reversalFirstRed': is_golden_wave_first_red(closes, params),
        'heikinFirstRed': is_heikin_first_red(ohlc),
        'ma5gtMa10': is_ma5_above_ma10(closes),
    }


def passes_ta_filters(signals: dict[str, bool], flags: dict[str, bool]) -> bool:
    if flags.get('reversalFirstRed') and not signals.get('reversalFirstRed'):
        return False
    if flags.get('heikinFirstRed') and not signals.get('heikinFirstRed'):
        return False
    if flags.get('ma5gtMa10') and not signals.get('ma5gtMa10'):
        return False
    return True


def truthy_flag(val: Any) -> bool:
    return str(val or '').strip().lower() in {'1', 'true', 'yes', 'on', 'y'}


def fetch_recent_bars_by_code(cursor, codes: list[str], limit_bars: int = 80) -> dict[str, list[dict[str, Any]]]:
    """Load up to `limit_bars` recent OHLC bars per code (asc), TWSE then TPEX fill."""
    if not codes:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}

    def load(sql: str, code_list: list[str]):
        if not code_list:
            return
        cursor.execute(sql, (code_list, limit_bars))
        rows = cursor.fetchall() or []
        by_code: dict[str, list] = defaultdict(list)
        for r in rows:
            if isinstance(r, dict):
                code = r.get('warrant_code')
                if code:
                    by_code[code].append(r)
            else:
                code = r[0]
                if code:
                    by_code[code].append({
                        'warrant_code': r[0],
                        'trade_date': r[1],
                        'open_price': r[2],
                        'high_price': r[3],
                        'low_price': r[4],
                        'close_price': r[5],
                        'volume': r[6],
                    })
        for code, items in by_code.items():
            if out.get(code):
                continue
            # rows come DESC by trade_date; reverse to ASC
            bars = []
            for item in reversed(items):
                try:
                    close = float(item.get('close_price'))
                except (TypeError, ValueError):
                    continue

                def num(key, _item=item):
                    try:
                        return float(_item.get(key))
                    except (TypeError, ValueError):
                        return None

                bars.append({
                    'open': num('open_price'),
                    'high': num('high_price'),
                    'low': num('low_price'),
                    'close': close,
                    'volume': item.get('volume'),
                })
            if bars:
                out[code] = bars

    load(
        """
        SELECT warrant_code, trade_date, open_price, high_price, low_price, close_price, volume
        FROM (
            SELECT warrant_code, trade_date, open_price, high_price, low_price, close_price, volume,
                   ROW_NUMBER() OVER (PARTITION BY warrant_code ORDER BY trade_date DESC) AS rn
            FROM tw_warrant_trade
            WHERE warrant_code = ANY(%s)
              AND close_price IS NOT NULL
        ) t
        WHERE rn <= %s
        ORDER BY warrant_code, trade_date DESC
        """,
        codes,
    )
    missing = [c for c in codes if c not in out]
    if missing:
        load(
            """
            SELECT warrant_code, trade_date, open_price, high_price, low_price, close_price, trade_volume AS volume
            FROM (
                SELECT warrant_code, trade_date, open_price, high_price, low_price, close_price, trade_volume,
                       ROW_NUMBER() OVER (PARTITION BY warrant_code ORDER BY trade_date DESC) AS rn
                FROM tpex_warrant_daily_quotes
                WHERE warrant_code = ANY(%s)
                  AND close_price IS NOT NULL
            ) t
            WHERE rn <= %s
            ORDER BY warrant_code, trade_date DESC
            """,
            missing,
        )

    return out


def filter_codes_by_ta(
    bars_by_code: dict[str, list[dict[str, Any]]],
    flags: dict[str, bool],
) -> list[str]:
    matched: list[str] = []
    for code, bars in bars_by_code.items():
        if not bars:
            continue
        signals = evaluate_ta_signals(bars)
        if passes_ta_filters(signals, flags):
            matched.append(code)
    return matched


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]
