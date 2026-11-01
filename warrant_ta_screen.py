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


DUO_KONG_PERIOD = 45

TA_FLAG_KEYS = (
    'ma5gtMa10',
    'ma10gtMa20',
    'maBullAlign',
    'maBearAlign',
    'ma5CrossUpMa10',
    'ma5CrossDownMa10',
    'closeAboveMa5',
    'duoKongCrossUp',
    'duoKongAbove',
    'duoKongCrossDown',
    'heikinFirstRed',
    'heikinFirstGreen',
    'heikinRed',
    'reversalFirstRed',
    'rsiLt30',
    'rsiGt70',
    'macdDifGt0',
    'macdGoldCross',
    'kdKgtD',
    'kdGoldCrossLow',
    'breakPrevHigh',
    'volumeUp',
)

LONG_LOOKBACK_KEYS = {
    'heikinFirstRed',
    'heikinFirstGreen',
    'heikinRed',
    'reversalFirstRed',
    'duoKongCrossUp',
    'duoKongAbove',
    'duoKongCrossDown',
    'rsiLt30',
    'rsiGt70',
    'macdDifGt0',
    'macdGoldCross',
    'kdKgtD',
    'kdGoldCrossLow',
    'ma10gtMa20',
    'maBullAlign',
    'maBearAlign',
}


def is_ma5_above_ma10(closes: list[float]) -> bool:
    if not closes:
        return False
    ma5 = calc_sma(closes, 5)  # type: ignore[arg-type]
    ma10 = calc_sma(closes, 10)  # type: ignore[arg-type]
    i = len(closes) - 1
    return ma5[i] is not None and ma10[i] is not None and ma5[i] > ma10[i]  # type: ignore[operator]


def calc_wma(values: list[float | None], period: int) -> list[float | None]:
    n = len(values)
    out: list[float | None] = [None] * n
    p = max(1, int(period))
    if p <= 1:
        return list(values)
    weight_sum = p * (p + 1) / 2
    for i in range(p - 1, n):
        acc = 0.0
        valid = True
        for j in range(p):
            v = values[i - j]
            if v is None:
                valid = False
                break
            acc += float(v) * (p - j)
        out[i] = acc / weight_sum if valid else None
    return out


def calc_duo_kong(closes: list[float], period: int = DUO_KONG_PERIOD) -> list[float | None]:
    n = max(2, int(period))
    n2 = max(1, n // 2)
    ns = max(1, int(n ** 0.5))
    wma_n = calc_wma(closes, n)  # type: ignore[arg-type]
    wma_n2 = calc_wma(closes, n2)  # type: ignore[arg-type]
    diff: list[float | None] = [None] * len(closes)
    for i, _ in enumerate(closes):
        a, b = wma_n2[i], wma_n[i]
        if a is not None and b is not None:
            diff[i] = 2 * a - b
    return calc_wma(diff, ns)


def _last_two(arr: list[float | None]) -> tuple[float | None, float | None]:
    if not arr or len(arr) < 2:
        return None, None
    return arr[-2], arr[-1]


def is_duo_kong_cross_up(closes: list[float], period: int = DUO_KONG_PERIOD) -> bool:
    if len(closes) < 2:
        return False
    dk = calc_duo_kong(closes, period)
    c_now, c_prev = closes[-1], closes[-2]
    d_now, d_prev = dk[-1], dk[-2]
    if None in (c_now, c_prev, d_now, d_prev):
        return False
    return c_now > d_now and c_prev <= d_prev  # type: ignore[operator]


def is_duo_kong_above(closes: list[float], period: int = DUO_KONG_PERIOD) -> bool:
    if not closes:
        return False
    dk = calc_duo_kong(closes, period)
    return closes[-1] is not None and dk[-1] is not None and closes[-1] > dk[-1]  # type: ignore[operator]


def is_duo_kong_cross_down(closes: list[float], period: int = DUO_KONG_PERIOD) -> bool:
    if len(closes) < 2:
        return False
    dk = calc_duo_kong(closes, period)
    c_now, c_prev = closes[-1], closes[-2]
    d_now, d_prev = dk[-1], dk[-2]
    if None in (c_now, c_prev, d_now, d_prev):
        return False
    return c_now < d_now and c_prev >= d_prev  # type: ignore[operator]


def is_heikin_first_green(bars: list[dict[str, Any]]) -> bool:
    ha = build_heikin_ashi(bars)
    if len(ha) < 2:
        return False
    last, prev = ha[-1], ha[-2]
    last_red = last['close'] >= last['open']
    prev_red = prev['close'] >= prev['open']
    return (not last_red) and prev_red


def is_heikin_red(bars: list[dict[str, Any]]) -> bool:
    ha = build_heikin_ashi(bars)
    if not ha:
        return False
    last = ha[-1]
    return last['close'] >= last['open']


def calc_rsi(closes: list[float], period: int = 14) -> list[float | None]:
    n = len(closes)
    gains: list[float | None] = [None] * n
    losses: list[float | None] = [None] * n
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        gains[i] = max(ch, 0.0)
        losses[i] = max(-ch, 0.0)
    avg_gain: list[float | None] = [None] * n
    avg_loss: list[float | None] = [None] * n
    p = max(1, period)
    g_sum = 0.0
    l_sum = 0.0
    count = 0
    prev_g = None
    prev_l = None
    for i in range(n):
        g, l = gains[i], losses[i]
        if g is None or l is None:
            continue
        if prev_g is None:
            g_sum += g
            l_sum += l
            count += 1
            if count == p:
                prev_g = g_sum / p
                prev_l = l_sum / p
                avg_gain[i] = prev_g
                avg_loss[i] = prev_l
        else:
            prev_g = (prev_g * (p - 1) + g) / p
            prev_l = (prev_l * (p - 1) + l) / p
            avg_gain[i] = prev_g
            avg_loss[i] = prev_l
    out: list[float | None] = [None] * n
    for i in range(n):
        g, l = avg_gain[i], avg_loss[i]
        if g is None or l is None:
            continue
        if g == 0 and l == 0:
            out[i] = 50.0
        elif l == 0:
            out[i] = 100.0
        else:
            out[i] = 100 - 100 / (1 + g / l)
    return out


def calc_ema_sma_seed(prices: list[float], period: int) -> list[float | None]:
    n = len(prices)
    out: list[float | None] = [None] * n
    p = max(1, int(period))
    mult = 2 / (p + 1)
    for i in range(n):
        if i < p - 1:
            continue
        if i == p - 1:
            chunk = prices[i - p + 1 : i + 1]
            out[i] = sum(chunk) / p
        elif out[i - 1] is not None:
            out[i] = (prices[i] - out[i - 1]) * mult + out[i - 1]  # type: ignore[operator]
    return out


def calc_macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9):
    fast_ema = calc_ema_sma_seed(closes, fast)
    slow_ema = calc_ema_sma_seed(closes, slow)
    dif: list[float | None] = [None] * len(closes)
    for i, _ in enumerate(closes):
        if fast_ema[i] is not None and slow_ema[i] is not None:
            dif[i] = fast_ema[i] - slow_ema[i]  # type: ignore[operator]
    dea: list[float | None] = [None] * len(dif)
    valid: list[float] = []
    first_idx = -1
    mult = 2 / (signal + 1)
    for i, d in enumerate(dif):
        if d is None:
            continue
        valid.append(d)
        if len(valid) == 1:
            first_idx = i
        if len(valid) < signal:
            continue
        if len(valid) == signal:
            dea[i] = sum(valid) / signal
        else:
            prev = None
            for j in range(i - 1, first_idx - 1, -1):
                if dea[j] is not None:
                    prev = dea[j]
                    break
            if prev is not None:
                dea[i] = (d - prev) * mult + prev
    return dif, dea


def calc_kd(bars: list[dict[str, Any]], n: int = 9, m1: int = 3, m2: int = 3):
    k: list[float | None] = [None] * len(bars)
    d: list[float | None] = [None] * len(bars)
    prev_k = 50.0
    prev_d = 50.0
    for i in range(len(bars)):
        if i < n - 1:
            continue
        lowest = float('inf')
        highest = float('-inf')
        ok = True
        for j in range(n):
            b = bars[i - j]
            if b.get('low') is None or b.get('high') is None or b.get('close') is None:
                ok = False
                break
            lowest = min(lowest, float(b['low']))
            highest = max(highest, float(b['high']))
        if not ok:
            continue
        close = float(bars[i]['close'])
        rsv = 0.0 if highest == lowest else ((close - lowest) / (highest - lowest)) * 100
        k_val = (prev_k * (m1 - 1) + rsv) / m1
        d_val = (prev_d * (m2 - 1) + k_val) / m2
        prev_k, prev_d = k_val, d_val
        k[i] = k_val
        d[i] = d_val
    return k, d


def evaluate_ta_signals(bars_asc: list[dict[str, Any]]) -> dict[str, bool]:
    """bars_asc: chronological OHLC dicts with open/high/low/close."""
    closes: list[float] = []
    volumes: list[float | None] = []
    ohlc: list[dict[str, Any]] = []
    for b in bars_asc:
        try:
            c = float(b.get('close'))
        except (TypeError, ValueError):
            continue
        closes.append(c)
        try:
            volumes.append(float(b.get('volume')))
        except (TypeError, ValueError):
            volumes.append(None)
        try:
            o = float(b.get('open'))
            h = float(b.get('high'))
            low = float(b.get('low'))
        except (TypeError, ValueError):
            continue
        ohlc.append({'open': o, 'high': h, 'low': low, 'close': c})
    params = resolve_golden_wave_params(len(closes))
    i = len(closes) - 1
    ma5 = calc_sma(closes, 5)  # type: ignore[arg-type]
    ma10 = calc_sma(closes, 10)  # type: ignore[arg-type]
    ma20 = calc_sma(closes, 20)  # type: ignore[arg-type]
    ma5_prev, ma5_now = _last_two(ma5)
    ma10_prev, ma10_now = _last_two(ma10)
    rsi = calc_rsi(closes, 14)
    dif, dea = calc_macd(closes)
    kd_k, kd_d = calc_kd(ohlc)
    rsi_now = rsi[i] if i >= 0 else None
    dif_prev, dif_now = _last_two(dif)
    dea_prev, dea_now = _last_two(dea)
    k_prev, k_now = _last_two(kd_k)
    d_prev, d_now = _last_two(kd_d)
    prev_bar = ohlc[-2] if len(ohlc) >= 2 else None
    last_bar = ohlc[-1] if ohlc else None
    vol_now = volumes[i] if i >= 0 else None
    vol_prev = volumes[i - 1] if i >= 1 else None
    return {
        'reversalFirstRed': is_golden_wave_first_red(closes, params),
        'heikinFirstRed': is_heikin_first_red(ohlc),
        'heikinFirstGreen': is_heikin_first_green(ohlc),
        'heikinRed': is_heikin_red(ohlc),
        'ma5gtMa10': is_ma5_above_ma10(closes),
        'ma10gtMa20': ma10_now is not None and ma20[i] is not None and ma10_now > ma20[i],  # type: ignore[operator]
        'maBullAlign': (
            ma5_now is not None and ma10_now is not None and ma20[i] is not None
            and ma5_now > ma10_now and ma10_now > ma20[i]  # type: ignore[operator]
        ),
        'maBearAlign': (
            ma5_now is not None and ma10_now is not None and ma20[i] is not None
            and ma5_now < ma10_now and ma10_now < ma20[i]  # type: ignore[operator]
        ),
        'ma5CrossUpMa10': (
            ma5_prev is not None and ma10_prev is not None and ma5_now is not None and ma10_now is not None
            and ma5_prev <= ma10_prev and ma5_now > ma10_now
        ),
        'ma5CrossDownMa10': (
            ma5_prev is not None and ma10_prev is not None and ma5_now is not None and ma10_now is not None
            and ma5_prev >= ma10_prev and ma5_now < ma10_now
        ),
        'closeAboveMa5': closes and ma5_now is not None and closes[-1] > ma5_now,  # type: ignore[operator]
        'duoKongCrossUp': is_duo_kong_cross_up(closes),
        'duoKongAbove': is_duo_kong_above(closes),
        'duoKongCrossDown': is_duo_kong_cross_down(closes),
        'rsiLt30': rsi_now is not None and rsi_now < 30,
        'rsiGt70': rsi_now is not None and rsi_now > 70,
        'macdDifGt0': dif_now is not None and dif_now > 0,
        'macdGoldCross': (
            dif_prev is not None and dea_prev is not None and dif_now is not None and dea_now is not None
            and dif_prev <= dea_prev and dif_now > dea_now
        ),
        'kdKgtD': k_now is not None and d_now is not None and k_now > d_now,
        'kdGoldCrossLow': (
            k_prev is not None and d_prev is not None and k_now is not None and d_now is not None
            and k_prev <= d_prev and k_now > d_now and k_now < 30
        ),
        'breakPrevHigh': bool(
            last_bar and prev_bar
            and last_bar['close'] > prev_bar['high']
        ),
        'volumeUp': bool(
            vol_now is not None and vol_prev is not None and vol_prev > 0 and vol_now >= vol_prev * 1.5
        ),
    }


def passes_ta_filters(signals: dict[str, bool], flags: dict[str, bool]) -> bool:
    for key in TA_FLAG_KEYS:
        if flags.get(key) and not signals.get(key):
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
