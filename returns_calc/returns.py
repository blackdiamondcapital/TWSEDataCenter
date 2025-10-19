from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Iterable

ROLLING_WINDOWS = {
    "weekly": 5,
    "monthly": 21,
    "quarterly": 63,
    "yearly": 252,
}


def normalize_prices(rows: list[dict]) -> pd.DataFrame:
    """Convert DB rows [{'date': date, 'close_price': Decimal}, ...] to a clean DataFrame.
    Ensures sorted by date and float dtype.
    """
    if not rows:
        return pd.DataFrame(columns=["date", "close"]).set_index("date")
    df = pd.DataFrame(rows)
    # rename to consistent name
    if "close_price" in df.columns:
        df = df.rename(columns={"close_price": "close"})
    # ensure datetime index
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    # to float
    df["close"] = pd.to_numeric(df["close"], errors="coerce").astype(float)
    # drop NaNs in close
    df = df[~df["close"].isna()]
    return df


def compute_returns_from_close(df: pd.DataFrame) -> pd.DataFrame:
    """Given a DataFrame with index=date and column 'close', compute returns columns.
    Returns DataFrame with columns: daily_return, weekly_return, monthly_return, quarterly_return, yearly_return, cumulative_return
    """
    if df.empty:
        return pd.DataFrame(index=df.index, columns=[
            "daily_return","weekly_return","monthly_return","quarterly_return","yearly_return","cumulative_return"
        ])

    out = pd.DataFrame(index=df.index)

    # daily
    out["daily_return"] = df["close"].pct_change()

    # rolling windows
    for name, win in ROLLING_WINDOWS.items():
        out[f"{name}_return"] = df["close"].pct_change(periods=win)

    # cumulative based on first available price
    first_price = df["close"].iloc[0]
    out["cumulative_return"] = df["close"] / first_price - 1.0

    # round to 6 decimals
    out = out.round(6)
    return out


def build_return_records(symbol: str, ret_df: pd.DataFrame) -> list[dict]:
    records: list[dict] = []
    for dt, row in ret_df.iterrows():
        rec = {
            "symbol": symbol,
            "date": dt.date(),
            "daily_return": safe_float(row.get("daily_return")),
            "weekly_return": safe_float(row.get("weekly_return")),
            "monthly_return": safe_float(row.get("monthly_return")),
            "quarterly_return": safe_float(row.get("quarterly_return")),
            "yearly_return": safe_float(row.get("yearly_return")),
            "cumulative_return": safe_float(row.get("cumulative_return")),
        }
        records.append(rec)
    return records


def safe_float(x):
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
            return None
        return float(x)
    except Exception:
        return None
