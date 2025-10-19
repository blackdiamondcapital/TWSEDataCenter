#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze exported UI logs to compute metrics and generate figures for JFDS paper.
Assumes CSV with columns like: time, level, message (UTF-8 with BOM supported).

Usage:
  python analyze_logs.py --input research/data/*.csv --outdir research/out

Outputs:
  - metrics_summary.csv
  - batch_durations.csv
  - throughput_over_time.csv
  - figures: batch_duration_hist.png, batch_duration_line.png, throughput.png, errors_over_time.png
"""
import argparse
import os
import re
import sys
from glob import glob
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid", context="talk")

# Regex patterns for parsing messages
RE_BATCH_DONE = re.compile(r"^📦?\s*批次\s*(\d+)/(\d+)\s*完成.*?耗時\s*([\d:分秒時\s]+).*?累計已處理\s*(\d+)/(\d+)\s*檔")
RE_BATCH_FAIL = re.compile(r"^批次\s*(\d+)\s*處理失敗.*?耗時\s*([\d:分秒時\s]+)")
RE_PROCESSED = re.compile(r"已處理\s*(\d+)/(\d+)\s*檔股票")
RE_TOTAL_DONE = re.compile(r"更新完成.*?(?:總耗時|耗時)\s*([\d:分秒時\s]+)")

TIME_COL_CANDIDATES = ["time", "timestamp", "日期", "時間", "datetime"]
LEVEL_COL_CANDIDATES = ["level", "等級", "級別"]
MESSAGE_COL_CANDIDATES = ["message", "訊息", "信息", "內容", "text"]


def find_col(df: pd.DataFrame, candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name in df.columns:
            return name
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    # fallback: use first/second/third columns heuristically
    if candidates is TIME_COL_CANDIDATES:
        return df.columns[0]
    if candidates is LEVEL_COL_CANDIDATES:
        return df.columns[1] if len(df.columns) > 1 else df.columns[0]
    return df.columns[2] if len(df.columns) > 2 else df.columns[-1]


def parse_duration_to_seconds(s: str) -> float:
    """Support formats like '1:23:45', '12分34秒', '2小時 3分 5秒', '3m 10s'."""
    if s is None:
        return np.nan
    s = s.strip()
    if not s:
        return np.nan
    # HH:MM:SS or MM:SS
    if ":" in s:
        parts = [p for p in s.split(":") if p]
        parts = list(map(float, parts))
        if len(parts) == 3:
            h, m, sec = parts
        elif len(parts) == 2:
            h, m, sec = 0, parts[0], parts[1]
        else:
            return np.nan
        return h * 3600 + m * 60 + sec
    # Chinese units
    h = m = sec = 0.0
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*小?時", s)
    if mobj:
        h = float(mobj.group(1))
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*分", s)
    if mobj:
        m = float(mobj.group(1))
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*秒", s)
    if mobj:
        sec = float(mobj.group(1))
    if h or m or sec:
        return h * 3600 + m * 60 + sec
    # English units
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*h", s, re.I)
    if mobj:
        h = float(mobj.group(1))
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*m(?!s)", s, re.I)
    if mobj:
        m = float(mobj.group(1))
    mobj = re.search(r"(\d+(?:\.\d+)?)\s*s", s, re.I)
    if mobj:
        sec = float(mobj.group(1))
    if h or m or sec:
        return h * 3600 + m * 60 + sec
    return np.nan


def load_logs(paths):
    frames = []
    for p in paths:
        df = pd.read_csv(p, encoding="utf-8-sig")
        df["__source_file"] = os.path.basename(p)
        frames.append(df)
    if not frames:
        raise FileNotFoundError("No CSV logs found.")
    df = pd.concat(frames, ignore_index=True)
    # normalize columns
    tcol = find_col(df, TIME_COL_CANDIDATES)
    lcol = find_col(df, LEVEL_COL_CANDIDATES)
    mcol = find_col(df, MESSAGE_COL_CANDIDATES)
    df = df.rename(columns={tcol: "time", lcol: "level", mcol: "message"})
    # parse time
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df[["time", "level", "message", "__source_file"]].sort_values("time")


def compute_batch_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        msg = str(r["message"]) if pd.notna(r["message"]) else ""
        m = RE_BATCH_DONE.search(msg)
        if m:
            b_idx, b_total, dur_s, processed, total = m.groups()
            rows.append({
                "batch_index": int(b_idx),
                "batch_total": int(b_total),
                "duration_s": parse_duration_to_seconds(dur_s),
                "processed_cum": int(processed),
                "processed_total": int(total),
                "time": r["time"],
                "source": r["__source_file"]
            })
            continue
        m2 = RE_BATCH_FAIL.search(msg)
        if m2:
            b_idx, dur_s = m2.groups()
            rows.append({
                "batch_index": int(b_idx),
                "batch_total": np.nan,
                "duration_s": parse_duration_to_seconds(dur_s),
                "processed_cum": np.nan,
                "processed_total": np.nan,
                "time": r["time"],
                "source": r["__source_file"],
                "failed": True,
            })
    return pd.DataFrame(rows)


def compute_throughput(df: pd.DataFrame, window="1min") -> pd.DataFrame:
    # Identify progress messages and compute per-minute processed deltas
    prog = []
    for _, r in df.iterrows():
        msg = str(r["message"]) if pd.notna(r["message"]) else ""
        m = RE_PROCESSED.search(msg)
        if m:
            cur, total = m.groups()
            prog.append({"time": r["time"], "processed": int(cur), "total": int(total)})
    if not prog:
        return pd.DataFrame()
    p = pd.DataFrame(prog).sort_values("time")
    p = p.set_index("time").resample(window).max().ffill()
    p["processed_delta"] = p["processed"].diff().clip(lower=0).fillna(0)
    p["throughput_per_min"] = p["processed_delta"]  # since window is 1min
    p = p.reset_index()
    return p


def compute_total_elapsed(df: pd.DataFrame) -> float:
    # Prefer explicit total in logs, else fallback to first-to-last timestamp
    dur_candidates = []
    for msg in df["message"].astype(str):
        m = RE_TOTAL_DONE.search(msg)
        if m:
            dur_candidates.append(parse_duration_to_seconds(m.group(1)))
    if dur_candidates:
        vals = [v for v in dur_candidates if pd.notna(v)]
        if vals:
            return float(np.median(vals))
    # fallback
    t = df["time"].dropna()
    if len(t) >= 2:
        return (t.max() - t.min()).total_seconds()
    return np.nan


def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)


def plot_and_save(fig, outpath):
    fig.tight_layout()
    fig.savefig(outpath, dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", nargs="+", default=None, help="CSV log paths or globs")
    ap.add_argument("--outdir", default="research/out", help="Output directory")
    args = ap.parse_args()

    paths = []
    if args.input:
        for pat in args.input:
            paths.extend(glob(pat))
    if not paths:
        print("No input CSVs provided.", file=sys.stderr)
        sys.exit(2)

    ensure_outdir(args.outdir)

    df = load_logs(paths)
    total_elapsed_s = compute_total_elapsed(df)

    # Batch metrics
    batch_df = compute_batch_metrics(df)
    if not batch_df.empty:
        batch_df.to_csv(os.path.join(args.outdir, "batch_durations.csv"), index=False, encoding="utf-8-sig")

        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(batch_df["duration_s"].dropna(), bins=20, ax=ax)
        ax.set_title("Batch Duration Distribution (s)")
        ax.set_xlabel("seconds")
        plot_and_save(fig, os.path.join(args.outdir, "batch_duration_hist.png"))

        fig, ax = plt.subplots(figsize=(12, 6))
        sns.lineplot(x="batch_index", y="duration_s", data=batch_df, marker="o", ax=ax)
        ax.set_title("Batch Duration over Index")
        ax.set_xlabel("batch index")
        ax.set_ylabel("seconds")
        plot_and_save(fig, os.path.join(args.outdir, "batch_duration_line.png"))

    # Throughput
    thr_df = compute_throughput(df)
    if not thr_df.empty:
        thr_df.to_csv(os.path.join(args.outdir, "throughput_over_time.csv"), index=False, encoding="utf-8-sig")
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.lineplot(x="time", y="throughput_per_min", data=thr_df, ax=ax)
        ax.set_title("Throughput (stocks/min)")
        ax.set_ylabel("stocks/min")
        plot_and_save(fig, os.path.join(args.outdir, "throughput.png"))

    # Error timeline
    err_df = df[df["level"].str.lower().eq("error")]
    if not err_df.empty:
        err_t = err_df.set_index("time").assign(count=1).resample("1min").sum().fillna(0)
        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(err_t.index, err_t["count"], drawstyle="steps-mid")
        ax.set_title("Errors per Minute")
        ax.set_ylabel("count")
        plot_and_save(fig, os.path.join(args.outdir, "errors_over_time.png"))

    # Summary
    summary = {
        "inputs": ";".join(os.path.basename(p) for p in paths),
        "total_elapsed_s": total_elapsed_s,
        "batches": int(batch_df["batch_index"].max()) if not batch_df.empty else np.nan,
        "batch_duration_median_s": float(batch_df["duration_s"].median()) if not batch_df.empty else np.nan,
        "batch_duration_p95_s": float(batch_df["duration_s"].quantile(0.95)) if not batch_df.empty else np.nan,
        "throughput_peak_per_min": float(thr_df["throughput_per_min"].max()) if not thr_df.empty else np.nan,
        "errors_total": int(err_df.shape[0]) if 'err_df' in locals() else 0,
    }
    pd.DataFrame([summary]).to_csv(os.path.join(args.outdir, "metrics_summary.csv"), index=False, encoding="utf-8-sig")

    print("Analysis complete. Outputs in:", args.outdir)


if __name__ == "__main__":
    main()
