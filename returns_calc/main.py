import argparse
from datetime import datetime
import pandas as pd
from rich.console import Console
from rich.progress import track

from db import ensure_tables, fetch_symbols, fetch_prices, fetch_existing_return_dates, upsert_returns
from returns import normalize_prices, compute_returns_from_close, build_return_records

console = Console()

def parse_args():
    p = argparse.ArgumentParser(description="Compute stock returns (daily/weekly/monthly/quarterly/yearly) from stock_prices to stock_returns")
    g_scope = p.add_mutually_exclusive_group(required=False)
    g_scope.add_argument("--all", action="store_true", help="Process all symbols in stock_prices")
    g_scope.add_argument("--symbol", type=str, help="Single symbol to process, e.g. 2330.TW")

    p.add_argument("--start", type=str, help="Start date YYYY-MM-DD", default=None)
    p.add_argument("--end", type=str, help="End date YYYY-MM-DD", default=None)
    p.add_argument("--limit", type=int, help="Limit number of symbols when using --all (for testing)", default=None)
    p.add_argument("--fill-missing", action="store_true", help="Only compute rows not existing in stock_returns")
    args = p.parse_args()
    # Default behavior: if neither --all nor --symbol provided, default to --all
    if not getattr(args, "all", False) and not getattr(args, "symbol", None):
        console.print("[yellow]No scope flag provided. Defaulting to --all.[/yellow]")
        args.all = True
    return args


def process_symbol(symbol: str, start: str | None, end: str | None, fill_missing: bool):
    # fetch price rows from DB
    rows = fetch_prices(symbol, start, end)
    if not rows:
        console.print(f"[yellow]No prices for {symbol} in given range[/yellow]")
        return 0

    # build DataFrame and compute returns
    price_df = normalize_prices(rows)
    ret_df = compute_returns_from_close(price_df)

    if ret_df.empty:
        console.print(f"[yellow]No computable returns for {symbol}[/yellow]")
        return 0

    # If fill-missing: filter out dates that already exist in stock_returns
    if fill_missing:
        existing_dates = fetch_existing_return_dates(symbol, start, end)
        if existing_dates:
            ret_df = ret_df.loc[[d for d in ret_df.index.date if d not in existing_dates]]
            ret_df.index = pd.to_datetime(ret_df.index)

    records = build_return_records(symbol, ret_df)
    written = upsert_returns(records)
    return written


def main():
    args = parse_args()
    ensure_tables()

    symbols = []
    if args.all:
        symbols = fetch_symbols(limit=args.limit)
    else:
        symbols = [args.symbol]

    total_written = 0
    for sym in track(symbols, description="Computing returns"):
        try:
            written = process_symbol(sym, args.start, args.end, args.fill_missing)
            total_written += written
        except Exception as e:
            console.print(f"[red]Error processing {sym}: {e}[/red]")

    console.print(f"[green]Done. Upserted {total_written} rows into stock_returns.[/green]")


if __name__ == "__main__":
    main()
