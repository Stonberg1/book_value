from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from data_fetcher import fetch_all_ticker_data
from scorer import compute_scores
from screener import run_screen
from universe_builder import build_sp500_universe
from visualizer import build_reports


def load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    return loaded


def _clear_cache_if_schema_changed(base_dir: Path) -> None:
    """Clear the ticker cache if any entry is missing quarterly_book_values (schema v2)."""
    cache_dir = base_dir / "data" / "cache"
    if not cache_dir.exists():
        return
    json_files = list(cache_dir.glob("*.json"))
    if not json_files:
        return
    needs_clear = False
    for f in json_files[:5]:
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
            data = payload.get("data") or {}
            if "quarterly_book_values" not in data:
                needs_clear = True
                break
        except Exception:
            continue
    if needs_clear:
        print("Cache cleared — schema updated for 5-year history")
        for f in json_files:
            try:
                f.unlink()
            except OSError:
                pass


def print_summary(screened_df: pd.DataFrame) -> None:
    qualified_count = len(screened_df)
    print(f"Qualified stocks: {qualified_count}")

    if screened_df.empty:
        print("No stocks qualified in the latest run.")
        return

        all_stocks = screened_df.sort_values("attractiveness_score", ascending=False)
        print("All qualifying stocks by attractiveness score:")
        for _, row in all_stocks.iterrows():
            anomaly_str = "  \u26a1 ANOMALY" if row.get("anomaly_flag") else ""
            hpb = row.get("historical_pb_median")
            hpb_str = f"{hpb:.2f}" if hpb is not None and str(hpb) != "nan" else "N/A"
            pb_anomaly = row.get("pb_anomaly_score", 0)
            print(
                " - "
                f"{row.get('ticker')} | "
                f"Attractiveness: {row.get('attractiveness_score', 0):.2f} | "
                f"Blue Chip: {row.get('blue_chip_score', 0):.2f} | "
                f"Value: {row.get('value_score', 0):.2f} | "
                f"Price: {row.get('current_price', 0):.2f} | "
                f"Book: {row.get('book_value_per_share', 0):.2f} | "
                f"5yr Median P/B: {hpb_str} | "
                f"Anomaly Score: {pb_anomaly:.1f}"
                f"{anomaly_str}"
            )


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    config = load_config(base_dir / "config.yaml")
    _clear_cache_if_schema_changed(base_dir)
    brk_b_cache = base_dir / "data" / "cache" / "BRK-B.json"
    if brk_b_cache.exists():
        brk_b_cache.unlink()

    universe_df = build_sp500_universe(config=config, base_dir=base_dir)
    fetched_df = fetch_all_ticker_data(universe_df=universe_df, config=config, base_dir=base_dir)
    scored_df = compute_scores(data_df=fetched_df, config=config)
    screened_df = run_screen(scored_df=scored_df, config=config)
    build_reports(screened_df=screened_df, config=config, base_dir=base_dir)

    print_summary(screened_df)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Unhandled error: {exc}", file=sys.stderr)
        sys.exit(1)
