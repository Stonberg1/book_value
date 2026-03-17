from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from scorer import compute_scores


def load_config(config_path: Path) -> Dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_cached_records(cache_dir: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for cache_file in sorted(cache_dir.glob("*.json")):
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            continue

        book = data.get("book_value_per_share")
        if book is None:
            continue

        try:
            if float(book) <= 0:
                continue
        except (TypeError, ValueError):
            continue

        records.append(data)
    return records


def month_end_close(history: List[Dict[str, Any]], as_of: pd.Timestamp) -> float | None:
    if not history:
        return None

    hist_df = pd.DataFrame(history)
    if hist_df.empty or "date" not in hist_df.columns or "close" not in hist_df.columns:
        return None

    hist_df["date"] = pd.to_datetime(hist_df["date"], errors="coerce")
    hist_df["close"] = pd.to_numeric(hist_df["close"], errors="coerce")
    hist_df = hist_df.dropna(subset=["date", "close"]).sort_values("date")
    if hist_df.empty:
        return None

    eligible = hist_df[hist_df["date"] <= as_of]
    if eligible.empty:
        return None

    return float(eligible.iloc[-1]["close"])


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    cache_dir = base_dir / "data" / "cache"

    config = load_config(base_dir / "config.yaml")
    cached_records = load_cached_records(cache_dir)

    if not cached_records:
        print("No valid cached records found in data/cache.")
        return

    data_df = pd.DataFrame(cached_records)
    scored_df = compute_scores(data_df=data_df, config=config)

    month_ends = pd.date_range(end=pd.Timestamp.today().normalize(), periods=12, freq="ME")

    rows: List[Dict[str, Any]] = []
    for _, row in scored_df.sort_values("ticker").iterrows():
        ticker = row.get("ticker")
        book = row.get("book_value_per_share")
        blue_chip = float(row.get("blue_chip_score", 0.0))
        history = row.get("price_history") or []

        qualified_months: List[str] = []
        for month_end in month_ends:
            close = month_end_close(history, month_end)
            if close is None:
                continue
            if close < book and blue_chip >= 50:
                qualified_months.append(month_end.strftime("%Y-%m"))

        rows.append(
            {
                "ticker": ticker,
                "blue_chip_score": round(blue_chip, 2),
                "below_book_months_when_bluechip_ge_50": ", ".join(qualified_months) if qualified_months else "-",
            }
        )

    out_df = pd.DataFrame(rows)
    print("Historical qualification check (cache only)")
    print("Condition: price < book_value AND blue_chip_score >= 50")
    print(f"Tickers analyzed: {len(out_df)}")
    print(f"Month-ends checked: {len(month_ends)} ({month_ends[0].strftime('%Y-%m')} to {month_ends[-1].strftime('%Y-%m')})")
    print()
    # Compact CSV output keeps the full table readable in terminal logs.
    print(out_df.to_csv(index=False))


if __name__ == "__main__":
    main()
