from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def run_screen(scored_df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    if scored_df.empty:
        return scored_df.copy()

    # Exclude sectors where trading below book value is structurally normal and does
    # not carry the same anomaly signal as it does for industrials, technology, or
    # consumer companies. Banks and insurers (Financials) have book values dominated
    # by loan portfolios and regulatory capital requirements; REITs (Real Estate) use
    # historical-cost depreciated asset values that typically understate market value.
    excluded_sectors = config.get("excluded_sectors", [])
    if excluded_sectors:
        df = scored_df[~scored_df["sector"].isin(excluded_sectors)].copy()
    else:
        df = scored_df.copy()

    filtered = df[
        (df["book_value_per_share"] > 0) & (df["current_price"] < df["book_value_per_share"])
    ].copy()

    if bool(config.get("exclude_prolonged_discounts", False)):
        max_days = int(config.get("max_discount_days", 365))
        filtered = filtered[filtered["days_below_book"] < max_days]

    min_blue_chip_score = config.get("min_blue_chip_score")
    if min_blue_chip_score is not None:
        filtered = filtered[filtered["blue_chip_score"] >= float(min_blue_chip_score)]

    # anomaly_flag: True when pb_anomaly_score >= 50, meaning the stock is trading at
    # less than half its typical 5-year price-to-book ratio — a strong anomaly signal.
    if "pb_anomaly_score" in filtered.columns:
        filtered["anomaly_flag"] = filtered["pb_anomaly_score"] >= 50
    else:
        filtered["anomaly_flag"] = False

    # Primary sort: attractiveness_score descending.
    # Alternative: sort by pb_anomaly_score descending to prioritize stocks showing
    # the largest deviation from their own historical P/B baseline.
    filtered = filtered.sort_values("attractiveness_score", ascending=False)

    top_n = config.get("top_n", 20)
    if top_n is not None:
        filtered = filtered.head(int(top_n))

    return filtered.reset_index(drop=True)
