from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _dividend_streak_score(year_counts: Dict[str, int] | None, cap_years: int) -> float:
    if not year_counts:
        return 0.0

    current_year = date.today().year
    consecutive_years = 0
    for year in range(current_year, current_year - 50, -1):
        if int(year_counts.get(str(year), 0)) >= 2:
            consecutive_years += 1
        else:
            break
    return min(consecutive_years, 25) / 25


def _interest_coverage_subscore(ocf: float | None, interest_expense: float | None, debt_to_equity_pct: float | None) -> float:
    if debt_to_equity_pct is not None and debt_to_equity_pct == 0:
        return 1.0

    if ocf is None or interest_expense is None:
        return 0.0

    denominator = abs(interest_expense)
    if denominator == 0:
        return 1.0 if (debt_to_equity_pct is not None and debt_to_equity_pct == 0) else 0.0

    coverage = ocf / denominator
    if coverage <= 0:
        return 0.0
    return _clamp(min(coverage, 15) / 15, 0.0, 1.0)


def _debt_to_equity_subscore(debt_to_equity_pct: float | None) -> float:
    if debt_to_equity_pct is None:
        return 0.0

    de_ratio = debt_to_equity_pct / 100
    if de_ratio < 0:
        return 0.0

    score = 1 - min(abs(de_ratio), 3) / 3
    return _clamp(score, 0.0, 1.0)


def _fcf_consistency_subscore(fcf_by_year: Dict[str, float | None] | None) -> float:
    if not fcf_by_year:
        return 0.0

    values = list(fcf_by_year.values())[:5]
    positive_years = sum(1 for value in values if value is not None and value > 0)
    return positive_years / 5


def _years_since_added(date_added: Any) -> float:
    parsed = pd.to_datetime(date_added, errors="coerce")
    if pd.isna(parsed):
        return 0.0
    years = (pd.Timestamp.today().normalize() - parsed).days / 365.25
    return max(0.0, years)


def _discount_duration_score(days_below_book: int | float | None) -> float:
    if days_below_book is None:
        return 0.0

    days = int(days_below_book)
    if days < 90:
        return 0.0
    if days < 180:
        return 25.0
    if days < 270:
        return 50.0
    if days < 365:
        return 75.0
    return 100.0


def compute_historical_pb_baseline(
    price_history: List[Dict[str, Any]] | None,
    quarterly_book_values: List[Dict[str, Any]] | None,
) -> float | None:
    """Interpolate quarterly book values to a daily series and return the 5-year median P/B ratio.

    Returns None if fewer than 4 quarters of book value data are available to ensure
    the baseline is statistically meaningful. yfinance typically provides 4-5 quarters
    of quarterly balance sheet data, so we require at least 4 quarters.
    """
    if not price_history or not quarterly_book_values:
        return None
    if len(quarterly_book_values) < 4:
        return None

    bv_dict: Dict[pd.Timestamp, float] = {}
    for q in quarterly_book_values:
        try:
            dt = pd.to_datetime(q["date"])
            bv = float(q["book_value_per_share"])
            if bv > 0:
                bv_dict[dt] = bv
        except (KeyError, TypeError, ValueError):
            continue

    if len(bv_dict) < 4:
        return None

    price_dict: Dict[pd.Timestamp, float] = {}
    for p in price_history:
        try:
            dt = pd.to_datetime(p["date"])
            close = p.get("close")
            if close is not None:
                val = float(close)
                if val > 0:
                    price_dict[dt] = val
        except (KeyError, TypeError, ValueError):
            continue

    if not price_dict:
        return None

    bv_series = pd.Series(bv_dict).sort_index()
    price_series = pd.Series(price_dict).sort_index()

    # Forward-fill quarterly book values to every trading day in the price series.
    combined_index = price_series.index.union(bv_series.index)
    bv_daily = bv_series.reindex(combined_index).ffill()
    bv_at_price_dates = bv_daily.reindex(price_series.index).dropna()

    if bv_at_price_dates.empty:
        return None

    prices_aligned = price_series.loc[bv_at_price_dates.index]
    pb_series = prices_aligned / bv_at_price_dates
    pb_series = pb_series[pb_series > 0]

    if pb_series.empty:
        return None

    return float(pb_series.median())


def compute_scores(data_df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    if data_df.empty:
        return data_df.copy()

    df = data_df.copy()

    dividend_streak_cap = int(config.get("dividend_streak_cap", 15))
    if dividend_streak_cap <= 0:
        dividend_streak_cap = 15

    weights = config.get("score_weights", {})
    market_cap_weight = float(weights.get("market_cap", 0.30))
    dividend_weight = float(weights.get("dividend_streak", 0.25))
    stability_weight = float(weights.get("financial_stability", 0.25))
    tenure_weight = float(weights.get("sp500_tenure", 0.20))

    valid_market_cap = df["market_cap"].where(df["market_cap"] > 0)
    log_mcap = np.log(valid_market_cap)
    df["market_cap_component"] = log_mcap.rank(pct=True).fillna(0.0)

    df["dividend_streak_component"] = df["dividend_year_counts"].apply(
        lambda counts: _dividend_streak_score(counts, dividend_streak_cap)
    )

    df["interest_coverage_component"] = df.apply(
        lambda row: _interest_coverage_subscore(
            row.get("operating_cash_flow"), row.get("interest_expense"), row.get("debt_to_equity")
        ),
        axis=1,
    )
    df["debt_to_equity_component"] = df["debt_to_equity"].apply(_debt_to_equity_subscore)
    df["fcf_consistency_component"] = df["free_cash_flow_by_year"].apply(_fcf_consistency_subscore)
    df["financial_stability_component"] = (
        df[["interest_coverage_component", "debt_to_equity_component", "fcf_consistency_component"]].mean(axis=1)
    )

    # S&P 400 tenure component retained in the legacy sp500_tenure_component column for compatibility.
    df["sp500_tenure_component"] = df["date_added"].apply(lambda x: min(_years_since_added(x), 30) / 30)

    df["blue_chip_score"] = 100 * (
        df["market_cap_component"] * market_cap_weight
        + df["dividend_streak_component"] * dividend_weight
        + df["financial_stability_component"] * stability_weight
        # S&P 400 tenure weight remains mapped from score_weights.sp500_tenure for backward compatibility.
        + df["sp500_tenure_component"] * tenure_weight
    )

    below_book_mask = (df["current_price"] < df["book_value_per_share"]) & (df["book_value_per_share"] > 0)
    raw_discount = (df["book_value_per_share"] - df["current_price"]) / df["book_value_per_share"] * 100
    df["raw_discount_pct"] = raw_discount.where(below_book_mask, 0.0)
    df["value_score"] = (df["raw_discount_pct"].clip(lower=0.0, upper=50.0) / 50.0) * 100

    df["discount_duration_score"] = df["days_below_book"].apply(_discount_duration_score)
    df["discount_penalty_multiplier"] = 1 - (df["discount_duration_score"] / 200.0)

    df["excluded_prolonged_discount"] = False
    if bool(config.get("exclude_prolonged_discounts", False)):
        max_days = int(config.get("max_discount_days", 365))
        df["excluded_prolonged_discount"] = df["days_below_book"] >= max_days

    current_bv = df["book_value_per_share"]
    past_bv = df["book_value_per_share_4q_ago"]
    yoy_change = (current_bv - past_bv) / past_bv
    df["book_value_yoy_change"] = yoy_change.replace([np.inf, -np.inf], np.nan)
    df["eroding_book_value"] = df["book_value_yoy_change"].lt(-0.10).fillna(False)

    blue_chip_weight = float(config.get("blue_chip_weight", 0.5))
    value_weight = float(config.get("value_weight", 0.5))

    pre_penalty = df["blue_chip_score"] * blue_chip_weight + df["value_score"] * value_weight
    df["attractiveness_score"] = pre_penalty * df["discount_penalty_multiplier"]

    # Historical P/B median and P/B anomaly score.
    if "quarterly_book_values" in df.columns:
        df["historical_pb_median"] = df.apply(
            lambda row: compute_historical_pb_baseline(
                row.get("price_history"),
                row.get("quarterly_book_values"),
            ),
            axis=1,
        )
    else:
        df["historical_pb_median"] = None

    bv_safe = df["book_value_per_share"].where(df["book_value_per_share"] > 0)
    df["current_pb"] = df["current_price"] / bv_safe

    def _pb_anomaly(row: pd.Series) -> float:
        try:
            median_pb = row["historical_pb_median"]
            curr_pb = row["current_pb"]
            if median_pb is None or pd.isna(median_pb):
                return 0.0
            if curr_pb is None or pd.isna(curr_pb):
                return 0.0
            median_pb = float(median_pb)
            curr_pb = float(curr_pb)
            if median_pb < 1.0:
                return 0.0
            score = (median_pb - curr_pb) / median_pb * 100.0
            return float(min(max(score, 0.0), 100.0))
        except (TypeError, ValueError, KeyError):
            return 0.0

    df["pb_anomaly_score"] = df.apply(_pb_anomaly, axis=1)

    return df
