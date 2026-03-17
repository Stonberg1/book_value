from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yfinance as yf

# Tickers excluded from fetching because yfinance returns data that makes
# price-to-book comparison invalid for them.
# BRK-B: yfinance reports Class A book value per share, which is ~1500x the
#         Class B share price, making the stock appear massively below book.
_EXCLUDED_TICKERS = {"BRK-B"}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _extract_row(cashflow_df: pd.DataFrame | None, aliases: List[str]) -> pd.Series | None:
    if cashflow_df is None or cashflow_df.empty:
        return None
    for alias in aliases:
        if alias in cashflow_df.index:
            return cashflow_df.loc[alias]
    return None


def _series_to_year_value_map(series: pd.Series | None, limit: int | None = None) -> Dict[str, float | None]:
    if series is None:
        return {}

    ordered_items = []
    for col, value in series.items():
        year = str(getattr(col, "year", col))
        ordered_items.append((year, _safe_float(value)))

    if limit is not None:
        ordered_items = ordered_items[:limit]
    return dict(ordered_items)


def _compute_days_below_book(price_history: List[Dict[str, Any]], book_value_per_share: float | None) -> int:
    if book_value_per_share is None or book_value_per_share <= 0:
        return 0

    days = 0
    for point in reversed(price_history):
        close = _safe_float(point.get("close"))
        if close is None:
            continue
        if close < book_value_per_share:
            days += 1
        else:
            break
    return days


def _extract_book_value_4q_ago(quarterly_balance_sheet: pd.DataFrame, shares_outstanding: float | None) -> float | None:
    if quarterly_balance_sheet is None or quarterly_balance_sheet.empty or not shares_outstanding:
        return None

    equity_aliases = [
        "Stockholders Equity",
        "Total Stockholder Equity",
        "Total Equity Gross Minority Interest",
    ]

    equity_row = None
    for alias in equity_aliases:
        if alias in quarterly_balance_sheet.index:
            equity_row = quarterly_balance_sheet.loc[alias]
            break

    if equity_row is None or len(equity_row) < 4:
        return None

    equity_4q_ago = _safe_float(equity_row.iloc[3])
    if equity_4q_ago is None or shares_outstanding <= 0:
        return None

    return equity_4q_ago / shares_outstanding


def _extract_quarterly_book_values(
    quarterly_balance_sheet: pd.DataFrame, shares_outstanding: float | None
) -> List[Dict[str, Any]]:
    """Extract a time series of quarterly book value per share from the balance sheet.

    For each quarter, divides total stockholders equity by shares outstanding.
    Falls back to point-in-time shares_outstanding when per-quarter share counts
    are not available in the balance sheet.
    """
    if quarterly_balance_sheet is None or quarterly_balance_sheet.empty:
        return []

    equity_aliases = [
        "Stockholders Equity",
        "Total Stockholder Equity",
        "Total Equity Gross Minority Interest",
    ]
    shares_aliases = [
        "Share Issued",
        "Ordinary Shares Number",
    ]

    equity_row = None
    for alias in equity_aliases:
        if alias in quarterly_balance_sheet.index:
            equity_row = quarterly_balance_sheet.loc[alias]
            break

    if equity_row is None:
        return []

    shares_row = None
    for alias in shares_aliases:
        if alias in quarterly_balance_sheet.index:
            shares_row = quarterly_balance_sheet.loc[alias]
            break

    out: List[Dict[str, Any]] = []
    for col in equity_row.index:
        equity = _safe_float(equity_row[col])
        if equity is None:
            continue

        shares = _safe_float(shares_row[col]) if shares_row is not None else shares_outstanding
        if shares is None or shares <= 0:
            continue

        bvps = equity / shares
        if bvps <= 0:
            continue

        try:
            date_str = pd.Timestamp(col).strftime("%Y-%m-%d")
        except Exception:
            continue

        out.append({"date": date_str, "book_value_per_share": round(bvps, 4)})

    return out


def _serialize_price_history(history_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if history_df is None or history_df.empty:
        return []

    closes = history_df[["Close"]].copy()
    closes = closes.dropna(subset=["Close"])

    out: List[Dict[str, Any]] = []
    for idx, row in closes.iterrows():
        date_str = pd.Timestamp(idx).strftime("%Y-%m-%d")
        out.append({"date": date_str, "close": _safe_float(row["Close"])})
    return out


def _serialize_dividend_counts(dividends: pd.Series) -> Dict[str, int]:
    if dividends is None or dividends.empty:
        return {}

    yearly = dividends.groupby(dividends.index.year).count()
    return {str(int(year)): int(count) for year, count in yearly.items()}


def _cache_is_fresh(cache_path: Path, ttl_hours: int) -> bool:
    if not cache_path.exists():
        return False
    age_seconds = datetime.now(tz=timezone.utc).timestamp() - cache_path.stat().st_mtime
    return age_seconds < ttl_hours * 3600


def _default_record(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": row.get("ticker"),
        "company": row.get("company"),
        "sector": row.get("sector"),
        "date_added": str(row.get("date_added")) if row.get("date_added") else None,
        "current_price": None,
        "book_value_per_share": None,
        "book_value_per_share_4q_ago": None,
        "market_cap": None,
        "debt_to_equity": None,
        "operating_cash_flow": None,
        "interest_expense": None,
        "free_cash_flow_by_year": {},
        "dividend_year_counts": {},
        "price_history": [],
        "quarterly_book_values": [],
        "days_below_book": 0,
    }


def _read_cache(cache_path: Path) -> Dict[str, Any] | None:
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("data")
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None


def _write_cache(cache_path: Path, data: Dict[str, Any]) -> None:
    payload = {"fetched_at": datetime.now(tz=timezone.utc).isoformat(), "data": data}
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)


def _fetch_single_ticker(row: Dict[str, Any], cache_dir: Path, cache_ttl_hours: int) -> Dict[str, Any]:
    ticker_symbol = str(row.get("ticker", "")).strip()
    if not ticker_symbol or ticker_symbol in _EXCLUDED_TICKERS:
        return _default_record(row)

    cache_path = cache_dir / f"{ticker_symbol}.json"
    if _cache_is_fresh(cache_path, cache_ttl_hours):
        cached = _read_cache(cache_path)
        if cached is not None:
            return cached

    record = _default_record(row)

    try:
        ticker = yf.Ticker(ticker_symbol)

        history_df = ticker.history(period="5y")
        price_history = _serialize_price_history(history_df)

        info = ticker.info or {}
        book_value_per_share = _safe_float(info.get("bookValue"))

        record["price_history"] = price_history
        record["current_price"] = price_history[-1]["close"] if price_history else _safe_float(info.get("currentPrice"))
        record["book_value_per_share"] = book_value_per_share
        record["market_cap"] = _safe_float(info.get("marketCap"))
        record["debt_to_equity"] = _safe_float(info.get("debtToEquity"))

        dividends = ticker.dividends
        record["dividend_year_counts"] = _serialize_dividend_counts(dividends)

        cashflow_df = ticker.cashflow
        ocf_row = _extract_row(cashflow_df, ["Operating Cash Flow", "Total Cash From Operating Activities"])
        fcf_row = _extract_row(cashflow_df, ["Free Cash Flow"])
        interest_row = _extract_row(cashflow_df, ["Interest Expense"])

        ocf_map = _series_to_year_value_map(ocf_row, limit=1)
        interest_map = _series_to_year_value_map(interest_row, limit=1)
        fcf_map = _series_to_year_value_map(fcf_row, limit=5)

        record["operating_cash_flow"] = next(iter(ocf_map.values()), None)
        record["interest_expense"] = next(iter(interest_map.values()), None)
        record["free_cash_flow_by_year"] = fcf_map

        shares_outstanding = _safe_float(info.get("sharesOutstanding"))
        quarterly_bs = ticker.quarterly_balance_sheet
        record["book_value_per_share_4q_ago"] = _extract_book_value_4q_ago(quarterly_bs, shares_outstanding)
        record["quarterly_book_values"] = _extract_quarterly_book_values(quarterly_bs, shares_outstanding)

        record["days_below_book"] = _compute_days_below_book(price_history, book_value_per_share)

    except Exception:
        # Missing upstream fields and transient API issues should not crash the pipeline.
        pass

    if record.get("book_value_per_share") is not None and record["book_value_per_share"] <= 0:
        record["book_value_per_share"] = None

    _write_cache(cache_path, record)
    return record


def fetch_all_ticker_data(universe_df: pd.DataFrame, config: Dict[str, Any], base_dir: Path | None = None) -> pd.DataFrame:
    root = base_dir or Path(__file__).resolve().parent
    cache_dir = root / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_ttl_hours = int(config.get("cache_ttl_hours", 24))

    rows = universe_df.to_dict(orient="records")
    records: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {
            executor.submit(_fetch_single_ticker, row, cache_dir, cache_ttl_hours): row.get("ticker")
            for row in rows
        }
        for future in as_completed(future_to_ticker):
            record = future.result()
            if record.get("book_value_per_share") is not None and record["book_value_per_share"] > 0:
                records.append(record)

    if not records:
        return pd.DataFrame(
            columns=[
                "ticker",
                "company",
                "sector",
                "date_added",
                "current_price",
                "book_value_per_share",
                "book_value_per_share_4q_ago",
                "market_cap",
                "debt_to_equity",
                "operating_cash_flow",
                "interest_expense",
                "free_cash_flow_by_year",
                "dividend_year_counts",
                "price_history",
                "quarterly_book_values",
                "days_below_book",
            ]
        )

    return pd.DataFrame(records)
