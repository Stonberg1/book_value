from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def _cache_is_fresh(cache_path: Path, max_age_days: int) -> bool:
    if not cache_path.exists():
        return False
    age_seconds = pd.Timestamp.utcnow().timestamp() - cache_path.stat().st_mtime
    return age_seconds < max_age_days * 86400


def _normalize_universe_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_col = None
    for candidate in ("Date added", "Date first added"):
        if candidate in df.columns:
            date_col = candidate
            break

    date_series = pd.Series(dtype="object") if date_col is None else df[date_col]
    normalized = pd.DataFrame(
        {
            "ticker": df.get("Symbol", pd.Series(dtype=str)).astype(str).str.strip().str.replace(".", "-", regex=False),
            "company": df.get("Security", pd.Series(dtype=str)).astype(str).str.strip(),
            "sector": df.get("GICS Sector", pd.Series(dtype=str)).astype(str).str.strip(),
            "date_added": pd.to_datetime(date_series, errors="coerce").dt.date,
        }
    )
    return normalized


def build_sp500_universe(config: Dict[str, Any], base_dir: Path | None = None) -> pd.DataFrame:
    root = base_dir or Path(__file__).resolve().parent
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    cache_path = data_dir / "sp500_universe.csv"
    universe_cache_days = int(config.get("universe_cache_days", 7))

    if _cache_is_fresh(cache_path, universe_cache_days):
        cached = pd.read_csv(cache_path)
        if "date_added" in cached.columns:
            cached["date_added"] = pd.to_datetime(cached["date_added"], errors="coerce").dt.date
        return cached[["ticker", "company", "sector", "date_added"]]

    table = pd.read_html(
        StringIO(requests.get(SP500_WIKI_URL, headers=_HEADERS, timeout=30).text),
        flavor="lxml",
    )[0]
    universe_df = _normalize_universe_columns(table)
    universe_df.to_csv(cache_path, index=False)
    return universe_df
