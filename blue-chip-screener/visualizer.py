from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


SECTOR_COLORS = {
    "Communication Services": "#1f77b4",
    "Consumer Discretionary": "#ff7f0e",
    "Consumer Staples": "#2ca02c",
    "Energy": "#d62728",
    "Financials": "#8c564b",
    "Health Care": "#e377c2",
    "Industrials": "#7f7f7f",
    "Information Technology": "#17becf",
    "Materials": "#bcbd22",
    "Real Estate": "#9467bd",
    "Utilities": "#393b79",
}


def _build_plot_df(screened_df: pd.DataFrame, lookback_months: int) -> pd.DataFrame:
    rows = []
    cutoff_date = (pd.Timestamp.today() - pd.DateOffset(months=lookback_months)).normalize()

    for _, stock in screened_df.iterrows():
        if stock.get("ticker") == "BRK-B":
            continue
        book_value = stock.get("book_value_per_share")
        if not book_value or book_value <= 0:
            continue

        history = stock.get("price_history") or []
        hpb = stock.get("historical_pb_median")
        pb_anomaly = float(stock.get("pb_anomaly_score") or 0.0)
        anomaly_flag_bool = bool(stock.get("anomaly_flag", False))
        for point in history:
            date_val = pd.to_datetime(point.get("date"), errors="coerce")
            close = point.get("close")
            if pd.isna(date_val) or close is None:
                continue
            if date_val < cutoff_date:
                continue

            valuation_pct = ((close - book_value) / book_value) * 100
            warning = "\u26A0 Eroding Book Value" if bool(stock.get("eroding_book_value", False)) else ""

            curr_pb = close / book_value
            if hpb is not None and not pd.isna(hpb):
                pb_line = f"5yr Median P/B: {float(hpb):.2f}  |  Current P/B: {curr_pb:.2f}"
            else:
                pb_line = f"Current P/B: {curr_pb:.2f}  (5yr median unavailable)"
            anomaly_marker = "  \u26A1" if anomaly_flag_bool else ""
            anomaly_line = f"Anomaly Score: {pb_anomaly:.0f}{anomaly_marker}"

            rows.append(
                {
                    "date": date_val,
                    "y_value": valuation_pct,
                    "ticker": stock.get("ticker"),
                    "company": stock.get("company"),
                    "sector": stock.get("sector"),
                    "price": close,
                    "book_value_per_share": book_value,
                    "valuation_pct": valuation_pct,
                    "attractiveness_score": stock.get("attractiveness_score"),
                    "days_below_book": stock.get("days_below_book"),
                    "warning": warning,
                    "pb_line": pb_line,
                    "anomaly_line": anomaly_line,
                }
            )

    return pd.DataFrame(rows)


def _create_sortable_table_html(df: pd.DataFrame) -> str:
    if df.empty:
        body = "<p>No qualifying stocks found.</p>"
    else:
        display_cols = [
            "ticker",
            "company",
            "sector",
            "current_price",
            "book_value_per_share",
                "current_pb",
                "historical_pb_median",
                "pb_anomaly_score",
                "anomaly_flag",
                "raw_discount_pct",
            "days_below_book",
            "market_cap_component",
            "dividend_streak_component",
            "interest_coverage_component",
            "debt_to_equity_component",
            "fcf_consistency_component",
            "financial_stability_component",
            "sp500_tenure_component",
            "blue_chip_score",
            "value_score",
            "discount_duration_score",
            "discount_penalty_multiplier",
            "eroding_book_value",
            "attractiveness_score",
        ]
        safe_df = df[[col for col in display_cols if col in df.columns]].copy()
        def _anomaly_row_style(row):
            if "anomaly_flag" in row.index and row["anomaly_flag"] is True:
                return ["background-color: #fffde7"] * len(row)
            return [""] * len(row)

        body = (
            safe_df.style
            .apply(_anomaly_row_style, axis=1)
            .hide(axis="index")
            .set_table_attributes('id="scoresTable" class="sortable"')
            .to_html()
        )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Blue Chip Screener Scores</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 20px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
    th {{ cursor: pointer; background: #f5f5f5; position: sticky; top: 0; }}
    tr:nth-child(even) {{ background: #fafafa; }}
  </style>
</head>
<body>
  <h1>Qualifying Stocks Score Table</h1>
  {body}
  <script>
    (function() {{
      const getCellValue = (tr, idx) => tr.children[idx].innerText || tr.children[idx].textContent;
      const comparer = (idx, asc) => (a, b) => ((v1, v2) =>
        !isNaN(parseFloat(v1)) && !isNaN(parseFloat(v2))
          ? parseFloat(v1) - parseFloat(v2)
          : v1.toString().localeCompare(v2)
      )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));

      document.querySelectorAll('th').forEach(th => th.addEventListener('click', (() => {{
        const table = th.closest('table');
        const tbody = table.querySelector('tbody');
        Array.from(tbody.querySelectorAll('tr'))
          .sort(comparer(Array.from(th.parentNode.children).indexOf(th), this.asc = !this.asc))
          .forEach(tr => tbody.appendChild(tr));
      }})));
    }})();
  </script>
</body>
</html>
"""


def build_reports(screened_df: pd.DataFrame, config: Dict[str, Any], base_dir: Path | None = None) -> None:
    root = base_dir or Path(__file__).resolve().parent
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    lookback_months = int(config.get("lookback_months", 12))
    plot_df = _build_plot_df(screened_df, lookback_months)

    fig = go.Figure()

    if not plot_df.empty:
        sectors = sorted([s for s in plot_df["sector"].dropna().unique()])
        fallback_colors = px.colors.qualitative.Bold
        sector_color_map: Dict[str, str] = {}
        for idx, sector in enumerate(sectors):
            sector_color_map[sector] = SECTOR_COLORS.get(sector, fallback_colors[idx % len(fallback_colors)])

        for ticker in sorted(plot_df["ticker"].dropna().unique()):
            stock_data = plot_df[plot_df["ticker"] == ticker].sort_values("date")
            sector = stock_data["sector"].iloc[0]
            color = sector_color_map.get(sector, "#333333")

            fig.add_trace(
                go.Scatter(
                    x=stock_data["date"],
                    y=stock_data["y_value"],
                    mode="lines",
                    name=ticker,
                    line={"color": color, "width": 2},
                    connectgaps=False,
                    customdata=stock_data[
                        [
                            "ticker",
                            "company",
                            "price",
                            "book_value_per_share",
                            "valuation_pct",
                            "attractiveness_score",
                            "days_below_book",
                                "warning",
                                "pb_line",
                                "anomaly_line",
                        ]
                    ],
                    hovertemplate=(
                        "<b>%{customdata[0]} - %{customdata[1]}</b><br>"
                        "Price: $%{customdata[2]:.2f} | Book: $%{customdata[3]:.2f}<br>"
                        "%{customdata[4]:.1f}% vs book<br>"
                        "Attractiveness: %{customdata[5]:.1f}<br>"
                            "%{customdata[8]}<br>"
                            "%{customdata[9]}<br>"
                            "%{customdata[7]}<extra></extra>"
                    ),
                )
            )

            qualified_points = stock_data.dropna(subset=["y_value"])
            if not qualified_points.empty:
                last_point = qualified_points.iloc[-1]
                fig.add_annotation(
                    x=last_point["date"],
                    y=last_point["y_value"],
                    text=str(ticker),
                    showarrow=False,
                    xanchor="left",
                    yanchor="middle",
                    font={"size": 10, "color": color},
                )

    today_str = datetime.now().strftime("%Y-%m-%d")
    fig.add_hrect(
        y0=0,
        y1=50,
        fillcolor="rgba(0,180,0,0.06)",
        line_width=0,
        annotation_text="Above Book Value",
        annotation_position="top left",
    )
    fig.add_hrect(
        y0=-50,
        y1=0,
        fillcolor="rgba(180,0,0,0.06)",
        line_width=0,
        annotation_text="Below Book Value",
        annotation_position="bottom left",
    )
    fig.add_hline(y=0, line_dash="dash", annotation_text="Book Value", annotation_position="top left")
    fig.add_annotation(
        x=0.99,
        y=0.99,
        xref="paper",
        yref="paper",
        xanchor="right",
        yanchor="top",
        showarrow=False,
        align="left",
        bordercolor="#9aa0a6",
        borderwidth=1,
        borderpad=10,
        bgcolor="rgba(255,255,255,0.95)",
        text="<b>Hover a stock for details</b><br><br><br>",
        captureevents=False,
    )
    fig.update_layout(
        title=f"S&P 500 \u2014 Stocks Anomalously Below Book Value \u2014 {today_str}",
        xaxis_title="Date",
        yaxis_title="% Above/Below Book Value",
        yaxis={"range": [-50, 50]},
        template="plotly_white",
        hovermode="closest",
        hoverlabel={
            "align": "left",
            "font": {"family": "Segoe UI, Arial, sans-serif", "size": 12},
            "namelength": 40,
        },
    )

    fig.write_html(str(reports_dir / "index.html"), include_plotlyjs="cdn")

    table_html = _create_sortable_table_html(screened_df)
    (reports_dir / "scores_table.html").write_text(table_html, encoding="utf-8")
