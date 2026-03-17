"""Diagnose pb_anomaly_score computation for below-book stocks."""
import json
import sys
import os
from pathlib import Path

# Set working dir to project root
os.chdir(Path(__file__).parent)

# Suppress yfinance output
import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).parent))
from scorer import compute_historical_pb_baseline

tickers = ['C', 'CNC', 'KHC', 'TAP', 'TFC', 'MOS', 'ARE', 'CAG']
output_lines = []

for t in tickers:
    f = Path('data/cache') / (t + '.json')
    if not f.exists():
        output_lines.append(f'{t}: cache file not found')
        continue
    d = json.loads(f.read_text())['data']
    qbv = d.get('quarterly_book_values', [])
    ph = d.get('price_history', [])
    bv = d.get('book_value_per_share', 0) or 1
    price = d.get('current_price', 0) or 0
    curr_pb = round(price / bv, 3)
    median_pb = compute_historical_pb_baseline(ph, qbv)
    output_lines.append(f'{t}: qbv_count={len(qbv)}, ph_count={len(ph)}, curr_pb={curr_pb}, median_pb={round(median_pb,3) if median_pb else None}')
    if qbv:
        output_lines.append(f'  first 2 qbv: {qbv[:2]}')

Path('diag_out.txt').write_text('\n'.join(output_lines), encoding='utf-8')
