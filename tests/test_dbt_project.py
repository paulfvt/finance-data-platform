"""
Cohérence entre le projet dbt et le code Python.
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from config.tickers import TICKERS  # noqa: E402


def test_tickers_seed_matches_config():
    """
    Le seed dbt et config/tickers.py doivent lister les mêmes actifs, dans le
    même ordre : l'extraction suit l'un, les paires de Gold suivent l'autre.
    """
    with open(ROOT / "dbt" / "seeds" / "tickers.csv", encoding="utf-8") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: int(r["display_order"]))

    assert [(r["ticker_name"], r["ticker"]) for r in rows] == list(TICKERS.items())
