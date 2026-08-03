"""
Couche Gold, matrices de corrélation glissantes et agregats entre actifs,
a partir des donnees Silver.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.tickers import TICKERS  # noqa: E402

SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"


def load_all_returns() -> pd.DataFrame:
    """
    Charge le rendement journalier de chaque ticker suivi et les fusionne
    en une seule table large (une colonne par ticker, indexee par date).
    """
    series = {}
    for ticker_name in TICKERS:
        path = SILVER_DIR / f"{ticker_name}.parquet"
        df = pd.read_parquet(path, columns=["date", "daily_return"])
        series[ticker_name] = df.set_index("date")["daily_return"]

    returns = pd.DataFrame(series)
    return returns


if __name__ == "__main__":
    df = load_all_returns()
    print(df)