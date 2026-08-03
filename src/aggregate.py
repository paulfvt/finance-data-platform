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

def align_calendar(returns: pd.DataFrame) -> pd.DataFrame:
    """
    Aligne tous les tickers sur un calendrier commun (tous les jours,
    y compris week-ends). Les marchés traditionnels sont fermes le
    week-end : on propage leur derniere valeur connue (forward-fill)
    plutot que de laisser un trou, pour que les correlations glissantes
    puissent être calculées sur des fenetres continues.

    Limite assumee : un rendement forward-fille le week-end vaut 0
    (pas de nouvelle info), ce qui peut legerement lisser les
    correlations impliquant la crypto sur ces jours-la.
    """
    full_range = pd.date_range(returns.index.min(), returns.index.max(), freq="D")
    aligned = returns.reindex(full_range)
    aligned = aligned.ffill()
    aligned.index.name = "date"
    return aligned

if __name__ == "__main__":
    df = load_all_returns()
    df = align_calendar(df)
    print(df)