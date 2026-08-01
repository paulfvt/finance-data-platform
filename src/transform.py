"""
Couche Silver, nettoyage, alignement calendaire et calcul de metriques
à partir des donnees Bronze.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.tickers import TICKERS  # noqa: E402

BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"


def load_bronze_history(ticker_name: str) -> pd.DataFrame:
    """
    Charge et concatene tous les fichiers Bronze d'un ticker, toutes dates
    de run confondues. Necessaire car Bronze ne stocke qu'une fenetre
    glissante de quelques jours par run, pas l'historique complet.
    """
    files = sorted(BRONZE_DIR.glob(f"*/{ticker_name}.parquet"))
    if not files:
        raise FileNotFoundError(
            f"Aucun fichier Bronze trouve pour {ticker_name} dans {BRONZE_DIR}"
        )

    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    return df

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance renvoie des colonnes en MultiIndex (ex: ('Close', 'BTC-USD'))
    même pour un seul ticker. On les aplatit en noms simples et lisibles.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[0] else col[1] for col in df.columns]
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
    return df
if __name__ == "__main__":
    df = load_bronze_history("bitcoin")
    df = flatten_columns(df)
    print(df.columns.tolist())
    print(df)