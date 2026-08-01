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


if __name__ == "__main__":
    df = load_bronze_history("bitcoin")
    print(df)