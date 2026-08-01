"""
Couche Silver, nettoyage, alignement calendaire et calcul de metriques
à partir des donnees Bronze.
"""

import ast
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.tickers import TICKERS  # noqa: E402

BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
    Après le passage par Parquet, les colonnes yfinance à plusieurs niveaux
    (ex: ('Close', 'BTC-USD')) sont relues comme de simples chaînes de
    caractères ressemblant à des tuples (ex: "('Close', 'BTC-USD')"),
    et non comme de vrais tuples Python. On les re-parse avant de les
    aplatir.
    """
    def flatten_one(col):
        if isinstance(col, str) and col.startswith("("):
            try:
                col = ast.literal_eval(col)
            except (ValueError, SyntaxError):
                return col
        if isinstance(col, tuple):
            return col[0] if col[0] else col[1]
        return col

    df.columns = [str(flatten_one(col)).strip().lower().replace(" ", "_") for col in df.columns]
    return df

def clean_ticker_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplique par date (un run peut recuperer plusieurs fois le même
    jour via la fenetre de lookback), trie chronologiquement, et
    supprime les lignes avec un prix de cloture manquant ou aberrant.
    """
    df = flatten_columns(df)

    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)

    before = len(df)
    df = df[df["close"].notna() & (df["close"] > 0)]
    dropped = before - len(df)
    if dropped:
        logger.warning("%d ligne(s) supprimee(s) (close manquant ou <= 0)", dropped)

    return df

if __name__ == "__main__":
    df = load_bronze_history("bitcoin")
    df = clean_ticker_history(df)
    print(df)