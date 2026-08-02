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

import numpy as np

MOVING_AVERAGE_WINDOWS = (20, 50)
VOLATILITY_WINDOW = 20

SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"

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

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le rendement logarithmique journalier, les moyennes mobiles
    (20/50 jours) et la volatilité glissante (écart-type des rendements
    sur 20 jours).
    """
    df = df.copy()
    price_ratio = df["close"] / df["close"].shift(1)
    df["daily_return"] = np.log(price_ratio.where(price_ratio > 0))

    for window in MOVING_AVERAGE_WINDOWS:
        df[f"ma_{window}"] = df["close"].rolling(window=window, min_periods=1).mean()

    df["volatility_20d"] = df["daily_return"].rolling(
        window=VOLATILITY_WINDOW, min_periods=2
    ).std()

    return df

def save_silver(df: pd.DataFrame, ticker_name: str) -> Path:
    """
    Sauvegarde l'historique Silver complet d'un ticker. Pas partitionné
    par date comme Bronze : c'est une table d'historique unique,
    régénérée à chaque run à partir de tout l'historique Bronze.
    """
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SILVER_DIR / f"{ticker_name}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("Silver sauvegardé : %s (%d lignes)", out_path, len(df))
    return out_path


def run_transformation() -> list[Path]:
    """Point d'entrée principal : transforme l'historique Bronze de chaque ticker suivi."""
    saved_paths = []
    errors = {}

    for ticker_name in TICKERS:
        try:
            raw = load_bronze_history(ticker_name)
            cleaned = clean_ticker_history(raw)
            enriched = compute_metrics(cleaned)
            path = save_silver(enriched, ticker_name)
            saved_paths.append(str(path))
        except Exception as exc:
            logger.error("Transformation abandonnée pour %s : %s", ticker_name, exc)
            errors[ticker_name] = str(exc)

    if errors:
        logger.warning("Transformation terminée avec %d erreur(s) : %s", len(errors), errors)
    else:
        logger.info("Transformation terminée sans erreur (%d tickers).", len(saved_paths))

    return saved_paths


if __name__ == "__main__":
    run_transformation()