"""
Couche Bronze, extraction brute des donnees de marche via yfinance
"""

from datetime import date, timedelta
from pathlib import Path
import logging
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.tickers import TICKERS
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MAX_RETRIES = 3

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"

def fetch_ticker(ticker_symbol: str, lookback_days: int = 5):
    """Recupere les cours d'un ticker, avec plusieurs tentatives en cas d'echec."""
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=lookback_days)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(ticker_symbol, start=start.isoformat(), end=end.isoformat(), progress=False)
            if df.empty:
                raise ValueError(f"Aucune donnee retournee pour {ticker_symbol}")
            df = df.reset_index()
            df["ticker"] = ticker_symbol
            return df
        except Exception as exc:
            last_error = exc
            logger.warning("Tentative %s/%s echouee pour %s : %s", attempt, MAX_RETRIES, ticker_symbol, exc)

    raise RuntimeError(f"Echec de l'extraction pour {ticker_symbol} apres {MAX_RETRIES} tentatives") from last_error

def save_bronze(df: pd.DataFrame, ticker_name: str, run_date: date) -> Path:
    """
    Sauvegarde le DataFrame brut en Parquet, partitionné par date d'exécution.
    Écriture atomique (fichier temporaire puis renommage) pour éviter la
    corruption si plusieurs exécutions du pipeline se chevauchent (le
    déclenchement à chaque déverrouillage peut produire des runs rapprochés).
    """
    out_dir = DATA_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker_name}.parquet"
    tmp_path = out_dir / f"{ticker_name}.parquet.tmp"

    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)

    logger.info("Sauvegarde : %s (%d lignes)", out_path, len(df))
    return out_path

def run_extraction(run_date: date | None = None) -> list[Path]:
    """Point d'entree principal : extrait tous les tickers suivis."""
    run_date = run_date or date.today()
    saved_paths = []
    errors = {}

    for ticker_name, ticker_symbol in TICKERS.items():
        try:
            df = fetch_ticker(ticker_symbol)
            path = save_bronze(df, ticker_name, run_date)
            saved_paths.append(str(path))
        except Exception as exc:
            logger.error("Extraction abandonnee pour %s : %s", ticker_name, exc)
            errors[ticker_name] = str(exc)

    if errors:
        logger.warning("Extraction terminee avec %d erreur(s) : %s", len(errors), errors)
    else:
        logger.info("Extraction terminee sans erreur (%d tickers).", len(saved_paths))

    return saved_paths

if __name__ == "__main__":
    run_extraction()