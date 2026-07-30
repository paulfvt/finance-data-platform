"""
Couche Bronze, extraction brute des donnees de marche via yfinance
"""

from datetime import date, timedelta
from pathlib import Path
import yfinance as yf
import logging

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


def save_bronze(df, ticker_name: str, run_date: date) -> Path:
    """Sauvegarde le DataFrame brut en Parquet, partitionné par date d'exécution."""
    out_dir = DATA_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker_name}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("Sauvegardé : %s (%d lignes)", out_path, len(df))
    return out_path


if __name__ == "__main__":
    df = fetch_ticker("BTC-USD")
    save_bronze(df, "bitcoin", date.today())