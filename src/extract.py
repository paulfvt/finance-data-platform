"""
Couche Bronze, extraction brute des donnees de marche via yfinance
"""

from datetime import date, timedelta
from pathlib import Path
import logging
import sys
import pandas as pd
import shutil
import tempfile

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.tickers import TICKERS
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MAX_RETRIES = 3

# Fenêtre relue à chaque run : quelques jours suffisent quand le pipeline tourne
# souvent, mais après une coupure (PC éteint, Docker arrêté) il faut remonter
# jusqu'au dernier run réussi, sinon l'historique garde un trou définitif.
MIN_LOOKBACK_DAYS = 5
MAX_LOOKBACK_DAYS = 365
LOOKBACK_MARGIN_DAYS = 2  # week-ends et jours fériés autour de la coupure

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"


def last_bronze_date(data_dir: Path, before: date) -> date | None:
    """
    Date du dernier dossier de run Bronze (data/bronze/AAAA-MM-JJ) strictement
    antérieur à `before`, ou None s'il n'y en a pas.

    Le dossier du jour est ignoré : chaque run du jour le réécrit. S'il comptait,
    le 2e run de la journée verrait « dernier run = aujourd'hui », prendrait une
    fenêtre minimale et écraserait le rattrapage fait par le 1er run.
    """
    run_dates = []
    for child in data_dir.glob("*"):
        if not child.is_dir():
            continue
        try:
            run_date = date.fromisoformat(child.name)
        except ValueError:
            continue  # dossier qui n'est pas une date : on l'ignore
        if run_date < before:
            run_dates.append(run_date)
    return max(run_dates, default=None)


def compute_lookback_days(last_run: date | None, today: date) -> int:
    """
    Nombre de jours à relire pour combler le trou depuis le dernier run.
    Sans historique, on remonte au maximum pour avoir de quoi calculer
    les moyennes mobiles et la volatilité dès le premier run.
    """
    if last_run is None:
        return MAX_LOOKBACK_DAYS
    gap = (today - last_run).days + LOOKBACK_MARGIN_DAYS
    return max(MIN_LOOKBACK_DAYS, min(gap, MAX_LOOKBACK_DAYS))


def fetch_ticker(ticker_symbol: str, lookback_days: int = MIN_LOOKBACK_DAYS):
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
    Écriture d'abord dans un répertoire temporaire local au conteneur (véritable
    filesystem Linux, contrairement au volume monté Windows où l'atomicité du
    rename n'est pas garantie), puis copie finale vers le volume partagé.
    """
    out_dir = DATA_DIR / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker_name}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_local_path = Path(tmp.name)

    df.to_parquet(tmp_local_path, index=False)
    shutil.copyfile(str(tmp_local_path), str(out_path))
    tmp_local_path.unlink()

    logger.info("Sauvegarde : %s (%d lignes)", out_path, len(df))
    return out_path

def run_extraction(run_date: date | None = None) -> list[Path]:
    """Point d'entree principal : extrait tous les tickers suivis."""
    run_date = run_date or date.today()
    lookback_days = compute_lookback_days(last_bronze_date(DATA_DIR, before=run_date), run_date)
    logger.info("Fenêtre d'extraction : %d jours", lookback_days)
    saved_paths = []
    errors = {}

    for ticker_name, ticker_symbol in TICKERS.items():
        try:
            df = fetch_ticker(ticker_symbol, lookback_days)
            path = save_bronze(df, ticker_name, run_date)
            saved_paths.append(str(path))
        except Exception as exc:
            logger.error("Extraction abandonnee pour %s : %s", ticker_name, exc)
            errors[ticker_name] = str(exc)

    # Les tickers réussis sont déjà sauvegardés. On lève quand même une erreur pour
    # que la tâche passe en échec dans Airflow (et soit relancée) au lieu
    # d'afficher un run vert qui cache des tickers manquants.
    if errors:
        raise RuntimeError(f"Extraction en échec pour {len(errors)} ticker(s) : {errors}")

    logger.info("Extraction terminee sans erreur (%d tickers).", len(saved_paths))
    return saved_paths

if __name__ == "__main__":
    run_extraction()