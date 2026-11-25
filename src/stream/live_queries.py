"""
Métriques live calculées en SQL avec DuckDB, directement sur les fichiers
Parquet écrits par le consommateur (data/live/bars). Aucune base à maintenir :
DuckDB lit les fichiers à la demande, en lecture seule, pendant que le
consommateur continue d'écrire.

Choix de calcul :
- Rendement log minute par minute, mis à NULL si la bougie précédente n'est
  pas la minute d'avant (PC éteint, flux coupé) : sinon un « rendement d'une
  minute » couvrirait des heures.
- Volatilité glissante sur une fenêtre de 30 minutes en TEMPS (RANGE), pas en
  lignes (ROWS) : après un trou, 30 lignes pourraient couvrir des heures.
- Volatilité affichée seulement à partir de MIN_OBS rendements, annualisée
  sur 525 600 minutes (la crypto cote 24 h/24, 7 j/7).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from src.stream.storage import LIVE_BARS_DIR

VOL_WINDOW_MIN = 30
MIN_OBS = 10
MINUTES_PER_YEAR = 525_600

METRICS_SQL = f"""
WITH bars AS (
    SELECT *
    FROM read_parquet($files, hive_partitioning = false, union_by_name = true)
    WHERE minute >= $since
),
returns AS (
    SELECT
        *,
        CASE
            WHEN lag(minute) OVER w = minute - INTERVAL 1 MINUTE
            THEN ln(close / lag(close) OVER w)
        END AS log_return
    FROM bars
    WINDOW w AS (PARTITION BY symbol ORDER BY minute)
)
SELECT
    symbol, minute, open, high, low, close, volume, buy_volume, vwap, trade_count,
    buy_volume / volume AS buy_ratio,
    log_return,
    count(log_return) OVER vol AS vol_obs,
    CASE
        WHEN count(log_return) OVER vol >= {MIN_OBS}
        THEN stddev_samp(log_return) OVER vol * sqrt({MINUTES_PER_YEAR})
    END AS vol_30m_annualized
FROM returns
WINDOW vol AS (
    PARTITION BY symbol ORDER BY minute
    RANGE BETWEEN INTERVAL {VOL_WINDOW_MIN - 1} MINUTES PRECEDING AND CURRENT ROW
)
ORDER BY symbol, minute
"""


def bar_files(root: Path, since: datetime) -> list[str]:
    """
    Fichiers horaires à lire depuis `since`. On ne liste que les dossiers de
    date utiles (élagage des partitions fait à la main) : le dashboard ne relit
    pas tout l'historique à chaque rafraîchissement.
    """
    days = {(since + timedelta(days=d)).date() for d in range((date.today() - since.date()).days + 2)}
    files = []
    for symbol_dir in root.glob("symbol=*"):
        for day in days:
            files += sorted(str(p) for p in symbol_dir.glob(f"date={day:%Y-%m-%d}/hour=*.parquet"))
    return files


def live_metrics(since: datetime, root: Path = LIVE_BARS_DIR) -> pd.DataFrame:
    """
    Bougies et métriques live depuis `since`. Le calcul de la fenêtre a besoin
    des VOL_WINDOW_MIN minutes d'avant : on lit un peu plus tôt puis on coupe.
    """
    read_from = since - timedelta(minutes=VOL_WINDOW_MIN)
    files = bar_files(root, read_from)
    if not files:
        return pd.DataFrame()
    df = duckdb.execute(METRICS_SQL, {"files": files, "since": read_from}).df()
    return df[df["minute"] >= pd.Timestamp(since)].reset_index(drop=True)


def live_status(root: Path = LIVE_BARS_DIR, now: datetime | None = None) -> pd.DataFrame:
    """
    Fraîcheur du flux par symbole : dernière bougie écrite et retard par rapport
    à maintenant. Sert au dashboard pour afficher « flux arrêté depuis X min ».
    """
    now = now or datetime.now(timezone.utc)
    files = bar_files(root, now - timedelta(days=1))
    if not files:
        return pd.DataFrame(columns=["symbol", "last_minute", "last_close", "minutes_behind"])
    return duckdb.execute(
        """
        SELECT symbol,
               max(minute) AS last_minute,
               arg_max(close, minute) AS last_close,
               date_diff('minute', max(minute), $now) AS minutes_behind
        FROM read_parquet($files, hive_partitioning = false, union_by_name = true)
        GROUP BY symbol
        ORDER BY symbol
        """,
        {"files": files, "now": now},
    ).df()
