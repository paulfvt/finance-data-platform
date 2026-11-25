"""
Écriture idempotente des bougies live en Parquet.

Disposition : data/live/bars/symbol=BTC-USD/date=2026-11-25/hour=10.parquet
- Un fichier par symbole et par heure (48 fichiers/jour) : assez gros pour
  éviter le problème des petits fichiers, assez petit pour être réécrit à
  chaque bougie sans coût notable (60 lignes au plus).
- Upsert sur (symbol, minute) : on relit le fichier de l'heure, on remplace ou
  ajoute les minutes reçues, on réécrit. Rejouer les mêmes bougies après un
  crash redonne exactement le même fichier : l'écriture est idempotente.
- Écriture via un fichier temporaire local au conteneur, puis copie vers le
  volume monté depuis Windows (même précaution qu'en Bronze, voir README).
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pandas as pd

LIVE_BARS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "live" / "bars"
KEY = ["symbol", "minute"]


def hour_file(root: Path, symbol: str, minute: pd.Timestamp) -> Path:
    return root / f"symbol={symbol}" / f"date={minute:%Y-%m-%d}" / f"hour={minute:%H}.parquet"


def _read_file(path: Path) -> pd.DataFrame:
    # partitioning=None : sans ça, pyarrow lit « symbol=BTC-USD » dans le chemin,
    # ajoute une colonne symbol déduite du dossier, et la fusion avec la colonne
    # symbol du fichier échoue (types incompatibles). Le fichier contient déjà
    # toutes ses colonnes : on ne déduit rien du chemin.
    return pd.read_parquet(path, partitioning=None)


def _write_atomically(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        df.to_parquet(tmp_path, index=False)
        shutil.copyfile(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def upsert_bars(bars: list[dict], root: Path = LIVE_BARS_DIR) -> list[Path]:
    """Écrit les bougies dans leurs fichiers horaires (upsert sur symbol+minute). Renvoie les fichiers touchés."""
    if not bars:
        return []
    new = pd.DataFrame(bars)
    new["minute"] = pd.to_datetime(new["minute"], utc=True)

    written = []
    for (symbol, hour), group in new.groupby(["symbol", new["minute"].dt.floor("h")]):
        path = hour_file(root, symbol, hour)
        if path.exists():
            existing = _read_file(path)
            merged = pd.concat([existing, group], ignore_index=True)
        else:
            merged = group
        # keep="last" : la version qui vient d'arriver remplace l'ancienne
        merged = merged.drop_duplicates(subset=KEY, keep="last").sort_values("minute").reset_index(drop=True)
        _write_atomically(merged, path)
        written.append(path)
    return written


def last_written_minutes(root: Path = LIVE_BARS_DIR) -> dict[str, pd.Timestamp]:
    """Dernière minute écrite par symbole (lue dans le fichier horaire le plus récent de chaque symbole)."""
    latest = {}
    for symbol_dir in root.glob("symbol=*"):
        files = sorted(symbol_dir.glob("date=*/hour=*.parquet"))   # tri lexical = tri chronologique
        if files:
            latest[symbol_dir.name.removeprefix("symbol=")] = _read_file(files[-1])["minute"].max()
    return latest


def read_bars(root: Path = LIVE_BARS_DIR) -> pd.DataFrame:
    """Relit toutes les bougies live (pour les tests et les contrôles ; le dashboard passera par DuckDB)."""
    files = sorted(root.glob("symbol=*/date=*/hour=*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([_read_file(f) for f in files], ignore_index=True)
