"""
Contrat de schéma de la couche Bronze.

yfinance renvoie des colonnes à deux niveaux, par exemple ('Close', 'BTC-USD').
Écrites telles quelles en Parquet, elles deviennent des chaînes de caractères
"('Close', 'BTC-USD')" dont le nom change d'un actif à l'autre : impossible de
réunir les 8 actifs dans une même table SQL. Bronze reste brute pour les
VALEURS (aucun nettoyage, aucun calcul), mais ses COLONNES suivent ce contrat.
"""

from __future__ import annotations

import ast

import pandas as pd

BRONZE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "ticker"]
BRONZE_DTYPES = {
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",   # yfinance renvoie selon les actifs des entiers ou des flottants
    "ticker": "string",
}


def _column_name(col) -> str:
    """('Close', 'BTC-USD'), "('Close', 'BTC-USD')" ou 'Close' -> 'close'."""
    if isinstance(col, str) and col.startswith("("):
        try:
            col = ast.literal_eval(col)
        except (ValueError, SyntaxError):
            pass
    if isinstance(col, tuple):
        col = col[0] if col[0] else col[1]
    return str(col).strip().lower().replace(" ", "_")


def to_bronze_schema(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """
    Met un DataFrame yfinance (ou un ancien fichier Bronze) au format du contrat.
    Les valeurs ne sont pas modifiées : seuls les noms de colonnes, leur ordre
    et les types le sont. Lève une erreur si une colonne attendue manque.
    """
    out = df.reset_index() if "Date" in (df.index.names or []) else df.copy()
    out.columns = [_column_name(c) for c in out.columns]
    if ticker is not None:
        out["ticker"] = ticker

    missing = [c for c in BRONZE_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes pour le schéma Bronze : {missing} (reçu : {list(out.columns)})")

    out = out[BRONZE_COLUMNS].astype(BRONZE_DTYPES)
    # Une seule unité de temps (les anciens fichiers mélangent ms et ns), sans fuseau
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None).astype("datetime64[ms]")
    return out
