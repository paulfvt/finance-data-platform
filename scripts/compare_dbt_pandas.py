"""
Contrôle de la migration vers dbt : compare les sorties dbt (data/dbt/) aux
sorties pandas historiques (data/silver/, data/gold/), ligne à ligne.

Critères : mêmes lignes (mêmes clés), NULL aux mêmes endroits, et valeurs
numériques égales à une tolérance relative de 1e-9 (pandas et DuckDB
n'additionnent pas les flottants dans le même ordre).

    python scripts/compare_dbt_pandas.py silver
    python scripts/compare_dbt_pandas.py gold
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RTOL = 1e-9


def compare(ref: pd.DataFrame, got: pd.DataFrame, keys: list[str], numeric: list[str]) -> list[str]:
    problems = []
    merged = ref.merge(got, on=keys, how="outer", suffixes=("_pandas", "_dbt"), indicator=True)
    only = merged[merged["_merge"] != "both"]
    if len(only):
        problems.append(f"{len(only)} lignes présentes d'un seul côté :\n{only[keys + ['_merge']].head(10)}")
    both = merged[merged["_merge"] == "both"]
    for col in numeric:
        a, b = both[f"{col}_pandas"].astype(float), both[f"{col}_dbt"].astype(float)
        null_mismatch = (a.isna() != b.isna()).sum()
        if null_mismatch:
            problems.append(f"{col} : {null_mismatch} NULL à des endroits différents")
        ok = a.notna() & b.notna()
        if ok.any():
            rel = (np.abs(a[ok] - b[ok]) / np.maximum(np.abs(a[ok]), 1e-12)).max()
            if rel > RTOL:
                problems.append(f"{col} : écart relatif max {rel:.2e} > {RTOL:.0e}")
    print(f"{len(both)} lignes comparées sur {len(numeric)} colonnes numériques")
    return problems


def silver() -> list[str]:
    frames = []
    for path in sorted((DATA / "silver").glob("*.parquet")):
        df = pd.read_parquet(path)
        df["ticker_name"] = path.stem
        frames.append(df.rename(columns={"date": "price_date"}))
    ref = pd.concat(frames, ignore_index=True)
    got = pd.read_parquet(DATA / "dbt" / "silver_daily_prices.parquet")
    for df in (ref, got):
        df["price_date"] = pd.to_datetime(df["price_date"]).dt.normalize()
    numeric = ["open", "high", "low", "close", "volume", "daily_return", "ma_20", "ma_50", "volatility_20d"]
    return compare(ref, got, ["ticker_name", "price_date"], numeric)


def gold() -> list[str]:
    ref = pd.read_parquet(DATA / "gold" / "correlations_30d.parquet").rename(columns={"date": "price_date"})
    got = pd.read_parquet(DATA / "dbt" / "gold_correlations_30d.parquet")
    for df in (ref, got):
        df["price_date"] = pd.to_datetime(df["price_date"]).dt.normalize()
    return compare(ref, got, ["price_date", "asset_1", "asset_2"], ["correlation_30d"])


if __name__ == "__main__":
    layer = sys.argv[1] if len(sys.argv) > 1 else "silver"
    issues = {"silver": silver, "gold": gold}[layer]()
    if issues:
        print("ÉCARTS :\n- " + "\n- ".join(issues))
        sys.exit(1)
    print(f"{layer} : dbt identique à pandas (tolérance relative {RTOL:.0e})")
