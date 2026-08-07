"""
Tests pour la couche Silver (src/transform.py).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.transform import flatten_columns  # noqa: E402


def test_flatten_columns_handles_tuple_strings():
    """
    Après un passage par Parquet, les colonnes yfinance à plusieurs niveaux
    reviennent comme des chaînes ressemblant à des tuples, ex: "('Close', 'BTC-USD')".
    flatten_columns doit les convertir en noms simples.
    """
    df = pd.DataFrame({
        "('Date', '')": ["2026-01-01"],
        "('Close', 'BTC-USD')": [100.0],
        "('Volume', 'BTC-USD')": [1000],
        "('ticker', '')": ["BTC-USD"],
    })

    result = flatten_columns(df)

    assert list(result.columns) == ["date", "close", "volume", "ticker"]


def test_flatten_columns_handles_plain_columns():
    """Des colonnes déjà simples ne doivent pas être cassées."""
    df = pd.DataFrame({"Date": ["2026-01-01"], "Close": [100.0]})

    result = flatten_columns(df)

    assert list(result.columns) == ["date", "close"]