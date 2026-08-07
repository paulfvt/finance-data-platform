"""
Tests pour la couche Silver (src/transform.py).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.transform import flatten_columns  # noqa: E402
from src.transform import clean_ticker_history  # noqa: E402
from src.transform import compute_metrics  # noqa: E402


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


def test_clean_ticker_history_removes_invalid_prices():
    """Les lignes avec un prix de clôture nul, négatif ou manquant doivent être filtrées."""
    df = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
        "close": [100.0, 0.0, -5.0, None],
    })

    result = clean_ticker_history(df)

    assert len(result) == 1
    assert result.iloc[0]["close"] == 100.0


def test_clean_ticker_history_deduplicates_by_date():
    """Un même jour apparaissant plusieurs fois (fenêtre de lookback qui se chevauche) ne doit compter qu'une fois."""
    df = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-01", "2026-01-02"],
        "close": [100.0, 100.0, 105.0],
    })

    result = clean_ticker_history(df)

    assert len(result) == 2


def test_clean_ticker_history_sorts_chronologically():
    """Les dates doivent être triées, même si la source les fournit dans le désordre."""
    df = pd.DataFrame({
        "date": ["2026-01-03", "2026-01-01", "2026-01-02"],
        "close": [110.0, 100.0, 105.0],
    })

    result = clean_ticker_history(df)

    assert result["date"].is_monotonic_increasing


def test_compute_metrics_first_row_has_no_return():
    """Le premier jour n'a pas de veille : le rendement journalier doit être NaN."""
    df = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        "close": [100.0, 105.0],
    })

    result = compute_metrics(df)

    assert pd.isna(result.iloc[0]["daily_return"])
    assert not pd.isna(result.iloc[1]["daily_return"])


def test_compute_metrics_return_direction_is_correct():
    """Un prix qui monte doit donner un rendement positif, qui baisse un rendement négatif."""
    df = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
        "close": [100.0, 110.0, 100.0],
    })

    result = compute_metrics(df)

    assert result.iloc[1]["daily_return"] > 0
    assert result.iloc[2]["daily_return"] < 0


def test_compute_metrics_moving_average_columns_exist():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        "close": [100.0, 105.0],
    })

    result = compute_metrics(df)

    assert "ma_20" in result.columns
    assert "ma_50" in result.columns
    assert "volatility_20d" in result.columns