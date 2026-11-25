"""
Tests du contrat de schéma Bronze (src/bronze_schema.py).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.bronze_schema import BRONZE_COLUMNS, to_bronze_schema  # noqa: E402


def yfinance_frame(symbol="BTC-USD") -> pd.DataFrame:
    """Forme réelle renvoyée par yf.download : colonnes MultiIndex (Price, Ticker), index Date."""
    idx = pd.DatetimeIndex(["2026-11-24", "2026-11-25"], name="Date")
    cols = pd.MultiIndex.from_tuples(
        [("Close", symbol), ("High", symbol), ("Low", symbol), ("Open", symbol), ("Volume", symbol)],
        names=["Price", "Ticker"],
    )
    return pd.DataFrame([[100.0, 101.0, 99.0, 99.5, 1000], [102.0, 103.0, 100.0, 100.5, 2000]], index=idx, columns=cols)


def test_yfinance_frame_gets_the_contract_columns():
    out = to_bronze_schema(yfinance_frame(), ticker="BTC-USD")

    assert list(out.columns) == BRONZE_COLUMNS
    assert out["close"].tolist() == [100.0, 102.0]
    assert out["ticker"].tolist() == ["BTC-USD", "BTC-USD"]
    assert str(out["date"].dtype) == "datetime64[ms]"
    assert str(out["volume"].dtype) == "float64"


def test_old_bronze_file_with_tuple_string_columns():
    """Anciens fichiers : colonnes relues comme chaînes "('Close', 'BTC-USD')"."""
    old = pd.DataFrame({
        "('Date', '')": pd.to_datetime(["2026-09-14"]).astype("datetime64[ns]"),
        "('Close', 'BTC-USD')": [78163.38], "('High', 'BTC-USD')": [79568.95],
        "('Low', 'BTC-USD')": [76367.37], "('Open', 'BTC-USD')": [76830.28],
        "('Volume', 'BTC-USD')": [29909358382], "('ticker', '')": ["BTC-USD"],
    })
    out = to_bronze_schema(old)

    assert list(out.columns) == BRONZE_COLUMNS
    assert out.loc[0, "close"] == 78163.38
    assert out.loc[0, "volume"] == 29909358382
    assert str(out["date"].dtype) == "datetime64[ms]"


def test_same_columns_whatever_the_ticker():
    """Le nom des colonnes ne dépend plus de l'actif : c'est ce qui permet l'union en SQL."""
    a = to_bronze_schema(yfinance_frame("BTC-USD"), ticker="BTC-USD")
    b = to_bronze_schema(yfinance_frame("^VIX"), ticker="^VIX")
    assert list(a.columns) == list(b.columns)


def test_values_are_not_modified():
    raw = yfinance_frame()
    out = to_bronze_schema(raw, ticker="BTC-USD")
    assert out["open"].tolist() == raw[("Open", "BTC-USD")].tolist()
    assert out["volume"].tolist() == [1000.0, 2000.0]


def test_missing_column_is_an_error():
    broken = yfinance_frame().drop(columns=[("Volume", "BTC-USD")])
    with pytest.raises(ValueError, match="volume"):
        to_bronze_schema(broken, ticker="BTC-USD")
