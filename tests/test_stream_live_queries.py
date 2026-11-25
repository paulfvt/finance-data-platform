"""
Tests des métriques live DuckDB (src/stream/live_queries.py), sur des bougies
synthétiques écrites avec le vrai module de stockage.
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.stream.live_queries import MIN_OBS, MINUTES_PER_YEAR, live_metrics, live_status  # noqa: E402
from src.stream.storage import upsert_bars  # noqa: E402

T0 = datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)


def bars(closes, start=T0, symbol="BTC-USD"):
    out = []
    for i, close in enumerate(closes):
        out.append({
            "symbol": symbol, "minute": start + timedelta(minutes=i),
            "open": close, "high": close, "low": close, "close": close,
            "volume": 2.0, "buy_volume": 0.5, "vwap": close, "trade_count": 10,
            "first_trade_id": i, "last_trade_id": i,
        })
    return out


def test_log_return_between_consecutive_minutes(tmp_path):
    upsert_bars(bars([100.0, 110.0]), root=tmp_path)
    df = live_metrics(since=T0, root=tmp_path)

    assert math.isnan(df.loc[0, "log_return"])                 # pas de minute précédente
    assert df.loc[1, "log_return"] == pytest.approx(math.log(110 / 100))
    assert df.loc[1, "buy_ratio"] == pytest.approx(0.25)


def test_no_return_across_a_gap(tmp_path):
    """PC éteint entre 10:01 et 10:30 : pas de « rendement d'une minute » sur 29 minutes."""
    upsert_bars(bars([100.0, 101.0]) + bars([150.0], start=T0 + timedelta(minutes=30)), root=tmp_path)
    df = live_metrics(since=T0, root=tmp_path)

    assert df.loc[1, "log_return"] == pytest.approx(math.log(101 / 100))
    assert math.isnan(df.loc[2, "log_return"])


def test_volatility_needs_enough_observations(tmp_path):
    closes = [100.0 * (1.001 if i % 2 else 0.999) ** i for i in range(MIN_OBS + 1)]
    upsert_bars(bars(closes), root=tmp_path)
    df = live_metrics(since=T0, root=tmp_path)

    assert df["vol_30m_annualized"].iloc[:MIN_OBS].isna().all()   # moins de MIN_OBS rendements
    assert not math.isnan(df["vol_30m_annualized"].iloc[MIN_OBS])


def test_volatility_window_is_time_based(tmp_path):
    """
    40 minutes de prix agités, un trou d'une heure, puis 12 minutes de prix
    plats : la fenêtre en temps ne doit plus voir l'agitation d'avant le trou.
    """
    agitated = [100.0 * (1.01 if i % 2 else 0.99) for i in range(40)]
    flat = [100.0 + 0.0001 * i for i in range(12)]
    upsert_bars(bars(agitated) + bars(flat, start=T0 + timedelta(minutes=100)), root=tmp_path)
    df = live_metrics(since=T0, root=tmp_path)

    last = df.iloc[-1]
    assert last["vol_obs"] == 11                                  # seulement les rendements d'après le trou
    assert last["vol_30m_annualized"] < 0.01                      # plus rien de l'agitation


def test_volatility_matches_definition(tmp_path):
    closes = [100.0, 101.0, 100.5, 102.0, 101.0, 103.0, 102.5, 104.0, 103.0, 105.0, 104.0, 106.0]
    upsert_bars(bars(closes), root=tmp_path)
    df = live_metrics(since=T0, root=tmp_path)

    rets = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    mean = sum(rets) / len(rets)
    std = math.sqrt(sum((r - mean) ** 2 for r in rets) / (len(rets) - 1))
    assert df["vol_30m_annualized"].iloc[-1] == pytest.approx(std * math.sqrt(MINUTES_PER_YEAR))


def test_since_cuts_output_but_keeps_window_history(tmp_path):
    """On ne renvoie que depuis `since`, mais la fenêtre utilise les minutes d'avant."""
    closes = [100.0 + (i % 3) for i in range(40)]
    upsert_bars(bars(closes), root=tmp_path)
    since = T0 + timedelta(minutes=35)
    df = live_metrics(since=since, root=tmp_path)

    assert df["minute"].min() == since
    assert df["vol_obs"].iloc[0] == 30                             # fenêtre pleine dès la 1re ligne


def test_live_status_reports_freshness(tmp_path):
    upsert_bars(bars([100.0, 101.0, 102.0]) + bars([50.0], symbol="ETH-USD"), root=tmp_path)
    status = live_status(root=tmp_path, now=T0 + timedelta(minutes=10)).set_index("symbol")

    assert status.loc["BTC-USD", "last_close"] == 102.0
    assert status.loc["BTC-USD", "minutes_behind"] == 8
    assert status.loc["ETH-USD", "minutes_behind"] == 10


def test_empty_storage(tmp_path):
    assert live_metrics(since=T0, root=tmp_path).empty
    assert live_status(root=tmp_path, now=T0).empty
