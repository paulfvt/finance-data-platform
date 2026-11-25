"""
Tests de l'écriture idempotente des bougies live (src/stream/storage.py).
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.stream.storage import read_bars, upsert_bars  # noqa: E402


def bar(minute: int, hour: int = 10, symbol: str = "BTC-USD", close: float = 100.0, trades: int = 5) -> dict:
    return {
        "symbol": symbol,
        "minute": datetime(2026, 11, 25, hour, minute, tzinfo=timezone.utc),
        "open": 99.0, "high": 101.0, "low": 98.0, "close": close,
        "volume": 1.5, "buy_volume": 1.0, "vwap": 100.0, "trade_count": trades,
        "first_trade_id": 1, "last_trade_id": trades,
    }


def test_files_are_split_by_symbol_and_hour(tmp_path):
    paths = upsert_bars([bar(58, hour=9), bar(1, hour=10), bar(1, symbol="ETH-USD")], root=tmp_path)

    rel = sorted(str(p.relative_to(tmp_path)).replace("\\", "/") for p in paths)
    assert rel == [
        "symbol=BTC-USD/date=2026-11-25/hour=09.parquet",
        "symbol=BTC-USD/date=2026-11-25/hour=10.parquet",
        "symbol=ETH-USD/date=2026-11-25/hour=10.parquet",
    ]


def test_replaying_the_same_bars_is_idempotent(tmp_path):
    """Cas du redémarrage après crash : les mêmes bougies sont recalculées et réécrites."""
    bars = [bar(0), bar(1), bar(2)]
    upsert_bars(bars, root=tmp_path)
    first = read_bars(tmp_path)

    upsert_bars(bars, root=tmp_path)
    upsert_bars(bars[1:], root=tmp_path)
    again = read_bars(tmp_path)

    assert len(again) == 3
    assert again.equals(first)


def test_new_minutes_are_appended_in_order(tmp_path):
    upsert_bars([bar(5)], root=tmp_path)
    upsert_bars([bar(7), bar(6)], root=tmp_path)

    minutes = read_bars(tmp_path)["minute"].dt.minute.tolist()
    assert minutes == [5, 6, 7]


def test_a_rewritten_minute_replaces_the_old_version(tmp_path):
    upsert_bars([bar(5, close=100.0, trades=5)], root=tmp_path)
    upsert_bars([bar(5, close=101.0, trades=6)], root=tmp_path)

    df = read_bars(tmp_path)
    assert len(df) == 1
    assert df.iloc[0]["close"] == 101.0
    assert df.iloc[0]["trade_count"] == 6


def test_last_written_minutes_per_symbol(tmp_path):
    from src.stream.storage import last_written_minutes
    upsert_bars([bar(58, hour=9), bar(3, hour=10), bar(1, hour=10), bar(59, hour=9, symbol="ETH-USD")], root=tmp_path)

    latest = {s: m.to_pydatetime() for s, m in last_written_minutes(tmp_path).items()}
    assert latest == {
        "BTC-USD": datetime(2026, 11, 25, 10, 3, tzinfo=timezone.utc),
        "ETH-USD": datetime(2026, 11, 25, 9, 59, tzinfo=timezone.utc),
    }


def test_empty_batch_writes_nothing(tmp_path):
    assert upsert_bars([], root=tmp_path) == []
    assert read_bars(tmp_path).empty
