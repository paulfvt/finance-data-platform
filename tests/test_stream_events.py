"""
Tests de la logique pure du producteur streaming (src/stream/events.py).
Les messages d'exemple reprennent le format réel du websocket Coinbase.
"""

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.stream.events import backoff_delay, missed_trades, normalize  # noqa: E402

MATCH = {
    "type": "match",
    "trade_id": 1146571709,
    "side": "buy",
    "size": "0.00000002",
    "price": "58807.84",
    "product_id": "BTC-USD",
    "sequence": 140242110577,
    "time": "2026-11-25T09:43:51.096021Z",
}


def test_normalize_match():
    event = normalize(MATCH)

    assert event["symbol"] == "BTC-USD"
    assert event["trade_id"] == 1146571709
    assert event["price"] == pytest.approx(58807.84)
    assert event["size"] == pytest.approx(0.00000002)
    assert event["side"] == "buy"
    assert event["event_time"] == "2026-11-25T09:43:51.096021Z"
    assert "ingested_at" in event


def test_normalize_keeps_last_match():
    """last_match est un vrai trade (renvoyé à la connexion) : on le garde."""
    assert normalize({**MATCH, "type": "last_match"}) is not None


@pytest.mark.parametrize("message", [
    {"type": "heartbeat", "product_id": "BTC-USD", "last_trade_id": 1},
    {"type": "subscriptions", "channels": []},
    {**MATCH, "price": "0"},
    {**MATCH, "size": "-1"},
    {**MATCH, "price": "abc"},
    {**MATCH, "side": "unknown"},
    {k: v for k, v in MATCH.items() if k != "trade_id"},
])
def test_normalize_rejects_non_trades_and_bad_values(message):
    assert normalize(message) is None


def test_missed_trades():
    assert missed_trades(None, 100) == 0      # premier trade vu
    assert missed_trades(100, 101) == 0       # suite normale
    assert missed_trades(100, 105) == 4       # 101..104 perdus
    assert missed_trades(105, 105) == 0       # doublon (last_match après reconnexion)
    assert missed_trades(105, 103) == 0       # retour en arrière : pas un trou


def test_backoff_grows_then_caps():
    delays = [backoff_delay(n, jitter=0) for n in range(1, 10)]
    assert delays[:4] == [1, 2, 4, 8]
    assert max(delays) == 60


def test_backoff_jitter_stays_bounded():
    for _ in range(100):
        assert 0.8 <= backoff_delay(1) <= 1.2
