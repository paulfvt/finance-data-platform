"""
Tests de l'agrégation des trades en bougies d'une minute (src/stream/bars.py) :
fermeture par watermark, trades en retard, doublons, offset sûr à valider.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.stream.bars import BarAggregator  # noqa: E402

LATENESS_S = 10


def trade(second: float, trade_id: int, price: float = 100.0, size: float = 1.0,
          side: str = "buy", symbol: str = "BTC-USD", minute: int = 0) -> dict:
    """Trade à 10:<minute>:<second> UTC le 25/11/2026."""
    sec = int(second)
    micro = int(round((second - sec) * 1_000_000))
    ts = datetime(2026, 11, 25, 10, minute, sec, micro, tzinfo=timezone.utc)
    return {
        "symbol": symbol, "trade_id": trade_id, "price": price, "size": size,
        "side": side, "event_time": ts.isoformat().replace("+00:00", "Z"),
    }


def feed(agg: BarAggregator, trades: list[dict], partition: int = 0, start_offset: int = 0) -> list:
    closed = []
    for i, t in enumerate(trades):
        closed += agg.add(t, partition=partition, offset=start_offset + i)
    return closed


@pytest.fixture
def agg():
    return BarAggregator(allowed_lateness_s=LATENESS_S)


def test_bar_stays_open_until_watermark_passes_its_end(agg):
    closed = feed(agg, [trade(5, 1), trade(30, 2), trade(5, 3, minute=1)])
    # Watermark = 10:01:05 - 10 s = 10:00:55 < 10:01:00 : la bougie 10:00 reste ouverte
    assert closed == []

    closed = feed(agg, [trade(10, 4, minute=1)], start_offset=3)
    # Watermark = 10:01:10 - 10 s = 10:01:00 : la bougie 10:00 se ferme
    assert len(closed) == 1
    assert closed[0]["minute"] == datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)


def test_ohlcv_vwap_and_order_flow(agg):
    trades = [
        trade(1, 10, price=100, size=1, side="buy"),
        trade(20, 11, price=110, size=3, side="sell"),
        trade(40, 12, price=90, size=1, side="buy"),
        trade(59, 13, price=105, size=5, side="sell"),
        trade(15, 14, minute=1),   # fait passer le watermark
    ]
    [bar] = feed(agg, trades)

    assert bar["symbol"] == "BTC-USD"
    assert (bar["open"], bar["high"], bar["low"], bar["close"]) == (100, 110, 90, 105)
    assert bar["volume"] == 10
    assert bar["buy_volume"] == 2
    assert bar["vwap"] == pytest.approx((100 * 1 + 110 * 3 + 90 * 1 + 105 * 5) / 10)
    assert bar["trade_count"] == 4
    assert (bar["first_trade_id"], bar["last_trade_id"]) == (10, 13)


def test_open_and_close_follow_trade_id_not_arrival_order(agg):
    """Deux trades arrivés dans le désordre (même seconde) : l'ouverture reste le plus petit trade_id."""
    trades = [trade(1, 21, price=101), trade(1, 20, price=100), trade(50, 23, price=104),
              trade(50, 22, price=103), trade(15, 24, minute=1)]
    [bar] = feed(agg, trades)
    assert bar["open"] == 100
    assert bar["close"] == 104


def test_late_trade_inside_lateness_is_kept(agg):
    """Un trade de 10:00:58 arrivé après un trade de 10:01:05 : encore dans les 10 s, il compte."""
    trades = [trade(30, 1), trade(5, 3, minute=1), trade(58, 2), trade(15, 4, minute=1)]
    [bar] = feed(agg, trades)
    assert bar["trade_count"] == 2
    assert agg.late_trades == 0


def test_trade_arriving_after_its_bar_closed_is_counted_and_dropped(agg):
    closed = feed(agg, [trade(30, 1), trade(15, 3, minute=1)])
    assert len(closed) == 1

    closed = feed(agg, [trade(59, 2)], start_offset=2)   # bougie 10:00 déjà fermée
    assert closed == []
    assert agg.late_trades == 1


def test_duplicate_trade_is_ignored(agg):
    """last_match renvoyé à la reconnexion, ou message rejoué après un redémarrage."""
    trades = [trade(10, 1, size=2), trade(10, 1, size=2), trade(20, 2, size=1), trade(15, 3, minute=1)]
    [bar] = feed(agg, trades)
    assert bar["volume"] == 3
    assert bar["trade_count"] == 2
    assert agg.duplicate_trades == 1


def test_watermark_is_per_symbol(agg):
    """Un BTC très récent ne doit pas fermer la bougie ETH : l'ordre n'est garanti que par symbole."""
    feed(agg, [trade(30, 1, symbol="ETH-USD")], partition=0)
    closed = feed(agg, [trade(30, 1, symbol="BTC-USD", minute=5)], partition=1)
    assert closed == []


def test_several_bars_close_at_once_in_order(agg):
    trades = [trade(10, 1), trade(10, 2, minute=1), trade(10, 3, minute=2), trade(30, 4, minute=5)]
    closed = feed(agg, trades)
    assert [b["minute"].minute for b in closed] == [0, 1, 2]


def test_safe_commit_offset_points_to_oldest_open_trade(agg):
    """
    Valider l'offset du dernier message lu perdrait les trades des bougies
    encore ouvertes si le consommateur plantait. L'offset sûr est celui du
    plus ancien trade encore dans une bougie ouverte.
    """
    feed(agg, [trade(10, 1), trade(20, 2)], start_offset=100)         # offsets 100, 101 : bougie 10:00 ouverte
    assert agg.safe_commit_offsets() == {0: 100}

    feed(agg, [trade(15, 3, minute=1)], start_offset=102)              # ferme 10:00, ouvre 10:01
    assert agg.safe_commit_offsets() == {0: 102}


def test_safe_commit_offset_when_nothing_is_open(agg):
    feed(agg, [trade(10, 1), trade(15, 2, minute=1)], start_offset=7)
    agg.close_until(datetime(2026, 11, 25, 10, 5, tzinfo=timezone.utc))
    # Plus rien d'ouvert : on peut valider juste après le dernier message lu
    assert agg.safe_commit_offsets() == {0: 9}


def test_close_until_closes_idle_symbol(agg):
    """
    Symbole sans nouveau trade : son watermark n'avance plus. Le consommateur
    ferme alors les bougies d'après l'horloge (close_until).
    """
    feed(agg, [trade(30, 1, symbol="ETH-USD")])
    closed = agg.close_until(datetime(2026, 11, 25, 10, 1, LATENESS_S, tzinfo=timezone.utc))
    assert len(closed) == 1
    assert closed[0]["symbol"] == "ETH-USD"
