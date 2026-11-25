"""
Tests de la garantie at-least-once du consommateur (src/stream/writer.py),
sans broker : un faux journal Kafka (liste de messages par partition) et une
fonction de validation qui mémorise les offsets validés.

Propriété vérifiée : quel que soit le moment où le consommateur est tué, la
reprise au dernier offset validé donne exactement les mêmes bougies sur disque
qu'un traitement sans interruption.
"""

import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.stream.storage import read_bars  # noqa: E402
from src.stream.writer import BarsWriter  # noqa: E402

T0 = datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc)
FAR_FUTURE = T0 + timedelta(days=1)


def make_log(seed: int = 0, minutes: int = 6) -> dict[int, list[str]]:
    """
    Faux journal : partition 0 = ETH, partition 1 = BTC, ~1 trade toutes les
    2 s, avec quelques trades en retard (dans et hors tolérance) et un doublon.
    """
    rng = random.Random(seed)
    log = {0: [], 1: []}
    for partition, symbol in ((0, "ETH-USD"), (1, "BTC-USD")):
        tid = 1000
        for i in range(minutes * 30):
            ts = T0 + timedelta(seconds=2 * i)
            if rng.random() < 0.05:
                ts -= timedelta(seconds=rng.choice([5, 25]))   # retard : 5 s toléré, 25 s non
            tid += 1
            trade = {"symbol": symbol, "trade_id": tid, "price": 100 + rng.random(),
                     "size": rng.random(), "side": rng.choice(["buy", "sell"]),
                     "event_time": ts.isoformat().replace("+00:00", "Z")}
            log[partition].append(json.dumps(trade))
            if rng.random() < 0.02:
                log[partition].append(json.dumps(trade))        # doublon (last_match, rejeu)
    return log


def interleave(log: dict[int, list[str]], start: dict[int, int]) -> list[tuple[int, int, str]]:
    """Ordre de lecture : on alterne les partitions, comme un consommateur réel."""
    out = []
    cursors = dict(start)
    while any(cursors[p] < len(log[p]) for p in log):
        for p in sorted(log):
            if cursors[p] < len(log[p]):
                out.append((p, cursors[p], log[p][cursors[p]]))
                cursors[p] += 1
    return out


def run(log, root, start=None, stop_after=None):
    """Consomme le journal depuis `start` ; s'arrête brutalement après `stop_after` messages (crash)."""
    committed = dict(start or {p: 0 for p in log})
    writer = BarsWriter(commit=committed.update, root=root)
    for n, (p, o, value) in enumerate(interleave(log, committed)):
        if stop_after is not None and n == stop_after:
            return committed          # crash : pas de flush, l'état en mémoire est perdu
        writer.handle(value, p, o)
    if stop_after is not None:
        return committed              # crash juste après le dernier message lu
    writer.idle(now=FAR_FUTURE)       # à jour, bien plus tard : tout se ferme
    return committed


def bars_on_disk(root):
    return read_bars(root).sort_values(["symbol", "minute"]).reset_index(drop=True)


def test_full_run_writes_one_bar_per_symbol_and_minute(tmp_path):
    run(make_log(), tmp_path)
    df = bars_on_disk(tmp_path)
    # 2 symboles x 6 minutes, plus parfois 09:59 (trade « en retard » généré tout au début)
    assert 12 <= len(df) <= 14
    assert not df.duplicated(["symbol", "minute"]).any()


def test_wall_clock_does_not_close_bars_while_catching_up(tmp_path):
    """
    Rattrapage après redémarrage : les trades rejoués datent de 10:00 alors
    qu'il est bien plus tard. Tant qu'on lit des messages, aucune fermeture
    d'après l'horloge : la bougie garde tous ses trades.
    """
    log = make_log(minutes=2)
    committed = {p: 0 for p in log}
    writer = BarsWriter(commit=committed.update, root=tmp_path)
    for p, o, value in interleave(log, committed):
        writer.handle(value, p, o)      # pas d'idle() pendant le rattrapage
    writer.idle(now=FAR_FUTURE)

    df = bars_on_disk(tmp_path)
    assert df[df["minute"] == T0]["trade_count"].min() >= 25   # ~30 trades par minute, pas 1


@pytest.mark.parametrize("seed", range(5))
def test_crash_and_resume_gives_the_same_bars(tmp_path, seed):
    log = make_log(seed)
    run(log, tmp_path / "reference")
    expected = bars_on_disk(tmp_path / "reference")

    total = sum(len(v) for v in log.values())
    crash_points = sorted(random.Random(seed).sample(range(10, total), 3))

    root = tmp_path / "crashy"
    committed = None
    for crash in crash_points:                             # 3 crashs successifs
        committed = run(log, root, start=committed, stop_after=crash - (sum(committed.values()) if committed else 0))
    run(log, root, start=committed)                        # dernière reprise jusqu'au bout

    assert bars_on_disk(root).equals(expected)


def test_replayed_late_trade_does_not_overwrite_a_written_bar(tmp_path):
    """
    Bug évité : une bougie fermée et écrite, puis un trade en retard (ignoré),
    puis un crash. Au rejeu, sans mémoire de ce qui est écrit, ce trade
    recréerait la bougie avec un seul trade et l'upsert écraserait la bonne.
    """
    def t(minute, second, tid):
        ts = T0 + timedelta(minutes=minute, seconds=second)
        return json.dumps({"symbol": "BTC-USD", "trade_id": tid, "price": 100.0, "size": 1.0,
                           "side": "buy", "event_time": ts.isoformat().replace("+00:00", "Z")})

    log = {0: [t(0, 10, 1), t(0, 20, 2), t(0, 30, 3),       # bougie 10:00 : 3 trades
               t(1, 5, 4), t(1, 15, 5),                     # ferme 10:00 (écrite), 10:01 ouverte
               t(0, 40, 6),                                 # en retard, hors tolérance : ignoré
               t(1, 30, 7)]}

    committed = run(log, tmp_path, stop_after=len(log[0]))  # crash à la fin, avant la fermeture de 10:01
    assert committed == {0: 3}                              # reprise au 1er trade de 10:01 (offset 3)

    run(log, tmp_path, start=committed)                     # rejeu : relit le trade en retard (offset 5)
    df = bars_on_disk(tmp_path)
    bar_1000 = df[df["minute"] == T0].iloc[0]
    assert bar_1000["trade_count"] == 3                     # pas écrasée par une bougie à 1 trade
