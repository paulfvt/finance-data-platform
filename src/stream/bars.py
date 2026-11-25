"""
Agrégation des trades en bougies d'une minute, en temps d'événement.

Logique pure (aucune dépendance Kafka ni fichier) pour être testée seule.
Le consommateur lui passe chaque trade avec sa partition et son offset, et
récupère les bougies fermées.

Concepts :
- Watermark par symbole = heure du trade le plus récent vu - allowed_lateness.
  « Tout ce qui est plus ancien que le watermark est censé être arrivé. »
  Une bougie se ferme quand le watermark atteint sa fin.
- Trade en retard : arrive alors que sa bougie est déjà fermée. Il est compté
  (late_trades) puis ignoré, pour ne pas réécrire une bougie déjà publiée.
- Doublon : même (symbole, trade_id) déjà agrégé dans une bougie ouverte.
- Offset sûr : par partition, l'offset du plus ancien trade encore dans une
  bougie ouverte. Valider au-delà perdrait ces trades si le consommateur
  plantait avant d'avoir écrit leur bougie.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

MINUTE = timedelta(minutes=1)


def parse_event_time(value: str) -> datetime:
    """Heure ISO 8601 Coinbase ("...Z") vers datetime UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def minute_start(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class _OpenBar:
    symbol: str
    minute: datetime
    open_id: int
    open: float
    close_id: int
    close: float
    high: float
    low: float
    volume: float = 0.0
    buy_volume: float = 0.0
    notional: float = 0.0   # somme prix x quantité, pour la VWAP
    trade_count: int = 0
    trade_ids: set[int] = field(default_factory=set)
    min_offset: dict[int, int] = field(default_factory=dict)   # partition -> plus petit offset

    def add(self, trade: dict, partition: int, offset: int) -> None:
        tid, price, size = trade["trade_id"], trade["price"], trade["size"]
        if tid < self.open_id:
            self.open_id, self.open = tid, price
        if tid > self.close_id:
            self.close_id, self.close = tid, price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.volume += size
        self.notional += price * size
        if trade["side"] == "buy":
            self.buy_volume += size
        self.trade_count += 1
        self.trade_ids.add(tid)
        self.min_offset[partition] = min(offset, self.min_offset.get(partition, offset))

    def to_record(self) -> dict:
        return {
            "symbol": self.symbol,
            "minute": self.minute,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "buy_volume": self.buy_volume,
            "vwap": self.notional / self.volume,
            "trade_count": self.trade_count,
            "first_trade_id": self.open_id,
            "last_trade_id": self.close_id,
        }


class BarAggregator:
    def __init__(self, allowed_lateness_s: float = 10.0) -> None:
        self.allowed_lateness = timedelta(seconds=allowed_lateness_s)
        self._open: dict[str, dict[datetime, _OpenBar]] = {}    # symbole -> minute -> bougie
        self._max_event_time: dict[str, datetime] = {}
        self._closed_until: dict[str, datetime] = {}            # symbole -> fin de la dernière bougie fermée
        self._next_offset: dict[int, int] = {}                  # partition -> dernier offset lu + 1
        self.late_trades = 0
        self.duplicate_trades = 0

    # --- entrée -------------------------------------------------------------

    def add(self, trade: dict, partition: int, offset: int) -> list[dict]:
        """Ajoute un trade. Renvoie les bougies que ce trade a permis de fermer."""
        self._next_offset[partition] = max(offset + 1, self._next_offset.get(partition, 0))
        symbol = trade["symbol"]
        ts = parse_event_time(trade["event_time"])
        minute = minute_start(ts)

        closed_until = self._closed_until.get(symbol)
        if closed_until is not None and minute + MINUTE <= closed_until:
            self.late_trades += 1
            return []

        bars = self._open.setdefault(symbol, {})
        bar = bars.get(minute)
        if bar is None:
            bar = bars[minute] = _OpenBar(
                symbol=symbol, minute=minute,
                open_id=trade["trade_id"], open=trade["price"],
                close_id=trade["trade_id"], close=trade["price"],
                high=trade["price"], low=trade["price"],
            )
        elif trade["trade_id"] in bar.trade_ids:
            self.duplicate_trades += 1
            return []
        bar.add(trade, partition, offset)

        previous = self._max_event_time.get(symbol)
        if previous is None or ts > previous:
            self._max_event_time[symbol] = ts
        watermark = self._max_event_time[symbol] - self.allowed_lateness
        return self._close_symbol(symbol, watermark)

    def close_until(self, now: datetime) -> list[dict]:
        """
        Ferme, pour tous les symboles, les bougies finies avant now - allowed_lateness.
        Sert quand un symbole ne reçoit plus de trades : son watermark n'avance plus
        tout seul, c'est l'horloge du consommateur qui prend le relais.
        """
        watermark = now - self.allowed_lateness
        closed = []
        for symbol in list(self._open):
            closed += self._close_symbol(symbol, watermark)
        return closed

    def mark_closed_until(self, symbol: str, until: datetime) -> None:
        """
        Déclare fermées toutes les bougies de `symbol` qui finissent avant `until`.
        Au redémarrage, l'agrégateur est vide : sans ça, un trade en retard
        rejoué recréerait une bougie déjà écrite avec un seul trade, et l'upsert
        écraserait la bonne version.
        """
        current = self._closed_until.get(symbol)
        if current is None or until > current:
            self._closed_until[symbol] = until

    # --- offsets ------------------------------------------------------------

    def safe_commit_offsets(self) -> dict[int, int]:
        """
        Offset à valider par partition (convention Kafka : le prochain offset à
        lire). Pour une partition qui a encore des trades dans une bougie
        ouverte, c'est l'offset du plus ancien d'entre eux ; sinon, juste après
        le dernier message lu.
        """
        safe = dict(self._next_offset)
        for bars in self._open.values():
            for bar in bars.values():
                for partition, offset in bar.min_offset.items():
                    safe[partition] = min(safe.get(partition, offset), offset)
        return safe

    # --- interne ------------------------------------------------------------

    def _close_symbol(self, symbol: str, watermark: datetime) -> list[dict]:
        bars = self._open.get(symbol, {})
        ready = sorted(m for m in bars if m + MINUTE <= watermark)
        closed = [bars.pop(m).to_record() for m in ready]
        if ready:
            # Les bougies encore ouvertes sont toutes après la dernière fermée :
            # cette borne ne fait qu'avancer.
            self._closed_until[symbol] = ready[-1] + MINUTE
        if not bars:
            self._open.pop(symbol, None)
        return closed
