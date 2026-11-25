"""
Logique du consommateur, sans dépendance Kafka : agrégation, écriture, puis
validation des offsets. La validation est une fonction injectée, ce qui permet
de simuler un crash dans les tests sans broker.

Garantie « au moins une fois » (at-least-once) :
1. Les trades sont agrégés en mémoire (BarAggregator).
2. Les bougies fermées sont écrites sur disque (upsert idempotent).
3. SEULEMENT ENSUITE, on valide les offsets sûrs (plus ancien trade encore
   dans une bougie ouverte, par partition).

Après un crash, on reprend au dernier offset validé : les trades rejoués
reconstruisent les bougies à l'identique, et l'upsert les réécrit sans doublon.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.stream.bars import BarAggregator
from src.stream.storage import LIVE_BARS_DIR, last_written_minutes, upsert_bars

logger = logging.getLogger("consumer")

CommitFn = Callable[[dict[int, int]], None]


class BarsWriter:
    def __init__(self, commit: CommitFn, root: Path = LIVE_BARS_DIR, allowed_lateness_s: float = 10.0) -> None:
        self.commit = commit
        self.root = root
        self.agg = BarAggregator(allowed_lateness_s=allowed_lateness_s)
        self.pending: list[dict] = []     # bougies fermées pas encore écrites
        self.bars_written = 0

        # Reprise : ce qui est déjà sur disque est fermé pour de bon.
        for symbol, minute in last_written_minutes(root).items():
            self.agg.mark_closed_until(symbol, minute.to_pydatetime() + timedelta(minutes=1))

    def handle(self, value: bytes | str, partition: int, offset: int) -> None:
        """Un message lu. Les bougies ne se ferment ici que par le temps des événements (watermark)."""
        try:
            trade = json.loads(value)
            self.pending += self.agg.add(trade, partition, offset)
        except (ValueError, KeyError, TypeError) as exc:
            # Message illisible : on le saute (et on le dit) plutôt que de bloquer la partition.
            logger.error("Message ignoré (partition %d, offset %d) : %s", partition, offset, exc)
        if self.pending:
            self.flush()

    def idle(self, now: datetime | None = None) -> None:
        """
        Appelé quand le consommateur est à jour (plus aucun message à lire) :
        on ferme alors d'après l'horloge les bougies des symboles inactifs.

        Surtout PAS pendant un rattrapage : après un redémarrage, on rejoue des
        trades de 10:02 alors qu'il est 10:30. Fermer d'après l'horloge fermerait
        la bougie 10:02 dès son premier trade rejoué, l'écrirait par-dessus la
        bonne, puis rejetterait tous les suivants comme « en retard ».
        """
        self.pending += self.agg.close_until(now or datetime.now(timezone.utc))
        if self.pending:
            self.flush()

    def flush(self) -> None:
        """Écrit les bougies fermées, PUIS valide les offsets. Jamais dans l'autre sens."""
        if self.pending:
            upsert_bars(self.pending, root=self.root)
            self.bars_written += len(self.pending)
            self.pending.clear()
        offsets = self.agg.safe_commit_offsets()
        if offsets:
            self.commit(offsets)
