"""
Logique pure du producteur : normalisation des messages Coinbase, détection
des trades manqués, délai de reconnexion. Aucune dépendance réseau ici, pour
pouvoir tout tester sans websocket ni broker.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

# "match" = un trade. "last_match" = le dernier trade connu, renvoyé par Coinbase
# à chaque (re)connexion : c'est un vrai trade, mais il peut avoir déjà été envoyé
# avant une coupure. On le garde, le consommateur déduplique par trade_id.
TRADE_TYPES = {"match", "last_match"}


def normalize(message: dict) -> dict | None:
    """
    Transforme un message Coinbase en événement de trade au format du topic,
    ou renvoie None si ce n'est pas un trade valide (heartbeat, abonnement,
    prix ou quantité aberrants).
    """
    if message.get("type") not in TRADE_TYPES:
        return None
    try:
        price = float(message["price"])
        size = float(message["size"])
        event = {
            "symbol": message["product_id"],
            "trade_id": int(message["trade_id"]),
            "price": price,
            "size": size,
            "side": message["side"],
            # Heure du trade côté plateforme (event time), pas heure de réception.
            # C'est elle qui décide dans quelle bougie tombe le trade.
            "event_time": message["time"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    except (KeyError, TypeError, ValueError):
        return None

    if price <= 0 or size <= 0 or event["side"] not in {"buy", "sell"}:
        return None
    return event


def missed_trades(last_trade_id: int | None, trade_id: int) -> int:
    """
    Nombre de trades sautés entre deux trades successifs d'un même symbole
    (les trade_id Coinbase se suivent sans trou par symbole). 0 si rien n'est
    perdu, ou si c'est le premier trade vu, ou si c'est un doublon / un retour
    en arrière (last_match après reconnexion).
    """
    if last_trade_id is None or trade_id <= last_trade_id:
        return 0
    return trade_id - last_trade_id - 1


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0, jitter: float = 0.2) -> float:
    """
    Délai avant la tentative de reconnexion n° `attempt` (1, 2, 3...) :
    exponentiel (1 s, 2 s, 4 s...) plafonné à `cap`, avec un peu d'aléa pour
    ne pas retaper le serveur au même rythme exact à chaque fois.
    """
    delay = min(cap, base * 2 ** (attempt - 1))
    return delay * (1 + random.uniform(-jitter, jitter))
