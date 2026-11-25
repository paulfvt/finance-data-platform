"""
Producteur streaming : websocket Coinbase (trades BTC/ETH) -> topic Redpanda.

- Clé Kafka = symbole : tous les trades d'un symbole vont dans la même
  partition, donc restent dans l'ordre.
- Producteur idempotent (acks=all) : un retry réseau ne crée pas de doublon
  dans le topic.
- Reconnexion automatique avec délai exponentiel, et reconnexion forcée si
  plus aucun message n'arrive pendant STALL_TIMEOUT_S (flux figé).
- Les trades perdus pendant une coupure sont détectés (trou dans les
  trade_id) et comptés dans les logs : on ne peut pas les récupérer par le
  websocket, mais on sait qu'ils manquent.

Lancement : python -m src.stream.producer (variables d'environnement ci-dessous).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time

import websockets
from confluent_kafka import KafkaError, Message, Producer

from src.stream.events import backoff_delay, missed_trades, normalize

WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
PRODUCT_IDS = os.getenv("PRODUCT_IDS", "BTC-USD,ETH-USD").split(",")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades")

STALL_TIMEOUT_S = 30   # Coinbase envoie un heartbeat par seconde : 30 s de silence = flux figé
STATS_EVERY_S = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("producer")


class Stats:
    """Compteurs remis à zéro à chaque ligne de log, pour suivre le débit."""

    def __init__(self) -> None:
        self.sent = 0
        self.rejected = 0
        self.missed = 0
        self.delivery_errors = 0
        self.last_log = time.monotonic()

    def maybe_log(self) -> None:
        now = time.monotonic()
        if now - self.last_log < STATS_EVERY_S:
            return
        logger.info(
            "Dernière minute : %d trades envoyés, %d messages rejetés, %d trades manqués, %d erreurs de livraison",
            self.sent, self.rejected, self.missed, self.delivery_errors,
        )
        self.sent = self.rejected = self.missed = self.delivery_errors = 0
        self.last_log = now


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # implique acks=all et des retries sans doublon
        # Même hachage de clé que les clients Java et rpk (murmur2). Par défaut,
        # librdkafka hache en CRC32 : la même clé irait dans une autre partition
        # selon le client qui produit, et l'ordre par symbole ne tiendrait plus.
        "partitioner": "murmur2_random",
        "linger.ms": 50,              # regroupe les messages par petits lots
        "compression.type": "lz4",
        "client.id": "coinbase-producer",
    })


async def stream_trades(
    producer: Producer, stats: Stats, stop: asyncio.Event, last_trade_ids: dict[str, int]
) -> None:
    """
    Une session websocket : s'abonne, publie les trades, rend la main à la coupure.
    `last_trade_ids` vient de l'appelant et survit aux reconnexions : c'est
    justement après une coupure qu'il faut comparer avec le dernier trade vu.
    """

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            stats.delivery_errors += 1
            logger.error("Livraison échouée (%s) : %s", msg.key(), err)

    async with websockets.connect(WS_URL, open_timeout=15, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": PRODUCT_IDS,
            "channels": ["matches", "heartbeat"],
        }))
        logger.info("Abonné à %s sur %s", PRODUCT_IDS, WS_URL)

        while not stop.is_set():
            raw = await asyncio.wait_for(ws.recv(), timeout=STALL_TIMEOUT_S)
            message = json.loads(raw)

            if message.get("type") == "error":
                raise ConnectionError(f"Erreur Coinbase : {message}")

            event = normalize(message)
            if event is None:
                if message.get("type") in {"match", "last_match"}:
                    stats.rejected += 1
                    logger.warning("Trade rejeté : %s", message)
                continue

            symbol = event["symbol"]
            gap = missed_trades(last_trade_ids.get(symbol), event["trade_id"])
            if gap:
                stats.missed += gap
                logger.warning("%d trade(s) %s manqué(s) avant le trade %d", gap, symbol, event["trade_id"])
            last_trade_ids[symbol] = max(event["trade_id"], last_trade_ids.get(symbol, 0))

            producer.produce(TOPIC, key=symbol, value=json.dumps(event), on_delivery=on_delivery)
            producer.poll(0)   # déclenche les callbacks de livraison sans bloquer
            stats.sent += 1
            stats.maybe_log()


async def run() -> None:
    producer = build_producer()
    stats = Stats()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)   # docker stop envoie SIGTERM
        except NotImplementedError:
            pass   # Windows : Ctrl+C lève KeyboardInterrupt à la place

    attempt = 0
    last_trade_ids: dict[str, int] = {}   # dernier trade_id vu par symbole, sur toute la vie du processus
    try:
        while not stop.is_set():
            try:
                await stream_trades(producer, stats, stop, last_trade_ids)
                attempt = 0
            except (OSError, asyncio.TimeoutError, websockets.WebSocketException,
                    ConnectionError, json.JSONDecodeError) as exc:
                attempt += 1
                delay = backoff_delay(attempt)
                logger.warning("Connexion perdue (%s : %s), nouvel essai dans %.1f s", type(exc).__name__, exc, delay)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
    finally:
        remaining = producer.flush(10)
        logger.info("Arrêt du producteur (%d message(s) non livré(s))", remaining)


if __name__ == "__main__":
    asyncio.run(run())
