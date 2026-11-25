"""
Consommateur streaming : topic crypto.trades -> bougies 1 min -> Parquet (data/live/bars).

Toute la logique (agrégation, écriture, ordre écriture -> validation) est dans
writer.py ; ce fichier ne fait que brancher Kafka dessus.

Lancement : python -m src.stream.consumer
"""

from __future__ import annotations

import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition

from src.stream.writer import BarsWriter

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bars-writer")
ALLOWED_LATENESS_S = float(os.getenv("ALLOWED_LATENESS_S", "10"))

STATS_EVERY_S = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("consumer")


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,      # c'est nous qui validons, après écriture
        "auto.offset.reset": "earliest",  # premier démarrage : on reprend tout le topic
        "client.id": "bars-writer",
    })


def run() -> None:
    consumer = build_consumer()

    def commit(offsets: dict[int, int]) -> None:
        consumer.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in offsets.items()], asynchronous=False)

    writer = BarsWriter(commit, allowed_lateness_s=ALLOWED_LATENESS_S)
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)   # docker stop
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC])
    logger.info("Abonné à %s (groupe %s), tolérance de retard %.0f s", TOPIC, GROUP_ID, ALLOWED_LATENESS_S)
    last_stats = time.monotonic()
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                writer.idle()         # à jour : on peut fermer d'après l'horloge
            else:
                if msg.error():
                    raise KafkaException(msg.error())
                writer.handle(msg.value(), msg.partition(), msg.offset())

            if time.monotonic() - last_stats >= STATS_EVERY_S:
                logger.info(
                    "Bougies écrites : %d | trades en retard ignorés : %d | doublons ignorés : %d | offsets sûrs : %s",
                    writer.bars_written, writer.agg.late_trades, writer.agg.duplicate_trades,
                    writer.agg.safe_commit_offsets(),
                )
                last_stats = time.monotonic()
    finally:
        # Arrêt propre : on écrit ce qui est fermé et on valide. Les bougies
        # encore ouvertes seront reconstruites au redémarrage à partir de
        # l'offset sûr : c'est tout l'intérêt de ne pas valider plus loin.
        writer.flush()
        consumer.close()
        logger.info("Arrêt du consommateur (%d bougies écrites)", writer.bars_written)


if __name__ == "__main__":
    run()
