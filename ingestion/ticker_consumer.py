"""Kafka ticker consumer - reads tickers from Kafka, writes to Redis cache."""

import signal

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from storage.redis import Redis
from util.constant import KAFKA_SERVER, REDIS_HOST, REDIS_PORT, SCHEMA_REGISTRY_URL
import sys
from loguru import logger

# logger = get_logger(__name__)


class TickerConsumer:
    """Consume ticker messages from Kafka and cache in Redis."""

    def __init__(self):
        self.running = True
        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)

        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.deserializer = AvroDeserializer(schema_registry_client=registry)
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "group.id": "ticker-consumer",
                "auto.offset.reset": "latest",
            }
        )
        self.consumer.subscribe(["raw_tickers"])

    def run(self):
        logger.info("Started consuming tickers")
        ctx = SerializationContext("raw_tickers", MessageField.VALUE)

        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            data = self.deserializer(msg.value(), ctx)
            self.redis.write_ticker(data["symbol"], data)

        self.consumer.close()
        self.redis.close()

    def stop(self):
        self.running = False


def main():
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    c = TickerConsumer()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT, lambda *_: c.stop())
    c.run()


if __name__ == "__main__":
    main()
