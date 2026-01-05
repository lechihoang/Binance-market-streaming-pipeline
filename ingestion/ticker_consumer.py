"""Consume tickers from Kafka and write to Redis."""

import os
import signal

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from storage.redis import Redis
from util.logging import setup_logging, get_logger

logger = get_logger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


class TickerConsumer:
    """Consume ticker messages from Kafka and cache in Redis."""

    def __init__(self):
        self.running = True
        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
        
        registry = SchemaRegistryClient({"url": REGISTRY_URL})
        self.deserializer = AvroDeserializer(schema_registry_client=registry)
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": "ticker-consumer",
            "auto.offset.reset": "latest",
        })
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
    setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    c = TickerConsumer()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT, lambda *_: c.stop())
    c.run()


if __name__ == "__main__":
    main()
