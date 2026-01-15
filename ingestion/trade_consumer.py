"""Kafka trade consumer - reads trades from Kafka, writes to Redis cache."""

import signal

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from storage.redis import Redis
from util.constant import KAFKA_SERVER, REDIS_HOST, REDIS_PORT, SCHEMA_REGISTRY_URL
from util.logging import get_logger, setup_logging

logger = get_logger(__name__)


class TradeConsumer:
    """Consume trade messages from Kafka and cache in Redis."""

    def __init__(self):
        self.running = True
        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT)

        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.deserializer = AvroDeserializer(schema_registry_client=registry)
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_SERVER,
                "group.id": "trade-consumer",
                "auto.offset.reset": "latest",
            }
        )
        self.consumer.subscribe(["raw_trades"])

    def run(self):
        logger.info("Started consuming trades")
        ctx = SerializationContext("raw_trades", MessageField.VALUE)

        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            data = self.deserializer(msg.value(), ctx)

            # Transform trade data for Redis storage
            trade = {
                "symbol": data["symbol"],
                "price": str(data["price"]),
                "quantity": str(data["quantity"]),
                "timestamp": data["trade_time"],
                "trade_id": data["trade_id"],
                "side": "SELL" if data["is_buyer_maker"] else "BUY",
            }

            self.redis.write_trade(data["symbol"], trade)

        self.consumer.close()
        self.redis.close()

    def stop(self):
        self.running = False


def main():
    setup_logging(level="INFO", json_output=True)
    c = TradeConsumer()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT, lambda *_: c.stop())
    c.run()


if __name__ == "__main__":
    main()
