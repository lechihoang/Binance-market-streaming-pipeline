from typing import Any

from loguru import logger

# logger = get_logger(__name__)


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        subject: str | None = None,
    ):
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        self.topic = topic
        subject = subject or f"{topic}-value"

        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        registered_schema = schema_registry_client.get_latest_version(subject)
        avro_serializer = AvroSerializer(
            schema_registry_client,
            registered_schema.schema,
            lambda obj, ctx: obj,
        )

        self._producer = SerializingProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 20,
                "queue.buffering.max.messages": 200000,
                "queue.buffering.max.kbytes": 1048576,
                "batch.size": 1000000,
                "message.timeout.ms": 30000,
                "value.serializer": avro_serializer,
                "key.serializer": lambda k, ctx: k.encode("utf-8") if k else None,
            }
        )

    def send(self, value: dict[str, Any], key: str | None = None) -> None:
        self._producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
        )
        # Poll to trigger message delivery and prevent queue full errors
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.flush()
