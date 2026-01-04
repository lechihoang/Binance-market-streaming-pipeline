"""Kafka Connector Module."""

import json
from typing import Any, Callable, Dict, Optional

from utils.logging import get_logger

logger = get_logger(__name__)


class KafkaConnector:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: Optional[str] = None,
        linger_ms: int = 0,
        batch_size: int = 16384,
        value_serializer: Optional[Callable[[Any], bytes]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.linger_ms = linger_ms
        self.batch_size = batch_size
        self._value_serializer = value_serializer
        self._producer = None
    
    @property
    def producer(self):
        if self._producer is None:
            from kafka import KafkaProducer
            
            serializer = self._value_serializer or (lambda v: json.dumps(v).encode('utf-8'))
            
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                linger_ms=self.linger_ms,
                batch_size=self.batch_size,
                value_serializer=serializer,
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.debug(f"KafkaProducer created: {self.bootstrap_servers}, linger_ms={self.linger_ms}")
        
        return self._producer
    
    def send(self, topic: str, value: Any, key: Optional[str] = None) -> None:
        self.producer.send(topic, value=value, key=key)
        if self.linger_ms == 0:
            self.producer.flush()
    
    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.debug("KafkaProducer closed")
