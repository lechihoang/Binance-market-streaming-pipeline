"""Connects to Binance WebSocket, streams raw trades/tickers into Kafka topics."""

import asyncio
import json
import time

import websockets

from util.constant import BINANCE_WS_URL, DEFAULT_SYMBOL, KAFKA_SERVER, SCHEMA_REGISTRY_URL
from util.kafka import KafkaProducer
from util.logging import get_logger, setup_logging

logger = get_logger(__name__)


class BinanceConnector:
    """Stream trades and tickers from Binance WebSocket to Kafka."""

    def __init__(self):
        streams = [f"{s.lower()}@trade" for s in DEFAULT_SYMBOL] + [f"{s.lower()}@ticker" for s in DEFAULT_SYMBOL]
        self.url = f"{BINANCE_WS_URL}?streams={'/'.join(streams)}"

        self.trades = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            schema_registry_url=SCHEMA_REGISTRY_URL,
            topic="raw_trades",
        )
        self.tickers = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            schema_registry_url=SCHEMA_REGISTRY_URL,
            topic="raw_tickers",
        )

    async def run(self):
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info(f"Connected, streaming {len(DEFAULT_SYMBOL)} symbols")
                    async for msg in ws:
                        data = json.loads(msg).get("data", {})
                        event = data.get("e")
                        ts = time.time_ns() // 1_000_000

                        if event == "trade":
                            self.trades.send(key=data["s"], value=self.to_trade(data, ts))
                        elif event == "24hrTicker":
                            self.tickers.send(key=data["s"], value=self.to_ticker(data, ts))
            except Exception as e:
                logger.error(f"Connection error: {e}, reconnecting...")
                await asyncio.sleep(1)

    def to_trade(self, d: dict, ts: int) -> dict:
        return {
            "event_type": d.get("e", ""),
            "event_time": d.get("E", 0),
            "symbol": d.get("s", ""),
            "trade_id": d.get("t", 0),
            "price": d.get("p", "0"),
            "quantity": d.get("q", "0"),
            "trade_time": d.get("T", 0),
            "is_buyer_maker": d.get("m", False),
            "ingestion_timestamp": ts,
            "stream_type": "trade",
        }

    def to_ticker(self, d: dict, ts: int) -> dict:
        return {
            "event_type": d.get("e", ""),
            "event_time": d.get("E", 0),
            "symbol": d.get("s", ""),
            "price_change": d.get("p", "0"),
            "price_change_percent": d.get("P", "0"),
            "weighted_avg_price": d.get("w", "0"),
            "last_price": d.get("c", "0"),
            "last_qty": d.get("Q", "0"),
            "open_price": d.get("o", "0"),
            "high_price": d.get("h", "0"),
            "low_price": d.get("l", "0"),
            "volume": d.get("v", "0"),
            "quote_volume": d.get("q", "0"),
            "open_time": d.get("O", 0),
            "close_time": d.get("C", 0),
            "first_trade_id": d.get("F", 0),
            "last_trade_id": d.get("L", 0),
            "trade_count": d.get("n", 0),
            "ingestion_timestamp": ts,
            "stream_type": "ticker",
        }


def main():
    setup_logging(level="INFO", json_output=True)
    logger.info("Starting Binance Connector...")
    asyncio.run(BinanceConnector().run())


if __name__ == "__main__":
    main()
