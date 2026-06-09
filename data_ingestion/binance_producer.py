import os
import json
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer
from binance import AsyncClient, BinanceSocketManager

load_dotenv()

# ── Binance credentials ─────────────────────────────────────────
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# ── Kafka config ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CRYPTO", "crypto-ticks")

# ── 5 crypto symbols ────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=100,
        compression_type="gzip",
    )


def build_trade_message(data: dict) -> dict:
    """
    Transforms a Binance trade event into our standard
    pipeline message schema.

    Binance raw trade fields:
        s = symbol, p = price, q = quantity,
        T = trade time (unix ms), t = trade ID
    """
    return {
        "symbol": data["s"],
        "price": float(data["p"]),
        "size": float(data["q"]),
        "timestamp": datetime.fromtimestamp(
            data["T"] / 1000,
            tz=timezone.utc
        ).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "binance",
        "type": "trade"
    }


async def run_stream():
    """
    Opens Binance multiplex WebSocket for all 5 crypto symbols,
    publishes each trade to Kafka.
    """
    producer = create_kafka_producer()
    logger.info(f"Kafka producer ready → {KAFKA_BOOTSTRAP_SERVERS}")

    client = await AsyncClient.create(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET
    )
    bm = BinanceSocketManager(client)

    # Build stream names: ["btcusdt@trade", "ethusdt@trade", ...]
    streams = [f"{symbol.lower()}@trade" for symbol in SYMBOLS]

    logger.info(f"Subscribing to: {SYMBOLS}")

    async with bm.multiplex_socket(streams) as stream:
        while True:
            msg = await stream.recv()

            if not msg or "data" not in msg:
                continue

            data = msg["data"]

            if data.get("e") != "trade":
                continue

            message = build_trade_message(data)
            producer.send(
                KAFKA_TOPIC,
                key=message["symbol"],
                value=message
            )
            logger.info(
                f"→ Kafka | {message['symbol']:<10} "
                f"${float(message['price']):>12.2f} | "
                f"size: {float(message['size']):.6f}"
            )

    await client.close_connection()


if __name__ == "__main__":
    logger.info("Starting Binance Crypto Producer")
    logger.info(f"Symbols: {SYMBOLS}")

    while True:
        try:
            asyncio.run(run_stream())
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully. Goodbye.")
            break
        except Exception as e:
            logger.error(f"Stream error: {e}")
            logger.info("Reconnecting in 10 seconds...")
            import time

            time.sleep(10)
