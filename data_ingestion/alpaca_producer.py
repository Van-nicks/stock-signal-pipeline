import os
import json
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer
from alpaca.data.live import StockDataStream
from alpaca.data.models import Trade

load_dotenv()

# ── Alpaca credentials ──────────────────────────────────────────
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

# ── Kafka config ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_STOCKS", "stocks-ticks")

# ── 30-symbol watchlist ─────────────────────────────────────────
WATCHLIST = [
    # Tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    # Semiconductors
    "AMD", "INTC", "AVGO",
    # Finance
    "JPM", "BAC", "GS", "V", "MA",
    # Healthcare
    "JNJ", "UNH", "PFE",
    # Consumer
    "WMT", "HD", "MCD",
    # Energy
    "XOM", "CVX",
    # Industrial
    "BA", "CAT",
    # Index ETFs — market context
    "SPY", "QQQ",
    # Large cap growth
    "NFLX", "ORCL", "CRM"
]


def create_kafka_producer() -> KafkaProducer:
    """
    Creates and returns a configured KafkaProducer.
    Called once at startup, reused for every message.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=100,
        compression_type="gzip",
    )


def build_trade_message(trade: Trade) -> dict:
    """
    Transforms an Alpaca Trade object into our standard
    pipeline message schema.
    """
    return {
        "symbol": trade.symbol,
        "price": float(trade.price),
        "size": float(trade.size),
        "timestamp": trade.timestamp.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "alpaca_iex",
        "type": "trade"
    }


async def run_stream():
    """
    Opens Alpaca WebSocket, subscribes to trades for all
    watchlist symbols, publishes each trade to Kafka.
    """
    producer = create_kafka_producer()
    logger.info(f"Kafka producer ready → {KAFKA_BOOTSTRAP_SERVERS}")

    stream = StockDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)

    async def on_trade(trade: Trade):
        message = build_trade_message(trade)
        producer.send(
            KAFKA_TOPIC,
            key=trade.symbol,
            value=message
        )
        logger.info(
            f"→ Kafka | {trade.symbol:<6} "
            f"${float(trade.price):>10.2f} | "
            f"size: {float(trade.size)}"
        )

    stream.subscribe_trades(on_trade, *WATCHLIST)

    logger.info(f"Subscribed to {len(WATCHLIST)} symbols on IEX feed")
    logger.info("Streaming live trades... (Ctrl+C to stop)")
    logger.info("Note: trades only flow during US market hours "
                "(9:30am–4:00pm EST / 2:30pm–9:00pm BST)")

    await stream.run()


if __name__ == "__main__":
    logger.info("Starting Alpaca Stock Producer")
    logger.info(f"Watchlist: {WATCHLIST}")

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