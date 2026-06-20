import os
import json
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer
from alpaca.data.live import StockDataStream
from alpaca.data.models import Trade
from alpaca.data import DataFeed
import certifi
import signal

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

load_dotenv()

# ── Alpaca credentials ──────────────────────────────────────────
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

# ── Kafka config ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_STOCKS", "stocks-ticks")

# ── Internal queue config ───────────────────────────────────────
INTERNAL_QUEUE_SIZE = 10000  # 30 symbols → higher burst than 5 crypto

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
        batch_size=65536,  # larger batch for 30-symbol burst
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


# ── Coroutine 2: Internal queue → Kafka ────────────────────────
async def kafka_sender(producer: KafkaProducer, queue: asyncio.Queue):
    """
    Drains internal queue and publishes to Kafka.
    Decoupled from WebSocket read loop — same pattern as binance_producer.
    """
    try:
        while True:
            message = await queue.get()
            try:
                producer.send(
                    KAFKA_TOPIC,
                    key=message["symbol"],
                    value=message
                )
                logger.info(
                    f"→ Kafka | {message['symbol']:<6} "
                    f"${float(message['price']):>10.2f} | "
                    f"size: {float(message['size'])} | "
                    f"queue: {queue.qsize()}"
                )
            except Exception as e:
                logger.error(f"Kafka send error: {e}")
            finally:
                queue.task_done()
    except asyncio.CancelledError:
        pass  # clean exit when task is cancelled


async def run_stream(producer: KafkaProducer, shutdown_event: asyncio.Event):
    """
    Opens Alpaca WebSocket, subscribes to trades for all
    watchlist symbols, publishes each trade to Kafka.
    Uses stream.stop() for clean shutdown instead of task cancellation.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=INTERNAL_QUEUE_SIZE)

    stream = StockDataStream(
        ALPACA_API_KEY,
        ALPACA_API_SECRET,
        feed=DataFeed.IEX,
        raw_data=False,
    )

    async def on_trade(trade: Trade):
        message = build_trade_message(trade)
        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning(
                f"Queue full — dropping {trade.symbol} @ {trade.price}"
            )

    stream.subscribe_trades(on_trade, *WATCHLIST)

    logger.info(f"Subscribed to {len(WATCHLIST)} symbols on IEX feed")
    logger.info("Streaming live trades... (Ctrl+C to stop)")
    logger.info(
        "Note: trades only flow during US market hours "
        "(9:30am–4:00pm EST / 2:30pm–9:00pm BST)"
    )

    kafka_task = asyncio.create_task(
        kafka_sender(producer, queue), name="kafka-task"
    )

    # ── Shutdown watcher ────────────────────────────────────────
    # Calls stream.stop() — lets _run_forever() exit naturally
    # instead of cancelling it abruptly (which leaves WebSocket threads hanging)
    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Stopping Alpaca stream...")
        stream.stop()  # ← proper public API, closes WebSocket from within
        kafka_task.cancel()  # ← safe to cancel now — stream has stopped

    watcher_task = asyncio.create_task(shutdown_watcher(), name="watcher-task")

    try:
        # _run_forever() exits cleanly when stream.stop() is called
        await stream._run_forever()
    except Exception as e:
        if not shutdown_event.is_set():
            raise  # only re-raise if this wasn't an intentional shutdown
    finally:
        # Clean up both tasks regardless of how we got here
        watcher_task.cancel()
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            pass
        logger.info("Stream stopped.")


async def main():
    producer = create_kafka_producer()
    logger.info(f"Kafka producer ready → {KAFKA_BOOTSTRAP_SERVERS}")

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # ── Signal handler cancels the main task cleanly ────────────
    def _handle_shutdown():
        logger.info("Shutdown signal received...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_shutdown)

    try:
        while not shutdown_event.is_set():
            try:
                await run_stream(producer, shutdown_event)
            except Exception as e:
                if shutdown_event.is_set():
                    break
                logger.error(f"Stream error: {e}")
                logger.info("Reconnecting in 10 seconds...")
                await asyncio.sleep(10)
    finally:
        logger.info("Shutting down gracefully. Goodbye.")
        producer.flush(timeout=3)
        producer.close()  # ← this kills the background Kafka threads
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    logger.info("Starting Alpaca Stock Producer")
    logger.info(f"Watchlist ({len(WATCHLIST)} symbols): {WATCHLIST}")
    asyncio.run(main())
