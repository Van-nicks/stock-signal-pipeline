import os
import json
import asyncio
import ssl
from datetime import datetime, timezone
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer
from binance import AsyncClient, BinanceSocketManager
import certifi
import aiohttp

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

load_dotenv()

# ── Binance credentials ─────────────────────────────────────────
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# ── Kafka config ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CRYPTO", "crypto-ticks")

# ── 5 crypto symbols ────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]

# Internal buffer between WebSocket reader and Kafka sender
INTERNAL_QUEUE_SIZE = 5000


# ── Subclass to inject custom SSL context ───────────────────────
class SSLAsyncClient(AsyncClient):
    def _init_session(self) -> aiohttp.ClientSession:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        return aiohttp.ClientSession(
            headers=self._get_headers(),
            connector=connector
        )


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=100,
        compression_type="gzip",
        batch_size=65536,  # 64KB batch — handles burst throughput better
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


# ── Coroutine 1: WebSocket reader → internal queue ─────────────
async def ws_reader(stream, internal_queue: asyncio.Queue):
    """
    Reads from Binance WebSocket as fast as possible.
    Drops messages if internal queue is full (backpressure).
    """
    dropped = 0
    while True:
        msg = await stream.recv()
        if not msg or "data" not in msg:
            continue
        data = msg["data"]
        if data.get("e") != "trade":
            continue

        try:
            internal_queue.put_nowait(data)
        except asyncio.QueueFull:
            dropped += 1
            if dropped % 100 == 0:
                logger.warning(f"Internal queue full — {dropped} messages dropped so far")


# ── Coroutine 2: Internal queue → Kafka ────────────────────────
async def kafka_sender(producer: KafkaProducer, internal_queue: asyncio.Queue):
    """
    Drains internal queue and sends to Kafka.
    Runs independently of WebSocket speed.
    """
    sent = 0
    while True:
        data = await internal_queue.get()
        message = build_trade_message(data)
        producer.send(
            KAFKA_TOPIC,
            key=message["symbol"],
            value=message
        )
        sent += 1
        logger.info(
            f"→ Kafka | {message['symbol']:<10} "
            f"${float(message['price']):>12.2f} | "
            f"size: {float(message['size']):.6f} | "
            f"queue: {internal_queue.qsize()}"
        )
        internal_queue.task_done()


async def run_stream(producer: KafkaProducer):
    """
    Opens Binance multiplex WebSocket for all 5 crypto symbols,
    publishes each trade to Kafka.
    """

    logger.info(f"Connecting → {KAFKA_BOOTSTRAP_SERVERS}")

    client = await SSLAsyncClient.create(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET
    )
    bm = BinanceSocketManager(client, max_queue_size=2000)

    # Build stream names: ["btcusdt@trade", "ethusdt@trade", ...]
    streams = [f"{symbol.lower()}@trade" for symbol in SYMBOLS]

    logger.info(f"Subscribing to: {SYMBOLS}")

    # Internal asyncio queue — decouples WS from Kafka
    internal_queue: asyncio.Queue = asyncio.Queue(maxsize=INTERNAL_QUEUE_SIZE)

    try:
        async with bm.multiplex_socket(streams) as stream:
            # Run both coroutines concurrently
            await asyncio.gather(
                ws_reader(stream, internal_queue),
                kafka_sender(producer, internal_queue),
            )
    finally:
        await client.close_connection()
        logger.info("Connection closed cleanly")


if __name__ == "__main__":
    logger.info("Starting Binance Crypto Producer")
    logger.info(f"Symbols: {SYMBOLS}")

    producer = create_kafka_producer()
    logger.info(f"Kafka producer ready → {KAFKA_BOOTSTRAP_SERVERS}")

    while True:
        try:
            asyncio.run(run_stream(producer))
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully. Goodbye.")
            break
        except Exception as e:
            logger.error(f"Stream error: {e}")
            logger.info("Reconnecting in 10 seconds...")
            import time

            time.sleep(10)
