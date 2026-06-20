import os
import certifi
import snowflake.connector
from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    window, max, min, sum, count,
    when, min_by, max_by
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# SSL fix — must be before any network call
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

load_dotenv()

# Snowflake
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# Spark Kafka connector JAR
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"

# Schema matching our producer message format
MESSAGE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("size", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source", StringType(), True),
    StructField("type", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockSignalStreaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation",
                "/tmp/spark-checkpoints")
        .getOrCreate()
    )


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )


def write_to_snowflake(batch_df, batch_id: int):
    """
    Collects Spark rows to driver, writes to Snowflake
    using direct SQL INSERT. No pyarrow dependency needed.
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping")
        return

    # Bring rows from Spark to Python driver
    rows = batch_df.collect()
    if not rows:
        return

    conn = get_snowflake_conn()
    cursor = conn.cursor()

    try:
        insert_sql = """
                     INSERT INTO LIVE_MARKET_DATA (symbol, asset_type, window_start, window_end, \
                                                   open_price, high_price, low_price, close_price, \
                                                   volume, vwap, trade_count, volume_surge) \
                     VALUES (%s, %s, %s, %s, \
                             %s, %s, %s, %s, \
                             %s, %s, %s, %s) \
                     """

        data = [
            (
                row.symbol,
                row.asset_type,
                row.window_start.strftime('%Y-%m-%d %H:%M:%S'),
                row.window_end.strftime('%Y-%m-%d %H:%M:%S'),
                float(row.open_price) if row.open_price else None,
                float(row.high_price) if row.high_price else None,
                float(row.low_price) if row.low_price else None,
                float(row.close_price) if row.close_price else None,
                float(row.volume) if row.volume else None,
                float(row.vwap) if row.vwap else None,
                int(row.trade_count) if row.trade_count else None,
                False  # volume_surge — full logic in signals/ later
            )
            for row in rows
        ]

        cursor.executemany(insert_sql, data)
        conn.commit()

        logger.info(
            f"Batch {batch_id}: {len(rows)} bars → Snowflake ✓"
        )

    except Exception as e:
        logger.error(f"Batch {batch_id}: write failed — {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def build_ohlcv_stream(spark: SparkSession):
    """
    Reads from Kafka, parses messages, computes 1-minute
    OHLCV bars with VWAP. Returns streaming DataFrame.
    """

    # Read raw bytes from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "stocks-ticks,crypto-ticks")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON → typed columns
    parsed_df = (
        raw_df
        .select(
            from_json(
                col("value").cast("string"),
                MESSAGE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn(
            "asset_type",
            when(col("source") == "binance", "crypto")
            .otherwise("stock")
        )
        .filter(col("event_time").isNotNull())
        .filter(col("price") > 0)
    )

    # 1-minute OHLCV aggregation with watermark
    ohlcv_df = (
        parsed_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("symbol"),
            col("asset_type")
        )
        .agg(
            min_by("price", "event_time").alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            max_by("price", "event_time").alias("close_price"),
            sum("size").alias("volume"),
            count("*").alias("trade_count"),
            (
                    sum(col("price") * col("size")) / sum("size")
            ).alias("vwap")
        )
        .select(
            col("symbol"),
            col("asset_type"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("volume"),
            col("vwap"),
            col("trade_count")
        )
    )

    return ohlcv_df


def main():
    logger.info("Starting Stock Signal — Spark Structured Streaming")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark {spark.version} ready")

    ohlcv_df = build_ohlcv_stream(spark)

    query = (
        ohlcv_df.writeStream
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .foreachBatch(write_to_snowflake)
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/ohlcv"
        )
        .start()
    )

    logger.info("Streaming query running.")
    logger.info("Trigger: every 30 seconds")
    logger.info("Bars appear in Snowflake ~2-3 min after window closes")
    logger.info("Ctrl+C to stop")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        query.stop()
        spark.stop()
        logger.info("Stopped cleanly.")


if __name__ == "__main__":
    main()
