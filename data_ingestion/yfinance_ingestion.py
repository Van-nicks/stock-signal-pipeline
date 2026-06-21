import os
import io
import time
import ssl
import certifi
import sys
import requests
import pandas as pd
import yfinance as yf
import boto3
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from loguru import logger

os.environ['SSL_CERT_FILE']      = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

# AWS config
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Pull config
BATCH_SIZE  = 50   # tickers per yfinance download call
BATCH_DELAY = 2    # seconds between batches — avoids rate limiting
HISTORY_YEARS = 2  # years of history to pull

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION
    )


def get_sp500_tickers() -> list[str]:
    """
    Returns S&P 500 tickers using a reliable fallback approach.
    Primary: fetch from Wikipedia via requests + certifi
    Fallback: use a known stable list of major S&P 500 components
    """
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        resp = requests.get(
            url,
            headers=headers,
            verify=certifi.where(),   # explicit cert bundle
            timeout=10
        )
        resp.raise_for_status()

        table = pd.read_html(io.StringIO(resp.text))[0]
        tickers = table["Symbol"].tolist()
        tickers = [t.replace(".", "-") for t in tickers]

        logger.info(f"Fetched {len(tickers)} S&P 500 tickers from Wikipedia")
        return tickers

    except Exception as e:
        logger.warning(f"Wikipedia fetch failed: {e}")
        logger.info("Falling back to yfinance S&P 500 constituents")

        # Fallback — reliable core S&P 500 stocks covering major sectors
        fallback = [
            # Tech
            "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AMD",
            "INTC","AVGO","ORCL","CRM","NFLX","ADBE","QCOM",
            # Finance
            "JPM","BAC","GS","MS","WFC","BLK","V","MA","AXP","C",
            # Healthcare
            "JNJ","UNH","PFE","MRK","ABBV","TMO","ABT","MDT","BMY","AMGN",
            # Consumer
            "WMT","AMZN","HD","MCD","SBUX","NKE","TGT","COST","LOW","TJX",
            # Energy
            "XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO","OXY","HAL",
            # Industrial
            "BA","CAT","GE","MMM","HON","UPS","RTX","LMT","DE","EMR",
            # ETFs for market context
            "SPY","QQQ","IWM","DIA","GLD",
        ]
        logger.info(f"Using fallback list: {len(fallback)} tickers")
        return fallback

def download_batch(
    tickers: list[str],
    start: str,
    end: str
) -> dict[str, pd.DataFrame]:
    """
    Downloads OHLCV data for a batch of tickers.
    Returns dict: {ticker: DataFrame}
    """
    try:
        raw = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            auto_adjust=True,   # adjusts prices for splits + dividends
            progress=False,     # suppress yfinance progress bar
            group_by="ticker",  # MultiIndex: (ticker, OHLCV)
        )
    except Exception as e:
        logger.error(f"Download failed for batch: {e}")
        return {}

    results = {}

    for ticker in tickers:
        try:
            # Single ticker returns flat DataFrame, not MultiIndex
            if len(tickers) == 1:
                df = raw.copy()
            else:
                df = raw[ticker].copy()

            # Drop rows where all price columns are NaN
            df = df.dropna(how="all")

            if df.empty:
                logger.warning(f"{ticker}: no data returned")
                continue

            # Flatten column names (removes MultiIndex if present)
            df.columns = [
                c if isinstance(c, str) else c[0]
                for c in df.columns
            ]

            # Standardise column names to lowercase
            df.columns = df.columns.str.lower().str.replace(" ", "_")

            # Add metadata
            df["symbol"]      = ticker
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()

            results[ticker] = df

        except Exception as e:
            logger.warning(f"{ticker}: processing error — {e}")

    return results

def save_to_s3(
    df: pd.DataFrame,
    ticker: str,
    s3_client
) -> bool:
    """
    Saves ticker DataFrame as Parquet to S3.
    Path: stocks/{ticker}.parquet
    Overwrites existing file — each run has complete history.
    """
    if df is None or df.empty:
        return False

    s3_key = f"stocks/{ticker}.parquet"

    try:
        # Serialise to Parquet in memory — no temp file on disk
        buffer = io.BytesIO()
        df.to_parquet(
            buffer,
            index=True,       # keep Date as index
            engine="pyarrow",
            compression="snappy"  # fast compression, good ratio
        )
        buffer.seek(0)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        return True

    except Exception as e:
        logger.error(f"{ticker}: S3 upload failed — {e}")
        return False

def run_historical_pull():
    """
    Pulls 2 years of daily OHLCV for all S&P 500 stocks.
    Run once to populate the data lake.
    Each ticker saved as stocks/{ticker}.parquet in S3.
    """
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=HISTORY_YEARS * 365)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    logger.info(f"Historical pull: {start_str} → {end_str}")

    tickers   = get_sp500_tickers()
    s3_client = get_s3_client()

    success = 0
    failed  = 0
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(tickers), BATCH_SIZE):
        batch     = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        logger.info(
            f"Batch {batch_num}/{total_batches} "
            f"({len(batch)} tickers): {batch[0]} → {batch[-1]}"
        )

        results = download_batch(batch, start_str, end_str)

        for ticker, df in results.items():
            if save_to_s3(df, ticker, s3_client):
                success += 1
            else:
                failed += 1

        failed += len(batch) - len(results)  # count tickers with no data

        # Respect rate limits between batches
        if i + BATCH_SIZE < len(tickers):
            time.sleep(BATCH_DELAY)

    logger.info(
        f"Historical pull complete: "
        f"{success} saved, {failed} failed "
        f"out of {len(tickers)} tickers"
    )
    return success, failed

def run_daily_pull(date_str: str = None):
    """
    Pulls one day of OHLCV for all S&P 500 stocks.
    Called by Airflow every evening after market close.
    Appends new row to each ticker's existing Parquet file.
    """
    if date_str is None:
        # Default to yesterday — same-day data isn't finalised until EOD
        yesterday = datetime.now() - timedelta(days=1)
        date_str  = yesterday.strftime("%Y-%m-%d")

    # yfinance end date is exclusive — add 1 day
    end_str = (
        datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    logger.info(f"Daily pull for: {date_str}")

    tickers   = get_sp500_tickers()
    s3_client = get_s3_client()

    success = 0
    failed  = 0
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(tickers), BATCH_SIZE):
        batch     = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        logger.info(f"Batch {batch_num}/{total_batches}")

        results = download_batch(batch, date_str, end_str)

        for ticker, new_df in results.items():
            try:
                # Load existing file from S3 if it exists
                s3_key = f"stocks/{ticker}.parquet"
                try:
                    obj = s3_client.get_object(
                        Bucket=S3_BUCKET, Key=s3_key
                    )
                    existing_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

                    # Append new row, drop duplicates on Date index
                    combined = pd.concat([existing_df, new_df])
                    combined = combined[
                        ~combined.index.duplicated(keep="last")
                    ].sort_index()

                except s3_client.exceptions.NoSuchKey:
                    # First time for this ticker — no existing file
                    combined = new_df

                if save_to_s3(combined, ticker, s3_client):
                    success += 1
                else:
                    failed += 1

            except Exception as e:
                logger.error(f"{ticker}: daily update failed — {e}")
                failed += 1

        failed += len(batch) - len(results)

        if i + BATCH_SIZE < len(tickers):
            time.sleep(BATCH_DELAY)

    logger.info(
        f"Daily pull complete: {success} updated, {failed} failed"
    )
    return success, failed

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "historical":
        logger.info("Mode: HISTORICAL (2-year full pull)")
        logger.info("This will take 10-15 minutes for all 500 stocks")
        run_historical_pull()

    elif mode == "daily":
        date = sys.argv[2] if len(sys.argv) > 2 else None
        logger.info(f"Mode: DAILY (date: {date or 'yesterday'})")
        run_daily_pull(date)

    else:
        logger.error(f"Unknown mode: {mode}. Use 'historical' or 'daily'")
        sys.exit(1)