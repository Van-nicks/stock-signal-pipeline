# Commands Reference

> All commands run from repo root unless specified.
> Always activate venv first: `source venv/bin/activate`

## Local Setup

### Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

```bash
cp .env.example .env
# Edit .env with real credentials
```

## Docker

### Start all services

```bash
docker-compose up -d
```

### Stop all services

```bash
docker-compose down
```

### View logs

```bash
docker-compose logs -f kafka
docker-compose logs -f spark
docker-compose logs -f airflow
```

## Kafka

### List topics

```bash
docker exec -it kafka kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

### Consume stocks topic

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stocks-ticks \
  --from-beginning
```

### Consume crypto topic

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-ticks \
  --from-beginning
```

## Data Ingestion

### Run Alpaca producer

```bash
python data_ingestion/alpaca_producer.py
```

### Run Binance producer

```bash
python data_ingestion/binance_producer.py
```

## dbt

### Run all models

```bash
cd dbt && dbt run
```

### Run tests

```bash
cd dbt && dbt test
```

### Generate and serve docs

```bash
cd dbt && dbt docs generate && dbt docs serve
```

## Testing

```bash
pytest tests/ -v
```

## Spark Streaming

### Run streaming job

```bash
python spark/jobs/streaming_job.py
```

### Clear checkpoints (force re-read from Kafka latest)

```bash
rm -rf /tmp/spark-checkpoints
```