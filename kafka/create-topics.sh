#!/bin/bash

echo "Waiting for Kafka to be ready..."
until kafka-topics --list --bootstrap-server kafka:29092 > /dev/null 2>&1; do
  echo "  Kafka not ready, retrying in 3s..."
  sleep 3
done

echo "Kafka is ready. Creating topics..."

kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --topic stocks-ticks

kafka-topics --create --if-not-exists \
  --bootstrap-server kafka:29092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --topic crypto-ticks

echo "Topics created successfully:"
kafka-topics --list --bootstrap-server kafka:29092