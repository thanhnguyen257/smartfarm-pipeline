#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
# cub kafka-ready -b kafka-broker:29092 1 20

echo "Creating topics..."

kafka-topics --bootstrap-server kafka-broker:29092 \
  --create \
  --if-not-exists \
  --topic farm_raw_telemetry \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=3600000 \
  --config retention.bytes=1073741824

kafka-topics --bootstrap-server kafka-broker:29092 \
  --create \
  --if-not-exists \
  --topic farm_raw_alerts \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=3600000 \
  --config retention.bytes=1073741824

kafka-topics --bootstrap-server kafka-broker:29092 \
  --create \
  --if-not-exists \
  --topic farm_cleaned_alerts \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=3600000 \
  --config retention.bytes=1073741824


echo "Kafka topics ready ✅"
