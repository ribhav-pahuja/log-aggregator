#!/usr/bin/env bash
# Create the logs topic (works against the compose Kafka container).
set -euo pipefail
TOPIC="${1:-logs}"
# Prefer internal listener when exec'ing into the broker container
docker exec alert-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 1
echo "Topic '$TOPIC' ready"
