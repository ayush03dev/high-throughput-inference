#!/usr/bin/env bash
# Increase Kafka topic partitions after upgrading from 8 → 32.
# Safe to re-run; only increases partition count.
set -euo pipefail

KAFKA_CONTAINER=$(docker compose ps -q kafka)
BS="--bootstrap-server localhost:9092"
KAFKA="/opt/kafka/bin"

for topic in inference.requests inference.requests.retry inference.completed; do
  echo "Resizing $topic to 32 partitions..."
  docker exec "$KAFKA_CONTAINER" $KAFKA/kafka-topics.sh $BS \
    --alter --topic "$topic" --partitions 32 2>/dev/null || \
  docker exec "$KAFKA_CONTAINER" $KAFKA/kafka-topics.sh $BS \
    --create --if-not-exists --topic "$topic" --partitions 32 --replication-factor 1
done

echo "Done. Current topics:"
docker exec "$KAFKA_CONTAINER" $KAFKA/kafka-topics.sh $BS --describe | grep -E "Topic:|PartitionCount"
