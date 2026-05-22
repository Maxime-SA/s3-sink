#!/bin/bash
set -e

echo "=== Starting services ==="
docker compose -f "$(dirname "$0")/docker-compose.yml" up -d

echo "=== Waiting for Kafka to be ready ==="
sleep 10

echo "=== Creating topics ==="
KAFKA_CONTAINER=$(docker compose -f "$(dirname "$0")/docker-compose.yml" ps -q kafka)
for i in $(seq 1 10); do
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic "topic-${i}" --partitions 6 --if-not-exists
  echo "  created topic-${i}"
done

echo "=== Creating MinIO bucket ==="
MC_CMD=""
if command -v mcli &> /dev/null; then
  MC_CMD="mcli"
elif command -v mc &> /dev/null; then
  MC_CMD="mc"
fi

if [ -z "$MC_CMD" ]; then
  echo "  mc/mcli not found, please install: pacman -S minio-client"
else
  $MC_CMD alias set local http://localhost:9000 minioadmin minioadmin
  $MC_CMD mb local/sink-output --ignore-existing
  echo "  created bucket: sink-output"
fi

echo "=== Done ==="
echo "Kafka:       localhost:9092"
echo "MinIO S3:    localhost:9000"
echo "MinIO UI:    localhost:9001 (minioadmin/minioadmin)"
