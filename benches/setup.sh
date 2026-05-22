#!/bin/bash
set -e

echo "=== Starting services ==="
docker compose -f "$(dirname "$0")/docker-compose.yml" up -d

echo "=== Waiting for Kafka to be ready ==="
sleep 10

echo "=== Creating topics ==="
for i in $(seq 1 10); do
  docker exec s3-sink-kafka-1 kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic "topic-${i}" --partitions 6 --if-not-exists
  echo "  created topic-${i}"
done

echo "=== Installing MinIO client ==="
if ! command -v mc &> /dev/null; then
  echo "  mc not found, please install: pacman -S minio-client"
  echo "  or: curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc"
else
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc mb local/sink-output --ignore-existing
  echo "  created bucket: sink-output"
fi

echo "=== Done ==="
echo "Kafka:       localhost:9092"
echo "MinIO S3:    localhost:9000"
echo "MinIO UI:    localhost:9001 (minioadmin/minioadmin)"
