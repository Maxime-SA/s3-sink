#!/bin/bash
set -e

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
