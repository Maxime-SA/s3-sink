# Benchmarks

## Prerequisites

- Docker (or Colima on macOS)
- Rust 1.93+
- `mc` or `mcli` (MinIO client) for bucket creation

## Steps

### 1. Start infrastructure

```bash
docker compose -f benches/docker-compose.yml up -d
```

This starts:
- **Kafka** (KRaft mode) on `localhost:9092`
- **MinIO** (S3-compatible) on `localhost:9000` (API) / `localhost:9001` (web UI)

### 2. Create topics and bucket

```bash
./benches/setup.sh
```

Creates 10 topics (`topic-1` through `topic-10`, 6 partitions each) and the `sink-output` MinIO bucket.

### 3. Build

```bash
cargo build --release --bin bench-producer --bin bench-sink
```

### 4. Produce test data

```bash
./target/release/bench-producer
```

Produces ~1MB per topic across 20 topics with varying payload sizes (4KB–10MB).

### 5. Run the sink

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export RUST_LOG=info

./target/release/bench-sink
```

### 6. Run with resource limits (optional)

To simulate a constrained environment (e.g. 1 CPU, 512MB RAM):

```bash
systemd-run --user --scope -p MemoryMax=512M -p CPUQuota=100% ./target/release/bench-sink
```

Or with Docker (no container image needed, just cgroups):

```bash
docker run --rm --network=host \
  --cpus="1.0" --memory="512m" \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_REGION=us-east-1 \
  -e RUST_LOG=info \
  -v ./target/release/bench-sink:/bench-sink:ro \
  -v /tmp/s3-sink-scratch:/tmp/s3-sink-scratch \
  debian:bookworm-slim /bench-sink
```

### 7. Inspect results

**Scratch files (local disk):**
```
/tmp/s3-sink-scratch/
```
Active files being written to before upload.

**Uploaded objects (MinIO):**
- Web UI: http://localhost:9001 (login: `minioadmin` / `minioadmin`)
- CLI: `mc ls local/sink-output --recursive`

### 8. Tear down

```bash
docker compose -f benches/docker-compose.yml down -v
```

The `-v` flag removes named volumes (Kafka data + MinIO objects).

## Reset consumer offsets

To re-consume existing data without re-producing, reset the consumer group offsets:

```bash
KAFKA_CONTAINER=$(docker compose -f benches/docker-compose.yml ps -q kafka)
docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group s3-sink-bench \
  --all-topics \
  --reset-offsets \
  --to-earliest \
  --execute
```

Or change the `group.id` in `bench_sink.rs`.
