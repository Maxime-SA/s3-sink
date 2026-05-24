#! /bin/bash

#!/bin/bash
INSTANCES=${1:-2}
ENV="AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=eu-west-1 RUST_LOG=warn,s3_sink=debug"

for i in $(seq 1 "$INSTANCES"); do
  systemd-run --user --scope \
    -p MemoryMax=1024M \
    -p CPUQuota=100% \
    --unit="bench-sink-$i" \
    env $ENV \
    ./target/release/bench-sink 2>&1 | sed "s/^/[$i] /" &
  sleep 0.5
done

echo "Launched $INSTANCES instances"
wait