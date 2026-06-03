#!/bin/bash
set -e

# --- Detect AWS availability zone for rack-aware consumption ---
# Fargate tasks can query the ECS Task Metadata Endpoint v4
# This sets client.rack so consumers prefer brokers in the same AZ

if [ -n "$ECS_CONTAINER_METADATA_URI_V4" ]; then
  TASK_METADATA=$(curl -s --max-time 2 "${ECS_CONTAINER_METADATA_URI_V4}/task" || true)

  if [ -n "$TASK_METADATA" ]; then
    AZ=$(echo "$TASK_METADATA" | jq -r '.AvailabilityZone // empty')

    if [ -n "$AZ" ]; then
      export CLIENT_RACK="$AZ"
      echo "Detected AZ: $AZ (setting client.rack)"
    else
      echo "WARNING: Could not detect AZ from task metadata"
    fi
  fi
else
  echo "WARNING: ECS_CONTAINER_METADATA_URI_V4 not set, skipping rack detection"
fi

# --- Start the sink ---
exec /app/s3-sink