# --- Stage 1: Build dependencies (cached layer) ---
FROM rust:1.87-slim AS deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    gcc \
    g++ \
    make \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only manifests to cache dependency compilation
COPY Cargo.toml Cargo.lock ./

# Create dummy source to compile dependencies only
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release --bin s3-sink && \
    rm -rf src

# --- Stage 2: Build application ---
FROM deps AS builder

COPY src/ src/

# Touch to invalidate only the final binary, not deps
RUN touch src/bin/main.rs src/lib.rs && \
    cargo build --release --bin s3-sink

# --- Stage 3: Runtime image ---
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    libzstd1 \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false appuser

WORKDIR /app

COPY --from=builder /app/target/release/s3-sink /app/s3-sink

COPY build/bootstrap.sh /app/bootstrap.sh

ENV SCRATCH_DIRECTORY=/tmp/s3-sink-scratch

RUN chmod +x /app/bootstrap.sh && \
    mkdir -p ${SCRATCH_DIRECTORY} && \
    chown appuser:appuser ${SCRATCH_DIRECTORY}

USER appuser

ENTRYPOINT ["/app/bootstrap.sh"]