# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM rust:slim AS builder

# Install librdkafka build dependencies (required by the rdkafka crate)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    cmake \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifests first so dependency layers are cached independently
# of source changes — avoids full recompile on every code edit.
COPY Cargo.toml Cargo.toml
COPY agent/Cargo.toml agent/Cargo.toml
COPY consumer/Cargo.toml consumer/Cargo.toml

# Stub source trees so Cargo can fetch and compile all dependencies.
# The real source is copied below and triggers only a final incremental recompile.
RUN mkdir -p agent/src consumer/src \
    && echo 'fn main() {}' > agent/src/main.rs \
    && echo 'fn main() {}' > consumer/src/main.rs

RUN cargo build --release -p agent

# Copy real source and touch to invalidate the stub artifact
COPY agent/src agent/src
RUN touch agent/src/main.rs && cargo build --release -p agent

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# ubuntu:24.04 ships glibc 2.39 which satisfies the builder's GLIBC_2.38 requirement
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/agent /usr/local/bin/agent

ENTRYPOINT ["/usr/local/bin/agent"]
