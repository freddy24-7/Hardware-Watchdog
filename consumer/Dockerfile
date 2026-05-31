# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM rust:slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.toml
COPY agent/Cargo.toml agent/Cargo.toml
COPY consumer/Cargo.toml consumer/Cargo.toml

RUN mkdir -p agent/src consumer/src \
    && echo 'fn main() {}' > agent/src/main.rs \
    && echo 'fn main() {}' > consumer/src/main.rs

RUN cargo build --release -p consumer

COPY consumer/src consumer/src
RUN touch consumer/src/main.rs && cargo build --release -p consumer

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/consumer /usr/local/bin/consumer

ENTRYPOINT ["/usr/local/bin/consumer"]
