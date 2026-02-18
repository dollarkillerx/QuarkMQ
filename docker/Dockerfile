FROM rust:1.84-slim AS builder

WORKDIR /build

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/quarkmq-protocol/Cargo.toml crates/quarkmq-protocol/Cargo.toml
COPY crates/quarkmq-storage/Cargo.toml crates/quarkmq-storage/Cargo.toml
COPY crates/quarkmq-broker/Cargo.toml crates/quarkmq-broker/Cargo.toml
COPY crates/quarkmq-cluster/Cargo.toml crates/quarkmq-cluster/Cargo.toml
COPY crates/quarkmq-server/Cargo.toml crates/quarkmq-server/Cargo.toml
COPY crates/quarkmq-client/Cargo.toml crates/quarkmq-client/Cargo.toml
COPY tests/integration/Cargo.toml tests/integration/Cargo.toml

# Create stub source files for dependency caching
RUN mkdir -p crates/quarkmq-protocol/src && echo "pub fn stub(){}" > crates/quarkmq-protocol/src/lib.rs && \
    mkdir -p crates/quarkmq-storage/src && echo "pub fn stub(){}" > crates/quarkmq-storage/src/lib.rs && \
    mkdir -p crates/quarkmq-broker/src && echo "pub fn stub(){}" > crates/quarkmq-broker/src/lib.rs && \
    mkdir -p crates/quarkmq-cluster/src && echo "pub fn stub(){}" > crates/quarkmq-cluster/src/lib.rs && \
    mkdir -p crates/quarkmq-server/src && echo "fn main(){}" > crates/quarkmq-server/src/main.rs && \
    echo "pub fn stub(){}" > crates/quarkmq-server/src/lib.rs && \
    mkdir -p crates/quarkmq-client/src && echo "pub fn stub(){}" > crates/quarkmq-client/src/lib.rs && \
    mkdir -p tests/integration/src && echo "pub fn stub(){}" > tests/integration/src/lib.rs

# Build dependencies only
RUN cargo build --release --bin quarkmq 2>/dev/null || true

# Copy actual source
COPY crates crates
COPY config config

# Touch to invalidate the cache for actual source
RUN find crates -name "*.rs" -exec touch {} +

# Build the binary
RUN cargo build --release --bin quarkmq

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false quarkmq && \
    mkdir -p /var/lib/quarkmq/data && \
    chown -R quarkmq:quarkmq /var/lib/quarkmq

COPY --from=builder /build/target/release/quarkmq /usr/local/bin/quarkmq
COPY config/quarkmq.toml /etc/quarkmq/quarkmq.toml

USER quarkmq

EXPOSE 9876

ENTRYPOINT ["quarkmq"]
CMD ["--config", "/etc/quarkmq/quarkmq.toml"]
