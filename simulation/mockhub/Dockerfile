# Build stage
FROM rust:1.89-slim AS builder

WORKDIR /app

# Copy manifests first
COPY Cargo.toml ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/mockhub* target/release/deps/mockhub*

# Now copy the real source code
COPY src ./src

# Build the actual binary (dependencies are cached, but binary is rebuilt)
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/mockhub /app/mockhub

# Expose the port the app runs on
EXPOSE 3000

# Set default environment variables (can be overridden)
ENV SYSTEM_PORT=5544

# Run the binary
CMD ["/app/mockhub"]