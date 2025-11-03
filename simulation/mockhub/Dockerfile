# Build stage
FROM rust:1.89-slim AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml ./

# Copy source code
COPY src ./src

# Build release binary
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
EXPOSE 5544

# Set default environment variables (can be overridden)
ENV SYSTEM_PORT=5544

# Run the binary
CMD ["/app/mockhub"]