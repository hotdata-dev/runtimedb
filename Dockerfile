FROM ubuntu:24.04

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built binary and default config
COPY target/release/server ./server
COPY config-local.toml ./config.toml

# Create directories for cache and state
RUN mkdir -p ./cache ./state

EXPOSE 3000

ENTRYPOINT ["./server"]
CMD ["--config", "config.toml"]