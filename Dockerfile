FROM ubuntu:24.04

LABEL org.opencontainers.image.source="https://github.com/hotdata-dev/runtimedb"
LABEL org.opencontainers.image.description="Federated query engine with on-demand caching. Run: docker run -p 3000:3000 ghcr.io/hotdata-dev/runtimedb | Custom config: docker run -p 3000:3000 -v ./config.toml:/app/config.toml ghcr.io/hotdata-dev/runtimedb"
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built binary and default config
COPY target/release/server ./server
COPY config-docker.toml ./config.toml

# Create directories for cache and state
RUN mkdir -p ./cache ./state

EXPOSE 3000

ENTRYPOINT ["./server"]
CMD ["config.toml"]