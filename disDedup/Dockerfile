FROM debian:bookworm-slim
WORKDIR /app
COPY bin/manager /usr/local/bin/manager
COPY bin/storage-node /usr/local/bin/storage-node
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*
COPY scripts/network/netem.sh /netem.sh
COPY scripts/network/netem_manager.sh /netem_manager.sh
RUN chmod +x /netem.sh /netem_manager.sh
ENTRYPOINT []
