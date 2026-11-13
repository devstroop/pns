FROM rust:1.96 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/devstroop-push /usr/local/bin/devstroop-push
ENV RUST_LOG=info
EXPOSE 50051 8080
CMD ["devstroop-push"]
