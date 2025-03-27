FROM lukemathwalker/cargo-chef:latest-rust-1.81.0 as chef
WORKDIR /app

FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim as runtime
WORKDIR /app
RUN apt-get update && apt-get install -y curl openssl ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/chai-api /usr/local/bin
ENV DATABASE_URL=postgresql://postgres:s3cr3t@db:5432/chai
EXPOSE 8080
CMD ["chai-api"]
