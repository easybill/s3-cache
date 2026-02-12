FROM rust:1-slim-bullseye AS builder

RUN apt-get update && apt-get install -y \
    gcc-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc

RUN rustup target add x86_64-unknown-linux-gnu

WORKDIR /build

COPY . .

RUN cargo build --release --bin s3_cache --target x86_64-unknown-linux-gnu

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/x86_64-unknown-linux-gnu/release/s3_cache /usr/local/bin/s3_cache

ENTRYPOINT ["/usr/local/bin/s3_cache"]
