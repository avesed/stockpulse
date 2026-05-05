FROM rust:1.83-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release --target x86_64-unknown-linux-musl 2>/dev/null \
    || (rustup target add x86_64-unknown-linux-musl && cargo build --release --target x86_64-unknown-linux-musl)

# --- Final image: scratch + static binary ---
FROM scratch

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/ipv6-rotator /ipv6-rotator

ENTRYPOINT ["/ipv6-rotator"]
