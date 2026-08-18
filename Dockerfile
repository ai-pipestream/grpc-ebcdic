# syntax=docker/dockerfile:1
#
# Multi-stage build for the grpc-ebcdic collector.
#
# The test suite runs inside the build stage and gates the image: there is no
# path to a runnable artifact that skips `cargo test`, because a decoder that
# is wrong about a sign nibble is worse than one that does not ship.
#
# The runtime stage is Debian slim with no shell needed for operation, no
# package manager left behind, and a non-root user. The service is diskless —
# record bytes never leave memory — so the container runs happily with
# `--read-only` and no writable volume:
#
#   docker build -t grpc-ebcdic .
#   docker run --rm --read-only --cap-drop=ALL -p 50063:50063 grpc-ebcdic

FROM rust:1.97-slim-trixie AS build

# protoc is the only build dependency beyond the Rust toolchain: build.rs
# drives tonic-prost-build, which shells out to it.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends protobuf-compiler; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Dependencies first, so a source-only change does not refetch the registry.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > build.rs \
    && cargo fetch --locked \
    && rm -rf src build.rs

COPY build.rs rustfmt.toml ./
COPY proto ./proto
COPY src ./src
COPY tests ./tests

# The gate. Unit tests, the streaming integration suite, and the health and
# reflection wiring all run here; a failure fails the build.
RUN cargo test --locked --release --all-targets

RUN cargo build --locked --release --bin grpc-ebcdic \
    && strip target/release/grpc-ebcdic

FROM debian:trixie-slim

# A system account with no login shell and no home directory to write into.
RUN set -eux; \
    groupadd --system --gid 10001 ebcdic; \
    useradd --system --uid 10001 --gid ebcdic \
        --home-dir /nonexistent --no-create-home \
        --shell /usr/sbin/nologin ebcdic

COPY --from=build /src/target/release/grpc-ebcdic /usr/local/bin/grpc-ebcdic

USER 10001:10001
EXPOSE 50063

# See src/main.rs for the full list; these are the ones worth defaulting.
ENV GRPC_EBCDIC_ADDR=0.0.0.0:50063 \
    GRPC_EBCDIC_MAX_DOCUMENT_MIB=512 \
    GRPC_EBCDIC_MAX_CONCURRENT_PARSES=64 \
    GRPC_EBCDIC_METRICS_INTERVAL_SECONDS=60

# No HEALTHCHECK instruction: the server implements grpc.health.v1.Health, and
# an orchestrator should probe that rather than have the image shell out to a
# client binary it would otherwise have no reason to carry.
ENTRYPOINT ["/usr/local/bin/grpc-ebcdic"]
