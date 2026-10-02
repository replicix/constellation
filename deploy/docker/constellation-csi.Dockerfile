# syntax=docker/dockerfile:1
# The one Constellation CSI image (plan 37 §14): static musl `constellation-csi`
# (entrypoint chosen by --controller / --node) plus the static musl
# `constellation` engine binary, which the node plugin starts as engine pods
# from this same image (daemon mode; no second engine image to build or scan).
#
#   make csi-image            # -> constellation-csi:dev
#
# Non-root by default (uid 65532). The node DaemonSet overrides to uid 0 +
# privileged: only it calls mount(2), and the control-acl service grant
# matches uid 0 (plan 37 §9). fuse3 is here for `fusermount3`.

FROM rust:1-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN rustup target add x86_64-unknown-linux-musl
ARG CONSTELLATION_GIT_DESCRIBE
ENV CONSTELLATION_GIT_DESCRIBE=$CONSTELLATION_GIT_DESCRIBE
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl \
        -p constellation -p constellation-csi \
    && cp target/x86_64-unknown-linux-musl/release/constellation \
          target/x86_64-unknown-linux-musl/release/constellation-csi /

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /constellation-csi /constellation /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/constellation-csi"]
