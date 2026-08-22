# syntax=docker/dockerfile:1
# Containerized test runner: builds the constellation binary plus the
# pjdfstest POSIX compliance suite, and packages them with fuse3, fio,
# and stress-ng so the full test suites (including the FUSE mount
# itself) run inside a container for reproducibility.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p constellation -p constellation-harness \
    && cp target/release/constellation /constellation \
    && cp target/release/harness /harness

FROM debian:bookworm-slim AS pjdfstest
# Pinned upstream commit for reproducible test semantics.
ARG PJDFSTEST_REF=85a8aea9e685999ef0540392fd80535f873d7ff7
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git ca-certificates gcc libc6-dev make autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/pjd/pjdfstest /opt/pjdfstest \
    && cd /opt/pjdfstest \
    && git checkout "$PJDFSTEST_REF" \
    && autoreconf -ifs \
    && ./configure \
    && make pjdfstest

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fuse3 util-linux diffutils ca-certificates curl \
        fio stress-ng openssl \
    && rm -rf /var/lib/apt/lists/* \
    && echo user_allow_other >> /etc/fuse.conf

# Containerized fault-injection harness: drives the host docker daemon
# (mounted socket) to start floci/toxiproxy as sibling containers, and
# runs the FUSE mounts inside this container. Run with:
#   network_mode: host, /dev/fuse, CAP_SYS_ADMIN, apparmor:unconfined,
#   -v /var/run/docker.sock:/var/run/docker.sock
FROM runtime-base AS harness
ARG DOCKER_VERSION=27.3.1
RUN arch="$(uname -m)" \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${arch}/docker-${DOCKER_VERSION}.tgz" \
       | tar xz --strip-components=1 -C /usr/local/bin docker/docker
COPY --from=build /constellation /usr/local/bin/constellation
COPY --from=build /harness /usr/local/bin/harness
ENV CONSTELLATION_BIN=/usr/local/bin/constellation
ENTRYPOINT ["harness"]
CMD ["run"]

# Suite runner (smoke / pjdfstest compliance / fio+stress-ng stress).
# Kept last so it stays the default build target.
FROM runtime-base AS suite
COPY --from=build /constellation /usr/local/bin/constellation
COPY --from=pjdfstest /opt/pjdfstest/pjdfstest /opt/pjdfstest/pjdfstest
COPY --from=pjdfstest /opt/pjdfstest/tests /opt/pjdfstest/tests
COPY tests/lib.sh tests/smoke.sh tests/compliance.sh tests/stress.sh \
     tests/pjdfstest-baseline.txt tests/docker/run-suite.sh \
     /usr/local/lib/constellation/
ENV CONSTELLATION_BIN=/usr/local/bin/constellation \
    PJDFSTEST_DIR=/opt/pjdfstest \
    SUITE_DIR=/usr/local/lib/constellation
ENTRYPOINT ["bash", "/usr/local/lib/constellation/run-suite.sh"]
