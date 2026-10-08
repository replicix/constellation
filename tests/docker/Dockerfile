# syntax=docker/dockerfile:1
# Containerized test runner: builds the constellation binary plus the
# pjdfstest POSIX compliance suite, and packages them with fuse3, fio,
# and stress-ng so the full test suites (including the FUSE mount
# itself) run inside a container for reproducibility.

FROM rust:1-bookworm AS build
# Extra cargo features for the binaries: empty by default, so the image is
# the shipped build — which on Linux already carries the FUSE-over-io_uring
# transport (plan 38 Z2c); reaching the ring at *run* time also needs a
# seccomp profile that permits `io_uring_setup(2)` (docker-compose.yml's
# `compliance-ring`).
ARG CARGO_FEATURES=
# The target cache is per checkout: cargo decides freshness by mtime, COPY
# keeps the context's mtimes, and every checkout builds at /src, so a
# target cache shared by two worktrees on one host hands one of them the
# other's newer artifacts (a binary that is not its source).
# `docker-compose.yml` passes its project name (by default the checkout's
# directory); other builds share the default id.
ARG TARGET_CACHE_ID=constellation-target
# CI (ci.yml's `integration` job) sets this to reuse the release binaries
# the shared `build` job already compiled on the host, instead of compiling
# them again inside the image: it copies them to ci-bin/ in the build
# context before `docker build`/`docker compose build` runs (docker-compose.yml
# passes the arg through). Empty (the default) keeps the local/dev path —
# compile here — unchanged.
ARG CI_PREBUILT=
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,id=${TARGET_CACHE_ID},target=/src/target \
    if [ -n "$CI_PREBUILT" ]; then \
        cp ci-bin/constellation /constellation && cp ci-bin/harness /harness; \
    else \
        cargo build --release ${CARGO_FEATURES:+--features "$CARGO_FEATURES"} \
            -p constellation -p constellation-harness \
        && cp target/release/constellation /constellation \
        && cp target/release/harness /harness; \
    fi

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

FROM debian:bookworm-slim AS xfstests-build
# Pinned upstream revision: 2026-08-29 xfstests-dev HEAD.
ARG XFSTESTS_REF=56c410ad0f69da5b13c5807bc47b4876dcfa02b2
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git ca-certificates gcc make autoconf automake libtool pkg-config \
       libaio-dev liburing-dev libattr1-dev libacl1-dev libcap-dev \
       libgdbm-dev uuid-dev xfslibs-dev xfsprogs \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git /opt/xfstests \
    && cd /opt/xfstests \
    && git checkout "$XFSTESTS_REF" \
    && make -j"$(nproc)"

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
#   label:disable (SELinux hosts),
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

# Nightly xfstests runner. Kept separate because its toolset and upstream
# checkout are too large for the fast PR image.
FROM runtime-base AS xfstests-runner
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       acl attr bc e2fsprogs gawk libaio1 liburing2 perl python3 quota xfsprogs \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m fsgqa \
    && useradd 123456-fsgqa \
    && groupadd fsgqa2
COPY --from=build /constellation /usr/local/bin/constellation
COPY --from=xfstests-build /opt/xfstests /opt/xfstests
COPY tests/docker/mount.fuse.constellation /sbin/mount.fuse.constellation
COPY tests/xfstests.sh tests/xfstests-exclude.txt tests/xfstests-baseline.txt \
     /usr/local/lib/constellation/
RUN chmod +x /sbin/mount.fuse.constellation /usr/local/lib/constellation/xfstests.sh
ENV XFSTESTS_DIR=/opt/xfstests \
    XFSTESTS_BASELINE=/usr/local/lib/constellation/xfstests-baseline.txt \
    XFSTESTS_EXCLUDE=/usr/local/lib/constellation/xfstests-exclude.txt
ENTRYPOINT ["bash", "/usr/local/lib/constellation/xfstests.sh"]

# Suite runner (smoke / pjdfstest compliance / fio+stress-ng stress).
# Kept last so it stays the default build target.
FROM runtime-base AS suite
COPY --from=build /constellation /usr/local/bin/constellation
# smoke.sh is a wrapper around `harness smoke`.
COPY --from=build /harness /usr/local/bin/harness
COPY --from=pjdfstest /opt/pjdfstest/pjdfstest /opt/pjdfstest/pjdfstest
COPY --from=pjdfstest /opt/pjdfstest/tests /opt/pjdfstest/tests
COPY tests/lib.sh tests/smoke.sh tests/compliance.sh tests/stress.sh \
     tests/pjdfstest-baseline.txt tests/docker/run-suite.sh \
     /usr/local/lib/constellation/
ENV CONSTELLATION_BIN=/usr/local/bin/constellation \
    PJDFSTEST_DIR=/opt/pjdfstest \
    SUITE_DIR=/usr/local/lib/constellation
ENTRYPOINT ["bash", "/usr/local/lib/constellation/run-suite.sh"]
