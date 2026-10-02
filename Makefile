# Local build and test shortcuts for Constellation.
#
#   make              # list targets
#   make check        # fast gate: fmt + clippy + unit tests + host smoke
#   make ci           # full CI mirror (needs docker, fuse3, fio, stress-ng)
#
# Override the release binary used by the harness:
#   make harness CONSTELLATION_BIN=/path/to/constellation

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

CARGO ?= cargo
TARGET_DIR ?= $(or $(CARGO_TARGET_DIR),target)
RELEASE_BIN := $(TARGET_DIR)/release/constellation
RELEASE_HARNESS := $(TARGET_DIR)/release/harness
RELEASE_CHAOS := $(TARGET_DIR)/release/chaos
RELEASE_CSI := $(TARGET_DIR)/release/constellation-csi
DEBUG_BIN := $(TARGET_DIR)/debug/constellation
DEBUG_HARNESS := $(TARGET_DIR)/debug/harness
DEBUG_CHAOS := $(TARGET_DIR)/debug/chaos
UPLOADBENCH := $(TARGET_DIR)/release/uploadbench

export CONSTELLATION_BIN ?= $(abspath $(RELEASE_BIN))
export CHAOS_BIN ?= $(abspath $(RELEASE_CHAOS))
export RUSTFLAGS ?=
export CARGO_TERM_COLOR ?= always

# Containerized suite selection (smoke compliance stress).
COMPOSE_SUITES ?=
HARNESS_SCENARIOS ?=
HARNESS_SEED ?= 42
# FUSE transports the matrix lane runs (plan 38 §6): `dev-fuse` and the
# ladder (`auto`), which falls back to `/dev/fuse` where the build or the
# kernel cannot grant the ring and must pass either way.
TRANSPORTS ?= dev-fuse auto
# Cargo features for the binaries the lanes build. The ring transport is
# off by default; `make ... CARGO_FEATURES=constellation-frontend-fuse/io-uring`
# (or `make build-uring`) is what puts it in.
CARGO_FEATURES ?=
FEATURE_FLAGS = $(if $(CARGO_FEATURES),--features $(CARGO_FEATURES),)
BENCH_FILES ?= 20000
UPLOADBENCH_LIVE_CONTROLLERS ?= aimd,pid
UPLOADBENCH_LIVE_DURATION ?= 60
UPLOADBENCH_INITIAL_CONCURRENCY ?= 4

# Local, machine-specific overrides (S3 buckets, etc) — gitignored, see
# local.mk.example. Silently absent is fine; BUCKET stays unset and
# `make uploadbench-live` will just fail with a clear "no BUCKET" error.
-include local.mk

.PHONY: help build build-release build-debug build-chaos test test-unit fmt fmt-check clippy lint \
	check ci clean smoke integration webui-check csi-sanity csi-image compose compose-down harness harness-docker \
	harness-list bench perf-regression xfstests perf-gate read-cpu-gate transport-matrix \
	harness-transport-matrix build-uring compliance-uring \
	dist-linux dist-macos deps FORCE \
	uploadbench-build uploadbench-sim uploadbench-live check-cross vfs-bench

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make [target]\n\nTargets:\n"} \
		/^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo
	@echo "Variables:"
	@echo "  COMPOSE_SUITES=\"...\"   suites for compose (default: all)"
	@echo "  HARNESS_SCENARIOS=\"..\" scenario names (default: all)"
	@echo "  HARNESS_SEED=$(HARNESS_SEED)         harness workload seed"
	@echo "  TRANSPORTS=\"$(TRANSPORTS)\"  FUSE transports for transport-matrix"
	@echo "  CARGO_FEATURES=\"$(CARGO_FEATURES)\"  cargo features for the built binaries"
	@echo "  BENCH_FILES=$(BENCH_FILES)       files for harness bench"
	@echo "  BUCKET=<local.mk>       s3://bucket/prefix for uploadbench-live (see local.mk.example)"
	@echo "  UPLOADBENCH_LIVE_CONTROLLERS=$(UPLOADBENCH_LIVE_CONTROLLERS)"
	@echo "  UPLOADBENCH_LIVE_DURATION=$(UPLOADBENCH_LIVE_DURATION) seconds per controller"
	@echo "  UPLOADBENCH_INITIAL_CONCURRENCY=$(UPLOADBENCH_INITIAL_CONCURRENCY)"

build: build-release ## Build constellation + harness + chaos (release)

build-release: $(RELEASE_BIN) $(RELEASE_HARNESS) $(RELEASE_CHAOS) ## Build release binaries

build-debug: $(DEBUG_BIN) $(DEBUG_HARNESS) $(DEBUG_CHAOS) ## Build debug binaries

build-chaos: $(RELEASE_CHAOS) ## Build chaos consistency tool (release)

# FORCE: make does not track Rust sources, so existing binaries would
# otherwise make these recipes no-ops. Cargo itself is incremental.
# `&:` = one recipe produces both outputs (GNU make 4.3+).
$(RELEASE_BIN) $(RELEASE_HARNESS) $(RELEASE_CHAOS) &: FORCE
	$(CARGO) build --release $(FEATURE_FLAGS) -p constellation -p constellation-harness -p constellation-chaos

$(DEBUG_BIN) $(DEBUG_HARNESS) $(DEBUG_CHAOS) &: FORCE
	$(CARGO) build $(FEATURE_FLAGS) -p constellation -p constellation-harness -p constellation-chaos

# constellation-csi (plan 37) is not a default workspace member (it is
# Kubernetes/Linux-only — see Cargo.toml), so it needs its own `-p` build
# rather than riding along with build-release's `&:` group above.
$(RELEASE_CSI): FORCE
	$(CARGO) build --release -p constellation-csi

FORCE: ;

test: test-unit ## Alias for unit tests

test-unit: ## Run workspace unit tests
	$(CARGO) test --workspace

fmt: ## Format all Rust code
	$(CARGO) fmt --all

fmt-check: ## Check formatting (CI)
	$(CARGO) fmt --all --check

clippy: ## Run clippy with warnings denied (CI)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

lint: fmt-check clippy ## Format check + clippy (CI lint job)

check: lint test-unit smoke ## Fast local gate (~seconds + smoke)

ci: lint test-unit compose harness ## Full CI mirror (slow; needs docker + tools)

clean: ## Remove build artifacts
	$(CARGO) clean
	rm -rf $(TARGET_DIR)

smoke: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Host smoke test (local file backend; needs fuse3)
	tests/smoke.sh

integration: $(RELEASE_BIN) ## Host integration (floci S3 in docker)
	tests/integration.sh

webui-check: $(RELEASE_BIN) ## Headless-Chrome check of the web UI's snapshots page (needs fuse3; SKIPs without google-chrome, or set CHROME_BIN)
	CONSTELLATION_BIN=$(RELEASE_BIN) tests/webui-headless.sh

CSI_IMAGE ?= constellation-csi:dev

csi-image: ## Build the constellation-csi image (static musl; one image for controller + node + engine pods)
	docker build -f deploy/docker/constellation-csi.Dockerfile \
		--build-arg CONSTELLATION_GIT_DESCRIBE="$$(git describe --tags --always --dirty 2>/dev/null)" \
		--build-arg JOBS="$${CARGO_BUILD_JOBS:-}" \
		-t $(CSI_IMAGE) .

csi-sanity: $(RELEASE_CSI) ## csi-sanity's Identity + Controller + Node groups against constellation-csi on its in-memory backends (plan 37 K1-K3; needs CSI_SANITY_BIN or csi-sanity on PATH)
	CONSTELLATION_CSI_BIN=$(abspath $(RELEASE_CSI)) tests/csi/sanity.sh

compose: ## Containerized FUSE suites (floci S3; needs docker)
	@if [ -n "$(COMPOSE_SUITES)" ]; then \
		tests/compose-test.sh $(COMPOSE_SUITES); \
	else \
		tests/compose-test.sh; \
	fi

compose-down: ## Containerized suites, then tear down compose stack
	@if [ -n "$(COMPOSE_SUITES)" ]; then \
		tests/compose-test.sh --down $(COMPOSE_SUITES); \
	else \
		tests/compose-test.sh --down; \
	fi

harness-list: $(RELEASE_HARNESS) ## List fault-injection scenarios
	$(RELEASE_HARNESS) list

harness: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Run fault-injection harness (needs docker + fuse3)
	@if [ -n "$(HARNESS_SCENARIOS)" ]; then \
		$(RELEASE_HARNESS) run $(HARNESS_SCENARIOS) --seed $(HARNESS_SEED); \
	else \
		$(RELEASE_HARNESS) run --seed $(HARNESS_SEED); \
	fi

harness-docker: ## Fault-injection harness fully in docker (host needs docker only)
	docker compose --profile test build harness
	docker compose --profile test run --rm harness \
		run $(HARNESS_SCENARIOS) --seed $(HARNESS_SEED)

bench: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Census-scale import benchmark (needs docker + fuse3)
	$(RELEASE_HARNESS) bench --files $(BENCH_FILES)

perf-regression: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Run local perf-regression suite (full, latency250, bw50)
	python3 tests/perf_regression/run_suite.py \
		--harness-bin $(RELEASE_HARNESS) \
		--constellation-bin $(RELEASE_BIN) \
		--out /tmp/constellation-perf-head.json \
		--logs-dir /tmp/constellation-perf-logs \
		--repetitions 1 \
		--seed 42 \
		--corpus-shape

check-cross: ## Type-check darwin (workspace) + windows-gnu (library crates) via zig cc; see tools/check-cross.sh
	tools/check-cross.sh

vfs-bench: ## VFS dispatch overhead (<1us/op) and per-op allocations, in-process, no kernel (plan 31 C7); exits 1 past the targets
	cargo bench -p constellation-engine --bench vfs_bench

perf-gate: $(RELEASE_BIN) $(RELEASE_HARNESS) ## vfs-bench (§6.9 dispatch + allocation ceilings), then benchmark rates against baseline
	tests/perf-gate.sh

read-cpu-gate: $(RELEASE_BIN) ## Daemon CPU-s/GiB + peak RSS on a real mount, fio-driven (plan 38 §6); needs fio, no root
	tests/read-cpu-gate.sh

# The lane's `auto` leg is only coverage with the ring built in; on a host
# that grants it, tests/transport-matrix.sh fails a leg that fell back.
transport-matrix harness-transport-matrix: CARGO_FEATURES = constellation-frontend-fuse/io-uring

transport-matrix: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Read-path scenarios once per FUSE transport (plan 38 §6)
	TRANSPORTS="$(TRANSPORTS)" tests/transport-matrix.sh

harness-transport-matrix: $(RELEASE_BIN) $(RELEASE_HARNESS) ## FULL fault-injection matrix once per FUSE transport (plan 38 §6; slow)
	TRANSPORTS="$(TRANSPORTS)" SCENARIOS=all tests/transport-matrix.sh

build-uring: ## Release binaries with the FUSE-over-io_uring transport built in (plan 38 §3(a))
	$(MAKE) build-release CARGO_FEATURES=constellation-frontend-fuse/io-uring

compliance-uring: ## pjdfstest in a container whose binary has the ring and whose seccomp permits io_uring (plan 38 §6)
	docker compose --profile test-uring build compliance-uring
	docker compose --profile test-uring run --rm compliance-uring

uploadbench-build: ## Build the adaptive-upload-concurrency benchmark
	$(CARGO) build -p uploadbench --release

uploadbench-sim: uploadbench-build ## Compare concurrency controllers against a deterministic simulated network (no AWS needed)
	$(UPLOADBENCH) sim --controllers fixed,aimd,pid \
		--duration-secs 90 --fault-start-secs 30 --fault-duration-secs 15 --fault-error-rate 0.7 \
		--csv /tmp/uploadbench-sim.csv

uploadbench-live: uploadbench-build ## Compare concurrency controllers against a real S3 bucket (needs BUCKET in local.mk + AWS creds)
	@if [ -z "$(BUCKET)" ]; then \
		echo "BUCKET is not set. Copy local.mk.example to local.mk and set BUCKET, or pass BUCKET=s3://... on the command line." >&2; \
		exit 1; \
	fi
	@BUCKET="$(BUCKET)" $(UPLOADBENCH) live --controllers "$(UPLOADBENCH_LIVE_CONTROLLERS)" \
		--duration-secs "$(UPLOADBENCH_LIVE_DURATION)" \
		--initial-concurrency "$(UPLOADBENCH_INITIAL_CONCURRENCY)" \
		--csv /tmp/uploadbench-live.csv

xfstests: ## Run generic xfstests lane in Docker
	docker compose --profile test build xfstests
	docker compose --profile test run --rm xfstests

dist-linux: ## Build static x86_64 Linux musl release archive
	docker build -f tests/docker/Dockerfile.dist --target export \
		--build-arg CONSTELLATION_GIT_DESCRIBE="$$(git describe --tags --always --dirty)" \
		--output type=local,dest=target/musl-out .
	@version=`target/musl-out/constellation --version | awk '{print $$2}'`; \
	name="constellation-$$version-x86_64-linux-musl"; \
	rm -rf "target/dist/$$name"; mkdir -p "target/dist/$$name"; \
	cp target/musl-out/constellation LICENSE README.md "target/dist/$$name/"; \
	tar -C target/dist -czf "target/dist/$$name.tar.gz" "$$name"; \
	echo "target/dist/$$name.tar.gz"

dist-macos: ## Build native macOS release archive
	@[ "$$(uname -s)" = Darwin ] || { echo "dist-macos must run on macOS"; exit 2; }
	$(CARGO) build --release -p constellation
	@version=`target/release/constellation --version | awk '{print $$2}'`; \
	arch=`uname -m`; name="constellation-$$version-$$arch-macos"; \
	rm -rf "target/dist/$$name"; mkdir -p "target/dist/$$name"; \
	cp target/release/constellation LICENSE README.md "target/dist/$$name/"; \
	tar -C target/dist -czf "target/dist/$$name.tar.gz" "$$name"; \
	echo "target/dist/$$name.tar.gz"

deps: ## Install host tools for the non-docker lanes (Debian/Ubuntu)
	# Only docker is required for `make compose` and `make harness-docker`;
	# these are for the faster host lanes (`make smoke`, `make harness`).
	sudo apt-get update
	sudo apt-get install -y fuse3 fio stress-ng docker.io docker-compose-v2
