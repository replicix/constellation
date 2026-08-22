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
TARGET_DIR ?= target
RELEASE_BIN := $(TARGET_DIR)/release/constellation
RELEASE_HARNESS := $(TARGET_DIR)/release/harness
DEBUG_BIN := $(TARGET_DIR)/debug/constellation
DEBUG_HARNESS := $(TARGET_DIR)/debug/harness

export CONSTELLATION_BIN ?= $(abspath $(RELEASE_BIN))
export RUSTFLAGS ?=
export CARGO_TERM_COLOR ?= always

# Containerized suite selection (smoke compliance stress).
COMPOSE_SUITES ?=
HARNESS_SCENARIOS ?=
HARNESS_SEED ?= 42
BENCH_FILES ?= 20000

.PHONY: help build build-release build-debug test test-unit fmt fmt-check clippy lint \
	check ci clean smoke integration compose compose-down harness harness-list bench deps

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make [target]\n\nTargets:\n"} \
		/^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo
	@echo "Variables:"
	@echo "  COMPOSE_SUITES=\"...\"   suites for compose (default: all)"
	@echo "  HARNESS_SCENARIOS=\"..\" scenario names (default: all)"
	@echo "  HARNESS_SEED=$(HARNESS_SEED)         harness workload seed"
	@echo "  BENCH_FILES=$(BENCH_FILES)       files for harness bench"

build: build-release ## Build constellation + harness (release)

build-release: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Build release binaries

build-debug: $(DEBUG_BIN) $(DEBUG_HARNESS) ## Build debug binaries

$(RELEASE_BIN) $(RELEASE_HARNESS):
	$(CARGO) build --release -p constellation -p constellation-harness

$(DEBUG_BIN) $(DEBUG_HARNESS):
	$(CARGO) build -p constellation -p constellation-harness

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

smoke: $(RELEASE_BIN) ## Host smoke test (local file backend; needs fuse3)
	tests/smoke.sh

integration: $(RELEASE_BIN) ## Host integration (floci S3 in docker)
	tests/integration.sh

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

bench: $(RELEASE_BIN) $(RELEASE_HARNESS) ## Census-scale import benchmark (needs docker + fuse3)
	$(RELEASE_HARNESS) bench --files $(BENCH_FILES)

deps: ## Install host tools (Debian/Ubuntu): fuse3, fio, stress-ng, docker
	sudo apt-get update
	sudo apt-get install -y fuse3 fio stress-ng docker.io docker-compose-v2
