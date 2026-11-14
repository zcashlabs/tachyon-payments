# CI gates for the Tachyon payment-protocol PIR workspace (spec section 4.4).
# The scripts live in tools/ci/ and use the Python 3 standard library only.
#
#   make check-fast [BASE=<commit>]                    affected packages since BASE (all if unset)
#   make check-package PACKAGE=<name> [TEST=<filter>] [FEATURES=<list>]
#   make check-correctness                             warm workspace tests, < 300 s, each test < 30 s
#   make check-full                                    fmt, boundary, workspace clippy and tests
#   make doctor [NETWORK=1]                            prerequisites; NETWORK=1 probes the testnet RPC
#   make fmt                                           format the workspace
#
# OFFLINE=1 passes --offline to cargo (dependencies already fetched).
# DRY_RUN=1 makes check-fast and check-package print their selection and commands only.

# Use rustup's default bin directory when cargo is not already on PATH.
CARGO_BIN := $(or $(CARGO_HOME),$(HOME)/.cargo)/bin
ifeq ($(shell command -v cargo 2>/dev/null),)
ifneq ($(wildcard $(CARGO_BIN)/cargo),)
export PATH := $(CARGO_BIN):$(PATH)
endif
endif

PYTHON ?= python3
CARGO ?= cargo
OFFLINE_FLAG := $(if $(filter 1,$(OFFLINE)),--offline)
DRY_RUN_FLAG := $(if $(filter 1,$(DRY_RUN)),--dry-run)

.PHONY: check check-fast check-package check-correctness check-full check-boundary check-tools doctor fmt

check: check-full

check-fast:
	$(PYTHON) tools/ci/fast.py $(if $(BASE),--base "$(BASE)") $(OFFLINE_FLAG) $(DRY_RUN_FLAG)

check-package:
	@test -n "$(PACKAGE)" || { echo "Set PACKAGE=<workspace package>"; exit 1; }
	$(PYTHON) tools/ci/fast.py --package "$(PACKAGE)" $(if $(TEST),--test "$(TEST)") $(if $(FEATURES),--features "$(FEATURES)") $(OFFLINE_FLAG) $(DRY_RUN_FLAG)

check-correctness:
	$(PYTHON) tools/ci/correctness.py $(OFFLINE_FLAG)

check-full: check-tools
	$(CARGO) fmt --all --check
	$(PYTHON) tools/ci/boundary.py $(OFFLINE_FLAG)
	$(CARGO) clippy --locked $(OFFLINE_FLAG) --workspace --all-targets -- -D warnings
	$(CARGO) test --locked $(OFFLINE_FLAG) --workspace --profile release-fast

check-boundary:
	$(PYTHON) tools/ci/boundary.py $(OFFLINE_FLAG)

# Unit tests of the CI scripts themselves.
check-tools:
	$(PYTHON) -m unittest discover -s tools/ci/tests

doctor:
	$(PYTHON) tools/ci/doctor.py $(if $(filter 1,$(NETWORK)),--network)

fmt:
	$(CARGO) fmt --all
