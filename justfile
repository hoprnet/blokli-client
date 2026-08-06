# ============================================================================
# Default Command
# ============================================================================

# Show available commands
default:
    @just --list

# ============================================================================
# Quick Workflows
# ============================================================================

# Quick check - format, clippy, and check
quick: fmt clippy check check-versions

# Verify the workspace `blokli-client` dependency matches the client crate version,
# so a published `blokli-inspector` always depends on the matching client release
check-versions:
    #!/usr/bin/env bash
    set -euo pipefail
    client_version="$(grep -m1 -E '^version' client/Cargo.toml | cut -d'"' -f2)"
    dep_version="$(grep -m1 -E '^blokli-client' Cargo.toml | sed -E 's/.*version = "([^"]+)".*/\1/')"
    if [[ "${client_version}" != "${dep_version}" ]]; then
      echo "error: blokli-client version mismatch: client/Cargo.toml is ${client_version}," >&2
      echo "       but [workspace.dependencies] requires ${dep_version}." >&2
      echo "       Update the workspace dependency so the published inspector depends on the matching client." >&2
      exit 1
    fi
    echo "blokli-client version in sync: ${client_version}"

# Development build and test cycle - format, check, and test
dev: fmt check test

# Watch for changes and run client checks continuously
watch:
    cargo watch -x "check --workspace" -x "test -p blokli-client"

# ============================================================================
# Build Commands
# ============================================================================

# Build all workspace packages in debug mode
build:
    cargo build --workspace

# Build all workspace packages in release mode with full optimizations
build-release:
    cargo build --workspace --release

# Check all workspace code without building binaries
check:
    cargo check --workspace

# Clean all build artifacts
clean:
    cargo clean

# ============================================================================
# Test Commands
# ============================================================================

# Run unit tests in the workspace
test:
    cargo test --workspace --exclude blokli-integration-tests --no-fail-fast

# Run tests for a specific package
test-package package:
    cargo test -p {{ package }} --no-fail-fast

# Run tests in single thread mode with output
test-debug:
    cargo test --workspace --exclude blokli-integration-tests -- --test-threads=1 --nocapture

# Run tests for a specific package with execution time reported
test-profile package:
    nix develop .#experiment -c cargo test -p {{ package }} --no-fail-fast -- -Z unstable-options --report-time

# Run all unit tests using nextest
nextest:
    cargo nextest run --workspace --exclude blokli-integration-tests

# Run tests for a specific package using nextest
nextest-package package:
    cargo nextest run -p {{ package }}

# Build and run the archived integration test suite
test-integration:
    nix run -L .#test-integration

# Refresh the Anvil contract addresses pinned in tests/integration/config-integration-anvil.toml
# from hopr-bindings' bundled contracts-addresses.json. Run after bumping hopr-bindings, or
# whenever `cargo test -p blokli-integration-tests config_contract_addresses_match_hopr_bindings` fails.
regen-integration-contracts:
    cargo test -p blokli-integration-tests --lib regenerate_contract_addresses_toml -- --exact --ignored

# ============================================================================
# Code Quality
# ============================================================================

# Format all code with the treefmt wrapper provided by the dev shell
fmt:
    treefmt

# Run clippy lints with warnings as errors
clippy:
    cargo clippy --workspace -- -D warnings

# Run clippy on all non-integration targets
clippy-all:
    cargo clippy --workspace --exclude blokli-integration-tests --all-targets -- -D warnings

# Automatically fix clippy warnings
clippy-fix:
    cargo clippy --workspace --fix --allow-dirty --allow-staged

# ============================================================================
# Run Commands
# ============================================================================

# Run the Blokli inspector CLI
run-inspector *args:
    cargo run -p blokli-inspector -- {{ args }}

# ============================================================================
# Documentation
# ============================================================================

# Generate and open documentation for workspace packages only
doc:
    cargo doc --workspace --no-deps --open

# Generate and open documentation including all dependencies
doc-all:
    cargo doc --workspace --open

# ============================================================================
# Dependency Management
# ============================================================================

# Update all dependencies to latest compatible versions
update:
    cargo update

# Show outdated dependencies that have newer versions available
outdated:
    cargo outdated

# Check for unused dependencies
cargo-udeps:
    nix develop .#experiment -c bash -c 'cargo udeps'
