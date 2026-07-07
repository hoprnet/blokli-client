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
quick: fmt clippy check

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
