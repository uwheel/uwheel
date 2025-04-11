# =============================================================================
# Justfile - Development Build & Test Commands
# =============================================================================
#
# Install Just command runner:    cargo install just
# Install dev dependencies:       just install
# List available commands:        just -l
#
# Reference documentation: https://github.com/casey/just
# =============================================================================

features := ""

_features := if features == "all" {
        "--all-features"
    } else if features != "" {
        "--features=" + features
    } else { "" }

# Installs required dev tools
install:
    cargo install --locked cargo-nextest

# Cleans everything through cargo clean
clean:
    cargo clean

# Runs cargo fmt
fmt:
    cargo fmt --all

# Checks for rust fmt issues
check-fmt:
    cargo fmt --all -- --check

# Runs clippy checks across the workspace
clippy:
    cargo clippy --all-targets -- -D warnings

check-wasm:
    rustup target add wasm32-unknown-unknown
    cargo check -p uwheel-demo --target wasm32-unknown-unknown 
check-no-std:
    rustup target add thumbv7m-none-eabi
    cargo check -p uwheel --lib --target thumbv7m-none-eabi --no-default-features

check-fuzz:
    (cd crates/uwheel/fuzz && cargo check)

query-test:
    ./scripts/query_tests.sh

# Run all lints
lint: check-fmt clippy

# Runs workspace tests using nextest
test:
    cargo nextest run {{ _features }}

test-sync:
    cargo nextest run -p uwheel --features "sync"

test-serde:
    cargo nextest run -p uwheel --features "serde"

test-doc:
    cargo test --workspace --doc

# Runs a local CI check (enables --all-features)
ci: 
    just lint test test-doc test-sync test-serde check-wasm check-fuzz check-no-std query-test
