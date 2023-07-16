#!/usr/bin/env bash
set -eux

# inspired by egui's check.sh file.

# no_std check
rustup target add thumbv7m-none-eabi
# wasm check
rustup target add wasm32-unknown-unknown

cargo check -p awheel --lib --target thumbv7m-none-eabi --no-default-features
cargo check -p awheel-demo --target wasm32-unknown-unknown
cargo check --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --  -D warnings -W clippy::all
cargo test --workspace
cargo test --workspace --doc

(cd crates/awheel && cargo test --features "tree, sync")
(cd crates/awheel && cargo check --features "tree")
(cd crates/awheel && cargo check --features "top_n")
(cd crates/awheel && cargo check --features "sync")
(cd crates/awheel/fuzz && cargo check)