#!/usr/bin/env bash
set -eux

# inspired by egui's check.sh file.

cargo +stable install cargo-hack --locked

# no_std check
rustup target add thumbv7m-none-eabi
# wasm check
rustup target add wasm32-unknown-unknown

cargo check -p awheel --lib --target thumbv7m-none-eabi --no-default-features
cargo hack check --all
cargo check -p awheel-demo --lib --target wasm32-unknown-unknown
cargo fmt --all -- --check
cargo hack clippy --workspace --all-targets --  -D warnings -W clippy::all
cargo hack test --workspace
cargo test --workspace --doc

(cd crates/awheel && cargo test --features "tree, sync")
(cd crates/awheel && cargo check --features "tree")
(cd crates/awheel && cargo check --features "top_n")
(cd crates/awheel && cargo check --features "sync")
(cd crates/awheel && cargo check --features "serde")
(cd crates/awheel && cargo check --features "sync, serde")
(cd crates/awheel && cargo check --features "sync, profiler")
(cd crates/awheel && cargo check --features "window, serde")
(cd crates/awheel/fuzz && cargo check)