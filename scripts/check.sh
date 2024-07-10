#!/usr/bin/env bash
set -eux

# inspired by egui's check.sh file.

cargo +stable install cargo-hack --locked

# no_std check
rustup target add thumbv7m-none-eabi
# wasm check
rustup target add wasm32-unknown-unknown

cargo check -p uwheel --lib --target thumbv7m-none-eabi --no-default-features
cargo hack check --all
# cargo check -p uwheel-demo --lib --target wasm32-unknown-unknown
cargo fmt --all -- --check
cargo hack clippy --workspace --all-targets --  -D warnings -W clippy::all
cargo hack test --workspace
# cargo test --workspace --doc

(cd crates/uwheel && cargo check --features "top_n")
(cd crates/uwheel && cargo check --features "sync")
(cd crates/uwheel && cargo check --features "serde")
(cd crates/uwheel && cargo check --features "simd")
(cd crates/uwheel && cargo check --features "sync, serde")
(cd crates/uwheel && cargo check --features "sync, timer")
(cd crates/uwheel && cargo check --features "sync, timer, serde")
(cd crates/uwheel && cargo check --features "timer, serde")
(cd crates/uwheel && cargo check --features "sync, profiler")
(cd crates/uwheel/fuzz && cargo check)
