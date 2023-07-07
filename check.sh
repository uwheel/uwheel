#!/usr/bin/env bash
set -eux

# inspired by egui's check.sh file.

# no_std check
rustup target add thumbv7m-none-eabi
# wasm check
rustup target add wasm32-unknown-unknown

cargo check -p haw  --lib --target thumbv7m-none-eabi --no-default-features
cargo check -p haw_demo  --target wasm32-unknown-unknown
cargo check --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --  -D warnings -W clippy::all
cargo test --workspace --doc

(cd crates/haw && cargo test --features "sync")

(cd crates/haw && cargo check --features "rkyv")
(cd crates/haw && cargo check --features "top_k")
(cd crates/haw && cargo check --features "map")
(cd crates/haw && cargo check --features "sync")

(cd crates/haw/fuzz && cargo check)