#!/bin/bash

# Low load
cargo run --release  --bin query_bench --  --queries 20000 --events-per-sec 1

# High load
cargo run --release --bin query_bench --  --queries 20000 --events-per-sec 50

# run with SIMD support for wheeldb
#RUSTFLAGS='-C target-cpu=native' cargo run --release --features "simd" --bin query_bench --  --queries 10000

# for debugging results
# cargo run --release --features "debug" --bin query_bench -- --queries 1
