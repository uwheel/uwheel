#!/bin/bash

# Low load
# cargo run --release --features "profiler" --bin analytical --  --queries 20000 --events-per-sec 1
# RUSTFLAGS='-C target-cpu=native' cargo run --release --features "hints" --bin analytical --  --queries 20000 --events-per-sec 1
# RUSTFLAGS='-C target-cpu=native' cargo run --release  --bin top_n --  --queries 20000 --events-per-sec 10
RUSTFLAGS='-C target-cpu=native' cargo run --release  --bin top_n --  --queries 20000 --events-per-sec 10

# High load
# cargo run --release --bin query_bench --  --queries 20000 --events-per-sec 20

# run with SIMD support for wheeldb
#RUSTFLAGS='-C target-cpu=native' cargo run --release --features "simd" --bin query_bench --  --queries 10000

# for debugging results
# cargo run --release --features "debug" --bin analytical -- --queries 2
