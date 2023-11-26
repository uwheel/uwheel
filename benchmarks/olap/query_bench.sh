#!/bin/bash

# echo "Running query bench with events per sec $batch_size"
# cargo run --release --bin query_bench --  --queries 10000

# run with SIMD support for wheeldb
#RUSTFLAGS='-C target-cpu=native' cargo run --release --features "simd" --bin query_bench --  --queries 10000

# for debugging results
cargo run --release --features "debug" --bin query_bench -- --queries 1
