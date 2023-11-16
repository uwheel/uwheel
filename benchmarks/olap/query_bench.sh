#!/bin/bash

# batch_sizes=(1)

echo "Running query bench with events per sec $batch_size"
RUSTFLAGS='-C target-cpu=native' cargo run --release --bin query_bench -- --events-per-sec 10
# for batch_size in "${batch_sizes[@]}"
# do
#   echo "Running query bench with events per sec $batch_size"
#   RUSTFLAGS='-C target-cpu=native' cargo run --release --bin query_bench -- --events-per-sec $batch_size
#   # cargo run --release --bin query_bench -- sum --events-per-min $batch_size
# done