#!/bin/bash

batch_sizes=(100 1000)

for batch_size in "${batch_sizes[@]}"
do
  echo "Running query bench with events per min $batch_size"
  cargo run --release --bin query_bench -- --events-per-min $batch_size
  cargo run --release --bin query_bench -- sum --events-per-min $batch_size
done