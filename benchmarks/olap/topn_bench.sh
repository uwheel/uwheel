#!/bin/bash

batch_sizes=(100 1000)

for batch_size in "${batch_sizes[@]}"
do
  echo "Running topn bench with events per min $batch_size"
  cargo run --release --bin top_n -- --events-per-min $batch_size
done