#!/bin/bash

threads=(1 4 8 16)

for threads in "${threads[@]}"
do
  cargo run --release --bin read_write -- read-only -t $threads
  cargo run --release --bin read_write -- read-write -t $threads
done