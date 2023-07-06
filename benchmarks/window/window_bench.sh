#!/bin/bash

range_sizes=(30 60 3600)

for range in "${range_sizes[@]}"
do
  echo "Running window bench with range $range and slide 10s"
  cargo run --release --bin synthetic -- -r $range
done