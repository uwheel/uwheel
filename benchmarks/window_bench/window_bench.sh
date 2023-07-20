#!/bin/bash

range_sizes=(30 60 1800 3600 86400 604800 30844800 31449600)

for range in "${range_sizes[@]}"
do
  echo "Running window bench with range $range and slide 10s and 10000 events per sec"
  cargo run --release --bin synthetic -- -r $range -e 10
done