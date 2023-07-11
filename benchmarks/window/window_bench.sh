#!/bin/bash

range_sizes=(30 60)

for range in "${range_sizes[@]}"
do
  echo "Running window bench with range $range and slide 10s and 1000 events per sec"
  cargo run --release --bin synthetic -- -r $range -e 1000
done

for range in "${range_sizes[@]}"
do
  echo "Running window bench with range $range and slide 10s and 10000 events per sec"
  cargo run --release --bin synthetic -- -r $range -e 10000
done