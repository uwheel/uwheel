#!/usr/bin/env bash
set -eux

file_path="crates/uwheel-query-tests/yellow_tripdata_2022-01.parquet"

if [ ! -e "$file_path" ]; then
  # If the file does not exist, run the script in that directory
  (cd crates/uwheel-query-tests && ./fetch_data.sh)
fi

(cd crates/uwheel-query-tests && cargo run --release -- --queries 1000)
