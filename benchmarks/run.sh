#!/bin/bash

mkdir -p results

FILE=data/citibike-tripdata.csv
DEBS12=data/debs12.csv
if [ -f "$FILE" ] && [ -f "$DEBS12" ]; then
    echo "NYC Citi Bike & DEBS12 Data found"
else
    echo "Downloading and preparing data"
    ./fetch_data.sh
fi

# Sets up mimalloc (git module init + compile)
(cd uwheel_bench && ./setup.sh)

echo "Starting NYC Citi Bike Window Small Range experiment (1/5)"
touch results/nyc_citi_bike_window_small_range.log
(cd uwheel_bench && cargo run --release --bin stream -- citi-bike small-range >> ../results/nyc_citi_bike_window_small_range.log )
echo "Finished NYC Citi Bike Window Small Range experiment (1/5)"

echo "Starting NYC Citi Bike Window Big Range experiment (2/5)"
touch results/nyc_citi_bike_window_big_range.log
(cd uwheel_bench && cargo run --release --bin stream -- citi-bike big-range >> ../results/nyc_citi_bike_window_big_range.log )
echo "Finished NYC Citi Bike Window Big Range experiment (2/5)"

echo "Starting DEBS12 Window Small Range experiment (3/5)"
touch results/debs12_window_small_range.log
(cd uwheel_bench && cargo run --release --bin stream -- debs12 small-range >> ../results/debs12_window_small_range.log )
echo "Finished DEBS12 Window Small Range experiment (3/5)"

echo "Starting DEBS12 Window Big Range experiment (4/5)"
touch results/debs12_window_big_range.log
(cd uwheel_bench && cargo run --release --bin stream -- debs12 big-range >> ../results/debs12_window_big_range.log )
echo "Finished DEBS12 Window Big Range experiment (4/5)"

# Remove old DuckDB files in case they exist
rm -f uwheel_bench/duckdb_ingestion.db uwheel_bench/duckdb_ingestion.db.wal

echo "Starting Analytical Benchmark (5/5)"
touch results/analytical_bench.log
# (cd uwheel_bench && RUSTFLAGS='-C target-cpu=native' cargo run --release  --bin analytical --  --queries 50000 --events-per-sec 1 >> ../results/analytical_bench.log )
(cd uwheel_bench && cargo run --release  --bin analytical --  --queries 50000 --events-per-sec 1 >> ../results/analytical_bench.log )
echo "Finished Analytical Benchmark (5/5)"
