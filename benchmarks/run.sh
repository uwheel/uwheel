#!/bin/bash

mkdir -p results

FILE=data/citibike-tripdata.csv
if [ -f "$FILE" ]; then
    echo "NYC Citi Bike Data found"
else
    echo "Downloading and preparing NYC Citi Bike data"
    ./fetch_data.sh
fi

# TODO: Download DEBS12 data

echo "Starting NYC Citi Bike Window Small Range experiment (1/6)"
touch results/nyc_citi_bike_window_small_range.log
(cd window && cargo run --release --bin real -- citi-bike small-range >> ../results/nyc_citi_bike_window_small_range.log )
echo "Finished NYC Citi Bike Window Small Range experiment (1/6)"

echo "Starting NYC Citi Bike Window Big Range experiment (2/6)"
touch results/nyc_citi_bike_window_big_range.log
(cd window && cargo run --release --bin real -- citi-bike big-range >> ../results/nyc_citi_bike_window_big_range.log )
echo "Finished NYC Citi Bike Window Big Range experiment (2/6)"

echo "Starting DEBS12 Window Small Range experiment (3/6)"
touch results/debs12_window_small_range.log
(cd window && cargo run --release --bin real -- debs12 small-range >> ../results/debs12_window_small_range.log )
echo "Finished DEBS12 Window Small Range experiment (3/6)"

echo "Starting DEBS12 Window Big Range experiment (4/6)"
touch results/debs12_window_big_range.log
(cd window && cargo run --release --bin real -- debs12 big-range >> ../results/debs12_window_big_range.log )
echo "Finished DEBS12 Window Big Range experiment (4/6)"

echo "Starting Analytical Benchmark (5/5)"
touch results/analytical_bench.log
(cd window && RUSTFLAGS='-C target-cpu=native' cargo run --release  --bin analytical --  --queries 20000 --events-per-sec >> ../results/analytical_bench.log )
echo "Finished Analytical Benchmark (5/5)"
