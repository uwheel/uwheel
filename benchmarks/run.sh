#!/bin/bash

mkdir -p results

FILE=data/citibike-tripdata.csv
if [ -f "$FILE" ]; then
    echo "NYC Citi Bike Data found"
else
    echo "Downloading and preparing NYC Citi Bike data"
    ./fetch_data.sh
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
    export LIBRARY_PATH=$LIBRARY_PATH:$(brew --prefix)/opt/openblas/lib:$(brew --prefix)/opt/lapack/lib
fi

echo "Starting NYC Citi Bike Window Sync experiment (1/5)"
touch results/nyc_citi_bike_window_sync.log
(cd window && cargo run --release --bin real --features "sync" >> ../results/nyc_citi_bike_window_sync.log )
echo "Finished NYC Citi Bike Window Sync experiment (1/5)"

echo "Starting NYC Citi Bike Window experiment (2/5)"
touch results/nyc_citi_bike_window.log
(cd window && cargo run --release --bin real >> ../results/nyc_citi_bike_window.log )
echo "Finished NYC Citi Bike Window experiment (2/5)"


echo "Starting Synthetic Window Insert experiment (3/5)"
touch results/synthetic_window_insert.log
(cd window && cargo run --release --bin synthetic -- insert >> ../results/synthetic_window_insert.log )
echo "Finished Synthetic Window Insert experiment (3/5)"

echo "Starting Synthetic Window Computation experiment (4/5)"
touch results/synthetic_window_computation.log
(cd window && cargo run --release --bin synthetic -- computation >> ../results/synthetic_window_computation.log )
echo "Finished Synthetic Window Computation experiment (4/5)"

echo "Starting TopN experiment (5/5)"
touch results/top_n.log
(cd olap && cargo run --release --bin top_n >> ../results/top_n.log)
echo "Finished TopN experiment (5/5)"
