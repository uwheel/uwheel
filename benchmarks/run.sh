#!/bin/bash

mkdir -p results

if [[ "$OSTYPE" == "darwin"* ]]; then
    export LIBRARY_PATH=$LIBRARY_PATH:$(brew --prefix)/opt/openblas/lib:$(brew --prefix)/opt/lapack/lib
fi

echo "Starting TopN experiment" 
touch results/top_n.log
cd olap && cargo run --release --bin top_n >> ../results/top_n.log
echo "Finished TopN experiment (1/5)" 

echo "Starting Synthetic Window Insert experiment" 
touch results/synthetic_window_insert.log
cd window && cargo run --release --bin synthetic -- insert >> ../results/synthetic_window_insert.log
echo "Finished Synthetic Window Insert experiment (2/5)" 

echo "Starting Synthetic Window Computation experiment" 
touch results/synthetic_window_computation.log
cd window && cargo run --release --bin synthetic -- computation >> ../results/synthetic_window_computation.log
echo "Finished Synthetic Window Computation experiment (3/5)" 