#!/bin/bash

git submodule update --init --recursive
cd mimalloc
mkdir -p out/release
cd out/release
cmake ../..
make

