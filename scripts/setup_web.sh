#!/usr/bin/env bash

# Credits: taken from the egui repo (https://github.com/emilk/egui/tree/master/scripts)

set -eu
script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$script_path/.."

# Pre-requisites:
rustup target add wasm32-unknown-unknown

# For generating JS bindings:
cargo install wasm-bindgen-cli --version 0.2.87
