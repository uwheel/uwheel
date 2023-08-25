#!/usr/bin/env bash

# Credit: taken and modified from the egui repo (https://github.com/emilk/egui/tree/master/scripts)

set -eu
script_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$script_path/.."

# Starts a local web-server that serves the contents of the `doc/` folder,
# i.e. the web-version of `awheel-demo`.

PORT=8888

echo "ensuring basic-http-server is installed…"
cargo install basic-http-server

echo "starting server…"
echo "serving at http://localhost:${PORT}"

(cd docs && basic-http-server --addr 0.0.0.0:${PORT} .)
