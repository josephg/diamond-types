#!/usr/bin/env bash
set -e
set -o xtrace

cargo build --release -p bench
sleep 5
taskset 0x1 nice -10 cargo run --release -p bench -- --bench $@
