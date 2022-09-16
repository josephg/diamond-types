#!/usr/bin/env bash
set -e
set -o xtrace

cargo criterion -p diamond-types-old --no-run
sleep 5
taskset 0x1 nice -10 cargo criterion -p diamond-types-old -- $@
