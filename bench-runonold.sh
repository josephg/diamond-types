#!/usr/bin/env bash
set -e
#set -o xtrace

start_time=$(date +%s)   # Capture start time in seconds

cargo build --release -p run_on_old --features bench

end_time=$(date +%s)     # Capture end time in seconds
# Calculate duration
duration=$((end_time - start_time))

# Check if duration is less than 1 second
if [ $duration -gt 1 ]; then
  echo "Waiting 5s for CPU to cool down"
  sleep 5
fi

#taskset 0x1 nice -10 cargo run --release -p bench -- --bench $@
taskset 0x1 nice -10 target/release/run_on_old --bench "$@"
