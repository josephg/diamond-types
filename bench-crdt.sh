#!/usr/bin/env bash
set -e
#set -o xtrace

start_time=$(date +%s)

cargo bench -p diamond-types-crdt --no-run

end_time=$(date +%s)     # in seconds
duration=$((end_time - start_time))

# Check if duration is less than 1 second
if [ $duration -gt 1 ]; then
  echo "Waiting 5s for CPU to cool down"
  sleep 5
fi

taskset 0x1 nice -10 cargo bench -p diamond-types-crdt -- "$@"
