# build-rust.sh

#!/bin/bash

set -e

THISDIR=$(dirname $0)
cd $THISDIR

#FLAGS=""
#MODE="debug"
FLAGS="--release"
MODE="release"

export SWIFT_BRIDGE_OUT_DIR="$(pwd)/crates/dt-swift/generated"
export RUSTFLAGS=""
# Build the project for the desired platforms:
cargo build $FLAGS --target x86_64-apple-darwin -p dt-swift
cargo build $FLAGS --target aarch64-apple-darwin -p dt-swift
mkdir -p ./target/universal-macos/"$MODE"

lipo \
    ./target/aarch64-apple-darwin/"$MODE"/libdt_swift.a \
    ./target/x86_64-apple-darwin/"$MODE"/libdt_swift.a \
    -create -output ./target/universal-macos/"$MODE"/libdt_swift.a

cargo build $FLAGS --target aarch64-apple-ios -p dt-swift
#cargo build --target x86_64-apple-ios
cargo build $FLAGS --target aarch64-apple-ios-sim -p dt-swift
mkdir -p ./target/universal-ios/"$MODE"

#lipo \
#    ./target/aarch64-apple-ios-sim/"$MODE"/libdt_swift.a \
#    -create -output ./target/universal-ios/"$MODE"/libdt_swift.a
#    ./target/aarch64-apple-ios/"$MODE"/libdt_swift.a \

swift-bridge-cli create-package \
  --bridges-dir "$SWIFT_BRIDGE_OUT_DIR" \
  --out-dir target/dt-swift \
  --ios ./target/aarch64-apple-ios/"$MODE"/libdt_swift.a \
  --simulator ./target/aarch64-apple-ios-sim/"$MODE"/libdt_swift.a \
  --macos ./target/universal-macos/"$MODE"/libdt_swift.a \
  --name DiamondTypes


#--simulator target/universal-ios/"$MODE"/libdt_swift.a \