# build-rust.sh

#!/bin/bash

set -e

THISDIR=$(dirname $0)
cd $THISDIR

export SWIFT_BRIDGE_OUT_DIR="$(pwd)/crates/dt-swift/generated"
export RUSTFLAGS=""
# Build the project for the desired platforms:
cargo build --target x86_64-apple-darwin -p dt-swift
cargo build --target aarch64-apple-darwin -p dt-swift
mkdir -p ./target/universal-macos/debug

lipo \
    ./target/aarch64-apple-darwin/debug/libdt_swift.a \
    ./target/x86_64-apple-darwin/debug/libdt_swift.a \
    -create -output ./target/universal-macos/debug/libdt_swift.a

cargo build --target aarch64-apple-ios -p dt-swift
#cargo build --target x86_64-apple-ios
cargo build --target aarch64-apple-ios-sim -p dt-swift
mkdir -p ./target/universal-ios/debug

#lipo \
#    ./target/aarch64-apple-ios-sim/debug/libdt_swift.a \
#    -create -output ./target/universal-ios/debug/libdt_swift.a
#    ./target/aarch64-apple-ios/debug/libdt_swift.a \

swift-bridge-cli create-package \
  --bridges-dir "$SWIFT_BRIDGE_OUT_DIR" \
  --out-dir target/dt-swift \
  --ios ./target/aarch64-apple-ios/debug/libdt_swift.a \
  --simulator ./target/aarch64-apple-ios-sim/debug/libdt_swift.a \
  --macos ./target/universal-macos/debug/libdt_swift.a \
  --name DiamondTypes


#--simulator target/universal-ios/debug/libdt_swift.a \