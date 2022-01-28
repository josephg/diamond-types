RUSTFLAGS=""
cd crates/diamond-wasm
wasm-pack build --target nodejs && brotli -f pkg/*.wasm && ls -l pkg   
