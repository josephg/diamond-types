RUSTFLAGS=""
cd crates/diamond-wasm-positional
wasm-pack build --target nodejs && brotli -f pkg/*.wasm && ls -l pkg   
