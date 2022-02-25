RUSTFLAGS=""
cd crates/diamond-wasm

echo "=== Before ==="
ls -l pkg
echo "=== After ==="
wasm-pack build --target nodejs && brotli -f pkg/*.wasm && ls -l pkg
