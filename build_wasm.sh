set -e

RUSTFLAGS=""
cd crates/diamond-wasm

echo "=== Before ==="
ls -l pkg
echo "=== After ==="
#wasm-pack build --target nodejs
#wasm-pack build --target bundler
#wasm-pack build --target web --dev
wasm-pack build --target web
sed -i '3i\ \ "type": "module",' pkg/package.json
sed -i 's/"module":/"main":/' pkg/package.json
brotli -f pkg/*.wasm
ls -l pkg
