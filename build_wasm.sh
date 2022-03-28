set -e

RUSTFLAGS=""
#cd crates/diamond-wasm

echo "=== Before ==="
ls -l pkg-web pkg-node || true
echo "=== After ==="
#wasm-pack build --target nodejs
#wasm-pack build --target bundler
#wasm-pack build --target web --dev

rm -rf pkg-*
wasm-pack build --target web --out-dir ../../pkg-web --out-name dt crates/diamond-wasm
wasm-pack build --target nodejs --out-dir ../../pkg-node --out-name dt crates/diamond-wasm

# sed -i '3i\ \ "type": "module",' pkg/package.json

# Set version
#sed -i.old 's/: "0.1.0"/: "0.1.1"/' pkg-*/package.json

# Web code needs to have "main" defined since its an es6 module package
sed -i.old 's/"module":/"main":/' pkg-web/package.json
sed -i.old 's/"name": "diamond-wasm"/"name": "diamond-types-web"/' pkg-web/package.json
sed -i.old 's/"name": "diamond-wasm"/"name": "diamond-types-node"/' pkg-node/package.json
sed -i.old 's/"files": \[/"files": \[\n    "dt_bg.wasm.br",/' pkg-web/package.json
perl -wlpi -e 'print "  \"type\": \"module\"," if $. == 2' pkg-web/package.json

rm pkg-*/package.json.old

brotli -f pkg-web/*.wasm
ls -l pkg-web pkg-node

cat pkg-web/package.json