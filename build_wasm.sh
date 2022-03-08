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
# sed -i '3i\ \ "type": "module",' pkg/package.json
sed -i.old 's/"module":/"main":/' pkg/package.json
sed -i.old 's/: "0.1.0"/: "0.1.1"/' pkg/package.json
perl -wlpi -e 'print "  \"type\": \"module\"," if $. == 2' pkg/package.json

rm pkg/package.json.old


brotli -f pkg/*.wasm
ls -l pkg
