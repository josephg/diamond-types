RUSTFLAGS="" npx snowpack build
cp build/dist/diamond-wasm/diamond_wasm_bg.wasm build/dist/index_bg.wasm
brotli build/dist/index_bg.wasm
scp -r build/* tvbox:public/diamond-vis/
