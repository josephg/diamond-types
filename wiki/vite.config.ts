import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

export default defineConfig({
  server: {
    // https: true,
    fs: {
      allow: ['../..'],
    },
    proxy: {
      '/api': {
        target: 'http://localhost:4321',
        ws: true,
      }
    }
  },
  plugins: [solidPlugin()],
  build: {
    minify: true,
    sourcemap: true,
    outDir: 'dist-client',
    target: 'esnext',
    // polyfillDynamicImport: false,
  },
});
