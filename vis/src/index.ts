import App from './App.svelte';
import init from 'diamond-wasm'


// export default app;

;(async () => {
  await init()

  var app = new App({
    target: document.body,
  });

  // Hot Module Replacement (HMR) - Remove this snippet to remove HMR.
  // Learn more: https://www.snowpack.dev/concepts/hot-module-replacement
  if (import.meta.hot) {
    import.meta.hot.accept();
    import.meta.hot.dispose(() => {
      app.$destroy();
    });
  }
})()


