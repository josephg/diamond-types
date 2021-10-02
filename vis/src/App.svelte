<script lang="ts">
  import {onMount} from 'svelte'
  import Editor from './Editor.svelte'
  import type {Doc} from 'diamond-wasm'

  let e1: Editor, e2: Editor

  const reset = () => {
    e1.reset()
    e2.reset()
  }

  const replicate = (from: Editor, to: Editor) => () => {
    to.replicateFrom(from.getDoc())
  }

  onMount(() => {


    return () => {
    }
  })
</script>

<div id="app">
  <Editor name="one" bind:this={e1} />
  <Editor name="two" bind:this={e2} />
  <!-- <textarea id="t1" spellcheck="false" autofocus placeholder="User one (type here)"></textarea>
  <textarea id="t2" spellcheck="false" placeholder="User two (type here)"></textarea>
  <pre id="info1" class="info"></pre>
  <pre id="info2" class="info"></pre> -->

  <button id="pushRight" on:click={replicate(e1, e2)}>-&gt;</button>
  <button id="pushLeft" on:click={replicate(e2, e1)}>&lt;-</button>
  <button id="reset" on:click={reset}>Clear</button>
</div>

<style>
  #app {
    display: grid;
    grid-template-rows: minmax(200px, 1fr) minmax(0, 5fr);
    /* grid-template-rows: 200px calc(100% - 200px); */
    grid-template-columns: 50% 50%;
    /* gap: 10px; */
    column-gap: 20px;
    background-color: rgb(15, 23, 31);

    position: fixed;
    top: 0; bottom: 0;
    left: 0; right: 0;

    font-size: 20px;
    height: 100%;
    padding: 10px;

    font-family: sans-serif;
  }


  button {
    /* display: inline-block; */
    position: fixed;
    top: 50px;
    margin: 0 auto;
    left: 50%;
    margin-left: -1em;
    min-width: 2em;
    padding: 0;
    right: 0;
    font-size: 30px;
    padding: 3px;
  }

  #pushRight {
    top: 50px;
  }

  #pushLeft {
    top: 100px;
  }

  #reset {
    top: 170px;
    font-size: 24px;
    width: 3em;
    margin-left: -1.5em;
  }
</style>

