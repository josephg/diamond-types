<script lang="ts">
import type { Doc } from "diamond-wasm";
import type { EnhancedEntries } from "./utils";

export let doc: Doc

export let entries: EnhancedEntries[]

export let rleItems: boolean = true

type TabName = 'info' | 'space' | 'time-crdt' | 'time-pos'

// let tabName: TabName = 'time-crdt'
// let tabName: TabName = 'info'
let tabName: TabName = 'space'

const setTab = (name: TabName) => (e: MouseEvent) => {
  tabName = name
  e.preventDefault()
}

const bgOrderColor = (order: number | string) => (
  order === 'ROOT'
    ? 'black'
    : entries.find(e => e.order <= order && order < (e.order + e.len))?.color ?? 'black'
)

const fgOrderColor = (order: number | string) => (
  order === 'ROOT' ? 'white' : 'black'
)

$: {
  console.log('rle info', rleItems);
}


// $: console.log(doc.get_internal_list_entries())

// let x = doc.get_txn_since

</script>


<!-- <pre class="info"></pre> -->
<div class='component'>
  <div class=nav>
    <a href='/' class:enabled={tabName == 'info'} on:click={setTab('info')}>Info</a>
    <a href='/' class:enabled={tabName == 'space'} on:click={setTab('space')}>Space</a>
    <a href='/' class:enabled={tabName == 'time-crdt'} on:click={setTab('time-crdt')}>Time (CRDT)</a>
    <a href='/' class:enabled={tabName == 'time-pos'} on:click={setTab('time-pos')}>Time (Pos)</a>
  </div>

  <div class=content>
    {#if tabName === 'info'}
      <ul class="infoTab">
        <li class=item>
          <span>Next order:</span>
          <span>{doc.get_next_order()}</span>
        </li>
        <li class=item>
          <span>Vector clock:</span>
          <div class=clocks>
            {#each doc.get_vector_clock() as clock}
              <div>{JSON.stringify(clock)}</div>
            {:else}
              <div>(implicitly 0 everywhere)</div>
            {/each}
          </div>
        </li>
        <li class=item>
          <span>Frontier:</span>
          <div class=clocks>
            {#each doc.get_frontier() as clock}
              <div>{JSON.stringify(clock)}</div>
            {/each}
          </div>
        </li>
      </ul>
    {:else if tabName === 'space'}
      {#if entries.length > 0}
        <div class="options">
          <label>
            <input type=checkbox bind:checked={rleItems}>
            RLE items
          </label>
        </div>
      {/if}
      <div class="spaceEntries">
        {#each entries as entry (entry.order)}
          <div style="background: {entry.color};">
            <div>Order: {entry.len > 1 ? `${entry.order} - ${entry.order + entry.len - 1}` : entry.order}</div>


            <div>Origin left: <span
              class=origin
              style="background-color: {bgOrderColor(entry.originLeft)}; color: {fgOrderColor(entry.originLeft)};"
            >{entry.originLeft}</span></div>

            {#if entry.isDeleted}
              <div>DELETED</div>
            {:else}
              <div>Content: <span style="font-family: monospace;">{JSON.stringify(entry.content)}</span></div>
            {/if}

            <div>Origin right: <span
              class=origin
              style="background-color: {bgOrderColor(entry.originRight)}; color: {fgOrderColor(entry.originRight)};"
            >{entry.originRight}</span></div>
          </div>
        {:else}
          <div style="color: white; font-style: italic;">Type something in the box above!</div>
        {/each}
      </div>

    {:else if tabName === 'time-crdt'}
      <pre>
        {JSON.stringify(doc.get_txn_since([]), null, 2)}
      </pre>
    {:else if tabName === 'time-pos'}
      <pre>
        {JSON.stringify(doc.as_positional_patch(), null, 2)}
      </pre>
    {/if}

  </div>

</div>

<style>


.component {
  /* background-color: rgb(0, 140, 255); */
  background-color: #222;
  border-top: 10px solid #444;
  margin: 0;
  /* padding: 5px; */

  grid-row: 2;
  /* overflow: clip; */
  color: white;

  display: flex;
  flex-direction: column;
}


/***** Nav bar *****/
.nav {
  background-color: black;
  color: white;
  width: 100%;
  /* line-height: 2; */
  padding: 5px;
}

.nav > * {
  padding: 5px 10px;
}

.enabled {
  color: red;
  /* background-color: #222; */
}

a {
  color: inherit;
  text-decoration: none;
  font-weight: bolder;
}


/***** Tab content container *****/
.content {
  padding: 5px;
  line-height: 1.4;
  height: 100%;
  overflow-y: auto;
  position: relative;
}

.content > * {
  margin: 5px;
}

.options {
  position: absolute;
  right: 0;
  padding: 10px;
  border-radius: 5px;
  background-color: #222;
}


/***** Info tab *****/

.infoTab {
  /* background-color: tomato; */
  height: 100%;
}

.item {
  margin: 10px 0;
}
.item > :first-child {
  font-weight: bold;
  /* background-color: tomato; */
  min-width: 50%;
}
.item > :nth-child(2) {
  /* font-weight: bold; */
  /* background-color: green; */
  font-family: monospace;
}

.clocks {
  margin-left: 20px;
}



/***** Space entries *****/

.spaceEntries {
  color: black;
  margin: 5px;
}

.spaceEntries > * {
  margin: 10px 0;
  padding: 5px;
}

.spaceEntries > * > :not(:first-child) {
  padding-left: 20px;
}

.origin {
  font-family: monospace;
  padding: 0 3px;
}


</style>