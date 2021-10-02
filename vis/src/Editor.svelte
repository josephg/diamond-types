<script lang="ts">
import {calcDiff, EnhancedEntries, getEnhancedEntries} from './utils'
import {createEventDispatcher, onMount} from 'svelte'
import { Doc } from 'diamond-wasm';
import DocInfo from './DocInfo.svelte';

export let name: string

let textarea: HTMLTextAreaElement

export let doc = new Doc(name)

let lastValue = ''
let value = ''

export const refresh = (newDoc: Doc = doc) => {
  doc = newDoc
  let newval = doc.get()
  if (newval != value) {
    lastValue = value = newval
  }
}

export const reset = () => {
  doc = new Doc(name)
  lastValue = value = ''
}

export const replicateFrom = (src: Doc) => {
  let v = doc.get_vector_clock()
  let changes = src.get_txn_since(v)
  doc.merge_remote_txns(changes)
  refresh()
}

export const getDoc = () => doc

const dispatch = createEventDispatcher()

interface Ctx {
  prevvalue: string
}

$: {
  if (value != lastValue) {
    let vClean = value.replace(/\r\n/g, '\n')
    let diff = calcDiff(lastValue, vClean)
    lastValue = vClean

    if (diff.del != 0) {
      doc.del(diff.pos, diff.del)
    }
    if (diff.ins != '') {
      doc.ins(diff.pos, diff.ins)
    }
    // console.log(JSON.stringify(diff))

    refresh()
  }
}

let rleItems: boolean = true
let entries: EnhancedEntries[]

$: {
  console.log('rle editor', rleItems);
}

$: {
  entries = getEnhancedEntries(value, rleItems, doc)
  // console.log(JSON.stringify(entries))
}

</script>

<div class="container">
  <div class="mirror">
    {#each entries as entry (entry.order)}
      {#if !entry.isDeleted}
        <span style="background: {entry.color};">{entry.content}</span>
      {/if}
    {/each}
  </div>
  <textarea
    bind:this={textarea}
    bind:value={value}
    spellcheck="false"
    placeholder="User {name} (type here)"
  ></textarea>
</div>
<DocInfo {doc} {entries} bind:rleItems={rleItems} />

<style>

.container {
  position: relative;
  margin: 3px;

  font: 20px monospace;
}

textarea {
  position: absolute;

  margin: 0;
  border: 0;
  width: 100%;
  height: 100%;
  font: inherit;
  padding: 0;


  grid-row: 1;

  background-color: transparent;
  border-radius: 5px;
}

.mirror {
  position: absolute;

  margin: 0;
  border: 0;
  width: 100%;
  height: 100%;

  background-color: white;
  color: transparent;

  white-space: pre-wrap;
  word-wrap: break-word;
  border-radius: 5px;
}

.mirror > span {
  border-radius: 5px;
}

</style>