import type { Doc } from "../../crates/diamond-wasm/pkg/diamond_wasm";

export type DiffResult = {pos: number, del: number, ins: string}

export const calcDiff = (oldval: string, newval: string): DiffResult => {
  // Strings are immutable and have reference equality. I think this test is O(1), so its worth doing.
  if (oldval === newval) return {pos: 0, del: 0, ins: ''}

  let oldChars = [...oldval]
  let newChars = [...newval]

  var commonStart = 0;
  while (oldChars[commonStart] === newChars[commonStart]) {
    commonStart++;
  }

  var commonEnd = 0;
  while (oldChars[oldChars.length - 1 - commonEnd] === newChars[newChars.length - 1 - commonEnd] &&
      commonEnd + commonStart < oldChars.length && commonEnd + commonStart < newChars.length) {
    commonEnd++;
  }

  const del = (oldChars.length !== commonStart + commonEnd)
    ? oldChars.length - commonStart - commonEnd
    : 0
  const ins = (newChars.length !== commonStart + commonEnd)
    ? newChars.slice(commonStart, newChars.length - commonEnd).join('')
    : ''

  return {
    pos: commonStart, del, ins
  }
};


// let colors = ['red', 'green', 'yellow', 'cyan', 'hotpink']
// let colors = [
//   '#f94144',
//   '#f3722c',
//   '#f8961e',
//   '#f9844a',
//   '#f9c74f',
//   '#90be6d',
//   '#43aa8b',
//   '#4d908e',
//   '#577590',
//   '#277da1',
// ]

let colors = [
  "#9b5de5",
  "#f15bb5",
  "#fee440",
  "#00bbf9",
  "#00f5d4",
]

let colorsFaint = colors.map(c => c + '33')

export type YjsEntry = {
  len: number,
  order: number,
  origin_left: number,
  origin_right: number
}

export type EnhancedEntries = YjsEntry & {
  order: number,
  content: string,
  len: number,
  color: string,
  isDeleted: boolean,
  originLeft: number | string,
  originRight: number | string,
}

export const getEnhancedEntries = (value: string, doc: Doc) => {
  let str = [...value]
  return (doc.get_internal_list_entries() as YjsEntry[])
    // .filter(e => e.len > 0)
    .map((e, x) => ({
      ...e,
      len: Math.abs(e.len),
      isDeleted: e.len < 0,
      content: e.len > 0 ? str.splice(0, e.len).join('') : '',
      // color: `radial-gradient(${colors[x % colors.length]}, transparent)`,
      // color: `radial-gradient(ellipse at top, ${colors[x % colors.length]}, transparent), radial-gradient(ellipse at bottom, ${colorsFaint[x % colors.length]}, transparent)`,
      // color: `radial-gradient(ellipse at top, red, transparent), radial-gradient(ellipse at bottom, blue, transparent)`,
      // color: `radial-gradient(circle, ${colors[x % colors.length]} 90%, ${colorsFaint[x % colors.length]} 100%)`,
      color: colors[x % colors.length],
      originLeft: e.origin_left == 4294967295 ? 'ROOT' : e.origin_left,
      originRight: e.origin_right == 4294967295 ? 'ROOT' : e.origin_right,
    }))
}