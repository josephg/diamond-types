
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
}

export type DTOp = {tag: 'Ins' | 'Del', start: number, end: number, fwd?: boolean, content?: string}

export const transformPosition = (cursor: number, {tag, start, end}: DTOp, is_left: boolean = true): number => {
  let len = end - start

  return (cursor < start || (cursor === start && is_left)) ? cursor
    : (tag === 'Ins') ? cursor + len
    : cursor < end ? start
    : cursor - len
}

const test0 = (cursor: number, op: DTOp, is_left: boolean, expected: number) => {
  const actual = transformPosition(cursor, op, is_left)
  if (actual !== expected) {
    console.error('TEST FAILED', cursor, op, 'is_left', is_left)
    console.error('  Expected:', expected, '/ Actual:', actual)
  }
}
const test = (cursor: number, op: DTOp, expectLeft: number, expectRight: number = expectLeft) => {
  test0(cursor, op, true, expectLeft)
  test0(cursor, op, false, expectRight)
}
test(10, {tag: 'Ins', start: 5, end: 9}, 14)
test(10, {tag: 'Ins', start: 8, end: 12}, 14)
test(10, {tag: 'Ins', start: 10, end: 14}, 10, 14) // Different outcome!
test(10, {tag: 'Ins', start: 11, end: 15}, 10)

test(10, {tag: 'Del', start: 5, end: 9}, 6)
test(10, {tag: 'Del', start: 6, end: 10}, 6)
test(10, {tag: 'Del', start: 6, end: 100}, 6)
test(10, {tag: 'Del', start: 60, end: 100}, 10)
test(10, {tag: 'Del', start: 10, end: 100}, 10)


// // This operates in unicode offsets to make it consistent with the equivalent
// // methods in other languages / systems.
// const transformPosition = (cursor: number, op: TextOp) => {
//   let pos = 0

//   for (let i = 0; i < op.length && cursor > pos; i++) {
//     const c = op[i]

//     // I could actually use the op_iter stuff above - but I think its simpler
//     // like this.
//     switch (typeof c) {
//       case 'number': { // skip
//         pos += c
//         break
//       }

//       case 'string': // insert
//         // Its safe to use c.length here because they're both utf16 offsets.
//         // Ignoring pos because the doc doesn't know about the insert yet.
//         const offset = strPosToUni(c)
//         pos += offset
//         cursor += offset
//         break

//       case 'object': // delete
//         cursor -= Math.min(dlen(c.d), cursor - pos)
//         break
//     }
//   }
//   return cursor
// }

// const transformSelection = (selection: number | [number, number], op: TextOp): number | [number, number] => (
//   typeof selection === 'number'
//     ? transformPosition(selection, op)
//     : selection.map(s => transformPosition(s, op)) as [number, number]
// )
