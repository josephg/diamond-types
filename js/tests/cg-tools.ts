import 'mocha'
import fs from 'fs'
import * as causalGraph from '../src/fancydb/causal-graph'
import assert from 'assert/strict'
import { LV, LVRange } from '../src/types'
import { pushRLEList } from '../src/fancydb/rle'

type HistItem = {
  span: LVRange,
  parents: LV[]
}

const histToCG = (hist: HistItem[]): causalGraph.CausalGraph => {
  const cg = causalGraph.create()

  for (const h of hist) {
    causalGraph.add(cg, 'testUser', h.span[0], h.span[1], h.parents)
  }


  return cg
}

const readJSONFile = <T>(filename: string): T[] => (
  fs.readFileSync(filename, 'utf-8')
    .split('\n')
    .filter(s => s.length > 0)
    .map(s => JSON.parse(s))
)

describe('causal graph utilities', () => {
  it('version contains time', () => {
    type VersionContainsTimeTest = {
      hist: HistItem[],
      frontier: number[],
      target: number,
      expected: boolean
    }

    const data = readJSONFile<VersionContainsTimeTest>('../test_data/causal_graph/version_contains.json')

    for (const {hist, frontier, target, expected} of data) {
      // console.log(hist)
      const cg = histToCG(hist)

      const actual = causalGraph.versionContainsTime(cg, frontier, target)
      if (expected !== actual) {
        console.dir(cg, {depth: null})
        console.dir(frontier, {depth: null})
        console.dir(target, {depth: null})
        console.log('expected:', expected, 'actual:', actual)
      }
      assert.equal(expected, actual)
    }
  })

  it('diff', () => {
    type DiffTest = {
      hist: HistItem[],
      a: number[],
      b: number[],
      expect_a: Range[],
      expect_b: Range[],
    }

    const data = readJSONFile<DiffTest>('../test_data/causal_graph/diff.json')


    for (const {hist, a, b, expect_a, expect_b} of data) {
      // console.log(hist)
      const cg = histToCG(hist)
      expect_a.reverse()
      expect_b.reverse()

      const {aOnly, bOnly} = causalGraph.diff(cg, a, b)
      // console.log(aOnly, expect_a)
      assert.deepEqual(aOnly, expect_a)
      assert.deepEqual(bOnly, expect_b)
    }
  })

  it('find conflicting', () => {
    type DiffFlagStr = 'OnlyA' | 'OnlyB' | 'Shared'
    type ConflictTest = {
      hist: HistItem[],
      a: number[],
      b: number[],
      expect_spans: [range: {start: number, end: number}, flag: DiffFlagStr][],
      expect_common: number[],
    }

    const flagToStr = (f: causalGraph.DiffFlag): DiffFlagStr => (
        f === causalGraph.DiffFlag.A ? 'OnlyA'
          : f === causalGraph.DiffFlag.B ? 'OnlyB'
          : 'Shared'
    )
    // const strToFlag = (f: DiffFlagStr): causalGraph.DiffFlag => (
    //   f === 'OnlyA' ? causalGraph.DiffFlag.A
    //     : f === 'OnlyB' ? causalGraph.DiffFlag.B
    //     : causalGraph.DiffFlag.Shared
    // )

    const data = readJSONFile<ConflictTest>('../test_data/causal_graph/conflicting.json')

    const test = ({hist, a, b, expect_spans, expect_common}: ConflictTest) => {
      const expectSpans = expect_spans.map(([{start, end}, flagStr]) => [[start, end], flagStr] as [LVRange, DiffFlagStr])

      const cg = histToCG(hist)
      // console.dir(cg, {depth: null})
      // console.dir(a, {depth: null})
      // console.dir(b, {depth: null})

      // console.log(expect_spans)

      const actualSpans: [LVRange, DiffFlagStr][] = []
      const actualCommon = causalGraph.findConflicting(cg, a, b, (range, flag) => {
        // console.log('emit', range, flag)

        // This is a bit of a horrible hack, but eh.
        pushRLEList<[LVRange, DiffFlagStr]>(actualSpans, [range, flagToStr(flag)], (a, b) => {
          if (a[0][0] === b[0][1] && a[1] === b[1]) {
            a[0][0] = b[0][0]
            return true
          } else return false
        })
      })
      actualSpans.reverse()
      // console.log('actual', actualSpans, 'expect', expectSpans)
      assert.deepEqual(expectSpans, actualSpans)
      assert.deepEqual(expect_common, actualCommon)
    }

    // test(data[19])
    for (let i = 0; i < data.length; i++) {
      // console.log(`======== ${i} =======`)
      test(data[i])
    }

  })
})