import 'mocha'
import fs from 'fs'
import * as causalGraph from '../src/fancydb/causal-graph'
import assert from 'assert/strict'

type Range = [start: number, end: number]
type HistItem = {
  span: Range,
  parents: number[]
}

const histToCG = (hist: HistItem[]): causalGraph.CausalGraph => {
  const cg = causalGraph.create()

  for (const h of hist) {
    causalGraph.add(cg, 'testUser', h.span[0], h.span[1], h.parents)
  }


  return cg
}

describe('causal graph utilities', () => {
  it('version contains time', () => {
    type VersionContainsTimeTest = {
      hist: HistItem[],
      frontier: number[],
      target: number,
      expected: boolean
    }

    const data: VersionContainsTimeTest[] = fs.readFileSync('../test_data/causal_graph/version_contains.json', 'utf-8')
      .split('\n')
      .filter(s => s.length > 0)
      .map(s => JSON.parse(s))

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

    const data: DiffTest[] = fs.readFileSync('../test_data/causal_graph/diff.json', 'utf-8')
      .split('\n')
      .filter(s => s.length > 0)
      .map(s => JSON.parse(s))


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
})