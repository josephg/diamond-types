import 'mocha'
import * as causalGraph from '../src/fancydb/causal-graph'
import assert from 'assert/strict'

describe('causal graph', () => {
  it('smoke test', () => {
    const cg = causalGraph.create()

    causalGraph.add(cg, 'seph', 10, 20, [])
    causalGraph.add(cg, 'mike', 10, 20, [])

    causalGraph.assignLocal(cg, 'seph', 5)

    const serialized = causalGraph.serialize(cg)
    const deserialized = causalGraph.fromSerialized(serialized)
    assert.deepEqual(cg, deserialized)
  })
})
