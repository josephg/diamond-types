const assert = require('assert/strict')
const dt = require('./dist')

const db = dt.createDb()

dt.localMapInsert(db, ['seph', 0], dt.ROOT, 'yo', {type: 'primitive', val: 123})
assert.deepEqual(dt.get(db), {yo: 123})

// concurrent changes
dt.applyRemoteOp(db, {
  id: ['mike', 0],
  globalParents: [],
  crdtId: dt.ROOT,
  action: {type: 'map', localParents: [], key: 'yo', val: {type: 'primitive', val: 321}},
})
assert.deepEqual(dt.get(db), {yo: 123})

dt.applyRemoteOp(db, {
  id: ['mike', 1],
  globalParents: [['mike', 0], ['seph', 0]],
  crdtId: dt.ROOT,
  action: {type: 'map', localParents: [['mike', 0], ['seph', 0]], key: 'yo', val: {type: 'primitive', val: 1000}},
})
assert.deepEqual(dt.get(db), {yo: 1000})

// Set a value in an inner map
const inner = dt.localMapInsert(db, ['seph', 1], dt.ROOT, 'stuff', {type: 'crdt', crdtKind: 'map'})
dt.localMapInsert(db, ['seph', 2], inner.id, 'cool', {type: 'primitive', val: 'definitely'})
assert.deepEqual(dt.get(db), {yo: 1000, stuff: {cool: 'definitely'}})


// Insert a set
const innerSet = dt.localMapInsert(db, ['seph', 2], dt.ROOT, 'a set', {type: 'crdt', crdtKind: 'set'})
dt.localSetInsert(db, ['seph', 3], innerSet.id, {type: 'primitive', val: 'whoa'})
dt.localSetInsert(db, ['seph', 4], innerSet.id, {type: 'crdt', crdtKind: 'map'})

console.log('db', dt.get(db))
console.log('db', db)