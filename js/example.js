const dt = require('./dist')

const db = dt.createDb()

dt.applyLocalOp(db, ['seph', 0], dt.ROOT, {
  type: 'map',
  key: 'yo',
  val: {type: 'primitive', val: 123}
})

dt.applyRemoteOp(db, {
  id: ['mike', 0],
  globalParents: [],
  localParents: [],
  crdtId: dt.ROOT,
  action: {type: 'map', key: 'yo', val: {type: 'primitive', val: 321}},
})

dt.applyRemoteOp(db, {
  id: ['mike', 1],
  globalParents: [['mike', 0], ['seph', 0]],
  localParents: [['mike', 0], ['seph', 0]],
  crdtId: dt.ROOT,
  action: {type: 'map', key: 'yo', val: {type: 'primitive', val: 1000}},
})


const inner = dt.applyLocalOp(db, ['seph', 1], dt.ROOT, {
  type: 'map',
  key: 'stuff',
  val: {type: 'crdt', crdtKind: 'map'}
})
dt.applyLocalOp(db, ['seph', 2], inner.id, {
  type: 'map',
  key: 'cool',
  val: {type: 'primitive', val: 'definitely'}
})


console.log('db', db.value)
console.log('db', db)