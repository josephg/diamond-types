import { CreateValue, LV, Operation, Primitive, ROOT, ROOT_LV } from '../types'
import * as causalGraph from './causal-graph.js'
import { CausalGraph } from './causal-graph.js'

type RegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', id: LV}

type MVRegister = [LV, RegisterValue][]

type CRDTInfo = {
  type: 'map',
  registers: {[k: string]: MVRegister},
} | {
  type: 'set',
  values: Map<LV, RegisterValue>,
} | {
  type: 'register',
  value: MVRegister,
}

export interface FancyDB {
  crdts: Map<LV, CRDTInfo>,
  cg: CausalGraph,
}

export function createDb(): FancyDB {
  const db: FancyDB = {
    crdts: new Map(),
    cg: causalGraph.create(),
  }

  db.crdts.set(ROOT_LV, {
    type: "map",
    registers: {}
  })

  return db
}


function removeRecursive(db: FancyDB, value: RegisterValue) {
  if (value.type !== 'crdt') return

  const crdt = db.crdts.get(value.id)
  if (crdt == null) return

  switch (crdt.type) {
    case 'map':
      for (const k in crdt.registers) {
        const reg = crdt.registers[k]
        for (const [version, value] of reg) {
          removeRecursive(db, value)
        }
      }
      break
    case 'register':
      for (const [version, value] of crdt.value) {
        removeRecursive(db, value)
      }
      break
    case 'set':
      for (const [id, value] of crdt.values) {
        removeRecursive(db, value)
      }
      break
    default: throw Error('Unknown CRDT type!?')
  }

  db.crdts.delete(value.id)
}

const errExpr = (str: string): never => { throw Error(str) }

function createCRDT(db: FancyDB, id: LV, type: 'map' | 'set' | 'register') {
  if (db.crdts.has(id)) {
    throw Error('CRDT already exists !?')
  }

  const crdtInfo: CRDTInfo = type === 'map' ? {
    type: "map",
    registers: {},
  } : type === 'register' ? {
    type: 'register',
    value: [],
  } : type === 'set' ? {
    type: 'set',
    values: new Map,
  } : errExpr('Invalid CRDT type')

  db.crdts.set(id, crdtInfo)
}

function mergeRegister(db: FancyDB, globalParents: LV[], oldPairs: MVRegister, localParents: LV[], newVersion: LV, newVal: CreateValue): MVRegister {
  const newPairs: MVRegister = []
  for (const [version, value] of oldPairs) {
    // Each item is either retained or removed.
    if (localParents.some(v2 => version === v2)) {
      // The item was named in parents. Remove it.
      // console.log('removing', value)
      removeRecursive(db, value)
    } else {
      // We're intending to retain this operation because its not explicitly
      // named, but that only makes sense if the retained version is concurrent
      // with the new version.
      if (causalGraph.versionContainsTime(db.cg, globalParents, version)) {
        throw Error('Invalid local parents in operation')
      }

      newPairs.push([version, value])
    }
  }

  let newValue: RegisterValue
  if (newVal.type === 'primitive') {
    newValue = newVal
  } else {
    // Create it.
    createCRDT(db, newVersion, newVal.crdtKind)
    newValue = {type: "crdt", id: newVersion}
  }

  newPairs.push([newVersion, newValue])
  newPairs.sort(([v1], [v2]) => v1 - v2)

  return newPairs
}

export function applyRemoteOp(db: FancyDB, op: Operation) {
  // if (causalGraph.tryRawToLV(db.cg, op.id[0], op.id[1]) != null) {
  //   // The operation is already known.
  //   console.warn('Operation already applied', op.id)
  //   return
  // }

  const newVersion = causalGraph.addRaw(db.cg, op.id, 1, op.globalParents)
  if (newVersion < 0) {
    // The operation is already known.
    console.warn('Operation already applied', op.id)
    return
  }

  const globalParents = causalGraph.mapParents(db.cg, op.globalParents)

  const crdtLV = causalGraph.rawToLV(db.cg, op.crdtId[0], op.crdtId[1])

  const crdt = db.crdts.get(crdtLV)
  if (crdt == null) {
    console.warn('CRDT has been deleted..')
    return
  }

  // Every register operation creates a new value, and removes 0-n other values.
  switch (op.action.type) {
    case 'registerSet': {
      if (crdt.type !== 'register') throw Error('Invalid operation type for target')
      const localParents = causalGraph.mapParents(db.cg, op.action.localParents)
      const newPairs = mergeRegister(db, globalParents, crdt.value, localParents, newVersion, op.action.val)

      crdt.value = newPairs
      break
    }
    case 'map': {
      if (crdt.type !== 'map') throw Error('Invalid operation type for target')

      const oldPairs = crdt.registers[op.action.key] ?? []
      const localParents = causalGraph.mapParents(db.cg, op.action.localParents)

      const newPairs = mergeRegister(db, globalParents, oldPairs, localParents, newVersion, op.action.val)

      crdt.registers[op.action.key] = newPairs
      break
    }
    case 'setInsert': case 'setDelete': { // Sets!
      if (crdt.type !== 'set') throw Error('Invalid operation type for target')

      // Set operations are comparatively much simpler, because insert
      // operations cannot be concurrent and multiple overlapping delete
      // operations are ignored.

      // throw Error('nyi')
      if (op.action.type == 'setInsert') {
        if (op.action.val.type === 'primitive') {
          crdt.values.set(newVersion, op.action.val)
        } else {
          createCRDT(db, newVersion, op.action.val.crdtKind)
          crdt.values.set(newVersion, {type: "crdt", id: newVersion})
        }
      } else {
        // Delete!
        const target = causalGraph.rawToLV(db.cg, op.action.target[0], op.action.target[1])
        let oldVal = crdt.values.get(target)
        if (oldVal != null) {
          removeRecursive(db, oldVal)
          crdt.values.delete(target)
        }
      }

      break
    }

    default: throw Error('Invalid action type')
  }
}

;(() => {

  const db = createDb()

  // localMapInsert(db, ['seph', 0], ROOT, 'yo', {type: 'primitive', val: 123})
  // assert.deepEqual(get(db), {yo: 123})

  // concurrent changes
  applyRemoteOp(db, {
    id: ['mike', 0],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'yo', val: {type: 'primitive', val: 'mike'}},
  })
  applyRemoteOp(db, {
    id: ['seph', 0],
    globalParents: [],
    crdtId: ROOT,
    action: {type: 'map', localParents: [], key: 'yo', val: {type: 'primitive', val: 'seph'}},
  })

  // assert.deepEqual(get(db), {yo: 123})

  applyRemoteOp(db, {
    id: ['mike', 1],
    // globalParents: [['mike', 0]],
    globalParents: [['mike', 0], ['seph', 0]],
    crdtId: ROOT,
    // action: {type: 'map', localParents: [['mike', 0]], key: 'yo', val: {type: 'primitive', val: 'both'}},
    action: {type: 'map', localParents: [['mike', 0], ['seph', 0]], key: 'yo', val: {type: 'primitive', val: 'both'}},
  })
  console.dir(db, {depth: null})
  // assert.deepEqual(get(db), {yo: 1000})
  
  // // Set a value in an inner map
  // const inner = localMapInsert(db, ['seph', 1], ROOT, 'stuff', {type: 'crdt', crdtKind: 'map'})
  // localMapInsert(db, ['seph', 2], inner.id, 'cool', {type: 'primitive', val: 'definitely'})
  // assert.deepEqual(get(db), {yo: 1000, stuff: {cool: 'definitely'}})
  
  
  // // Insert a set
  // const innerSet = localMapInsert(db, ['seph', 2], ROOT, 'a set', {type: 'crdt', crdtKind: 'set'})
  // localSetInsert(db, ['seph', 3], innerSet.id, {type: 'primitive', val: 'whoa'})
  // localSetInsert(db, ['seph', 4], innerSet.id, {type: 'crdt', crdtKind: 'map'})
  
  // console.log('db', get(db))
  // console.log('db', db)
  
  
  // assert.deepEqual(db, fromJSON(toJSON(db)))
})()