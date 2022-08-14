// This is a simplified database for the browser. No history is stored.
import Map2 from "map2"
import { Primitive, RawVersion, CreateValue, Operation, ROOT, DBValue, DBSnapshot, SnapCRDTInfo } from "./types.js"


type RegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', id: RawVersion}

type MVRegister = [RawVersion, RegisterValue][]

type CRDTInfo = {
  type: 'map',
  registers: {[k: string]: MVRegister},
} | {
  type: 'set',
  values: Map2<string, number, RegisterValue>,
} | {
  type: 'register',
  value: MVRegister,
}

/**
 * A SimpleDB is a lightweight in-memory database implementation designed for
 * use in the browser. It is optimized to be tiny, and it doesn't need to load
 * the causalgraph to work.
 *
 * We locally store the current version, but we need to trust the remote peer
 * to figure out what operations we need to merge in. This is a bit complex on
 * reconnect.
 */
export interface SimpleDB {
  version: RawVersion[],
  crdts: Map2<string, number, CRDTInfo>
}


export const versionEq = ([a1, s1]: RawVersion, [a2, s2]: RawVersion) => (a1 === a2 && s1 === s2)

export const frontierEq = (f1: RawVersion[], f2: RawVersion[]): boolean => {
  // Both frontiers should be sorted at this point anyway. It would be better
  // to assert they're sorted than re-sort.
  // They should also be free from duplicates.
  
  f1.sort(versionCmp); f2.sort(versionCmp)

  if (f1.length !== f2.length) return false
  for (let i = 0; i < f1.length; i++) {
    if (!versionEq(f1[i], f2[i])) return false
  }
  return true
}

const versionCmp = ([a1, s1]: RawVersion, [a2, s2]: RawVersion) => (
  a1 < a2 ? 1
    : a1 > a2 ? -1
    : s1 - s2
)

export const advanceFrontier = (frontier: RawVersion[], version: RawVersion, parents: RawVersion[]): RawVersion[] => {
  const f = frontier.filter(v => !parents.some(v2 => versionEq(v, v2)))
  f.push(version)
  return f.sort(versionCmp)
}

export function createDb(): SimpleDB {
  const db: SimpleDB = {
    version: [],
    crdts: new Map2(),
  }

  db.crdts.set(ROOT[0], ROOT[1], {
    type: "map",
    registers: {}
  })

  return db
}

function removeRecursive(state: SimpleDB, value: RegisterValue) {
  if (value.type !== 'crdt') return

  const crdt = state.crdts.get(value.id[0], value.id[1])
  if (crdt == null) return

  switch (crdt.type) {
    case 'map':
      for (const k in crdt.registers) {
        const reg = crdt.registers[k]
        for (const [version, value] of reg) {
          removeRecursive(state, value)
        }
      }
      break
    case 'register':
      for (const [version, value] of crdt.value) {
        removeRecursive(state, value)
      }
      break
    case 'set':
      for (const [agent, seq, value] of crdt.values) {
        removeRecursive(state, value)
      }
      break
    default: throw Error('Unknown CRDT type!?')
  }

  state.crdts.delete(value.id[0], value.id[1])
}

export function localRegisterSet(state: SimpleDB, id: RawVersion, regId: RawVersion, val: CreateValue): Operation {
  const crdt = state.crdts.get(regId[0], regId[1])
  if (crdt == null || crdt.type !== 'register') throw Error('invalid CRDT')

  const localParents = crdt.value.map(([version]) => version)
  const op: Operation = {
    id,
    crdtId: regId,
    globalParents: state.version,
    action: { type: 'registerSet', localParents, val }
  }
  // TODO: Inline this?
  applyRemoteOp(state, op)
  return op
}

export function localMapInsert(state: SimpleDB, id: RawVersion, mapId: RawVersion, key: string, val: CreateValue): Operation {
  const crdt = state.crdts.get(mapId[0], mapId[1])
  if (crdt == null || crdt.type !== 'map') throw Error('invalid CRDT')

  const localParents = (crdt.registers[key] ?? []).map(([version]) => version)
  const op: Operation = {
    id,
    crdtId: mapId,
    globalParents: state.version,
    action: { type: 'map', localParents, key, val }
  }
  // TODO: Could easily inline this - which would mean more code but higher performance.
  applyRemoteOp(state, op)
  return op
}

export function localSetInsert(state: SimpleDB, id: RawVersion, setId: RawVersion, val: CreateValue): Operation {
  const crdt = state.crdts.get(setId[0], setId[1])
  if (crdt == null || crdt.type !== 'set') throw Error('invalid CRDT')

  const op: Operation = {
    id,
    crdtId: setId,
    globalParents: state.version,
    action: { type: 'setInsert', val }
  }
  // TODO: Inline this?
  applyRemoteOp(state, op)
  return op
}

export function localSetDelete(state: SimpleDB, id: RawVersion, setId: RawVersion, target: RawVersion): Operation | null {
  const crdt = state.crdts.get(setId[0], setId[1])
  if (crdt == null || crdt.type !== 'set') throw Error('invalid CRDT')

  let oldVal = crdt.values.get(target[0], target[1])
  if (oldVal != null) {
    removeRecursive(state, oldVal)
    crdt.values.delete(target[0], target[1])

    return {
      id,
      crdtId: setId,
      globalParents: state.version,
      action: { type: 'setDelete', target }
    }
  } else { return null } // Already deleted.
}

const unwrapCRDT = (db: SimpleDB, id: RawVersion, key: null | string | RawVersion): RawVersion | null => {
  let crdt = db.crdts.get(id[0], id[1])!

  while (crdt.type === 'register') {
    // Unwrap registers
    let inner = crdt.value[0][1]
    if (inner.type === 'crdt') {
      id = inner.id
      crdt = db.crdts.get(id[0], id[1])!
    } else {
      throw Error('Cannot descend into register')
    }
  }

  if (key == null) return id

  let value: RegisterValue
  if (crdt.type === 'map' && typeof key === 'string') {
    const register: MVRegister = crdt.registers[key]
    if (register == null) throw Error(`Missing item at path ${key}`)
    if (register.length < 1) throw Error('Invalid register')
    
    value = register[0][1]
  } else if (crdt.type === 'set' && Array.isArray(key)) {
    const val = crdt.values.get(key[0], key[1])
    if (val == null) return null // The set item was deleted (probably remotely).
    // if (val == null) throw Error('Missing item at path')
    value = val
  } else {
    throw Error('Cannot descend into path')
  }

  if (value.type !== 'crdt') throw Error('Cannot unwrap primitive')
  return value.id
}

function containerAtPath(db: SimpleDB, path: (string | RawVersion)[]): [RawVersion, string | RawVersion | null] {
  // let crdt = db.crdts.get(ROOT[0], ROOT[1])!
  // let value: RegisterValue = {type: 'crdt', id: ROOT}
  let id = ROOT
  let key: null | string | RawVersion = null

  for (const p of path) {
    id = unwrapCRDT(db, id, key) ?? errExpr('Container deleted')
    key = p
  }

  return [id, key]
}

export function setAtPath(db: SimpleDB, id: RawVersion, path: (string | RawVersion)[], val: CreateValue): Operation {
  if (path.length === 0) throw Error('Invalid path')
  let [crdtId, key] = containerAtPath(db, path)

  if (Array.isArray(key)) {
    // If the container is a set, the set must store a register. Unwrap!
    crdtId = unwrapCRDT(db, crdtId, key) ?? errExpr('Container deleted')
    key = null
  }
  
  if (key == null) {
    return localRegisterSet(db, id, crdtId, val)
  } else {
    return localMapInsert(db, id, crdtId, key, val)
  }
}

const errExpr = (str: string): never => { throw Error(str) }

function createCRDT(state: SimpleDB, id: RawVersion, type: 'map' | 'set' | 'register') {
  if (state.crdts.has(id[0], id[1])) {
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
    values: new Map2,
  } : errExpr('Invalid CRDT type')

  state.crdts.set(id[0], id[1], crdtInfo)
}

function mergeRegister(state: SimpleDB, oldPairs: MVRegister, localParents: RawVersion[], newVersion: RawVersion, newVal: CreateValue): MVRegister {
  const newPairs: MVRegister = []
  for (const [version, value] of oldPairs) {
    // Each item is either retained or removed.
    if (localParents.some(v2 => versionEq(version, v2))) {
      // The item was named in parents. Remove it.
      // console.log('removing', value)
      removeRecursive(state, value)
    } else {
      newPairs.push([version, value])
    }
  }

  let newValue: RegisterValue
  if (newVal.type === 'primitive') {
    newValue = newVal
  } else {
    // Create it.
    createCRDT(state, newVersion, newVal.crdtKind)
    newValue = {type: "crdt", id: newVersion}
  }

  newPairs.push([newVersion, newValue])
  newPairs.sort(([v1], [v2]) => versionCmp(v1, v2))

  return newPairs
}

export function applyRemoteOp(state: SimpleDB, op: Operation) {
  state.version = advanceFrontier(state.version, op.id, op.globalParents)

  const crdt = state.crdts.get(op.crdtId[0], op.crdtId[1])
  if (crdt == null) {
    console.warn('CRDT has been deleted..')
    return
  }

  // Every map operation creates a new value, and removes 0-n other values.
  switch (op.action.type) {
    case 'registerSet': {
      if (crdt.type !== 'register') throw Error('Invalid operation type for target')
      const newPairs = mergeRegister(state, crdt.value, op.action.localParents, op.id, op.action.val)

      crdt.value = newPairs
      break
    }
    case 'map': {
      if (crdt.type !== 'map') throw Error('Invalid operation type for target')

      const oldPairs = crdt.registers[op.action.key] ?? []
      const newPairs = mergeRegister(state, oldPairs, op.action.localParents, op.id, op.action.val)

      crdt.registers[op.action.key] = newPairs
      break
    }
    case 'setInsert': case 'setDelete': { // Sets!
      if (crdt.type !== 'set') throw Error('Invalid operation type for target')

      // Set operations are comparatively much simpler, because insert
      // operations cannot be concurrent and multiple overlapping delete
      // operations are ignored.

      if (op.action.type == 'setInsert') {
        if (op.action.val.type === 'primitive') {
          crdt.values.set(op.id[0], op.id[1], op.action.val)
        } else {
          createCRDT(state, op.id, op.action.val.crdtKind)
          crdt.values.set(op.id[0], op.id[1], {type: "crdt", id: op.id})
        }
      } else {
        // Delete!
        let oldVal = crdt.values.get(op.action.target[0], op.action.target[1])
        if (oldVal != null) {
          removeRecursive(state, oldVal)
          crdt.values.delete(op.action.target[0], op.action.target[1])
        }
      }

      break
    }

    default: throw Error('Invalid action type')
  }
}

const registerToVal = (state: SimpleDB, r: RegisterValue): DBValue => (
  (r.type === 'primitive')
    ? r.val
    : get(state, r.id) // Recurse!
)

export function get(state: SimpleDB, crdtId: RawVersion = ROOT): DBValue {
  const crdt = state.crdts.get(crdtId[0], crdtId[1])
  if (crdt == null) { return null }

  switch (crdt.type) {
    case 'register': {
      // When there's a tie, the active value is based on the order in pairs.
      const activePair = crdt.value[0][1]
      return registerToVal(state, activePair)
    }
    case 'map': {
      const result: {[k: string]: DBValue} = {}
      for (const k in crdt.registers) {
        const activePair = crdt.registers[k][0][1]
        result[k] = registerToVal(state, activePair)
      }
      return result
    }
    case 'set': {
      const result = new Map2<string, number, DBValue>()

      for (const [agent, seq, value] of crdt.values) {
        result.set(agent, seq, registerToVal(state, value))
      }

      return result
    }
    default: throw Error('Invalid CRDT type in DB')
  }
}

export function getAtPath(db: SimpleDB, path: (string | RawVersion)[]): DBValue {
  let [crdtId, key] = containerAtPath(db, path)

  if (key == null) return get(db, crdtId)
  
  let crdt = db.crdts.get(crdtId[0], crdtId[1])!
  if (Array.isArray(key)) {
    if (crdt.type !== 'set') throw Error('Unexpected type')
    const val = crdt.values.get(key[0], key[1])
    if (val == null) throw Error('Missing key')
    return registerToVal(db, val)
  } else {
    if (crdt.type !== 'map') throw Error('Unexpected type')
    return registerToVal(db, crdt.registers[key][0][1])
  }
}

export function toSnapshot(state: SimpleDB): DBSnapshot {
  return {
    version: state.version,
    crdts: Array.from(state.crdts.entries()).map(([agent, seq, crdtInfo]) => {
      const info2: SnapCRDTInfo = crdtInfo.type === 'set' ? {
        type: crdtInfo.type,
        values: Array.from(crdtInfo.values)
      } : {...crdtInfo}
      return [agent, seq, info2]
    })
  }
}

export function fromSnapshot(jsonState: any): SimpleDB {
  return {
    version: jsonState.version,
    crdts: new Map2(jsonState.crdts.map(([agent, seq, info]: any) => {
      const info2 = info.type === 'set'
        ? {
          type: info.type,
          values: new Map2(info.values)
        }
        : info

      return [agent, seq, info2]
    }))
  } as SimpleDB
}
