// This is a simplified database for the browser. No history is stored.

import assert from "assert/strict"
import Map2 from "map2"

export const ROOT: Version = ['ROOT', 0]

export type Version = [agent: string, seq: number]

type Primitive = null | boolean | string | number | Primitive[] | {[k: string]: Primitive}

type CreateValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', crdtKind: 'map'}

type RegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', crdtKind: 'map', id: Version}

type MVRegister = [Version, RegisterValue][]

type CRDTInfo = {
  type: 'map',
  registers: {[k: string]: MVRegister},
  activeValue: {[k: string]: any},
}

interface DBState {
  version: Version[],
  value: {[k: string]: any}, // Fixed at time of DB creation.
  crdts: Map2<string, number, CRDTInfo>
}

type Action = {
  type: 'map', key: string, val: CreateValue
}

interface Operation {
  id: Version,
  globalParents: Version[],
  localParents: Version[],
  crdtId: Version,
  action: Action,
}

const versionEq = ([a1, s1]: Version, [a2, s2]: Version) => (a1 === a2 && s1 === s2)
const versionCmp = ([a1, s1]: Version, [a2, s2]: Version) => (
  a1 < a2 ? 1
    : a1 > a2 ? -1
    : s1 - s2
)

export const advanceFrontier = (frontier: Version[], version: Version, parents: Version[]): Version[] => {
  const f = frontier.filter(v => !parents.some(v2 => versionEq(v, v2)))
  f.push(version)
  return f.sort(versionCmp)
}

export function createDb(): DBState {
  const db: DBState = {
    version: [],
    value: {},
    crdts: new Map2(),
  }

  db.crdts.set(ROOT[0], ROOT[1], {
    type: "map",
    registers: {},
    activeValue: db.value
  })

  return db
}

function removeRecursive(state: DBState, crdtId: Version) {
  const crdt = state.crdts.get(crdtId[0], crdtId[1])
  if (crdt != null) {
    for (const k in crdt.registers) {
      const reg = crdt.registers[k]
      for (const [version, value] of reg) {
        if (value.type === 'crdt') {
          removeRecursive(state, value.id)
        }
      }
    }

    state.crdts.delete(crdtId[0], crdtId[1])
  }
}

export function applyLocalOp(state: DBState, id: Version, crdtId: Version, action: Action): Operation {
  const crdt = state.crdts.get(crdtId[0], crdtId[1])
  if (crdt == null) throw Error('invalid CRDT')

  const localParents = (crdt.registers[action.key] ?? []).map(([version]) => version)
  const op: Operation = {
    id,
    globalParents: state.version,
    localParents,
    crdtId,
    action
  }
  applyRemoteOp(state, op)
  return op
}

export function applyRemoteOp(state: DBState, op: Operation) {
  state.version = advanceFrontier(state.version, op.id, op.globalParents)

  const crdt = state.crdts.get(op.crdtId[0], op.crdtId[1])
  if (crdt == null) {
    console.warn('CRDT has been deleted..')
    return
  }

  // Every map operation creates a new value, and removes 0-n other values.
  assert.equal(op.action.type, 'map')

  const oldPairs = crdt.registers[op.action.key] ?? []
  const newPairs: MVRegister = []
  for (const [version, value] of oldPairs) {
    // Each item is either retained or removed.
    if (op.localParents.some(v2 => versionEq(version, v2))) {
      // The item was named in parents. Remove it.
      console.log('removing', value)
      if (value.type === 'crdt') {
        removeRecursive(state, value.id)
      }
    } else {
      newPairs.push([version, value])
    }
  }

  let newValue: RegisterValue
  if (op.action.val.type === 'primitive') {
    newValue = op.action.val
  } else {
    // Create it.
    if (state.crdts.has(op.id[0], op.id[1])) {
      throw Error('CRDT already exists !?')
    }

    newValue = {type: "crdt", crdtKind: "map", id: op.id}
    state.crdts.set(op.id[0], op.id[1], {
      type: "map",
      registers: {},
      activeValue: {}
    })
  }

  newPairs.push([op.id, newValue])
  newPairs.sort(([v1], [v2]) => versionCmp(v1, v2))

  // When there's a tie, the active value is based on the order in pairs.
  const activeValue = newPairs[0][1]
  if (activeValue.type === 'primitive') {
    crdt.activeValue[op.action.key] = activeValue.val
  } else {
    crdt.activeValue[op.action.key] = state.crdts.get(activeValue.id[0], activeValue.id[1])!.activeValue
  }

  crdt.registers[op.action.key] = newPairs
}