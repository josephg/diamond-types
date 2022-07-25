// This is a simplified database for the browser. No history is stored.
import Map2 from "map2"

export const ROOT: Version = ['ROOT', 0]

export type Version = [agent: string, seq: number]

type Primitive = null | boolean | string | number | Primitive[] | {[k: string]: Primitive}

type CreateValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', crdtKind: 'map' | 'set'}

type RegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', id: Version}

type MVRegister = [Version, RegisterValue][]

type CRDTInfo = {
  type: 'map',
  registers: {[k: string]: MVRegister},
  activeValue: {[k: string]: any},
} | {
  type: 'set',
  values: Map2<string, number, RegisterValue>,
  activeValue: Map2<string, number, any>,
}

interface DBState {
  version: Version[],
  value: {[k: string]: any}, // Fixed at time of DB creation.
  crdts: Map2<string, number, CRDTInfo>
}

type Action =
{ type: 'map', key: string, localParents: Version[], val: CreateValue }
| { type: 'setInsert', val: CreateValue }
| { type: 'setDelete', target: Version }

interface Operation {
  id: Version,
  globalParents: Version[],
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

function removeRecursive(state: DBState, value: RegisterValue) {
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
    case 'set':
      for (const [agent, seq, value] of crdt.values) {
        removeRecursive(state, value)
      }
      break
    default: throw Error('Unknown CRDT type!?')
  }

  state.crdts.delete(value.id[0], value.id[1])
}

export function localMapInsert(state: DBState, id: Version, mapId: Version, key: string, val: CreateValue): Operation {
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

export function localSetInsert(state: DBState, id: Version, setId: Version, val: CreateValue): Operation {
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

export function localSetDelete(state: DBState, id: Version, setId: Version, target: Version): Operation | null {
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


function createCRDT(state: DBState, id: Version, type: 'map' | 'set'): any {
  if (state.crdts.has(id[0], id[1])) {
    throw Error('CRDT already exists !?')
  }

  const crdtInfo: CRDTInfo = type === 'map' ? {
    type: "map",
    registers: {},
    activeValue: {}
  } : {
    type: 'set',
    values: new Map2,
    activeValue: new Map2,
  }

  state.crdts.set(id[0], id[1], crdtInfo)
  return crdtInfo.activeValue
}

export function applyRemoteOp(state: DBState, op: Operation) {
  state.version = advanceFrontier(state.version, op.id, op.globalParents)

  const crdt = state.crdts.get(op.crdtId[0], op.crdtId[1])
  if (crdt == null) {
    console.warn('CRDT has been deleted..')
    return
  }

  // Every map operation creates a new value, and removes 0-n other values.
  switch (op.action.type) {
    case 'map':
      if (crdt.type !== 'map') throw Error('Invalid operation type for target')

      const oldPairs = crdt.registers[op.action.key] ?? []
      const newPairs: MVRegister = []
      for (const [version, value] of oldPairs) {
        // Each item is either retained or removed.
        if (op.action.localParents.some(v2 => versionEq(version, v2))) {
          // The item was named in parents. Remove it.
          console.log('removing', value)
          removeRecursive(state, value)
        } else {
          newPairs.push([version, value])
        }
      }
    
      let newValue: RegisterValue
      if (op.action.val.type === 'primitive') {
        newValue = op.action.val
      } else {
        // Create it.
        createCRDT(state, op.id, op.action.val.crdtKind)
        newValue = {type: "crdt", id: op.id}
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
      break

    case 'setInsert': case 'setDelete': // Sets!
      if (crdt.type !== 'set') throw Error('Invalid operation type for target')

      // Set operations are comparatively much simpler, because insert
      // operations cannot be concurrent and multiple overlapping delete
      // operations are ignored.

      if (op.action.type == 'setInsert') {
        if (op.action.val.type === 'primitive') {
          crdt.values.set(op.id[0], op.id[1], op.action.val)
          crdt.activeValue.set(op.id[0], op.id[1], op.action.val.val)
        } else {
          const activeValue = createCRDT(state, op.id, op.action.val.crdtKind)
          crdt.values.set(op.id[0], op.id[1], {type: "crdt", id: op.id})
          crdt.activeValue.set(op.id[0], op.id[1], activeValue)
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

    default: throw Error('Invalid action type')
  }

}