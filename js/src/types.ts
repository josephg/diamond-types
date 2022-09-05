import Map2 from "map2"

export type RawVersion = [agent: string, seq: number]

export const ROOT: RawVersion = ['ROOT', 0]

/** Local version */
export type LV = number

export const ROOT_LV: LV = -1

export type Primitive = null
  | boolean
  | string
  | number
  | Primitive[]
  | {[k: string]: Primitive}

export type CreateValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', crdtKind: 'map' | 'set' | 'register'}

export type Action =
{ type: 'map', key: string, localParents: RawVersion[], val: CreateValue }
| { type: 'registerSet', localParents: RawVersion[], val: CreateValue }
| { type: 'setInsert', val: CreateValue }
| { type: 'setDelete', target: RawVersion }

export interface Operation {
  id: RawVersion,
  globalParents: RawVersion[],
  crdtId: RawVersion,
  action: Action,
}

export type DBValue = null
  | boolean
  | string
  | number
  | DBValue[]
  | {[k: string]: DBValue} // Map
  | Map2<string, number, DBValue> // Set.


/** Helper type for a list with at least 1 entry in it. */
export type AtLeast1<T> = [T, ...T[]]


export type SnapRegisterValue = {type: 'primitive', val: Primitive}
  | {type: 'crdt', id: RawVersion}
export type SnapMVRegister = [RawVersion, SnapRegisterValue][]
export type SnapCRDTInfo = {
  type: 'map',
  registers: {[k: string]: SnapMVRegister},
} | {
  type: 'set',
  values: [string, number, SnapRegisterValue][],
} | {
  type: 'register',
  value: SnapMVRegister,
}

export interface DBSnapshot {
  version: RawVersion[],
  crdts: [string, number, SnapCRDTInfo][]
}

export interface VersionSummary {[agent: string]: [number, number][]}
