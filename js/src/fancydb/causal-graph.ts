// The causal graph puts a bunch of edits (each at some [agent, seq] version
// pair) into a list.

import PriorityQueue from 'priorityqueuejs'
import bs from 'binary-search'
import {RawVersion} from '../types'
import { ROOT_LV } from '.'

export const ROOT_VERSION: RawVersion = ['ROOT', 0]

/** Local version */
export type LV = number

type CGEntry = {
  version: LV,
  vEnd: LV,

  agent: string,
  seq: number, // Seq for version.

  parents: LV[] // Parents for version
}

type ClientEntry = {
  seq: number,
  seqEnd: number,
  version: LV,
}

export interface CausalGraph {
  /** Map from localversion -> rawversion */
  entries: CGEntry[],
  /** Map from agent -> list of */
  agentToVersion: {[k: string]: ClientEntry[]},
  version: LV[]
}

export const create = (): CausalGraph => ({
  entries: [],
  agentToVersion: {},
  version: []
})

export const advanceFrontier = (frontier: LV[], vLast: LV, parents: LV[]): LV[] => {
  // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
  // for (const parent of op.parents) {
  //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
  // }

  const f = frontier.filter(v => !parents.includes(v))
  f.push(vLast)
  return f.sort((a, b) => a - b)
}

export const clientEntriesForAgent = (causalGraph: CausalGraph, agent: string): ClientEntry[] => (
  causalGraph.agentToVersion[agent] ??= []
)

const lastOr = <T, V>(list: T[], f: (t: T) => V, def: V): V => (
  list.length === 0 ? def : f(list[list.length - 1])
)

const nextVersion = (cg: CausalGraph): LV => (
  lastOr(cg.entries, e => e.vEnd, 0)
)

const tryAppendEntries = (a: CGEntry, b: CGEntry): boolean => {
  const canAppend = b.version === a.vEnd
    && a.agent === b.agent
    && a.seq + (a.vEnd - a.version) === b.seq
    && b.parents.length === 1 && b.parents[0] === a.vEnd - 1

  if (canAppend) {
    a.vEnd = b.vEnd
  }

  return canAppend
}

const tryAppendClient = (a: ClientEntry, b: ClientEntry): boolean => {
  const canAppend = b.seq === a.seqEnd
    && b.version === (a.version + (a.seqEnd - a.seq))

  if (canAppend) {
    a.seqEnd = b.seqEnd
  }
  return canAppend
}

const tryAppend = <T>(list: T[], newItem: T, tryAppend: (a: T, b: T) => boolean) => {
  if (list.length > 0) {
    if (tryAppend(list[list.length - 1], newItem)) return
  }
  list.push(newItem)
}

const findClientEntry = (cg: CausalGraph, agent: string, seq: number): ClientEntry | null => {
  const av = clientEntriesForAgent(cg, agent)

  const result = bs(av, seq, (entry, needle) => (
    needle < entry.seq ? 1
      : needle >= entry.seqEnd ? -1
      : 0
  ))

  if (result < 0) return null

  // Trim the incoming item.
  const clientEntry = av[result]
  const offset = seq - clientEntry.seq
  return offset === 0 ? clientEntry : {
    seq,
    seqEnd: clientEntry.seqEnd,
    version: clientEntry.version + offset
  }
}

export const addRaw = (cg: CausalGraph, id: RawVersion, len: number, rawParents: RawVersion[]): LV => {
  const parents = mapParents(cg, rawParents)

  return add(cg, id[0], id[1], id[1]+len, parents)
}

export const add = (cg: CausalGraph, agent: string, seqStart: number, seqEnd: number, parents: LV[]): LV => {
  const version = nextVersion(cg)

  while (true) {
    // Look for an equivalent existing entry in the causal graph starting at
    // seq_start. We only add the parts of the that do not already exist in CG.
    const existingEntry = findClientEntry(cg, agent, seqStart)
    // console.log(cg.agentToVersion[agent], seqStart, existingEntry)
    if (existingEntry == null) break // Insert start..end.

    if (existingEntry.seqEnd >= seqEnd) return -1 // Already inserted.

    // Or trim and loop.
    seqStart = existingEntry.seqEnd
    parents = [existingEntry.version + (existingEntry.seqEnd - existingEntry.seq) - 1]
  }

  const len = seqEnd - seqStart
  const vEnd = version + len
  const entry: CGEntry = {
    version,
    vEnd,

    agent,
    seq: seqStart,
    parents,
  }

  tryAppend(cg.entries, entry, tryAppendEntries)
  tryAppend(clientEntriesForAgent(cg, agent), { seq: seqStart, seqEnd, version}, tryAppendClient)

  cg.version = advanceFrontier(cg.version, vEnd - 1, parents)
  return version
}

export const assignLocal = (cg: CausalGraph, agent: string, num: number = 1): [number, LV] => {
  let version = nextVersion(cg)
  const av = clientEntriesForAgent(cg, agent)
  const seq = lastOr(av, ce => ce.seqEnd, 0)
  add(cg, agent, seq, seq + num, cg.version)

  return [seq, version]
}

export const findEntryContainingRaw = (cg: CausalGraph, v: LV): CGEntry => {
  const idx = bs(cg.entries, v, (entry, needle) => (
    needle < entry.version ? 1
    : needle >= entry.vEnd ? -1
    : 0
  ))
  if (idx < 0) throw Error('Invalid or unknown local version ' + v)
  return cg.entries[idx]
}
export const findEntryContaining = (cg: CausalGraph, v: LV): [CGEntry, number] => {
  const e = findEntryContainingRaw(cg, v)
  const offset = v - e.version
  return [e, offset]
}

export const localVersionToRaw = (cg: CausalGraph, v: LV): [string, number, LV[]] => {
  const [e, offset] = findEntryContaining(cg, v)
  const parents = offset === 0 ? e.parents : [v-1]
  return [e.agent, e.seq + offset, parents]
  // causalGraph.entries[localIndex]
}

// export const getParents = (cg: CausalGraph, v: LV): LV[] => (
//   localVersionToRaw(cg, v)[2]
// )

export const tryRawToLV = (cg: CausalGraph, agent: string, seq: number): LV | null => {
  if (agent === 'ROOT') return ROOT_LV

  const clientEntry = findClientEntry(cg, agent, seq)
  return clientEntry?.version ?? null
}
export const rawToLV = (cg: CausalGraph, agent: string, seq: number): LV => {
  if (agent === 'ROOT') return ROOT_LV

  const clientEntry = findClientEntry(cg, agent, seq)
  if (clientEntry == null) throw Error(`Unknown ID: (${agent}, ${seq})`)
  return clientEntry.version
}
export const mapParents = (cg: CausalGraph, parents: RawVersion[]): LV[] => (
  parents.map(([agent, seq]) => rawToLV(cg, agent, seq))
)


export interface VersionSummary {[agent: string]: [number, number][]}

const tryRangeAppend = (r1: [number, number], r2: [number, number]): boolean => {
  if (r1[1] === r2[0]) {
    r1[1] = r2[1]
    return true
  } else return false
}

export const summarizeVersion = (cg: CausalGraph): VersionSummary => {
  const result: VersionSummary = {}
  for (const k in cg.agentToVersion) {
    const av = cg.agentToVersion[k]
    if (av.length === 0) continue

    const versions: [number, number][] = []
    for (const ce of av) {
      tryAppend(versions, [ce.seq, ce.seqEnd], tryRangeAppend)
    }

    result[k] = versions
  }
  return result
}

// *** TOOLS ***

type DiffResult = {
  // These are (reversed) ranges.
  aOnly: [LV, LV][], bOnly: [LV, LV][]
}

const pushReversedRLE = (list: [LV, LV][], start: LV, end: LV) => {
  tryAppend(list, [start, end] as [number, number], (a, b) => {
    if (a[0] === b[1]) {
      a[0] = b[0]
      return true
    } else return false
  })
}

/**
 * This method takes in two versions (expressed as frontiers) and returns the
 * set of operations only appearing in the history of one version or the other.
 */
export const diff = (cg: CausalGraph, a: LV[], b: LV[]): DiffResult => {
  const enum DiffFlag { Shared, A, B }

  const flags = new Map<number, DiffFlag>()

  // Every order is in here at most once. Every entry in the queue is also in
  // itemType.
  const queue = new PriorityQueue<number>()

  // Number of items in the queue in both transitive histories (state Shared).
  let numShared = 0

  const enq = (v: LV, flag: DiffFlag) => {
    // console.log('enq', v, flag)
    const currentType = flags.get(v)
    if (currentType == null) {
      queue.enq(v)
      flags.set(v, flag)
      // console.log('+++ ', order, type, getLocalVersion(db, order))
      if (flag === DiffFlag.Shared) numShared++
    } else if (flag !== currentType && currentType !== DiffFlag.Shared) {
      // This is sneaky. If the two types are different they have to be {A,B},
      // {A,Shared} or {B,Shared}. In any of those cases the final result is
      // Shared. If the current type isn't shared, set it as such.
      flags.set(v, DiffFlag.Shared)
      numShared++
    }
  }

  for (const v of a) enq(v, DiffFlag.A)
  for (const v of b) enq(v, DiffFlag.B)

  // console.log('QF', queue, flags)

  const aOnly: [LV, LV][] = [], bOnly: [LV, LV][] = []


  const markRun = (start: LV, end: LV, flag: DiffFlag) => {
    if (end < start) throw Error('end < start')

    // console.log('markrun', start, end, flag)
    if (flag == DiffFlag.Shared) return
    const target = flag === DiffFlag.A ? aOnly : bOnly
    pushReversedRLE(target, start, end + 1)
  }

  // Loop until everything is shared.
  while (queue.size() > numShared) {
    let v = queue.deq()
    let flag = flags.get(v)!
    // It should be safe to remove the item from itemType here.

    // console.log('--- ', v, 'flag', flag, 'shared', numShared, 'num', queue.size())
    if (flag == null) throw Error('Invalid type')

    if (flag === DiffFlag.Shared) numShared--

    const e = findEntryContainingRaw(cg, v)
    // console.log(v, e)

    // We need to check if this entry contains the next item in the queue.
    while (!queue.isEmpty() && queue.peek() >= e.version) {
      const v2 = queue.deq()
      const flag2 = flags.get(v2)!
      // console.log('pop', v2, flag2)
      if (flag2 === DiffFlag.Shared) numShared--;

      if (flag2 !== flag) {
        // Mark from v2..v and continue.
        markRun(v2 + 1, v, flag)
        v = v2
        flag = DiffFlag.Shared
      }
    }

    // console.log(e, v, flag)
    markRun(e.version, v, flag)

    for (const p of e.parents) enq(p, flag)
  }

  aOnly.reverse()
  bOnly.reverse()
  return {aOnly, bOnly}
}


export const versionContainsTime = (cg: CausalGraph, frontier: LV[], target: LV): boolean => {
  if (frontier.includes(target)) return true

  const queue = new PriorityQueue<number>()
  for (const v of frontier) if (v > target) queue.enq(v)

  while (queue.size() > 0) {
    const v = queue.deq()
    console.log('deq v')

    if (v === target) {
      return true
    }
    const e = findEntryContainingRaw(cg, v)
    if (e.version <= target) return true

    // Clear any queue items pointing to this entry.
    while (!queue.isEmpty() && queue.peek() >= e.version) {
      queue.deq()
    }

    for (const p of e.parents) {
      if (p === target) return true
      else if (p > target) queue.enq(p)
    }
  }

  return false
}

/**
 * Two versions have one of 4 different relationship configurations:
 * - They're equal (a == b)
 * - They're concurrent (a || b)
 * - Or one dominates the other (a < b or b > a).
 *
 * This method depends on the caller to check if the passed versions are equal
 * (a === b). Otherwise it returns 0 if the operations are concurrent,
 * -1 if a < b or 1 if b > a.
 */
export const compareVersions = (cg: CausalGraph, a: LV, b: LV): number => {
  if (a > b) {
    return versionContainsTime(cg, [a], b) ? -1 : 0
  } else if (a < b) {
    return versionContainsTime(cg, [b], a) ? 1 : 0
  }
  throw new Error('a and b are equal')
}


// ;(() => {
//   const cg = create();

//   add(cg, 'seph', 10, 20, []);
//   add(cg, 'mike', 10, 20, []);
//   assignLocal(cg, 'seph', 5);
//   // console.log(assignLocal(cg, 'mike', 5));
//   // console.log(assignLocal(cg, 'james', 5));
//   // console.log(assignLocal(cg, 'seph', 5))
//   console.dir(cg, {depth: null})


//   console.log(diff(cg, [5, 15], [20]))

//   console.log(summarizeVersion(cg))
// })()