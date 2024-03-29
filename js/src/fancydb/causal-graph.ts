// The causal graph puts a bunch of edits (each at some [agent, seq] version
// pair) into a list.

import PriorityQueue from 'priorityqueuejs'
import bs from 'binary-search'
import {AtLeast1, LV, LVRange, Primitive, RawVersion, ROOT, ROOT_LV, VersionSummary} from '../types.js'
import { pushRLEList, tryRangeAppend, tryRevRangeAppend } from './rle.js'

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
  /** Current global version */
  version: LV[],

  /** Map from localversion -> rawversion */
  entries: CGEntry[],

  /** Map from agent -> list of */
  agentToVersion: {[k: string]: ClientEntry[]},
}

export const create = (): CausalGraph => ({
  entries: [],
  agentToVersion: {},
  version: []
})

/** Sort in ascending order. */
const sortVersions = (v: LV[]): LV[] => v.sort((a, b) => a - b)

export const advanceFrontier = (frontier: LV[], vLast: LV, parents: LV[]): LV[] => {
  // assert(!branchContainsVersion(db, order, branch), 'db already contains version')
  // for (const parent of op.parents) {
  //    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
  // }

  const f = frontier.filter(v => !parents.includes(v))
  f.push(vLast)
  return sortVersions(f)
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

const tryAppendClientEntry = (a: ClientEntry, b: ClientEntry): boolean => {
  const canAppend = b.seq === a.seqEnd
    && b.version === (a.version + (a.seqEnd - a.seq))

  if (canAppend) {
    a.seqEnd = b.seqEnd
  }
  return canAppend
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

export const hasVersion = (cg: CausalGraph, agent: string, seq: number): boolean => (
  findClientEntry(cg, agent, seq) != null
)

export const addRaw = (cg: CausalGraph, id: RawVersion, len: number, rawParents: RawVersion[]): LV => {
  const parents = rawToLVList(cg, rawParents)

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

  pushRLEList(cg.entries, entry, tryAppendEntries)
  pushRLEList(clientEntriesForAgent(cg, agent), { seq: seqStart, seqEnd, version}, tryAppendClientEntry)

  cg.version = advanceFrontier(cg.version, vEnd - 1, parents)
  return version
}

const versionCmp = ([a1, s1]: RawVersion, [a2, s2]: RawVersion) => (
  a1 < a2 ? 1
    : a1 > a2 ? -1
    : s1 - s2
)

export const tieBreakRegisters = <T>(cg: CausalGraph, data: AtLeast1<[LV, T]>): T => {
  let winner = data.reduce((a, b) => {
    // Its a bit gross doing this lookup multiple times for the winning item,
    // but eh. The data set will almost always contain exactly 1 item anyway.
    const rawA = lvToRaw(cg, a[0])
    const rawB = lvToRaw(cg, b[0])

    return versionCmp(rawA, rawB) < 0 ? a : b
  })

  return winner[1]
}

/**
 * Returns [seq, local version] for the new item (or the first item if num > 1).
 */
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

export const lvToRawWithParents = (cg: CausalGraph, v: LV): [string, number, LV[]] => {
  const [e, offset] = findEntryContaining(cg, v)
  const parents = offset === 0 ? e.parents : [v-1]
  return [e.agent, e.seq + offset, parents]
}

export const lvToRaw = (cg: CausalGraph, v: LV): RawVersion => {
  if (v === ROOT_LV) return ROOT
  const [e, offset] = findEntryContaining(cg, v)
  return [e.agent, e.seq + offset]
  // causalGraph.entries[localIndex]
}
export const lvToRawList = (cg: CausalGraph, parents: LV[]): RawVersion[] => (
  parents.map(v => lvToRaw(cg, v))
)


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
export const rawToLVList = (cg: CausalGraph, parents: RawVersion[]): LV[] => (
  parents.map(([agent, seq]) => rawToLV(cg, agent, seq))
)

export const summarizeVersion = (cg: CausalGraph): VersionSummary => {
  const result: VersionSummary = {}
  for (const k in cg.agentToVersion) {
    const av = cg.agentToVersion[k]
    if (av.length === 0) continue

    const versions: [number, number][] = []
    for (const ce of av) {
      pushRLEList(versions, [ce.seq, ce.seqEnd], tryRangeAppend)
    }

    result[k] = versions
  }
  return result
}

// *** TOOLS ***

type DiffResult = {
  // These are ranges. Unlike the rust code, they're in normal
  // (ascending) order.
  aOnly: LVRange[], bOnly: LVRange[]
}

const pushReversedRLE = (list: LVRange[], start: LV, end: LV) => {
  pushRLEList(list, [start, end] as [number, number], tryRevRangeAppend)
}


// Numerical values used by utility methods below.
export const enum DiffFlag { A=0, B=1, Shared=2 }

/**
 * This method takes in two versions (expressed as frontiers) and returns the
 * set of operations only appearing in the history of one version or the other.
 */
export const diff = (cg: CausalGraph, a: LV[], b: LV[]): DiffResult => {
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

  const aOnly: LVRange[] = [], bOnly: LVRange[] = []

  const markRun = (start: LV, endInclusive: LV, flag: DiffFlag) => {
    if (endInclusive < start) throw Error('end < start')

    // console.log('markrun', start, end, flag)
    if (flag == DiffFlag.Shared) return
    const target = flag === DiffFlag.A ? aOnly : bOnly
    pushReversedRLE(target, start, endInclusive + 1)
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

      if (flag2 !== flag) { // Mark from v2..=v and continue.
        // v2 + 1 is correct here - but you'll probably need a whiteboard to
        // understand why.
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
  if (target === ROOT_LV || frontier.includes(target)) return true

  const queue = new PriorityQueue<number>()
  for (const v of frontier) if (v > target) queue.enq(v)

  while (queue.size() > 0) {
    const v = queue.deq()
    // console.log('deq v')

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

const lvEq = (a: LV[], b: LV[]) => (
  a.length === b.length && a.every((val, idx) => b[idx] === val)
)

export function findConflicting(cg: CausalGraph, a: LV[], b: LV[], visit: (range: LVRange, flag: DiffFlag) => void): LV[] {
  // dbg!(a, b);

  // Sorted highest to lowest (so we get the highest item first).
  type TimePoint = {
    v: LV[], // Sorted in inverse order (highest to lowest)
    flag: DiffFlag
  }

  const pointFromVersions = (v: LV[], flag: DiffFlag) => ({
    v: v.length <= 1 ? v : v.slice().sort((a, b) => b - a),
    flag
  })

  // The heap is sorted such that we pull the highest items first.
  // const queue: BinaryHeap<(TimePoint, DiffFlag)> = BinaryHeap::new();
  const queue = new PriorityQueue<TimePoint>((a, b) => {
    for (let i = 0; i < a.v.length; i++) {
      if (b.v.length <= i) return 1
      const c = a.v[i] - b.v[i]
      if (c !== 0) return c
    }
    if (a.v.length < b.v.length) return -1

    return a.flag - b.flag
  })

  queue.enq(pointFromVersions(a, DiffFlag.A));
  queue.enq(pointFromVersions(b, DiffFlag.B));

  // Loop until we've collapsed the graph down to a single element.
  while (true) {
    let {v, flag} = queue.deq()
    // console.log('deq', v, flag)
    if (v.length === 0) return []

    if (v[0] === ROOT_LV) throw Error('Should not happen')

    // Discard duplicate entries.

    // I could write this with an inner loop and a match statement, but this is shorter and
    // more readable. The optimizer has to earn its keep somehow.
    // while queue.peek() == Some(&time) { queue.pop(); }
    while (!queue.isEmpty()) {
      const {v: peekV, flag: peekFlag} = queue.peek()
      // console.log('peek', peekV, v, lvEq(v, peekV))
      if (lvEq(v, peekV)) {
        if (peekFlag !== flag) flag = DiffFlag.Shared
        queue.deq()
      } else break
    }

    if (queue.isEmpty()) return v.reverse()

    // If this node is a merger, shatter it.
    if (v.length > 1) {
      // We'll deal with v[0] directly below.
      for (let i = 1; i < v.length; i++) {
        // console.log('shatter', v[i], 'flag', flag)
        queue.enq({v: [v[i]], flag})
      }
    }

    const t = v[0]
    const containingTxn = findEntryContainingRaw(cg, t)

    // I want an inclusive iterator :p
    const txnStart = containingTxn.version
    let end = t + 1

    // Consume all other changes within this txn.
    while (true) {
      if (queue.isEmpty()) {
        return [end - 1]
      } else {
        const {v: peekV, flag: peekFlag} = queue.peek()
        // console.log('inner peek', peekV, (queue as any)._elements)

        if (peekV.length >= 1 && peekV[0] >= txnStart) {
          // The next item is within this txn. Consume it.
          queue.deq()
          // console.log('inner deq', peekV, peekFlag)

          const peekLast = peekV[0]

          // Only emit inner items when they aren't duplicates.
          if (peekLast + 1 < end) {
            // +1 because we don't want to include the actual merge point in the returned set.
            visit([peekLast + 1, end], flag)
            end = peekLast + 1
          }

          if (peekFlag !== flag) flag = DiffFlag.Shared

          if (peekV.length > 1) {
            // We've run into a merged item which uses part of this entry.
            // We've already pushed the necessary span to the result. Do the
            // normal merge & shatter logic with this item next.
            for (let i = 1; i < peekV.length; i++) {
              // console.log('shatter inner', peekV[i], 'flag', peekFlag)

              queue.enq({v: [peekV[i]], flag: peekFlag})
            }
          }
        } else {
          // Emit the remainder of this txn.
          // console.log('processed txn', txnStart, end, 'flag', flag, 'parents', containingTxn.parents)
          visit([txnStart, end], flag)

          queue.enq(pointFromVersions(containingTxn.parents, flag))
          break
        }
      }
    }
  }
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



type SerializedCGEntryV1 = [
  version: LV,
  vEnd: LV,

  agent: string,
  seq: number, // Seq for version.

  parents: LV[] // Parents for version
]

export interface SerializedCausalGraphV1 {
  version: LV[],
  entries: SerializedCGEntryV1[],
}


export function serialize(cg: CausalGraph): SerializedCausalGraphV1 {
  return {
    version: cg.version,
    entries: cg.entries.map(e => ([
      e.version, e.vEnd, e.agent, e.seq, e.parents
    ]))
  }
}

export function fromSerialized(data: SerializedCausalGraphV1): CausalGraph {
  const cg: CausalGraph = {
    version: data.version,
    entries: data.entries.map(e => ({
      version: e[0], vEnd: e[1], agent: e[2], seq: e[3], parents: e[4]
    })),
    agentToVersion: {}
  }

  for (const e of cg.entries) {
    const len = e.vEnd - e.version
    pushRLEList(clientEntriesForAgent(cg, e.agent), {
      seq: e.seq, seqEnd: e.seq + len, version: e.version
    }, tryAppendClientEntry)
  }

  return cg
}
