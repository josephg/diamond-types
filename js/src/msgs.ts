import { DBSnapshot, Operation, VersionSummary } from "./types.js"

export type WSServerClientMsg = {
  type: 'snapshot',
  data: DBSnapshot,
  v: VersionSummary
} | {
  type: 'op',
  ops: Operation[]
}
