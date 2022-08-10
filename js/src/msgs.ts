import { DBSnapshot, Operation, VersionSummary } from "./types.js"

export type WSServerClientMsg = {
  type: 'snapshot',
  version: VersionSummary,
  data: DBSnapshot
} | {
  type: 'op',
  op: Operation
}

export type WSClientServerMsg = {
  type: 'op',
  op: Operation
}