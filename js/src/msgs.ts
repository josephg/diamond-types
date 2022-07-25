import { Operation, Primitive } from "./index.js"

export type WSServerClientMsg = {
  type: 'snapshot',
  data: Primitive
} | {
  type: 'op',
  op: Operation
}

export type WSClientServerMsg = {
  type: 'op',
  op: Operation
}