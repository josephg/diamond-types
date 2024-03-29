import {Console} from 'console'
import fs from 'fs'

import {default as init, Branch, Doc, OpLog} from './pkg/diamond_wasm.js'
// import * as x from './pkg/diamond_wasm.js'

global.console = new Console({
  stdout: process.stdout,
  stderr: process.stderr,
  inspectOptions: {depth: null}
})

const bytes = fs.readFileSync('pkg/diamond_wasm_bg.wasm')
const wasmModule = new WebAssembly.Module(bytes)
const wasmReady = init(wasmModule)



;(async () => {

  await wasmReady
  console.log('wasm init ok')

  let oplog = new OpLog("seph")
  oplog.ins(0, "hi there")

  let oplog2 = oplog.clone()

  let v = oplog.getLocalVersion()
  console.log('v', v, oplog.localToRemoteVersion(v))
  oplog.del(1, 2)
  let patch = oplog.getPatchSince(v)

  console.log('patch', patch)

  let result_v = oplog2.addFromBytes(patch)
  console.log('mergebytes returned', result_v)
  console.log(oplog.getOps())
  console.log(oplog2.getOps())

  console.log(oplog2.localToRemoteVersion([2, 3]))

  // let oplog3 = new OpLog()
  // oplog3.apply_op({ tag: 'Ins', start: 0, end: 8, fwd: true, content: 'yooo' })
  // console.log(oplog3.getOps())


  // console.log(new OpLog().toBytes())

  console.log('\n\n')
  let oplog3 = new OpLog()
  oplog3.setAgent('b')
  oplog3.ins(0, 'BBB', [])
  oplog3.setAgent('a')
  oplog3.ins(0, 'AAA', [])
  console.log('ops', oplog3.getOps())
  // console.log(oplog3.getXFSince([]))
  console.log('xf ops', oplog3.getXF())
  console.log("history", oplog3.getHistory())

  console.log('\n\n')
  let oplog4 = new OpLog('seph')
  let t = oplog4.ins(0, 'aaa')
  // And double delete
  oplog4.setAgent('a')
  oplog4.del(0, 2, [t])
  oplog4.setAgent('b')
  oplog4.del(1, 2, [t])
  console.log('ops', oplog4.getOps())
  // console.log(oplog4.getXFSince([]))
  console.log('xf ops', oplog4.getXF())

  console.log("history", oplog4.getHistory())
})()


// const {Console} = require('console')

// const {Branch, OpLog} = import('./pkg/diamond_wasm.js')

// console.log(Branch, OpLog)
//
// const ops = new OpLog()
// let t = ops.ins(0, "hi there")
// console.log(t)
// let t2 = ops.del(3, 3)
//
// console.log("local branch", ops.getLocalVersion())
// console.log("frontier", ops.getFrontier())
// console.log("ops", ops.getOps())
// console.log("history", ops.getHistory())
//
// console.log("bytes", ops.toBytes())
//
// oplog2 = OpLog.fromBytes(ops.toBytes())
// console.log("ops", oplog2.getOps())


// const branch = new Branch()
// branch.merge(ops, t)
// console.log('branch', `"${branch.get()}"`)
// console.log("branch branch", branch.getFrontier())

// const c2 = Checkout.all(ops)
// console.log(c2.get())



// const ops2 = new OpLog()
// ops2.ins(0, "aaa")
// ops2.ins(0, "bbb", [])
//
// const checkout2 = Checkout.all(ops2)
// console.log(checkout2.get())
// console.log("checkout branch", checkout2.getBranch())

// console.log(ops2.getOps())
// console.log(ops2.getHistory())

