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

  let x = [68,77,78,68,84,89,80,83,0,1,224,1,3,221,1,12,52,111,114,55,75,56,78,112,52,109,122,113,12,90,77,80,70,45,69,49,95,116,114,114,74,12,68,80,84,95,104,99,107,75,121,55,102,77,12,82,56,108,87,77,99,112,54,76,68,99,83,12,53,98,78,79,116,82,85,56,120,88,113,83,12,100,85,101,81,83,77,66,54,122,45,72,115,12,50,105,105,80,104,101,116,101,85,107,57,49,12,108,65,71,75,68,90,68,53,108,111,99,75,12,78,113,55,109,65,70,55,104,67,56,52,122,12,116,51,113,52,84,101,121,73,76,85,54,53,12,120,95,120,51,68,95,105,109,81,100,78,115,12,102,120,103,87,90,100,82,111,105,108,73,99,12,115,87,67,73,67,97,78,100,68,65,77,86,12,110,100,56,118,55,74,79,45,114,81,122,45,12,110,85,69,75,69,73,53,81,49,49,45,83,12,120,97,55,121,102,81,88,98,45,120,54,87,12,85,116,82,100,98,71,117,106,57,49,98,49,10,7,12,2,0,0,13,1,4,20,157,2,24,182,1,0,13,174,1,4,120,100,102,120,120,102,100,115,49,120,120,121,122,113,119,101,114,115,100,102,115,100,115,100,97,115,100,115,100,115,100,115,100,97,115,100,97,115,100,113,119,101,119,113,101,119,113,119,107,106,107,106,107,106,107,107,106,107,106,107,108,106,108,107,106,108,107,106,108,107,106,101,101,114,108,106,107,114,101,108,107,116,101,114,116,101,111,114,106,116,111,105,101,106,114,116,111,105,119,106,100,97,98,99,49,49,49,57,49,98,115,110,102,103,104,102,100,103,104,100,102,103,104,100,103,104,100,102,103,104,100,102,103,104,100,107,106,102,108,107,115,100,106,102,108,115,59,107,106,107,108,106,59,107,106,107,106,107,106,59,107,106,108,59,107,106,59,107,108,106,107,106,108,25,2,219,2,21,44,2,3,4,1,6,4,8,1,10,1,12,10,14,1,16,1,18,1,20,4,22,4,24,18,26,99,28,58,30,4,28,1,30,1,32,3,34,2,32,1,34,23,32,39,22,31,81,175,1,21,177,2,239,4,77,169,3,223,6,107,33,79,9,0,26,47,3,0,19,3,18,42,177,1,187,2,43,23,19,211,1,1,1,8,3,10,4,1,8,2,6,8,1,8,22,4,39,96,100,4,142,143,169,235] 
  Doc.fromBytes(x, "asdf")

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
// console.log("history", ops.txns())
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
// ops2.ins(0, "bbb", [-1])
//
// const checkout2 = Checkout.all(ops2)
// console.log(checkout2.get())
// console.log("checkout branch", checkout2.getBranch())

// console.log(ops2.getOps())
// console.log(ops2.txns())

