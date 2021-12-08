const {Console} = require('console')
global.console = new Console({
  stdout: process.stdout,
  stderr: process.stderr,
  inspectOptions: {depth: null}
})

const {Branch, OpLog} = require('./pkg/diamond_wasm_positional.js')

console.log(Branch, OpLog)

const ops = new OpLog()
let t = ops.ins(0, "hi there")
console.log(t)
let t2 = ops.del(3, 3)

console.log("local branch", ops.getLocalFrontier())
console.log("branch", ops.getFrontier())
console.log("ops", ops.toArray())
console.log("history", ops.txns())

console.log("bytes", ops.toBytes())

// const checkout = new Checkout()
// checkout.merge(ops, t)
// console.log('checkout', `"${checkout.get()}"`)
// console.log("checkout branch", checkout.getFrontier())

// const c2 = Checkout.all(ops)
// console.log(c2.get())



// const ops2 = new OpLog()
// ops2.ins(0, "aaa")
// ops2.ins(0, "bbb", [-1])
//
// const checkout2 = Checkout.all(ops2)
// console.log(checkout2.get())
// console.log("checkout branch", checkout2.getBranch())

// console.log(ops2.toArray())
// console.log(ops2.txns())

