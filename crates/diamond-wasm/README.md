# Diamond types JS wrapper library

This is a javascript + WASM wrapper around [diamond types](https://github.com/josephg/diamond-types).

Note the API is still in flux and will change.

This library is published as two separate modules: `diamond-types-web` and `diamond-types-node`.

> TODO: Fill me in!

Example usage:

```javascript
// Nodejs version:
const {Doc, Branch, OpLog} = require('diamond-types-node')

// console.log(new Doc().getRemoteVersion())

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
```

### Building

```
$ wasm-pack build --target nodejs
```

See example.js for a simple usage example. Note the API is in flux and will change.


# License

ISC