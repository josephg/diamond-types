import {default as init, Branch, Doc, OpLog} from 'diamond-wasm'
import {default as polka} from 'polka'
import {default as sirv} from 'sirv'
import {BraidStream, stream as braidStream} from '@braid-protocol/server'
import {default as bp} from 'body-parser'
import fs from 'fs'

const {raw} = bp

// This works if we add --experimental-import-meta-resolve
// console.log(import.meta.resolve!('diamond-wasm/diamond_wasm_bg.wasm'))
const bytes = fs.readFileSync('node_modules/diamond-wasm/diamond_wasm_bg.wasm')
const wasmModule = new WebAssembly.Module(bytes)
const wasmReady = init(wasmModule)
wasmReady.then(() => {
  console.log('wasm init ok')
  // console.log(new OpLog().toBytes())
})

const assets = sirv('public', {
  // maxAge: 31536000, // 1Y
  // immutable: true,
  brotli: true,
  dev: process.env.NODE_ENV !== 'production',
});
const clientCode = sirv('dist-client', {
  // maxAge: 31536000, // 1Y
  // immutable: true,
  brotli: true,
  dev: process.env.NODE_ENV !== 'production',
});

const app = polka().use(assets).use(clientCode)

const docs = new Map<string, OpLog>()
const braid_clients = new Map<string, Set<BraidStream>>()

const getDef = <K,V>(map: Map<K, V>, key: K, fn: (k: K) => V): V => {
  let value = map.get(key)
  if (value == null) {
    value = fn(key)
    map.set(key, value)
  }
  return value
}

const clientsForDoc = (name: string): Set<BraidStream> => {
  return getDef(braid_clients, name, () => new Set())
}

const getOpLog = async (name: string): Promise<OpLog> => {
  const oplog = getDef(docs, name, () => new OpLog())
  return oplog
  // const doc = new OpLog()
  // const bytes = doc.toBytes()
  // return bytes
}

const broadcastPatch = (name: string, oplog: OpLog, since_version: Uint32Array) => {
  const clients = clientsForDoc(name)


  const version = JSON.stringify(oplog.getFrontier())
  const patch = oplog.getPatchSince(since_version)

  console.log(`broadcasting ${patch.length} bytes to ${name} (${clients.size} peers)`)

  for (const c of clients) {
    // TODO: This is resending the document every time!
    c.append({
      version,
      patches: [patch],
    })
  }
}

// setInterval(async () => {
//   const doc = await getOpLog('foo')
//   // const clients = clientsForDoc('foo')

//   const before = doc.getLocalVersion()
//   doc.ins(0, Math.random() > 0.5 ? 'x' : '_')
//   broadcastPatch('foo', doc, before)

//   // // console.log('Appended to foo', patch.length)
//   // const version = JSON.stringify(doc.getFrontier())
//   // for (const c of clients) {
//   //   // TODO: This is resending the document every time!
//   //   c.append({
//   //     version,
//   //     patches: [patch],
//   //   })
//   // }
// }, 1000)

// All documents implicitly exist.
const DATA_URL_BASE = '/api/data/'
app.get(`${DATA_URL_BASE}*`, async (req, res, next) => {
  // console.log(req)

  const docName = req.path.slice(DATA_URL_BASE.length)
  if (docName == null || docName == '') return next()

  const oplog = await getOpLog(docName)
  const data = oplog.toBytes()
  const clients = clientsForDoc(docName)

  const stream = braidStream(res, {
    reqHeaders: req.headers,
    initialValue: data,
    contentType: 'application/diamond-types',
    patchType: 'application/diamond-types',
    onclose() {
      console.log('close 1')
      if (stream) {
        console.log('stream closed', req.socket.remoteAddress)
        clients.delete(stream)
      }
    },
  })
  if (stream) {
    console.log('added stream', req.socket.remoteAddress)
    clients.add(stream)
  } else {
    console.log('sent initial data to', req.socket.remoteAddress)
  }

  // res.end(`oh hai ${path}`)
})

app.post(`${DATA_URL_BASE}*`, raw({type: 'application/dt'}), async (req, res, next) => {
  const docName = req.path.slice(DATA_URL_BASE.length)
  if (docName == null || docName == '') return next()

  const oplog = await getOpLog(docName)
  // console.log('body', req.headers, req.body)
  if (req.body == null || !Buffer.isBuffer(req.body)) {
    res.sendStatus(406)
    res.end('Invalid data')
    console.log('invalid body', req.body)
  }

  const patch = req.body as Buffer
  console.log(`got patch ${patch.length} from ${req.socket.remoteAddress}`)

  const vBefore = oplog.getLocalVersion()
  try {
    oplog.mergeBytes(patch)
  } catch (e) {
    console.error(e)
    next(e)
  }

  broadcastPatch(docName, oplog, vBefore)
  res.end()
})


// let x = new OpLog()
// const bytes = x.toBytes()
// console.log(bytes)


wasmReady.then(() => {
  app.listen(4321, (err: any) => {
    if (err) throw err

    console.log('listening on port 4321')
  })
})