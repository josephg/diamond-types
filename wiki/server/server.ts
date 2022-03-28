import {default as init, Branch, Doc, OpLog} from 'diamond-wasm'
import {default as polka} from 'polka'
import {default as sirv} from 'sirv'
import {BraidStream, stream as braidStream} from '@braid-protocol/server'
import {default as bodyParser} from 'body-parser'
import fsp from 'fs/promises'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import rateLimit from '../common/ratelimit.js'
import { vEq } from '../common/utils.js'

// process.on('unhandledRejection', (err: Error) => {
//   console.log(err.stack)
//   throw err
// })

const {raw} = bodyParser

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
  single: true,
});

const app = polka().use(assets)

type DocStuff = {
  oplog: OpLog,
  savedVersion: Uint32Array,
  saveThis: () => void,
}

const docs = new Map<string, DocStuff>()
const braid_clients = new Map<string, Set<BraidStream>>()

const dirnameHere = path.dirname(fileURLToPath(import.meta.url))
const dirname = dirnameHere.endsWith('/dist/server')
  ? dirnameHere.slice(0, dirnameHere.length - '/dist/server'.length)
  : dirnameHere

const DATA_DIR = path.resolve(dirname, 'data')

console.log('Storing DT data files in', DATA_DIR)
// fs.mkdirSync(DATA_DIR, {recursive: true})

const getFilename = (name: string): string => {
  if (name.endsWith(path.sep)) {
    name += 'index'
  }
  return path.resolve(DATA_DIR, `${name}.dt`)
}

// const save = async (name: string, oplog: OpLog) => {
//   await fsp.writeFile(getFilename(name), oplog.toBytes())
// }

process.on('exit', () => {
  console.log('flushing!')

  // TODO: Only flush when the file has been changed.
  for (const [name, {oplog, savedVersion}] of docs) {
    if (!vEq(savedVersion, oplog.getLocalVersion())) {
      console.log('saving', name)
      fs.writeFileSync(getFilename(name), oplog.toBytes())
    }
  }
})

process.on('SIGINT', () => {
  // Catching this to
  // console.log('SIGINT!')
  process.exit(1)
})

const getDef = <K,V>(map: Map<K, V>, key: K, fn: (k: K) => V): V => {
  let value = map.get(key)
  if (value == null) {
    value = fn(key)
    map.set(key, value)
  }
  return value
}

const getDefAsync = async <K,V>(map: Map<K, V>, key: K, fn: (k: K) => Promise<V>): Promise<V> => {
  let value = map.get(key)
  if (value == null) {
    value = await fn(key)
    map.set(key, value)
  }
  return value
}

const clientsForDoc = (name: string): Set<BraidStream> => {
  return getDef(braid_clients, name, () => new Set())
}

const loadOplogOrNew = async (name: string): Promise<OpLog> => {
  let filename = getFilename(name)
  try {
    const bytes = await fsp.readFile(filename)
    return OpLog.fromBytes(bytes)
  } catch (e) {
    console.error(e)
    console.log('Creating fresh document for', name)
    return new OpLog()
  }
}

const getDocStuff = async (name: string): Promise<DocStuff> => {
  return getDefAsync(docs, name, async () => {
    const oplog = await loadOplogOrNew(name)
    const stuff = {
      oplog,
      savedVersion: oplog.getLocalVersion(),
      saveThis: rateLimit(1000, async () => {
        console.log('saving', name)
        const filename = getFilename(name)
        await fsp.mkdir(path.dirname(filename), {recursive: true})
        await fsp.writeFile(filename, oplog.toBytes())
        stuff.savedVersion = oplog.getLocalVersion()
      })
      // last_saved: Date.now()
    }
    return stuff
  })
}

const broadcastPatch = (name: string, oplog: OpLog, since_version: Uint32Array) => {
  const clients = clientsForDoc(name)

  const version = JSON.stringify(oplog.getRemoteVersion())
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

// All documents implicitly exist.
const DATA_URL_BASE = '/api/data/'
app.get(`${DATA_URL_BASE}*`, async (req, res, next) => {
  // console.log(req)

  const docName = req.path.slice(DATA_URL_BASE.length)
  if (docName == null || docName == '') return next()

  const {oplog} = await getDocStuff(docName)
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

app.patch(`${DATA_URL_BASE}*`, raw({type: 'application/diamond-types'}), async (req, res, next) => {
  // console.log(res, res.end, res.sendStatus)
  const docName = req.path.slice(DATA_URL_BASE.length)
  if (docName == null || docName == '') return next()

  const stuff = await getDocStuff(docName)
  const {oplog, saveThis} = stuff

  // console.log('body', req.headers, req.body)
  if (req.body == null || !Buffer.isBuffer(req.body)) {
    res.writeHead(406)
    res.end('Invalid data')
    console.log('invalid body', req.body)
    return
  }

  try {
    const patch = req.body as Buffer
    console.log(`got patch ${patch.length} from ${req.socket.remoteAddress}`)

    const vBefore = oplog.getLocalVersion()
    oplog.addFromBytes(patch)
    const vAfter = oplog.getLocalVersion()

    if (!vEq(vBefore, vAfter)) {
      saveThis()
      broadcastPatch(docName, oplog, vBefore)
    }

    res.end()
  } catch (e) {
    console.error(e)
    next(e)
  }
})

app.get('*', clientCode)

// let x = new OpLog()
// const bytes = x.toBytes()
// console.log(bytes)


wasmReady.then(() => {
  app.listen(4321, (err: any) => {
    if (err) throw err

    console.log('listening on port 4321')
  })
})