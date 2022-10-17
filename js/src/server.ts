import * as dt from './fancydb'
import polka from 'polka'
import * as bodyParser from 'body-parser'
import sirv from 'sirv'
import {WebSocket, WebSocketServer} from 'ws'
import * as http from 'http'
import { WSServerClientMsg } from './msgs.js'
import { Operation, ROOT_LV } from './types.js'
import { createAgent, rateLimit } from './utils.js'
import fs from 'fs'
import { hasVersion, summarizeVersion } from './fancydb/causal-graph'

const app = polka()
.use(sirv('public', {
  dev: true
}))

const DB_FILE = process.env['DB_FILE'] || 'db.dtjson'

const db = (() => {
  try {
    const bytes = fs.readFileSync(DB_FILE, 'utf8')
    const json = JSON.parse(bytes)
    return dt.fromSerialized(json)
  } catch (e: any) {
    if (e.code !== 'ENOENT') throw e

    console.log('Using new database file')
    return dt.createDb()
  }
})()

console.dir(dt.get(db), {depth: null})

const saveDb = rateLimit(100, () => {
  // console.log('saving')
  const json = dt.serialize(db)
  const bytes = JSON.stringify(json, null, 2)
  // return fs.promises.writeFile(DB_FILE, bytes)
  return fs.writeFileSync(DB_FILE, bytes)
})

db.onop = (db, op) => saveDb()

process.on('exit', () => {
  saveDb.flushSync()
})

process.on('SIGINT', () => {
  // Catching this to make sure we save!
  // console.log('SIGINT!')
  process.exit(1)
})

const clients = new Set<WebSocket>()

const broadcastOp = (ops: Operation[], exclude?: any) => {
  console.log('broadcast', ops)
  const msg: WSServerClientMsg = {
    type: 'ops',
    ops
  }

  const msgStr = JSON.stringify(msg)
  for (const c of clients) {
    // if (c !== exclude) {
    c.send(msgStr)
    // }
  }
}

if (dt.get(db).time == null) {
  console.log('Setting time = 0')
  const serverAgent = createAgent()
  dt.localMapInsert(db, serverAgent(), ROOT_LV, 'time', {type: 'primitive', val: 0})
}

app.post('/op', bodyParser.json(), (req, res, next) => {
  let ops = req.body as Operation[]
  console.log(`Got ${ops.length} from client`)

  ops = ops.filter(op => !hasVersion(db.cg, op.id[0], op.id[1]))
  ops.forEach(op => dt.applyRemoteOp(db, op))
  broadcastOp(ops)

  res.end()
})

const server = http.createServer(app.handler as any)
const wss = new WebSocketServer({server})

wss.on('connection', ws => {
  // console.dir(dt.toJSON(db), {depth: null})
  const msg: WSServerClientMsg = {
    type: 'snapshot',
    data: dt.toSnapshot(db),
    v: summarizeVersion(db.cg),
  }
  ws.send(JSON.stringify(msg))
  clients.add(ws)

  ws.on('message', (msgBytes) => {
    // const rawJSON = msgBytes.toString('utf-8')
    // const msg = JSON.parse(rawJSON) as WSClientServerMsg
    // // console.log('msg', msg)
    // switch (msg.type) {
    //   case 'op': {
    //     console.log(msg)
    //     msg.ops.forEach(op => dt.applyRemoteOp(db, op))
    //     broadcastOp(msg.ops, ws)
    //     break
    //   }
    // }
  })

  ws.on('close', () => {
    console.log('client closed')
    clients.delete(ws)
  })
})

server.listen(3003, () => {
  console.log('listening on port 3003')
})
