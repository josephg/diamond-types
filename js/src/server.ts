import * as dt from './simpledb.js'
import polka from 'polka'
import * as bodyParser from 'body-parser'
import sirv from 'sirv'
import {WebSocket, WebSocketServer} from 'ws'
import * as http from 'http'
import { WSClientServerMsg, WSServerClientMsg } from './msgs.js'
import { Operation, ROOT } from './types.js'

const app = polka()
.use(sirv('public', {
  dev: true
}))


const db = dt.createDb()

const clients = new Set<WebSocket>()

const broadcastOp = (op: Operation, exclude?: any) => {
  const msg: WSServerClientMsg = {
    type: 'op',
    op
  }

  const msgStr = JSON.stringify(msg)
  for (const c of clients) {
    if (c !== exclude) {
      c.send(msgStr)
    }
  }
}

const serverAgent = dt.createAgent()
dt.localMapInsert(db, serverAgent(), ROOT, 'time', {type: 'primitive', val: 0})

// setInterval(() => {
//   const val = (Math.random() * 100)|0
//   const op = dt.localMapInsert(db, serverAgent(), dt.ROOT, 'time', {type: 'primitive', val})
//   broadcastOp(op)
// }, 1000)

app.post('/db', bodyParser.json(), (req, res, next) => {
  console.log('body', req.body)
  res.end('<h1>hi</h1>')
})

const server = http.createServer(app.handler as any)
const wss = new WebSocketServer({server})

wss.on('connection', ws => {
  ws.send(JSON.stringify({type: 'snapshot', data: dt.toJSON(db)}))
  clients.add(ws)

  ws.on('message', (msgBytes) => {
    const rawJSON = msgBytes.toString('utf-8')
    const msg = JSON.parse(rawJSON) as WSClientServerMsg
    // console.log('msg', msg)
    switch (msg.type) {
      case 'op': {
        dt.applyRemoteOp(db, msg.op)
        broadcastOp(msg.op, ws)
        break
      }
    }
  })

  ws.on('close', () => {
    console.log('client closed')
    clients.delete(ws)
  })
})

server.listen(3003, () => {
  console.log('listening on port 3003')
})
