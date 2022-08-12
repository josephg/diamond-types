import * as dt from './simpledb.js'
import { WSClientServerMsg, WSServerClientMsg } from './msgs.js'
import { Operation, ROOT } from './types.js'
import { createAgent, versionInSummary } from './utils.js'

const agent = createAgent()
let db: null | dt.SimpleDB = null

let ws: WebSocket | null = null

let inflightOps: Operation[] = []
let pendingOps: Operation[] = []

const rerender = () => {
  const dbVal = dt.get(db!) as any
  const clockElem = document.getElementById('clock')
  clockElem!.textContent = dbVal.time

  const rawElem = document.getElementById('raw')
  rawElem!.innerText = `
RAW: ${JSON.stringify(dbVal, null, 2)}

Internal: ${JSON.stringify(dt.toSnapshot(db!), null, 2)}
`
}

const connect = () => {
  const loc = window.location
  const url = (loc.protocol === 'https:' ? 'wss://' : 'ws://')
    + loc.host
    + loc.pathname
    + 'ws'
  console.log('url', url)
  ws = new WebSocket(url)
  ws.onopen = (e) => {
    console.log('open', e)
  }

  ws.onmessage = (e) => {
    // console.log('msg', e.data)
  
    const data = JSON.parse(e.data) as WSServerClientMsg
  
    console.log('data', data)
  
    switch (data.type) {
      case 'snapshot': {
        const oldV = db?.version
        db = dt.fromSnapshot(data.data)
        console.log('got db')
        console.log(data.v)

        const ops = inflightOps
          .filter(op => !versionInSummary(data.v, op.id))
          // Pending changes were never sent to the server, so they can't be in the summary.
          .concat(pendingOps)

        for (const op of ops) {
            // Reapply our local operation onto the database
            dt.applyRemoteOp(db, op)
        }
        
        pendingOps = ops
        flush()

        if (oldV == null || !dt.frontierEq(oldV, db.version)) {
          console.log('version changed. Rerendering.', oldV, db.version)
          rerender()
        }
        break
      }
      case 'op': {
        let anyChange = false
        for (const op of data.ops) {
          const idx = inflightOps.findIndex(op2 => dt.versionEq(op.id, op2.id))
          if (idx >= 0) {
            // This is an acknowledgement of a local operation. Discard it.
            inflightOps.splice(idx, 1)
          } else {
            dt.applyRemoteOp(db!, op)
            anyChange = true
          }
        }
        console.log('if', inflightOps, 'pending', pendingOps)

        if (anyChange) rerender()

        flush()
  
        break
      }
    }
  }
  
  ws.onclose = (e) => {
    console.log('WS closed', e)
    ws = null
    setTimeout(() => {
      connect()
    }, 3000)
  }

  ws.onerror = (e) => {
    console.error('WS error', e)
  }
}

connect()

const decrButton = document.getElementById('decrement')!
const incrButton = document.getElementById('increment')!

const flush = () => {
  if (ws != null && inflightOps.length === 0 && pendingOps.length > 0) {
    // console.log('flush!')
    inflightOps.push(...pendingOps)
    pendingOps.length = 0

    const msg: WSClientServerMsg = {
      type: 'op',
      ops: inflightOps
    }

    ws.send(JSON.stringify(msg))
    // setTimeout(() => {
    //   ws?.send(JSON.stringify(msg))
    // }, 2000)
  }
}

const editTime = (newVal: number) => {
  const op = dt.localMapInsert(db!, agent(), ROOT, 'time', {type: 'primitive', val: newVal})
  pendingOps.push(op)
  flush()
  // ws?.send(JSON.stringify(msg))

  rerender()
}

incrButton.onclick = () => {
  const dbVal = dt.get(db!) as any
  editTime((dbVal.time ?? 0) + 1)
}
decrButton.onclick = () => {
  const dbVal = dt.get(db!) as any
  editTime((dbVal.time ?? 0) - 1)
}