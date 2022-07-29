import * as dt from './simpledb.js'
import { WSClientServerMsg, WSServerClientMsg } from './msgs.js'
import { ROOT } from './types.js'
import { createAgent } from './utils.js'

const agent = createAgent()
let db: null | dt.SimpleDB = null

let ws: WebSocket | null = null

const updateBrowser = () => {
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
        db = dt.fromSnapshot(data.data)
        console.log('got db')
        updateBrowser()
        break
      }
      case 'op': {
        dt.applyRemoteOp(db!, data.op)
        updateBrowser()
  
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

const editTime = (newVal: number) => {
  const op = dt.localMapInsert(db!, agent(), ROOT, 'time', {type: 'primitive', val: newVal})
  const msg: WSClientServerMsg = {
    type: 'op',
    op
  }
  ws?.send(JSON.stringify(msg))

  updateBrowser()
}

incrButton.onclick = () => {
  const dbVal = dt.get(db!) as any
  editTime((dbVal.time ?? 0) + 1)
}
decrButton.onclick = () => {
  const dbVal = dt.get(db!) as any
  editTime((dbVal.time ?? 0) - 1)
}