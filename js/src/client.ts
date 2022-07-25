import * as dt from './index.js'
import { WSClientServerMsg, WSServerClientMsg } from './msgs.js'

const agent = dt.createAgent()
let db: null | dt.DBState = null

let ws: WebSocket | null = null

const updateBrowser = () => {
  const dbVal = dt.get(db!) as any
  const clockElem = document.getElementById('clock')
  clockElem!.textContent = dbVal.time

  const rawElem = document.getElementById('raw')
  rawElem!.innerText = `
RAW: ${JSON.stringify(dbVal, null, 2)}



Internal: ${JSON.stringify(dt.toJSON(db!), null, 2)}
`
}

const connect = () => {
  ws = new WebSocket('ws://' + window.location.host + window.location.pathname + 'ws')
  ws.onopen = (e) => {
    console.log('open', e)
  }

  ws.onmessage = (e) => {
    // console.log('msg', e.data)
  
    const data = JSON.parse(e.data) as WSServerClientMsg
  
    console.log('data', data)
  
    switch (data.type) {
      case 'snapshot': {
        db = dt.fromJSON(data.data)
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
}

connect()

const decrButton = document.getElementById('decrement')!
const incrButton = document.getElementById('increment')!

const editTime = (newVal: number) => {
  const op = dt.localMapInsert(db!, agent(), dt.ROOT, 'time', {type: 'primitive', val: newVal})
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