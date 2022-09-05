import * as dt from './simpledb.js'
import { WSServerClientMsg } from './msgs.js'
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
    pendingOps = inflightOps.concat(pendingOps)
    inflightOps.length = 0
  }

  ws.onmessage = (e) => {
    // console.log('msg', e.data)

    const data = JSON.parse(e.data) as WSServerClientMsg
  
    // console.log('data', data)
  
    switch (data.type) {
      case 'snapshot': {
        let changed = true
        if (db == null) {
          db = dt.fromSnapshot(data.data)
        } else {
          changed = dt.mergeSnapshot(db, data.data, data.v, inflightOps.concat(pendingOps))
        }

        if (changed) {
          console.log('version changed. Rerendering.')
          rerender()
        }

        flush()
        break
      }
      case 'ops': {
        let anyChange = false
        for (const op of data.ops) {
          if (inflightOps.find(op2 => dt.versionEq(op.id, op2.id)) == null) {
            dt.applyRemoteOp(db!, op)
            anyChange = true
          }
        }

        // console.log(anyChange, 'if', inflightOps, 'pending', pendingOps)
        if (anyChange) rerender()

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

let flushing = false
const flush = () => {
  if (!flushing && ws != null && inflightOps.length === 0 && pendingOps.length > 0) {
    // console.log('flush!')
    flushing = true
    inflightOps.push(...pendingOps)
    pendingOps.length = 0

    ;(async () => {
      try {
        // console.log('Sending ops')
        const response = await fetch('/op', {
          method: 'post',
          cache: 'no-store',
          headers: {
            'content-type': 'application/json',
          },
          body: JSON.stringify(inflightOps)
        })
        // console.log('Got response', response.status, response.statusText)
        flushing = false

        const status = response.status
        if (status < 400) {
          // console.log('ok')
          inflightOps.length = 0
          flush()
        } else {
          console.error('Could not submit op:', status, response.statusText)
          setTimeout(flush, 3000)
        }
      } catch (e) {
        console.log('Could not submit op:', e)
        flushing = false
        setTimeout(flush, 3000)
      }
    })()
  }
}

const editTime = (newVal: number) => {
  const op = dt.setAtPath(db!, agent(), ['time'], {type: 'primitive', val: newVal})
  pendingOps.push(op)
  flush()
  // ws?.send(JSON.stringify(msg))

  rerender()
}

incrButton.onclick = () => {
  const oldVal = dt.getAtPath(db!, ['time']) as number ?? 0
  editTime(oldVal + 1)
}
decrButton.onclick = () => {
  const oldVal = dt.getAtPath(db!, ['time']) as number ?? 0
  editTime(oldVal - 1)
}
