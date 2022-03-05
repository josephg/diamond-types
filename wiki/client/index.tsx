/* @refresh reload */
import { render } from 'solid-js/web';
import {Signal, createSignal, createEffect, on, onMount} from 'solid-js'
import './index.css';

import {calcDiff, DTOp, transformPosition} from './utils'
import {default as init, Doc} from 'diamond-wasm'
import {subscribe, ClientOpts} from '@braid-protocol/client'
// import * as foo from '@braid-protocol/client'

// console.log(foo)

// import App from './App';

// render(() => <App />, document.getElementById('root') as HTMLElement);

const vEq = (a: Uint32Array, b: Uint32Array): boolean => {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

const docName = 'foo2'
const apiUrl = `/api/data/${docName}`

const Editor = (props: Record<string, any>) => {
  let textarea: HTMLTextAreaElement
  // console.log(props.doc)

  let last_version = props.version()
  let prev_value = ''

  createEffect(on(props.version, () => {
    const doc: Doc = props.doc
    let new_version = doc.getLocalVersion()
    if (vEq(last_version, new_version)) {
      console.log('Doc has not changed')
      return
    }

    console.log('lv', last_version, 'v', new_version)
    // // console.log(props.version())
    // console.log('v', v, doc.getLocalVersion())
    let {selectionStart, selectionEnd} = textarea
    // console.log(textarea.selectionStart, textarea.selectionEnd)
    for (const op of doc.xfSince(last_version)) {
      // console.log(op)
      selectionStart = transformPosition(selectionStart, op, true)
      selectionEnd = transformPosition(selectionEnd, op, true)
    }
    textarea.value = prev_value = doc.get()
    textarea.setSelectionRange(selectionStart, selectionEnd)
    last_version = doc.getLocalVersion()
  }))

  onMount(() => {
    ;['textInput', 'keydown', 'keyup', 'select', 'cut', 'paste', 'input'].forEach(eventName => {
      textarea.addEventListener(eventName, e => {
        setTimeout(() => {
          let new_value = textarea.value
          if (new_value !== prev_value) {
            // applyChange(remoteCtx, otdoc.get(), new_value.replace(/\r\n/g, '\n'))
            let {pos, del, ins} = calcDiff(prev_value, new_value.replace(/\r\n/g, '\n'))
            const doc = props.doc as Doc

            let old_version = doc.getLocalVersion()
            if (del > 0) doc.del(pos, del)
            if (ins !== '') doc.ins(pos, ins)
            let patch = doc.getPatchSince(old_version)
            console.log('sending patch', patch)
            fetch(apiUrl, {
              method: 'POST',
              headers: {
                'content-type': 'application/dt',
              },
              body: patch,
            })

            last_version = doc.getLocalVersion()
            prev_value = new_value
          }
        }, 0)
      }, false)
    })

  })

  return (
    <textarea ref={textarea!} placeholder='Type here yo' autofocus>
      {(props.doc as Doc).get()}
    </textarea>
  )
}

const letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
const randomId = (len = 12) => (
  Array.from(crypto.getRandomValues(new Uint8Array(len)))
    .map(x => letters[x % letters.length])
    .join('')
)


init().then(async () => {
  // console.log(new OpLog().toBytes())

  // let x = await (await fetch('/api/data/foo')).body
  // console.log(x.)

  // let b = await (await fetch('/api/data/foo')).arrayBuffer()
  // console.log(b)

  const braid = await subscribe<Doc>(apiUrl, {
    parseDoc(contentType, data) {
      const id = randomId()
      console.log('parseDoc', id)
      // console.log('data', data)
      return Doc.fromBytes(data as any, id)
    },
    applyPatch(_doc, patchType, patch) {
      // console.log('applyPatch')
      const doc = _doc as Doc
      doc.mergeBytes(patch)
      // console.log(doc.getLocalVersion())
      // console.log(doc.get())
      return doc
    }
  })

  let doc = braid.initialValue as Doc
  console.log('initial value', braid.initialValue)

  let [version, setVersion] = createSignal(doc.getLocalVersion())

  ;(async () => {
    for await (const msg of braid.updates) {
      // console.log('msg', msg.value.get())
      // setDoc(msg.value)
      setVersion(doc.getLocalVersion())
    }
  })()

  render(
    () => <Editor doc={doc} version={version} />,
    document.getElementById('root') as HTMLElement
  )
})