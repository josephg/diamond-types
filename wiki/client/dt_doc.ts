// This contains the mechanismy code for interacting with diamond types

import { ClientOpts, subscribe } from "@braid-protocol/client"
import { default as init, Doc } from "diamond-types-web"
import { strPosToUni, uniToStrPos } from "unicount"
import { assert, calcDiff, transformPosition, vEq, wait } from "../common/utils"

;(window as any)['Doc'] = Doc

const letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
const randomId = (len = 12) => (
  Array.from(crypto.getRandomValues(new Uint8Array(len)))
    .map(x => letters[x % letters.length])
    .join('')
)

export type Status = 'connecting' | 'connected' | 'waiting'

const empty = () => {}

interface DTOpts {
  setStatus?(s: Status): void,
  setInfo?(info: string): void,
}

export async function subscribeDT(url: string, elem: HTMLTextAreaElement, opts: DTOpts = {}) {
  await init()

  const setStatus = opts.setStatus ?? empty
  const setInfo = opts.setInfo ?? empty

  let placeholder = elem.placeholder
  elem.placeholder = 'Loading...'
  // elem.disabled = true

  // So this is a bit dirty. I want to pull out the document and update
  // server_version each time we get a patch. But I'm not using the headers
  // from braid - instead I'm pulling out the versions from the doc itself
  // (and mergeBytes()).
  const braidOpts: ClientOpts = {
    parseDoc(contentType, data) {
      const id = randomId()
      console.log('parseDoc', id)
      // console.log('data', data)
      let doc = Doc.fromBytes(data as any, id)
      let version = doc.getLocalVersion()
      console.log('v', Array.from(version), 'contents', JSON.stringify(doc.get()))
      console.log([...doc.get()])

      return [doc, version]
    },
    applyPatch([doc, version], patchType, patch) {
      // console.log('applyPatch')
      // console.log('doc', JSON.stringify(Array.from(doc.toBytes())))
      // console.log('patch', JSON.stringify(Array.from(patch)))
      let merge_version = doc.mergeBytes(patch)
      let new_version = doc.mergeVersions(version, merge_version)
      return [doc, new_version]
    }
  }
  setStatus('connecting')
  let braid = await subscribe<[Doc, Uint32Array]>(url, braidOpts)
  elem.placeholder = placeholder
  // elem.disabled = false

  const [doc, initialVersion] = braid.initialValue

  // Ignoring braid's version here, since DT packs its own.

  // This value & version describe what we're currently showing in the browser.
  let last_value = doc.get()
  let last_version = initialVersion

  // The server is sometimes somewhere behind the version we know locally.
  // Note this is imperfect - with the current impl, we're gonna sometimes
  // send redundant data.
  let server_version = last_version
  // console.log('server version', Array.from(server_version))

  const updateInfo = () => {
    setInfo(`${vEq(last_version, server_version) ? 'Up to date' : 'Versions differ!'}

local: ${JSON.stringify([...last_version])}
${JSON.stringify(doc.localToRemoteVersion(last_version))}

server: ${JSON.stringify([...server_version])}
${JSON.stringify(doc.localToRemoteVersion(server_version))}`)
  }
  updateInfo()

  elem.value = last_value

  setStatus('connected')

  const mergeChanges = (new_server_version: Uint32Array) => {
    // Got a remote change!
    //
    // We need to:
    // - Update the contents of the document
    // - And update the user's cursor
    let new_version = doc.getLocalVersion()
    if (vEq(new_version, last_version)) return

    server_version = new_server_version
    // console.log('server version ->', Array.from(server_version))

    let new_value = doc.get()

    let {selectionStart, selectionEnd} = elem

    // For optimization reasons this should be using doc.charsToWchars, but we
    // need to call that before the new content is merged in to doc. TODO!
    selectionStart = strPosToUni(last_value, selectionStart)
    selectionEnd = strPosToUni(last_value, selectionEnd)

    for (const op of doc.xfSince(last_version)) {
      selectionStart = transformPosition(selectionStart, op, true)
      selectionEnd = transformPosition(selectionEnd, op, true)
    }

    // Need to update the value before we set the selection range back out.
    elem.value = last_value = new_value
    last_version = new_version

    elem.setSelectionRange(
      doc.charsToWchars(selectionStart),
      doc.charsToWchars(selectionEnd)
    )

    assert(vEq(doc.getLocalVersion(), last_version))
    updateInfo()
  }

  ;(async () => {
    while (true) {
      for await (const msg of braid.updates) {
        mergeChanges(msg.value![1]);
      }

      console.warn('connection GONE')

      while (true) {
        setStatus('waiting')
        // console.log('Waiting...')
        await wait(3000)
        // console.warn('Reconnecting...')
        setStatus('connecting')

        try {
          braid = await subscribe<[Doc, Uint32Array]>(url, {
            // knownAtVersion: doc.(),
            ...braidOpts,
            knownDoc: [doc, last_version],
            // This is super dirty.
            parseDoc(contentType, data) {
              console.log('parseDoc reconnect')
              // We're getting a new snapshot here, but we can just merge it into
              // the document state.
              let version = doc.mergeBytes(data)

              // If the server has rolled back somehow, update the server.
              server_version = version

              // console.log('v', Array.from(version))
              return [doc, version]
            },
          })

          mergeChanges(braid.initialValue[1])
          console.log('reconnected!')
          setStatus('connected')
          tryFlush()
          break;
        } catch (e) {
          console.warn('Could not reconnect:', e)
        }
      }
    }
  })()

  // I'm going to limit flush() to only allow 1 request in-flight at a time.
  let req_inflight = false
  let req_queued = false

  const actuallyFlush = async () => {
    assert(vEq(doc.getLocalVersion(), last_version))
    let merge_version = last_version
    // console.log('flushing', merge_version, server_version)

    let patch = doc.getPatchSince(server_version)
    // console.log('sending patch', patch, Array.from(server_version), '->', Array.from(merge_version))

    req_inflight = true
    try {
      // Add a delay for demos on /delay! TODO: Take me out
      if (url === '/api/data/wiki/delay') {
        await wait(3000)
      }

      const response = await fetch(url, {
        method: 'PATCH',
        headers: {
          'content-type': 'application/diamond-types',
        },
        redirect: 'follow',
        body: patch,
      })

      if (response.status >= 400) {
        // Bump to error case below.
        throw Error('Network error')
      }

      // This is sort of unnecessary because the server will send us our own patch
      // back anyway. But it should be harmess.
      server_version = doc.mergeVersions(server_version, merge_version)
      // console.log('resp server version ->', Array.from(server_version))
      req_inflight = false
      updateInfo()

      // Flush again
      tryFlush()
    } catch (e) {
      console.error('Error flushing', e)
      req_inflight = false
      // Try again every few seconds.
      if (!req_queued) {
        setTimeout(() => {
          req_queued = false
          tryFlush()
        }, 3000)
        req_queued = true
      }
    }
  }
  function tryFlush() {
    assert(vEq(doc.getLocalVersion(), last_version))
    if (!req_inflight && !vEq(server_version, last_version)) {
      actuallyFlush()
    }
  }

  ;['textInput', 'keydown', 'keyup', 'select', 'cut', 'paste', 'input'].forEach(eventName => {
    elem.addEventListener(eventName, e => {
      setTimeout(() => {
        assert(vEq(doc.getLocalVersion(), last_version))
        let new_value = elem.value
        if (new_value !== last_value) {
          // applyChange(remoteCtx, otdoc.get(), new_value.replace(/\r\n/g, '\n'))
          let {pos, del, ins} = calcDiff(last_value, new_value.replace(/\r\n/g, '\n'))

          if (del > 0) doc.del(pos, del)
          if (ins !== '') doc.ins(pos, ins)
          // console.log('server version', Array.from(server_version))

          last_version = doc.getLocalVersion()
          last_value = new_value

          tryFlush()
          updateInfo()
        }
      }, 0)
    }, false)
  })

}