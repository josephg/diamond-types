/* @refresh reload */
import { render } from 'solid-js/web';
import {Signal, createSignal, createEffect, on, onMount} from 'solid-js'
import './index.css';

import {calcDiff, DTOp, transformPosition} from './utils'
import {default as init, Doc} from 'diamond-wasm'
import {subscribe, ClientOpts} from '@braid-protocol/client'
import {strPosToUni, uniToStrPos} from 'unicount'
import { subscribeDT } from './dt_doc';
// import * as foo from '@braid-protocol/client'

// console.log(foo)

// import App from './App';

// render(() => <App />, document.getElementById('root') as HTMLElement);

const docName = 'foo2'
const apiUrl = `/api/data/${docName}`

const Editor = (props: Record<string, any>) => {
  let textarea: HTMLTextAreaElement

  onMount(() => {
    subscribeDT(apiUrl, textarea)
  })

  return (
    <textarea ref={textarea!} placeholder='Type here yo' autofocus>
    </textarea>
  )
}


render(
  () => <Editor />,
  document.getElementById('root') as HTMLElement
)
