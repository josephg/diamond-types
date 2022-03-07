/* @refresh reload */
import { render } from 'solid-js/web';
import {createSignal, onMount} from 'solid-js'
import './index.css';

import { Status, subscribeDT } from './dt_doc';
// import * as foo from '@braid-protocol/client'

// console.log(foo)

// import App from './App';

// render(() => <App />, document.getElementById('root') as HTMLElement);

// const docName = 'foo2'
const docName = `wiki${location.pathname}`
console.log(`Editing ${docName}`)
const apiUrl = `/api/data/${docName}`

const Editor = (props: Record<string, any>) => {
  let textarea: HTMLTextAreaElement

  const [status, setStatus] = createSignal('Loading')

  onMount(() => {
    subscribeDT(apiUrl, textarea, status => {
      console.log('STATUS', status)
      switch (status) {
        case Status.Connected: setStatus('Connected'); break
        case Status.Connecting: setStatus('Connecting'); break
        case Status.Waiting: setStatus('Disconnected!! Waiting to reconnect...'); break
      }
    })
  })

  return (<>
      <div class='status'>{status()}</div>
      <textarea ref={textarea!} placeholder='Type here yo' autofocus>
      </textarea>
    </>)
}


render(
  () => <Editor />,
  document.getElementById('root') as HTMLElement
)
