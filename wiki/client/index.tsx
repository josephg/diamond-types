/* @refresh reload */
import { render, Show } from 'solid-js/web';
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

  const [status, setStatus] = createSignal('')

  onMount(() => {
    subscribeDT(apiUrl, textarea, status => {
      console.log('STATUS', status)
      switch (status) {
        case 'connected': setStatus(''); break
        case 'connecting': setStatus('Connecting'); break
        case 'waiting': setStatus('Disconnected!! Waiting to reconnect...'); break
      }
    })
  })

  return (<>
      <div id='statusContainer'>
        <Show when={status() !== ''}>
          <div class='status'>
            {status()}
          </div>
        </Show>
      </div>
      <textarea ref={textarea!} placeholder='Type here yo' autofocus>
      </textarea>
    </>)
}


render(
  () => <Editor />,
  document.getElementById('root') as HTMLElement
)
