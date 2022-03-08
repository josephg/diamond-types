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

const connectionMsg: {[K in Status]: string} = {
  connected: 'Connected',
  connecting: 'Connecting...',
  waiting: 'Disconnected!! Waiting to reconnect...'
}

const Editor = (props: Record<string, any>) => {
  let textarea: HTMLTextAreaElement

  const [status, setStatus] = createSignal<Status>('connecting')
  const [info, setInfo] = createSignal<string>('Loading...')
  const [showInfo, setShowInfo] = createSignal<boolean>(false)
  const toggleShow = () => setShowInfo(!showInfo())
  // const toggleShow = () => {
  //   console.log('xxx')
  //   setShowInfo(true)
  // }

  onMount(() => {
    subscribeDT(apiUrl, textarea, {
      setStatus(status) {
        console.log('STATUS', status)
        setStatus(status)
      },
      setInfo
    })
  })

  return (<>
      <div id='statusContainer'>
        <div class={status()}></div>
      </div>

      <textarea ref={textarea!} placeholder='Type here yo' autofocus></textarea>

      <Show when={showInfo()}>
        <div id='info'>{info()}</div>
      </Show>
      <button id='showInfo' onClick={toggleShow}>info</button>
    </>)
}


render(
  () => <Editor />,
  document.getElementById('root') as HTMLElement
)
