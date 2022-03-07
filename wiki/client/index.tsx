/* @refresh reload */
import { render } from 'solid-js/web';
import {onMount} from 'solid-js'
import './index.css';

import { subscribeDT } from './dt_doc';
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
