import {default as init, Doc} from "./pkg/diamond_js.js"

var applyChange = function(ctx, oldval, newval) {
  // Strings are immutable and have reference equality. I think this test is O(1), so its worth doing.
  if (oldval === newval) return;

  var commonStart = 0;
  while (oldval.charAt(commonStart) === newval.charAt(commonStart)) {
    commonStart++;
  }

  var commonEnd = 0;
  while (oldval.charAt(oldval.length - 1 - commonEnd) === newval.charAt(newval.length - 1 - commonEnd) &&
      commonEnd + commonStart < oldval.length && commonEnd + commonStart < newval.length) {
    commonEnd++;
  }

  if (oldval.length !== commonStart + commonEnd) {
    ctx.remove(commonStart, oldval.length - commonStart - commonEnd, newval);
  }
  if (newval.length !== commonStart + commonEnd) {
    ctx.insert(commonStart, newval.slice(commonStart, newval.length - commonEnd), newval);
  }
};


const attach = (elem, ctx) => {
  ctx.prevvalue = ''

  // This function generates operations from the changed content in the textarea.
  const genOp = () => {
    // In a timeout so the browser has time to propogate the event's changes to the DOM.
    setTimeout(() => {
      let newvalue = elem.value;
      if (newvalue !== ctx.prevvalue) {
        applyChange(ctx, ctx.prevvalue, newvalue.replace(/\r\n/g, '\n'));
        ctx.prevvalue = newvalue
      }
    }, 0);
  };

  var eventNames = ['textInput', 'keydown', 'keyup', 'select', 'cut', 'paste'];
  for (var i = 0; i < eventNames.length; i++) {
    var e = eventNames[i];
    elem.addEventListener(e, genOp, false);
  }
  genOp()

  ctx.onChange = newvalue => {
    ctx.prevvalue = newvalue
    elem.value = newvalue
  }
}

;(async () => {
  await init()

  // console.log(doc.get())

  // Just the first one for now.

  let textElems = document.querySelectorAll('textarea')
  let infoElems = document.querySelectorAll('.info')
  let docs = [new Doc('one'), new Doc('two')]
  let ctxs = []

  // const refresh = () => {
  //   textElems[0].value = docs[0].get()
  //   textElems[1].value = docs[1].get()
  // }

  for(let i = 0; i < 2; i++) {
    let doc = docs[i]

    let ctx = {
      remove(pos, len, newval) {
        console.log('remove', pos, len)
        doc.del(pos, len)
        this.update(newval)
      },
      insert(pos, content, newval) {
        console.log('insert', pos, content);
        doc.ins(pos, content)
        this.update(newval)
      },
      reset() {
        doc.free()
        doc = docs[i] = new Doc(['one', 'two'][i])
        this.render()
      },
      render() {
        this.onChange(doc.get())
        this.update()
      },
      update(expectedText) {
        if (expectedText != null && doc.get() !== expectedText) {
          console.error("Document content does not match")
          // this.onChange(doc.get())
        }

        // console.log(doc.get_txn_since())
        let remoteOps = doc.get_txn_since()
        // info.textContent = JSON.stringify(remoteOps, null, 2)
        let listEntries = doc.get_internal_list_entries()
        infoElems[i].textContent = `Internal entries (by space) ${JSON.stringify(listEntries, null, 2)}

Vector clock ${JSON.stringify(doc.get_vector_clock(), null, 2)}

Remote IDs (by time) ${JSON.stringify(remoteOps, null, 2)}
        `
      }
    }
    ctxs[i] = ctx
    attach(textElems[i], ctx)
    ctx.render()
  }
  window.docs = docs

  const replicate = (dest_idx, src_idx) => {
    let v = docs[dest_idx].get_vector_clock()
    let changes = docs[src_idx].get_txn_since(v)
    docs[dest_idx].merge_remote_txns(changes)
    ctxs[dest_idx].render()
  }

  let leftBtn = document.querySelector('#pushLeft')
  leftBtn.onclick = () => replicate(0, 1)
  let rightBtn = document.querySelector('#pushRight')
  rightBtn.onclick = () => replicate(1, 0)

  let resetBtn = document.querySelector('#reset')
  resetBtn.onclick = () => {
    ctxs[0].reset()
    ctxs[1].reset()
  }

})()
