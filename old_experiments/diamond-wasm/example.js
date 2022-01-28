const {uniToStrPos} = require('unicount')
const {Console} = require('console')
global.console = new Console({
  stdout: process.stdout,
  stderr: process.stderr,
  inspectOptions: {depth: null}
})

const traversal_to_op = t => {
  let {content, traversal, components} = t

  let map_component = c => {
    // console.log(c)
    if (c.Retain != null) {
      return c.Retain

    } else if (c.Ins != null) {
      // console.log(c)
      if (c.Ins.content_known) {

        let pos = uniToStrPos(content, c.Ins.len)
        let start = content.substring(0, pos)
        content = content.substring(pos)

        return start
      } else {
        return 'x'.repeat(c.Ins.len)
      }

    } else if (c.Del != null) {
      return {d: c.Del}

    } else {
      console.error('component', c)
      throw Error('Invalid component')
    }
  }

  return traversal != null
      ? traversal.map(map_component)
      : components.map(t => t.map(map_component))
}

const {Doc} = require('./pkg/diamond_js.js')
const d = new Doc("aaaa")

d.ins(0, "hi there")
d.del(3, 5) // "hi "
d.ins(3, "everyone!")

console.log(`doc content '${d.get()}'`)
console.log('order', d.get_next_order())

console.log('all traversal changes', traversal_to_op(d.traversal_ops_flat(0)))
console.log('all traversal changes', traversal_to_op(d.traversal_ops_since(0)))

// const all_txns = d.get_txn_since()
// console.log('all txns', all_txns)


// console.log('positional changes', d.positional_ops_since(0))
// console.log('all traversal changes flat', traversal_to_op(d.traversal_ops_flat(8)))

// console.log('some traversal changes', traversal_to_op(d.traversal_ops_since_branch([
//   {agent: 'aaaa', seq: 8}
// ])))

// const [patches, attr] = d.attributed_patches_since(0)
// console.log('patches since 0', traversal_to_op(patches))
// console.log('by', attr)

d.ins_at_order(0, "QQQQQ", 0, true)
console.log(d.get())

















// const version = d.get_vector_clock()
// console.log('vector clock', version)

// const all_txns = d.get_txn_since()
// console.log('all txns', all_txns)

// const d2 = new Doc('bbbb')
// d2.ins(0, 'yoooo')
// d2.merge_remote_txns(all_txns)
// console.log(`resulting document '${d2.get()}'`) // 'hi there'

// const frontier = d.get_frontier()
// console.log('frontier', frontier)
