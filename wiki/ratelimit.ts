export default function rateLimit(min_delay: number, fn: () => void) {
  let next_call = 0
  let timer: NodeJS.Timeout | null = null

  return () => {
    let now = Date.now()

    if (next_call <= now) {
      // Just call the function.
      next_call = now + min_delay

      if (timer != null) {
        clearTimeout(timer)
        timer = null
      }
      fn()
    } else {
      // Queue the function call.
      if (timer == null) {
        timer = setTimeout(() => {
          timer = null
          next_call = Date.now() + min_delay
          fn()
        }, next_call - now)
      } // Otherwise its already queued.
    }
  }
}

// let f = rateLimit(300, () => { console.log('called') })

// f()
// f()
// console.log('\n\n')
// f()

// setTimeout(() => {
//   f()
//   f()
// }, 310)