import { RawVersion, VersionSummary } from "./types"

export function createAgent(): () => RawVersion {
  const agent = Math.random().toString(36).slice(2)
  let seq = 0
  return () => ([agent, seq++])
}

type RateLimit = {
  force(): void,
  (): void,
}

export function rateLimit(min_delay: number, fn: () => void): RateLimit {
  let next_call = 0
  let timer: NodeJS.Timeout | null = null

  const rl = () => {
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

  rl.force = () => {
    if (timer != null) {
      clearTimeout(timer)
      timer = null
      fn()
    }
  }

  return rl
}

export const versionInSummary = (vs: VersionSummary, [agent, seq]: RawVersion): boolean => {
  const ranges = vs[agent]
  if (ranges == null) return false
  // This could be implemented using a binary search, but thats probably fine here.
  return ranges.find(([from, to]) => seq >= from && seq < to) !== undefined
}
