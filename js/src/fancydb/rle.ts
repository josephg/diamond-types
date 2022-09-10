import { LVRange } from "../types"

export const pushRLEList = <T>(list: T[], newItem: T, tryAppend: (a: T, b: T) => boolean) => {
  if (list.length > 0) {
    if (tryAppend(list[list.length - 1], newItem)) return
  }
  list.push(newItem)
}

export const tryRangeAppend = (r1: LVRange, r2: LVRange): boolean => {
  if (r1[1] === r2[0]) {
    r1[1] = r2[1]
    return true
  } else return false
}

export const tryRevRangeAppend = (r1: LVRange, r2: LVRange): boolean => {
  if (r1[0] === r2[1]) {
    r1[0] = r2[0]
    return true
  } else return false
}