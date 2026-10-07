import { useSyncExternalStore } from 'react'
import { ApiError } from '../api'

// The console's live state: paused (space), stale (the heartbeat query is failing: "console
// offline"), and the debug toggle that shows where each panel's data comes from. One store,
// read with useLiveState().

export type LiveState = {
  paused: boolean
  /** The heartbeat poll (overview) failed and hasn't recovered since. */
  stale: boolean
  /** Last time the heartbeat answered. */
  lastOkAt: number
  staleError?: string
  showSources: boolean
}

const SRC_KEY = 'vlrelay.console.sources'
let state: LiveState = {
  paused: false,
  stale: false,
  lastOkAt: 0,
  showSources: (() => {
    try {
      return localStorage.getItem(SRC_KEY) === '1'
    } catch {
      return false
    }
  })(),
}
const listeners = new Set<() => void>()
function set(p: Partial<LiveState>) {
  state = { ...state, ...p }
  listeners.forEach((l) => l())
}
const subscribe = (l: () => void) => {
  listeners.add(l)
  return () => {
    listeners.delete(l)
  }
}
export const getLive = () => state
export const useLiveState = () => useSyncExternalStore(subscribe, getLive)

export const togglePaused = () => set({ paused: !state.paused })
export function toggleSources() {
  const v = !state.showSources
  try {
    localStorage.setItem(SRC_KEY, v ? '1' : '0')
  } catch {
    /* per-tab */
  }
  set({ showSources: v })
}
export function heartbeatOk() {
  if (state.stale || !state.lastOkAt || Date.now() - state.lastOkAt > 1000) set({ stale: false, staleError: undefined, lastOkAt: Date.now() })
}
export function heartbeatFailed(e: unknown) {
  set({ stale: true, staleError: e instanceof Error ? e.message : String(e) })
}

/** An endpoint this relay doesn't have: the admin router answers an unknown path with a JSON 404 "no such endpoint". */
export function isUnsupported(e: unknown): boolean {
  return e instanceof ApiError && (e.status === 501 || (e.status === 404 && /no such endpoint/i.test(e.message)) || (e.status === 404 && e.error === 'HTTP 404'))
}
