import { useEffect, useState } from 'react'
import { api } from './api'
import { useLoad, type Load } from './hooks'

/** GET `path` (re-fetched every `pollMs`), with the time the last answer arrived. */
export function useApi<T>(path: string | null, params?: Record<string, string | number | boolean | undefined>, pollMs?: number): Load<T> & { at?: number } {
  const [at, setAt] = useState<number>()
  const key = path === null ? null : path + JSON.stringify(params ?? {})
  const l = useLoad<T | undefined>(
    async () => {
      if (path === null) return undefined
      const r = await api<T>(path, { params })
      setAt(Date.now())
      return r
    },
    [key],
    pollMs,
  )
  return { ...l, data: l.data as T | undefined, at }
}

/** Global keyboard shortcuts, ignored while typing in a field. */
export function useKey(handler: (e: KeyboardEvent) => void, deps: unknown[]) {
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement | null
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable)) return
      if (e.metaKey || e.ctrlKey || e.altKey) return
      handler(e)
    }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)
}
