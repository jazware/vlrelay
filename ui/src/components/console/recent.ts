// The last details opened in this tab, for ⌘K's "Recent". Kept in sessionStorage beside the admin
// token, so it never outlives the tab.

export type RecentItem = { type: string; id: string; title: string; kind: string; at: number }

const KEY = 'vlrelay.console.recent'
const KEEP = 6

export function recentList(): RecentItem[] {
  try {
    const v = JSON.parse(sessionStorage.getItem(KEY) ?? '[]')
    return Array.isArray(v) ? v.filter((x) => x && typeof x.type === 'string' && typeof x.id === 'string') : []
  } catch {
    return []
  }
}

export function noteRecent(r: Omit<RecentItem, 'at'>) {
  const list = [{ ...r, at: Date.now() }, ...recentList().filter((x) => x.type !== r.type || x.id !== r.id)].slice(0, KEEP)
  try {
    sessionStorage.setItem(KEY, JSON.stringify(list))
  } catch {
    /* not kept: the palette just shows less */
  }
}
