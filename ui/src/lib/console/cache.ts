import { keepPreviousData, QueryClient, replaceEqualDeep, skipToken, useQuery, type QueryKey } from '@tanstack/react-query'
import { compareVersions, type Case, type Change, type ChangeKind, type HostDetail, type HostList, type HostRow } from '../api'
import { getFeed, useFeedLive } from './feed'
import { getLive, useLiveState } from './live'

// The console's one cache (TanStack Query). Every panel reads a query by its key, so the drawer,
// the tables, the tiles and the banners show one copy of a row. What changes it: a fetch, a write's
// answer (writes.ts patches the cache with it), and the change feed (feed.ts), which invalidates the
// keys a change names. Rows that carry a version never go back to an older one.

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // the polls and the feed are the retries
      retry: false,
      refetchOnWindowFocus: false,
      // an operator glancing back at a background tab sees current numbers, not a gap
      refetchIntervalInBackground: true,
      staleTime: 1000,
    },
  },
})
const qc = queryClient

/** Every key the console reads. A prefix (`['hosts']`, `['host', name]`) invalidates everything under it. */
export const keys = {
  overview: () => ['overview'],
  publicStats: () => ['public'],
  cluster: () => ['cluster'],
  quorum: () => ['quorum', 'status'],
  quorumHistory: () => ['quorum', 'history'],
  pipeline: () => ['pipeline'],
  consumers: () => ['consumers'],
  discovery: () => ['discovery'],
  plc: () => ['plc'],
  store: () => ['store'],
  settings: (node = '') => ['settings', node],
  /** One page or filter of `GET hosts`, or another view of the host table (`tierCounts`). */
  hosts: (q: unknown) => ['hosts', q],
  /** `GET hosts/{host}`. */
  host: (name: string) => ['host', name],
  /** The newest row of a host any answer carried: what the guards compare against. */
  hostRow: (name: string) => ['host', name, 'row'],
  admissions: () => ['admissions'],
  rejectsTop: (reason: string) => ['rejectsTop', reason],
  policy: () => ['policy', 'wire'],
  policyFull: () => ['policy', 'full'],
  policySource: () => ['policy', 'source'],
  policyAudit: () => ['policy', 'audit'],
  policyUsage: () => ['policy', 'usage'],
  signals: () => ['policy', 'signals'],
  policyDefaults: () => ['policyDefaults'],
  rules: () => ['rules', 'list'],
  rulesAudit: () => ['rules', 'audit'],
  /** `all`, or one status. */
  cases: (status: string) => ['cases', status],
  case: (id: string | number) => ['case', String(id)],
  caseEvidence: (id: string | number) => ['case', String(id), 'evidence'],
  takedowns: () => ['takedowns'],
  account: (did: string) => ['account', did],
  accounts: (q: string) => ['accounts', q],
} satisfies Record<string, (...a: never[]) => QueryKey>

// ---------------------------------------------------------------- reading

export type Live<T> = {
  data?: T
  error?: unknown
  /** A fetch is running (the first, or a new key while `keep` shows the last one). */
  loading: boolean
  /** When the data shown was fetched. */
  at?: number
  /** Kept current by the change feed rather than a poll. */
  live: boolean
  reload: () => void
}

export type LiveOpts<T> = {
  /** Poll this often whatever the feed does: live numbers (rates, lag, series) no event announces. */
  poll?: number
  /** Poll this often only while the feed is down (a 404 on an older relay, or reconnecting): what change events cover. */
  fallback?: number
  /** Keep showing the last key's data while a new key loads (a filter typed). */
  keep?: boolean
  enabled?: boolean
  /** Keep the cached copy when an answer is older than it (versions, `updatedAtMs`). */
  older?: (next: T, cached: T) => boolean
}

/**
 * One query, read by any number of panels. Space pauses every poll; the feed's invalidations
 * wait for the resume (`resumeLive`).
 */
export function useLive<T>(key: QueryKey, fn: () => Promise<T>, o: LiveOpts<T> = {}): Live<T> {
  const { paused } = useLiveState()
  const feedLive = useFeedLive()
  const covered = !o.poll && !!o.fallback
  const every = paused ? false : (o.poll ?? (feedLive ? false : (o.fallback ?? false)))
  const older = o.older
  const q = useQuery<T>({
    queryKey: key,
    queryFn: o.enabled === false ? skipToken : fn,
    refetchInterval: every || false,
    placeholderData: o.keep ? keepPreviousData : undefined,
    // the feed says when an event-covered query changes: no refetch on every mount
    staleTime: covered && feedLive ? 30_000 : 1000,
    structuralSharing: older ? (a, b) => (a !== undefined && older(b as T, a as T) ? a : replaceEqualDeep(a, b)) : true,
  })
  return {
    data: q.data,
    error: q.error ?? undefined,
    loading: q.isFetching,
    at: q.dataUpdatedAt || undefined,
    live: covered && feedLive && !q.error,
    reload: () => void q.refetch(),
  }
}

/** What a key holds now, outside React (palette providers, dialogs). */
export const cached = <T>(key: QueryKey): T | undefined => qc.getQueryData<T>(key)

/** Refetch what's under a key (now, or when live updates resume). */
export const refresh = (key: QueryKey) => qc.invalidateQueries({ queryKey: key, refetchType: getLive().paused ? 'none' : 'active' })

/** Space released: refetch whatever went stale while the panels were frozen. */
export const resumeLive = () => qc.refetchQueries({ type: 'active', stale: true })

// ---------------------------------------------------------------- versions

/** `next` is strictly older than `cur` (two versions that don't compare say nothing: not older). */
export const olderVersion = (next?: string | number | null, cur?: string | number | null) =>
  next != null && cur != null && compareVersions(String(next), String(cur)) === -1

export const olderRow = (next: HostRow, cur: HostRow) => olderVersion(next.version, cur.version)
export const olderCase = (next: Case, cur: Case) => next.updatedAtMs < cur.updatedAtMs

/**
 * A host's status as its owner (the node reading it) last reported it on the feed. A fetch
 * answers with the serving node's view, which can trail the owner's: the row's `ownerVersion`
 * says how much of the owner it has heard, in the owner's own counter, so the two compare.
 */
type OwnerStatus = Pick<HostRow, 'status' | 'backpressureReason'> & { node: string; version: string }
const ownerKey = (host: string) => ['host', host, 'owner']

function withOwnerStatus(row: HostRow): HostRow {
  const o = qc.getQueryData<OwnerStatus>(ownerKey(row.host))
  // only the owner's own events speak for its status, and a row that has heard them wins
  if (!o || o.node !== row.node || (row.ownerVersion != null && (compareVersions(row.ownerVersion, o.version) ?? 1) >= 0)) return row
  return o.status === row.status && o.backpressureReason === row.backpressureReason ? row : { ...row, status: o.status, backpressureReason: o.backpressureReason }
}

/** What a host's row says about its state, as opposed to its rates. */
const stateOf = (r: HostRow) => [r.version, r.status, r.backpressureReason, r.tier, r.throttle, r.rule, r.maxAccounts, r.throttledAccounts, r.node].join('|')

/**
 * The newer of an answer's host row and the cached one. When the answer wins it's cached
 * (`hydrate`), and when it moves the host's state every other copy (its detail, the lists
 * showing it) takes it at once, so the drawer and the tables never disagree.
 */
export function reconcileRow(answer: HostRow, hydrate = true): HostRow {
  const k = keys.hostRow(answer.host)
  const cur = qc.getQueryData<HostRow>(k)
  if (cur && olderRow(answer, cur)) return cur
  let row = withOwnerStatus(answer)
  // a listing never says `pending`: the same version read back hasn't landed either
  if (cur?.pending && !row.pending && row.version != null && cur.version != null && compareVersions(row.version, cur.version) === 0) row = { ...row, pending: true }
  if (row.pending && cur && unsettled.has(cur)) unsettled.add(row)
  if (hydrate || cur) qc.setQueryData(k, row)
  if (cur && stateOf(cur) !== stateOf(row)) spread(row)
  return row
}

function spread(row: HostRow) {
  qc.setQueryData<HostDetail>(keys.host(row.host), (d) => (d && !olderRow(row, d.row) ? { ...d, row } : d))
  qc.setQueriesData({ queryKey: ['hosts'] }, (l) => patchList(l, row))
}

function patchList(l: unknown, row: HostRow): unknown {
  const list = l as HostList | undefined
  if (!list || !Array.isArray(list.hosts) || !list.hosts.some((h) => h.host === row.host)) return l
  return { ...list, hosts: list.hosts.map((h) => (h.host === row.host && !olderRow(row, h) ? { ...row, history: row.history ?? h.history } : h)) }
}

/** A host's row as an answer (an action, a hint) has it now: into the row cache, its detail and every list showing it. */
export function writeHostRow(answer: HostRow): HostRow {
  const won = reconcileRow(answer)
  spread(won)
  return won
}

/** The newer of an answer's case and the cached one (lists hydrate each case's own key). */
export function reconcileCase(c: Case): Case {
  const k = keys.case(c.id)
  const cur = qc.getQueryData<Case>(k)
  if (cur && olderCase(c, cur)) return cur
  qc.setQueryData(k, c)
  return c
}

/** A case as a write answered it: its own key and every case list, the open list losing it once it isn't open. */
export function writeCase(c: Case) {
  if (reconcileCase(c) !== c) return
  for (const [k, l] of qc.getQueriesData<Case[]>({ queryKey: ['cases'] })) {
    if (!l) continue
    const status = k[1]
    const fits = status === 'all' || status === c.status
    const has = l.some((x) => x.id === c.id)
    const next = has ? (fits ? l.map((x) => (x.id === c.id ? c : x)) : l.filter((x) => x.id !== c.id)) : fits ? [c, ...l] : l
    if (next !== l) qc.setQueryData(k, next)
  }
}

// ---------------------------------------------------------------- what a change touches

/**
 * The keys a change to `kind` (one id, or `*` for the whole kind) makes stale, aggregates
 * included: the overview's counts, the banners' host lists, the host drawer's rule and cases.
 * The feed and every write go through here.
 */
export function keysFor(kind: ChangeKind, id = '*', hint?: { host?: string }): QueryKey[] {
  const one = id !== '*'
  switch (kind) {
    case 'host':
      return [one ? keys.host(id) : ['host'], ['hosts'], keys.overview()]
    case 'policy':
      // tier limits and caps show on every host
      return [['policy'], ['hosts'], ['host'], keys.overview()]
    case 'rules':
      return [['rules'], ['hosts'], ['host']]
    case 'takedown':
      return [keys.takedowns(), one ? keys.account(id) : ['account'], ['accounts']]
    case 'account':
      return [one ? keys.account(id) : ['account'], ['accounts'], hint?.host ? keys.host(hint.host) : ['host'], ['hosts']]
    case 'cluster':
      return [keys.cluster(), ['quorum'], keys.overview()]
    case 'consumer':
      return [keys.consumers(), keys.overview()]
    case 'discovery':
      return [keys.discovery()]
    case 'plc':
      return [keys.plc()]
    case 'case':
      return [['cases'], one ? keys.case(id) : ['case'], keys.overview(), ['host']]
  }
}

const pending = new Map<string, QueryKey>()
let flushTimer: ReturnType<typeof setTimeout> | undefined
let lastFlush = 0

/**
 * Invalidates keys. The feed's are batched: at most one round a second, so hosts flapping
 * refetch each list once rather than once per host. A write's (`now`) go at once.
 */
export function invalidate(ks: QueryKey[], now = false) {
  for (const k of ks) pending.set(JSON.stringify(k), k)
  if (now) return flushInvalidations()
  flushTimer ??= setTimeout(flushInvalidations, Math.max(150, lastFlush + 1000 - Date.now()))
}

function flushInvalidations() {
  if (flushTimer) clearTimeout(flushTimer)
  flushTimer = undefined
  lastFlush = Date.now()
  const ks = [...pending.values()]
  pending.clear()
  const refetchType = getLive().paused ? 'none' : 'active'
  for (const k of ks) void qc.invalidateQueries({ queryKey: k, refetchType })
}

/** Everything shown is refetched. */
export const invalidateAll = () => qc.invalidateQueries({ refetchType: getLive().paused ? 'none' : 'active' })

// ---------------------------------------------------------------- the feed

type Versioned = { version?: string | number | null }
/**
 * Rows not yet settled where the console reads: patched from another node's hint (the serving
 * node may not have applied the change yet), or a host action answered `pending`. The next
 * change to such a host refetches the lists and counts even if it moves nothing.
 */
const unsettled = new WeakSet<HostRow>()
export const markUnsettled = (r: HostRow) => void unsettled.add(r)
/** The cached copy is already at (or past) the change's version: a replay, or a second node's copy of it. */
const seen = (c: Change, cur: Versioned | undefined) => cur?.version != null && (compareVersions(c.version, String(cur.version)) ?? 1) <= 0
/** The owner's change the cached row has already heard (`ownerVersion` is in the owner's counter). */
const heard = (c: Change, cur: HostRow | undefined) => !!cur && cur.node === c.node && cur.ownerVersion != null && (compareVersions(c.version, cur.ownerVersion) ?? 1) <= 0

/** One change from the feed: patch what its hint says, then invalidate what it touches. Applying one twice changes nothing. */
export function applyChange(c: Change) {
  if (c.kind === 'host' && c.id !== '*') {
    const cur = qc.getQueryData<HostRow>(keys.hostRow(c.id))
    if (seen(c, cur) || heard(c, cur)) return
    const h = c.hint
    const byOwner = !cur || cur.node === c.node
    if (h?.status && byOwner) qc.setQueryData<OwnerStatus>(ownerKey(c.id), { status: h.status, backpressureReason: h.backpressureReason ?? null, node: c.node, version: c.version })
    if (cur && h && (h.status || h.tier)) {
      const patched: HostRow = {
        ...cur,
        status: h.status ?? cur.status,
        tier: h.tier ?? cur.tier,
        backpressureReason: h.status ? (h.backpressureReason ?? null) : cur.backpressureReason,
        version: c.version,
        ownerVersion: byOwner ? c.version : cur.ownerVersion,
        pending: undefined,
      }
      const won = writeHostRow(patched)
      if (c.node !== getFeed().node) unsettled.add(won)
    }
    return invalidate(hostKeys(c.id, cur, h))
  }
  if (c.kind === 'policy' && (seen(c, cached(keys.policyFull())) || seen(c, cached(keys.policy())))) return
  if (c.kind === 'rules' && seen(c, cached<Versioned[]>(keys.rules())?.[0])) return
  invalidate(keysFor(c.kind, c.id, c.hint))
}

/**
 * What one host's change touches. Its detail always. When its status or tier moved (or the hint
 * can't say), the overview's counts and the host lists it could enter or leave: one showing it,
 * one filtered on its old or new status or tier, the tier counts. Lists only rates move in poll
 * anyway, so a busy relay's stream of host changes doesn't refetch every list each second.
 */
function hostKeys(host: string, prev: HostRow | undefined, h: Change['hint']): QueryKey[] {
  const out: QueryKey[] = [keys.host(host)]
    const moved = !prev || !h || unsettled.has(prev) || (h.status !== undefined && h.status !== prev.status) || (h.tier !== undefined && h.tier !== prev.tier) || (h.status !== undefined && (h.backpressureReason ?? null) !== (prev.backpressureReason ?? null))
  if (!moved) return out
  out.push(keys.overview())
  for (const [k, d] of qc.getQueriesData<unknown>({ queryKey: ['hosts'] })) {
    const q = (k[1] ?? {}) as { status?: string; tier?: string; tierCounts?: string }
    const rows = (d as HostList | undefined)?.hosts
    const touched =
      !h ||
      (Array.isArray(rows) && rows.some((r) => r.host === host)) ||
      (q.tierCounts !== undefined && h.tier !== prev?.tier) ||
      (q.status !== undefined && (q.status === h.status || q.status === prev?.status)) ||
      (q.tier !== undefined && (q.tier === h.tier || q.tier === prev?.tier))
    if (touched) out.push(k)
  }
  return out
}

/** Changes may have been missed (a resync): refetch what's shown. */
export const feedLost = () => void invalidateAll()
