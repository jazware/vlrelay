import { publicStats, type Case, type ClusterView, type Consumer, type DiscoveryView, type FullPolicyDoc, type HostList, type Overview, type PolicyDoc, type PublicStats, type QCounts, type QRequests, type QuorumHistory, type QuorumView, type SettingsView, type StoreView } from '../api'
import * as A from './adminAdapter'
import { createPoller } from './live'

// One shared poll per thing the console shows (createPoller in live.ts). The overview is the
// heartbeat: when it fails the shell shows "Not updating". A few values the API has no series for
// (the stream's own rate, the commit latency, each consumer's rate) are kept here from the polls,
// so their sparklines fill in while the page is open.

const KEEP = 150
const series = new Map<string, number[]>()
let seriesVersion = 0

function push(key: string, v: number | undefined | null) {
  if (v === undefined || v === null || !isFinite(v)) return
  let a = series.get(key)
  if (!a) series.set(key, (a = []))
  a.push(v)
  if (a.length > KEEP) a.splice(0, a.length - KEEP)
}

/** A client-side series (oldest first); empty until a couple of polls have landed. */
export const seriesOf = (key: string): number[] => series.get(key) ?? []
export const seriesTick = () => seriesVersion

export const overviewPoll = createPoller<Overview>(A.overview, 2000, {
  heartbeat: true,
  onData: (o) => {
    push('stream', o.streamEventsPerSec ?? o.eventsOutPerSec)
    for (const n of o.byNode ?? []) if (!n.stale) push(`node-in:${n.node}`, n.eventsInPerSec)
    seriesVersion++
  },
})

// ---------------------------------------------------------------- what the quorum polls show over time

/** Bucket requests per second over the last minute, summed over the members that answer. */
export type ReqRates = { windowSecs: number; total: QCounts; byPurpose: Record<string, QCounts>; byComponent: Record<string, QCounts>; byOp: Record<string, number> }
/** An epoch change the console saw (a fallback for one no member's history lists). */
export type SeenEpoch = { atMs: number; from: number; epoch: number; leader: string | null }

const REQ_WINDOW_MS = 60_000
const reqSamples = new Map<string, { at: number; r: QRequests }[]>()
let reqRates: ReqRates | undefined
const epochs: SeenEpoch[] = []
let lastF: number | undefined
let lastEpoch: number | undefined
let fMovedAt: number | undefined

export const requestRates = () => reqRates
export const seenEpochs = () => epochs
/** When the console last saw F move (to within a poll), or undefined before it has. */
export const flushSeenAt = () => fMovedAt

const zero = (): QCounts => ({ a: 0, b: 0, free: 0 })
function addRate(into: Record<string, QCounts>, k: string, now: QCounts, then: QCounts | undefined, secs: number) {
  const c = (into[k] ??= zero())
  c.a += Math.max(0, now.a - (then?.a ?? 0)) / secs
  c.b += Math.max(0, now.b - (then?.b ?? 0)) / secs
  c.free += Math.max(0, now.free - (then?.free ?? 0)) / secs
}

function observeRequests(q: QuorumView, at: number) {
  const out: ReqRates = { windowSecs: 0, total: zero(), byPurpose: {}, byComponent: {}, byOp: {} }
  let any = false
  for (const n of q.nodes) {
    const r = n.stale ? undefined : n.status?.requests
    if (!r?.total) continue
    let s = reqSamples.get(n.node)
    if (!s) reqSamples.set(n.node, (s = []))
    // a restarted process counts from 0 again
    if (s.length && r.total.a + r.total.b < s[s.length - 1].r.total.a + s[s.length - 1].r.total.b) s.length = 0
    s.push({ at, r })
    while (s.length > 2 && at - s[0].at > REQ_WINDOW_MS) s.shift()
    if (s.length < 2) continue
    const first = s[0].r
    const secs = (at - s[0].at) / 1000
    if (secs < 1) continue
    any = true
    out.windowSecs = Math.max(out.windowSecs, secs)
    const tot = { total: zero() }
    addRate(tot, 'total', r.total, first.total, secs)
    out.total.a += tot.total.a
    out.total.b += tot.total.b
    out.total.free += tot.total.free
    for (const [k, v] of Object.entries(r.by_purpose ?? {})) addRate(out.byPurpose, k, v, first.by_purpose?.[k], secs)
    for (const [k, v] of Object.entries(r.by_component ?? {})) addRate(out.byComponent, k, v, first.by_component?.[k], secs)
    for (const [k, v] of Object.entries(r.by_op ?? {})) out.byOp[k] = (out.byOp[k] ?? 0) + Math.max(0, v - (first.by_op?.[k] ?? 0)) / secs
  }
  if (!any) return
  reqRates = out
  push('req-a', out.total.a)
  push('req-b', out.total.b)
  for (const [k, v] of Object.entries(out.byPurpose)) push(`req:${k}`, v.a + v.b)
}

function observeLeader(q: QuorumView, at: number) {
  const lead = q.nodes.find((n) => n.status?.role === 'leader' && !n.stale)?.status
  const any = lead ?? q.nodes.find((n) => !n.stale && n.status)?.status
  if (any) {
    if (lastEpoch !== undefined && any.epoch !== lastEpoch) {
      epochs.unshift({ atMs: at, from: lastEpoch, epoch: any.epoch, leader: lead?.id ?? null })
      if (epochs.length > 50) epochs.pop()
    }
    lastEpoch = any.epoch
  }
  if (!lead) return
  push('commit-p99', lead.commit_us?.p99 / 1000)
  push('commit-p50', lead.commit_us?.p50 / 1000)
  if (lastF !== undefined && lastF !== lead.flushed) fMovedAt = at
  lastF = lead.flushed
}

export const quorumPoll = createPoller<A.Optional<QuorumView>>(A.quorum, 2000, {
  onData: (q) => {
    if (!q.supported) return
    const at = Date.now()
    observeLeader(q.data, at)
    observeRequests(q.data, at)
    seriesVersion++
  },
})

/** Every member's leadership changes (GET cluster/quorum/history), for the leadership history. */
export const historyPoll = createPoller<QuorumHistory>(A.quorumHistory, 5000)

/** Host discovery's sources (the leader's, asked through any node). */
export const discoveryPoll = createPoller<DiscoveryView>(A.discovery, 5000)

/** The public stats: the page at / polls them, the console reads uptime from them. */
export const publicPoll = createPoller<PublicStats>(publicStats, 2000)
export const clusterPoll = createPoller<ClusterView>(A.cluster, 5000)
export const consumersPoll = createPoller<Consumer[]>(A.consumers, 5000, {
  onData: (cs) => {
    const live = new Set(cs.map((c) => `cons:${c.node}/${c.id}`))
    for (const k of series.keys()) if (k.startsWith('cons:') && !live.has(k)) series.delete(k)
    for (const c of cs) push(`cons:${c.node}/${c.id}`, c.eventsPerSec)
    seriesVersion++
  },
})
export const openCasesPoll = createPoller<Case[]>(A.openCases, 10_000)
export const policyPoll = createPoller<PolicyDoc>(A.policy, 30_000)
export const policyFullPoll = createPoller<FullPolicyDoc>(A.policyFull, 30_000)
export const settingsPoll = createPoller<SettingsView>(A.settings, 60_000)
/** This node's object store: requests by purpose and class with their rates, bytes, latency, the last retention pass. */
export const storePoll = createPoller<StoreView>(A.store, 5000, {
  onData: (v) => {
    // the first answer has no window: its rates are 0, not a measurement
    if (v.windowSecs > 0) {
      push('store-a', v.total.perSec.a)
      push('store-b', v.total.perSec.b)
      seriesVersion++
    }
  },
})
/** Throttled hosts (they fall behind instead of dropping), for the banners. */
export const throttledPoll = createPoller<HostList>(() => A.hosts({ status: 'throttled', sort: 'lag', desc: true, limit: 200 }), 5000)
/** The busiest hosts with their caps, for the "at the account cap" banner. */
export const capPoll = createPoller<HostList>(() => A.hosts({ sort: 'accounts', desc: true, limit: 400 }), 30_000)

/** The slow-consumer cutoff from the full policy (consumers.slowConsumerLagSecs), else 120 s. */
export function slowLagMs(p?: FullPolicyDoc): number {
  const c = (p?.policy as { consumers?: { slowConsumerLagSecs?: number } } | undefined)?.consumers
  return (c?.slowConsumerLagSecs ?? 120) * 1000
}
export const isSlow = (c: Consumer, cutoffMs: number) => !c.backfilling && c.lagMs > cutoffMs / 4
