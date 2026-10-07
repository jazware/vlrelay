import { skipToken, useQuery } from '@tanstack/react-query'
import { ApiError, publicStats, type Account, type Case, type CaseStatus, type ClusterView, type Consumer, type DiscoveryView, type DomainRule, type FullPolicyDoc, type HostDetail, type HostRow, type Overview, type PolicyDoc, type PublicStats, type QCounts, type QRequests, type QuorumHistory, type QuorumView, type SettingsView, type StoreView } from '../api'
import * as A from './adminAdapter'
import { keys, olderCase, olderVersion, reconcileCase, reconcileRow, useLive, type Live } from './cache'
import { heartbeatFailed, heartbeatOk } from './live'
import { serverSaw, type Json, type PolicyBase } from './policyDraft'

// Every query the console shares, on the one cache (cache.ts). `poll` is for live numbers no
// event announces (rates, lag, series); `fallback` polls what the change feed covers, only while
// the feed is down. The overview is the heartbeat: when it fails the shell says "Not updating".
// A few values the API has no series for (the stream's own rate, the commit latency, each
// consumer's rate) are kept here from the answers, so their sparklines fill in while the page is open.

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

/** A client-side series (oldest first); empty until a couple of answers have landed. */
export const seriesOf = (key: string): number[] => series.get(key) ?? []
export const seriesTick = () => seriesVersion

let lastSeq: number | undefined
let seqMovedAt: number | undefined
/** When the console last saw the newest seq move (to within a poll), or undefined before it has. */
export const seqSeenAt = () => seqMovedAt

// ---------------------------------------------------------------- the relay at a glance

async function fetchOverview(): Promise<Overview> {
  let o: Overview
  try {
    o = await A.overview()
  } catch (e) {
    // a 401 already sent the console back to the token form
    if (!(e instanceof ApiError && e.status === 401)) heartbeatFailed(e)
    throw e
  }
  heartbeatOk()
  if (lastSeq !== undefined && o.lastSeq !== lastSeq) seqMovedAt = Date.now()
  lastSeq = o.lastSeq
  push('stream', o.streamEventsPerSec ?? o.eventsOutPerSec)
  for (const n of o.byNode ?? []) if (!n.stale) push(`node-in:${n.node}`, n.eventsInPerSec)
  seriesVersion++
  return o
}
export const useOverview = () => useLive(keys.overview(), fetchOverview, { poll: 2000 })

/** The public stats: the page at / reads them, the console its uptime. */
export const usePublicStats = () => useLive<PublicStats>(keys.publicStats(), publicStats, { poll: 2000 })

export const useCluster = () => useLive<ClusterView>(keys.cluster(), A.cluster, { poll: 5000, older: (n, c) => olderVersion(n.lastSeq, c.lastSeq) })

// ---------------------------------------------------------------- what the quorum statuses show over time

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

async function fetchQuorum(): Promise<A.Optional<QuorumView>> {
  const q = await A.quorum()
  if (q.supported) {
    const at = Date.now()
    observeLeader(q.data, at)
    observeRequests(q.data, at)
    seriesVersion++
  }
  return q
}
/** Every member's /qlog/status, or unsupported on a relay without the quorum log. */
export const useQuorum = () => useLive(keys.quorum(), fetchQuorum, { poll: 2000 })

/** Every member's leadership changes (GET cluster/quorum/history), for the leadership history. */
export const useQuorumHistory = (enabled = true) => useLive<QuorumHistory>(keys.quorumHistory(), A.quorumHistory, { fallback: 5000, enabled })

export const usePipeline = () => useLive(keys.pipeline(), A.pipelineOpt, { poll: 5000 })

// ---------------------------------------------------------------- streams, discovery, the store

async function fetchConsumers(): Promise<Consumer[]> {
  const cs = await A.consumers()
  const live = new Set(cs.map((c) => `cons:${c.node}/${c.id}`))
  for (const k of series.keys()) if (k.startsWith('cons:') && !live.has(k)) series.delete(k)
  for (const c of cs) push(`cons:${c.node}/${c.id}`, c.eventsPerSec)
  seriesVersion++
  return cs
}
/** Every member's sockets (rates move, so it polls; a connect or disconnect refetches at once). */
export const useConsumers = () => useLive(keys.consumers(), fetchConsumers, { poll: 5000 })

/** Host discovery's sources (the leader's, asked through any node). */
export const useDiscovery = () => useLive<DiscoveryView>(keys.discovery(), A.discovery, { fallback: 5000 })
export const usePlc = () => useLive(keys.plc(), A.plc, { fallback: 5000 })
export const useAdmissions = () => useLive(keys.admissions(), A.admissions, { poll: 10_000 })

async function fetchStore(): Promise<StoreView> {
  const v = await A.store()
  // the first answer has no window: its rates are 0, not a measurement
  if (v.windowSecs > 0) {
    push('store-a', v.total.perSec.a)
    push('store-b', v.total.perSec.b)
    seriesVersion++
  }
  return v
}
/** This node's object store: requests by purpose and class with their rates, bytes, latency, the last retention pass. */
export const useStore = () => useLive(keys.store(), fetchStore, { poll: 5000 })

/** A node's flags (this node's when `node` is empty). They change with a restart, so slowly. */
export const useSettings = () => useLive<SettingsView>(keys.settings(), A.settings, { poll: 60_000 })

// ---------------------------------------------------------------- hosts

/** The query as a key: what's left out isn't part of it. */
const hostQueryKey = (q: A.HostQuery) => Object.fromEntries(Object.entries(q).filter(([, v]) => v !== undefined && v !== ''))

/** A page of hosts; each row is the newest the console has seen of it, and a page of up to 500 hydrates the rows. */
export const fetchHosts = (q: A.HostQuery) => A.hosts(q).then((l) => ({ ...l, hosts: l.hosts.map((r) => reconcileRow(r, l.hosts.length <= 500)) }))

/** One filter of `GET hosts`. Rates are in the rows, so it polls (`poll`, 5 s by default). */
export const useHostList = (q: A.HostQuery, o: { poll?: number; keep?: boolean; enabled?: boolean } = {}) =>
  useLive(keys.hosts(hostQueryKey(q)), () => fetchHosts(q), { poll: o.poll ?? 5000, keep: o.keep, enabled: o.enabled })

/**
 * The hosts a domain rule decides, busiest first (`hosts?rule=`), and how many. A `rules` change
 * refetches it with the rule set. `q` (the pattern's domain, which every host it decides
 * contains) narrows it on an older relay, which ignores `rule`: there the rows are filtered here
 * and the count is the rule's own `matches`.
 */
export function useRuleHosts(r: DomainRule, limit = 50) {
  const l = useHostList({ q: r.pattern.replace(/^\*\./, ''), rule: r.id, sort: 'events', desc: true, limit }, { poll: 15_000 })
  const rows = l.data?.hosts ?? []
  const filtered = rows.some((h) => h.rule !== r.id)
  return { ...l, hosts: filtered ? rows.filter((h) => h.rule === r.id) : rows, total: l.data && !filtered ? l.data.total : r.matches }
}

/** Throttled hosts (they fall behind instead of dropping), for the banners and the badge. */
export const useThrottledHosts = () => useHostList({ status: 'throttled', sort: 'lag', desc: true, limit: 200 })
/** The hosts at their account cap (all of them counted, the busiest 400 listed), for the "at the account cap" banner. */
export const useCapHosts = () => useHostList({ flag: 'atCap', sort: 'accounts', desc: true, limit: 400 }, { poll: 30_000 })

/** How many hosts each tier has (a `limit 0` call each: the total still comes back). */
export const useTierCounts = (tiers: string[]) =>
  useLive(keys.hosts({ tierCounts: tiers.join(',') }), () => Promise.all(tiers.map((t) => A.hosts({ tier: t, sort: 'host', desc: false, limit: 0 }).then((r) => [t, r.total] as const))), { poll: 30_000, keep: true })

/** One host: its row (the newest the console has), limits, rejects, series and actions. Polls for the series. */
export const useHostDetail = (name: string) =>
  useLive<HostDetail>(keys.host(name), () => A.host(name).then((d) => ({ ...d, row: reconcileRow(d.row) })), { poll: 2000 })

/** The newest row of a host any answer carried (a list, its detail, an action), without asking. */
export function useHostRow(name: string): HostRow | undefined {
  return useQuery<HostRow>({ queryKey: keys.hostRow(name), queryFn: skipToken, staleTime: Infinity }).data
}

export const useRejectsTop = (reason: string) => useLive(keys.rejectsTop(reason), () => A.rejectsTop(reason, 10), { poll: 5000 })

// ---------------------------------------------------------------- policy

/** The policy document the page edits: the whole one (`policy/full`), or the tier form on an older relay. */
export async function readPolicySource(): Promise<PolicyBase> {
  const full = await A.policyFullOptional()
  if (full.supported) {
    const d = full.data
    return { mode: 'full', version: d.version, updatedAtMs: d.updatedAtMs, updatedBy: d.updatedBy, note: d.note, body: d.policy }
  }
  const d = await A.policy()
  return { mode: 'wire', version: d.version, updatedAtMs: d.updatedAtMs, updatedBy: d.updatedBy, body: d.policy as unknown as Json }
}
/** Every answer goes to the draft, which notices a newer version under it. */
const fetchPolicySource = () => readPolicySource().then((doc) => (serverSaw(doc), doc))

const olderDoc = (n: { version: number }, c: { version: number }) => n.version < c.version
export const usePolicy = () => useLive<PolicyDoc>(keys.policy(), A.policy, { fallback: 30_000, older: olderDoc })
export const usePolicyFull = () => useLive<FullPolicyDoc>(keys.policyFull(), A.policyFull, { fallback: 30_000, older: olderDoc })
export const usePolicySource = () => useLive(keys.policySource(), fetchPolicySource, { fallback: 10_000, older: olderDoc })
export const usePolicyAudit = () => useLive(keys.policyAudit(), A.policyAudit, { fallback: 15_000 })
export const usePolicyDefaults = () => useLive(keys.policyDefaults(), A.policyDefaults, { poll: 300_000 })
/** The cluster budgets against what this node sees them spend. */
export const usePolicyUsage = () => useLive(keys.policyUsage(), A.budgetUse, { poll: 10_000 })
/** Each spam rule's threshold and its ten heaviest keys on this node. */
export const useSignals = () => useLive(keys.signals(), A.spamSignals, { poll: 10_000 })

/** The slow-consumer cutoff from the full policy (consumers.slowConsumerLagSecs), else 120 s. */
export function slowLagMs(p?: FullPolicyDoc): number {
  const c = (p?.policy as { consumers?: { slowConsumerLagSecs?: number } } | undefined)?.consumers
  return (c?.slowConsumerLagSecs ?? 120) * 1000
}
export const isSlow = (c: Consumer, cutoffMs: number) => !c.backfilling && c.lagMs > cutoffMs / 4

// ---------------------------------------------------------------- moderation

/** The rule set: every row carries the set's version, so an older answer is the first row's version behind. */
export const useRules = () => useLive(keys.rules(), A.domainRules, { fallback: 10_000, older: (n, c) => olderVersion(n[0]?.version, c[0]?.version) })
export const useRulesAudit = () => useLive(keys.rulesAudit(), A.domainRulesAudit, { fallback: 30_000 })

const fetchCases = (status?: CaseStatus) => A.cases(status).then((l) => l.map(reconcileCase))
/** Every case, for the status counts and the lists. */
export const useCases = () => useLive<Case[]>(keys.cases('all'), () => fetchCases(), { fallback: 10_000 })
export const useOpenCases = () => useLive<Case[]>(keys.cases('open'), () => fetchCases('open'), { fallback: 10_000 })
export const useCase = (id: string) => useLive<Case>(keys.case(id), () => A.caseOf(Number(id)).then(reconcileCase), { fallback: 5000, older: olderCase })
export const useCaseEvidence = (id: number) => useLive(keys.caseEvidence(id), () => A.caseEvidence(id), { fallback: 10_000 })

/** Every account under a takedown, newest first. */
export const useTakedowns = () => useLive(keys.takedowns(), A.takedowns, { fallback: 30_000 })
export const useAccount = (did: string) => useLive<Account>(keys.account(did), () => A.account(did), { fallback: 10_000 })
export const useAccountSearch = (q: string) => useLive<Account[]>(keys.accounts(q), () => A.accounts(q), { fallback: 15_000, enabled: !!q })

export type { Live }
