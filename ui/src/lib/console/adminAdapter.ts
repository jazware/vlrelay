import {
  api,
  ApiError,
  type RejectTop,
  enc,
  publicStats,
  type Account,
  type AdmissionLog,
  type Case,
  type CaseDetail,
  type CaseStatus,
  type ClusterView,
  type Consumer,
  type DiscoveryView,
  type DomainRule,
  type DomainRuleInput,
  type FullPolicyDoc,
  type FullPolicyUpdate,
  type HostAction,
  type HostDetail,
  type HostList,
  type HostRow,
  type HostStatus,
  type Overview,
  type PipelineView,
  type PlcView,
  type Policy,
  type PolicyAudit,
  type PolicyDoc,
  type PolicyUsage,
  type QStatus,
  type QuorumHistory,
  type QuorumView,
  type StoreView,
  type TailFrame,
  type Released,
  type SettingsView,
  type SignalsView,
  type TakedownEntry,
} from '../api'
import { isUnsupported } from './live'

// The console's one way into the admin API (docs/admin-api.md). Pages call these instead of
// api() so the endpoint list lives in one file, and so the panels the console design assumes but
// the relay doesn't serve yet degrade the same way everywhere: those answer
// { supported: false, endpoint } without a request (no 404s in the browser console), and a page
// shows a "needs a newer vlRelay" placeholder. CONSOLE.md lists them for the backend lane; when
// one lands, replace its `missing(...)` with the call.

export type Optional<T> = { supported: true; data: T } | { supported: false; endpoint: string; why: string }

const missing = <T>(endpoint: string, why: string): Promise<Optional<T>> => Promise.resolve({ supported: false, endpoint, why })

async function optional<T>(endpoint: string, run: () => Promise<T>): Promise<Optional<T>> {
  try {
    return { supported: true, data: await run() }
  } catch (e) {
    if (isUnsupported(e)) return { supported: false, endpoint, why: '' }
    throw e
  }
}

// ---------------------------------------------------------------- what the relay serves

export const overview = () => api<Overview>('overview')

export type HostSort = 'host' | 'events' | 'errors' | 'accounts' | 'seq' | 'tier' | 'status' | 'since' | 'lag' | 'throttled' | 'source'
export type HostQuery = {
  q?: string
  tier?: string
  status?: HostStatus | ''
  /** a source, a prefix ending in `:` or `*` (`bootstrap:` is every seed relay), or `none` (not recorded) */
  source?: string
  /** only hosts with (true) or without (false) throttled accounts */
  throttled?: boolean
  flag?: HostFlag
  sort: HostSort
  desc: boolean
  limit?: number
  offset?: number
}
/** `lagging`: connected, throttled or in backpressure and over a minute behind; `erroring`: over 10% of frames rejected. */
export type HostFlag = 'atCap' | 'lagging' | 'erroring' | 'throttledOrAtCap'
/** Server-side filter, sort and page (`limit` default 10,000 on the server). */
export const hosts = (q: HostQuery) =>
  api<HostList>('hosts', { params: { q: q.q || undefined, tier: q.tier || undefined, status: q.status || undefined, source: q.source || undefined, throttled: q.throttled, flag: q.flag, sort: q.sort, desc: q.desc, limit: q.limit, offset: q.offset } })
export const host = (h: string) => api<HostDetail>(`hosts/${enc(h)}`)
export const hostAction = (h: string, a: HostAction) => api<HostRow>(`hosts/${enc(h)}/action`, { body: a })
/** What hostAction sends, for the confirm dialog's footer. */
export const hostActionCall = (h: string, a: HostAction) => `POST /admin/api/hosts/${h}/action ${JSON.stringify(a)}`

export const cluster = () => api<ClusterView>('cluster')
/** Every member's sockets, each with its `node` and `readTier`. */
export const consumers = () => api<Consumer[]>('consumers')
/** Consumer ids are per node: `node` names the one serving it. Another member's is kicked there with the nodes' --qlog-admin-token. */
export const kickConsumer = (id: number, node: string) => api(`consumers/${id}/kick`, { method: 'POST', params: { node: node || undefined } })
export const kickCall = (id: number, node: string) => `POST /admin/api/consumers/${id}/kick${node ? `?node=${node}` : ''}`
export const openCases = () => api<Case[]>('cases', { params: { status: 'open' } })
export const pipeline = () => api<PipelineView>('ops/pipeline')
export const policy = () => api<PolicyDoc>('policy')
export const policyFull = () => api<FullPolicyDoc>('policy/full')
export const settings = () => api<SettingsView>('settings')

let hasQuorum: boolean | undefined
/**
 * Every member's /qlog/status, or unsupported on a relay without the quorum log. The public
 * stats say which (`quorum: null`), so a single relay never polls an endpoint that 404s.
 */
export async function quorum(): Promise<Optional<QuorumView>> {
  if (hasQuorum === undefined) {
    try {
      hasQuorum = (await publicStats()).quorum !== null
    } catch {
      hasQuorum = true
    }
  }
  if (!hasQuorum) return { supported: false, endpoint: 'cluster/quorum', why: 'this relay runs without the quorum log' }
  return optional('cluster/quorum', () => api<QuorumView>('cluster/quorum'))
}

export type MembersChange = { members: string[]; addrs: Record<string, string> }
/** A membership change, sent on to the leader with the nodes' --qlog-admin-token. */
export const changeMembers = (c: MembersChange) => api<unknown>('cluster/quorum/members', { body: c })
export const changeMembersCall = (c: MembersChange) => `POST /admin/api/cluster/quorum/members ${JSON.stringify(Object.keys(c.addrs).length ? c : { members: c.members })}`

/** This node's ack backlog (the pipeline), or unsupported where the route isn't served. */
export async function pipelineOpt(): Promise<Optional<PipelineView>> {
  try {
    return { supported: true, data: await pipeline() }
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return { supported: false, endpoint: 'GET ops/pipeline', why: e.message }
    throw e
  }
}

// ---------------------------------------------------------------- policy, moderation, settings

/** The engine's whole document as a fresh relay has it. */
export const policyDefaults = () => api<Record<string, unknown>>('policy/defaults')
export const policyAudit = () => api<PolicyAudit[]>('policy/audit')
/** The whole document, or unsupported on a relay that only has the tier form (`policy`). */
export const policyFullOptional = () => optional('policy/full', () => api<FullPolicyDoc>('policy/full'))
export const savePolicyFull = (u: FullPolicyUpdate) => api<FullPolicyDoc>('policy/full', { method: 'PUT', body: u })
export const savePolicy = (u: { baseVersion: number; policy: Policy; note: string }) => api<PolicyDoc>('policy', { method: 'PUT', body: u })
/** What a policy save sends, for the review dialog's footer. */
export const savePolicyCall = (full: boolean, baseVersion: number, note: string) =>
  `PUT /admin/api/${full ? 'policy/full' : 'policy'} {"baseVersion":${baseVersion},"policy":{…},"note":${JSON.stringify(note)}}`

export const domainRules = () => api<DomainRule[]>('domain-rules')
export const domainRulesAudit = () => api<PolicyAudit[]>('domain-rules/audit')
export const createRule = (r: DomainRuleInput) => api<DomainRule>('domain-rules', { body: r })
export const updateRule = (id: number, r: DomainRuleInput) => api<DomainRule>(`domain-rules/${id}`, { method: 'PUT', body: r })
export const deleteRule = (id: number) => api<void>(`domain-rules/${id}`, { method: 'DELETE' })

/** Every case when `status` is left out (the filter counts need them all). */
export const cases = (status?: CaseStatus) => api<Case[]>('cases', { params: { status } })
export const caseOf = (id: number) => api<Case>(`cases/${id}`)
export const caseEvidence = (id: number) => optional('cases/{id}/evidence', () => api<CaseDetail>(`cases/${id}/evidence`))
export const updateCase = (id: number, u: { status?: CaseStatus | null; note: string }) => api<Case>(`cases/${id}`, { body: u })
export const updateCaseCall = (id: number, u: { status?: CaseStatus | null; note: string }) => `POST /admin/api/cases/${id} ${JSON.stringify(u)}`

/** A DID, a handle or a handle prefix ending in `*`; up to 100. */
export const accounts = (q: string) => api<Account[]>('accounts', { params: { q } })
export const account = (did: string) => api<Account>(`accounts/${enc(did)}`)
export const takedown = (did: string, reason: string) => api<Account>(`accounts/${enc(did)}/takedown`, { body: { reason } })
export const untakedown = (did: string) => api<Account>(`accounts/${enc(did)}/untakedown`, { method: 'POST' })

/** Host discovery as the leader runs it: each source's last and next run and its counts. */
export const discovery = () => api<DiscoveryView>('discovery')
/** Runs one source now (its key), or every enabled one. */
export const runDiscovery = (source?: string) => api<DiscoveryView>('discovery/run', { body: source ? { source } : {} })
export const runDiscoveryCall = (source?: string) => `POST /admin/api/discovery/run ${JSON.stringify(source ? { source } : {})}`

/** This node's last 500 requestCrawl outcomes, newest first, and today's new-host budget. */
export const admissions = () => api<AdmissionLog>('hosts/admissions')

/** Lifts every account a host created throttled past its cap (through the leader's log). */
export const releaseThrottled = (h: string) => api<Released>(`hosts/${enc(h)}/release-throttled`, { method: 'POST' })

/** The PLC export as the leader reads it (`enabled` false without --plc-export); any node answers. */
export const plc = () => api<PlcView>('ops/plc')

/** com.atproto.sync.requestCrawl on this relay: the same admission path a PDS takes. */
export async function requestCrawl(hostname: string): Promise<void> {
  const r = await fetch('/xrpc/com.atproto.sync.requestCrawl', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hostname }) })
  if (!r.ok) {
    let body: { error?: string; message?: string } = {}
    try {
      body = await r.json()
    } catch {
      /* not JSON */
    }
    throw new ApiError(r.status, body.error ?? `HTTP ${r.status}`, body.message ?? (r.status === 404 ? 'This server has no requestCrawl (the admin demo has none).' : ''))
  }
}

// ---------------------------------------------------------------- answered by this node

/**
 * Frames this node read: with `rejects` the ones that never reached the stream (rejected or
 * held), with `host` that host's frames (passed ones carry their seq). Newest first.
 */
export const tail = (o: { host?: string; rejects?: boolean; sinceMs?: number; limit?: number }) =>
  api<TailFrame[]>('ops/tail', { params: { host: o.host || undefined, rejects: o.rejects ? 1 : undefined, sinceMs: o.sinceMs, limit: o.limit } })

/** The object store as this node uses it: requests by purpose and class, rates, bytes, latency, the last retention pass. */
export const store = () => api<StoreView>('store')

/** `retain/qlog` as the leader's last pass wrote it (snake_case, as in the bucket). */
export type RetainReport = {
  pruned_seq: number
  plan: {
    at_ms: number
    horizon_secs: number
    flushed: number
    segments: number
    segment_bytes: number
    deletable: { ordinal: number; first: number; last: number; bytes: number; age_secs: number }[]
    deletable_bytes: number
    pruned_seq_after: number
    stale_segments: number[]
    states: { path: string; current: boolean; referenced: boolean; objects: number; bytes: number; stale_checkpoints: string[]; deletable: boolean }[]
  }
  applied?: { segments: number; segment_bytes: number; pruned_seq: number; state_paths: string[]; state_objects: number; state_bytes: number; kept: string[] }
}
/** The one place that reads the report's wire shape. */
export function retentionOf(v?: StoreView): RetainReport | undefined {
  const r = v?.retention as (RetainReport & { plan: { state?: RetainReport['plan']['states'] } }) | null | undefined
  if (!r || typeof r !== 'object' || !r.plan) return undefined
  // admin_demo's report leaves some lists out and names `states` `state`
  const p = r.plan
  const a = r.applied
  return {
    pruned_seq: r.pruned_seq ?? 0,
    plan: {
      ...p,
      deletable: p.deletable ?? [],
      deletable_bytes: p.deletable_bytes ?? 0,
      pruned_seq_after: p.pruned_seq_after ?? r.pruned_seq ?? 0,
      stale_segments: p.stale_segments ?? [],
      states: (p.states ?? p.state ?? []).map((x) => ({ ...x, stale_checkpoints: x.stale_checkpoints ?? [], referenced: !!x.referenced, deletable: !!x.deletable })),
    },
    applied: a && { ...a, pruned_seq: a.pruned_seq ?? r.pruned_seq ?? 0, state_paths: a.state_paths ?? [], kept: a.kept ?? [] },
  }
}

/** When F last moved: the leader's flush.last_at_ms, else (older builds) when this console saw F move. */
export const flushedAt = (lead: QStatus | undefined, seenAt: number | undefined) => lead?.flush?.last_at_ms || seenAt

// ---------------------------------------------------------------- the quorum's history, budgets, signals, takedowns

/** Every member's leadership changes, newest first, and the members that didn't answer. */
export const quorumHistory = () => api<QuorumHistory>('cluster/quorum/history')

/** The leader flushes to the commit index it has now (the nodes' --qlog-admin-token). */
export const flushNow = () => api<QStatus>('cluster/quorum/flush', { method: 'POST' })
export const flushNowCall = 'POST /admin/api/cluster/quorum/flush'

/** The cluster budgets against what this node sees them spend. */
export const budgetUse = () => api<PolicyUsage>('policy/usage')

/** Each spam rule's threshold and its ten heaviest keys on this node. */
export const spamSignals = () => api<SignalsView>('policy/signals')

/** Every account under a relay takedown, newest first. */
export const takedowns = () => api<TakedownEntry[]>('takedowns')

/** A member's flags, asked over the peer protocol (this node's when `node` is empty). */
export const settingsOf = (node: string) => api<SettingsView>('settings', { params: { node: node || undefined } })

// ---------------------------------------------------------------- what the console wants next

export type RejectTopHost = RejectTop

let hasRejectsTop: boolean | undefined
/**
 * The hosts sending the most rejects for one reason, cluster-wide. A relay that doesn't serve it
 * answers 404 once; after that the console stops asking until the page reloads.
 */
export async function rejectsTop(reason: string, limit = 10): Promise<Optional<RejectTopHost[]>> {
  if (hasRejectsTop === false) return { supported: false, endpoint: 'GET ops/rejects/top', why: '' }
  const r = await optional('GET ops/rejects/top', () => api<RejectTopHost[]>('ops/rejects/top', { params: { reason, limit } }))
  hasRejectsTop = r.supported
  return r
}

/** Endpoints the design assumes but the relay doesn't serve: answer with `missing(endpoint, why)` and list them here (CONSOLE.md has the table). */
export const MISSING: readonly (readonly [endpoint: string, feeds: string])[] = []
export { missing }
