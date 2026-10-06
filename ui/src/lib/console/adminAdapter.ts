import {
  api,
  ApiError,
  enc,
  publicStats,
  type Account,
  type AdmissionLog,
  type Case,
  type CaseDetail,
  type CaseStatus,
  type ClusterView,
  type Consumer,
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
  type QuorumView,
  type Released,
  type SettingsView,
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

export type HostSort = 'host' | 'events' | 'errors' | 'accounts' | 'seq' | 'tier' | 'status' | 'since' | 'lag'
export type HostQuery = { q?: string; tier?: string; status?: HostStatus | ''; sort: HostSort; desc: boolean; limit?: number; offset?: number }
/** Server-side filter, sort and page (`limit` default 10,000 on the server). */
export const hosts = (q: HostQuery) => api<HostList>('hosts', { params: { q: q.q || undefined, tier: q.tier || undefined, status: q.status || undefined, sort: q.sort, desc: q.desc, limit: q.limit, offset: q.offset } })
export const host = (h: string) => api<HostDetail>(`hosts/${enc(h)}`)
export const hostAction = (h: string, a: HostAction) => api<HostRow>(`hosts/${enc(h)}/action`, { body: a })
/** What hostAction sends, for the confirm dialog's footer. */
export const hostActionCall = (h: string, a: HostAction) => `POST /admin/api/hosts/${h}/action ${JSON.stringify(a)}`

export const cluster = () => api<ClusterView>('cluster')
export const consumers = () => api<Consumer[]>('consumers')
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

/** This node's last 500 requestCrawl outcomes, newest first, and today's new-host budget. */
export const admissions = () => api<AdmissionLog>('hosts/admissions')

/** Lifts every account a host created throttled past its cap (through the leader's log). */
export const releaseThrottled = (h: string) => api<Released>(`hosts/${enc(h)}/release-throttled`, { method: 'POST' })

export async function plc(): Promise<Optional<PlcView>> {
  try {
    return { supported: true, data: await api<PlcView>('ops/plc') }
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return { supported: false, endpoint: 'ops/plc', why: e.message }
    throw e
  }
}

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

// ---------------------------------------------------------------- what the console wants next


export type TailFrame = { atMs: number; host: string; did: string; kind: 'reject' | 'held'; reason?: string; upstreamSeq?: number }
/** A sample of frames that never reached the firehose (rejected or read but held), and one host's frames at full rate. */
export const tailFrames = (_host?: string) => missing<TailFrame[]>('GET ops/tail?host=&rejects=1', 'rejected and held frames in the tail, and following one host')

/** A history for each of the busiest hosts, so the exchange and the Busiest table can draw real trunks. */
export const topHostHistory = () => missing<Record<string, number[]>>('overview.topHosts[].history', 'per-host rate history on the overview')

/** When the leader last flushed (F's age); the status only has F itself. */
export const lastFlush = () => missing<{ atMs: number }>('qlog status.flush.last_at_ms', 'the time of the last flush')

export type CostModel = { hostsPerMonth: number; bucketPerMonth: number; classAPerSec: number; classBPerSec: number; storedBytes: number }
/** The monthly bill: node prices from config, bucket requests and storage priced by provider. */
export const costModel = () => missing<CostModel>('GET store/cost', 'the cost model')


/** How many accounts each host has that were created throttled past its cap. */
export const throttledAccounts = () => missing<Record<string, number>>('HostRow.throttledAccounts', 'accounts created throttled, per host')

export type BudgetUse = { plcLookupsPerSec: number; newAccountsPerMin: number }
/** What the cluster budgets are spending now, against policy.cluster. */
export const budgetUse = () => missing<BudgetUse>('GET policy/usage', 'live use of the cluster budgets')

export type SignalTop = { signal: string; keys: { key: string; count: number }[] }
/** The heaviest keys each spam signal is tracking, against its threshold. */
export const spamSignals = () => missing<SignalTop[]>('GET policy/signals', 'the heaviest keys per spam signal')

/** Every account taken down on this relay; today the API only answers per DID. */
export const takedowns = () => missing<Account[]>('GET takedowns', 'the list of takedowns')

/** Another node's flags (the settings endpoint answers for the node you reach). */
export const settingsOf = (node: string) => missing<SettingsView>(`GET settings?node=${node}`, "another node's flags")

/** Everything above, for CONSOLE.md and the "needs a newer vlRelay" placeholders. */
export const MISSING = [
  ['GET ops/tail?host=&rejects=1', 'Overview tail: rejected and held frames (the firehose only carries what passed), and following one host at full rate'],
  ['overview.topHosts[].history', 'Overview: per-host rate series for the exchange trunks and the Busiest hosts sparklines (the console keeps its own from polls until then)'],
  ['qlog status.flush.last_at_ms', 'Overview health line: "flushed N s ago" (F alone has no time)'],
  ['GET store/cost', 'Overview rail and health line: the monthly bill (node prices, bucket requests by class, storage)'],
  ['HostRow.throttledAccounts', 'Moderation › Accounts created throttled: how many each host has (the panel lists hosts at their cap until then)'],
  ['GET policy/usage', 'Policy › Cluster budgets: PLC lookups/s and new accounts/min against their budgets (new hosts today comes from hosts/admissions)'],
  ['GET policy/signals', 'Policy › Spam signals and Moderation: the heaviest key per signal against its threshold (open cases stand in until then)'],
  ['GET takedowns', 'Moderation: every account taken down here (today the API answers per DID)'],
  ['GET settings?node=', 'Settings: each node’s flags side by side, and the flags that differ across nodes'],
] as const
