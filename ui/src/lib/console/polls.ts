import { publicStats, type Case, type ClusterView, type Consumer, type FullPolicyDoc, type HostList, type Overview, type PolicyDoc, type PublicStats, type QuorumView, type SettingsView } from '../api'
import * as A from './adminAdapter'
import { createPoller } from './live'

// One shared poll per thing the console shows (createPoller in live.ts). The overview is the
// heartbeat: when it fails the shell shows "Not updating". A few values the API has no series for
// (the stream's own rate, the commit latency, each busy host's rate) are kept here from the polls,
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
    for (const h of o.topHosts) push(`host:${h.host}`, h.eventsPerSec)
    for (const n of o.byNode ?? []) if (!n.stale) push(`node-in:${n.node}`, n.eventsInPerSec)
    seriesVersion++
  },
})

export const quorumPoll = createPoller<A.Optional<QuorumView>>(A.quorum, 2000, {
  onData: (q) => {
    if (!q.supported) return
    const lead = q.data.nodes.find((n) => n.status?.role === 'leader' && !n.stale)?.status
    if (lead) {
      push('commit-p99', lead.commit_us?.p99 / 1000)
      push('commit-p50', lead.commit_us?.p50 / 1000)
    }
    seriesVersion++
  },
})

/** The public stats: the page at / polls them, the console reads uptime from them. */
export const publicPoll = createPoller<PublicStats>(publicStats, 2000)
export const clusterPoll = createPoller<ClusterView>(A.cluster, 5000)
export const consumersPoll = createPoller<Consumer[]>(A.consumers, 5000)
export const openCasesPoll = createPoller<Case[]>(A.openCases, 10_000)
export const policyPoll = createPoller<PolicyDoc>(A.policy, 30_000)
export const policyFullPoll = createPoller<FullPolicyDoc>(A.policyFull, 30_000)
export const settingsPoll = createPoller<SettingsView>(A.settings, 60_000)
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
