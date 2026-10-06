import type { ReactNode } from 'react'
import { Chip, Swatch, type BannerSpec } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import { hostActionDialog } from '../../components/console/hostActions'
import { REASON_LABEL } from '../../components/relay'
import type { Consumer, HostRow, RejectReason } from '../../lib/api'
import { fmtMs, fmtNum, plural } from '../../lib/console/fmt'
import { isSlow } from '../../lib/console/polls'
import type { RelayView } from '../../lib/console/relay'
import { Link } from '../../lib/router'

// What the Overview and Hosts pages share: the banners, reject wording, a node tag.

export const REASON_WHAT: Record<RejectReason, string> = {
  'bad-signature': "the commit's signature doesn't verify against the account's key",
  'invalid-commit': "the commit or its blocks don't check out",
  'rev-out-of-order': "the rev didn't move forward (often a replay after a reconnect)",
  'prev-data-mismatch': "prevData doesn't match the stored data CID; the account waits for a #sync",
  'wrong-host': 'the DID document names another PDS',
  'unknown-did': "the DID couldn't be resolved",
  'too-large': 'the frame is over the size limits',
  'rate-limited': "a host's rate or a new-account budget was spent",
  takendown: 'the account is taken down on this relay',
  inactive: 'the account is deactivated, suspended, throttled or deleted',
  malformed: "the frame isn't valid DAG-CBOR",
}
export const reasonLabel = (r: string) => REASON_LABEL[r as RejectReason] ?? r

export function NodeTag({ view, id }: { view?: RelayView; id: string }) {
  if (!id) return <span className="muted">—</span>
  const n = view?.byId.get(id)
  return (
    <span className="cx-cellid">
      <Swatch color={n?.color} title={n ? undefined : 'not a node this console knows'} />
      <span className="mono sm">{id}</span>
    </span>
  )
}

const hostLink = (h: string) => (
  <button type="button" className="cx-linklike" onClick={() => openPanel('host', h)}>
    {h}
  </button>
)

/** The relay-wide notices: the quorum, nodes not answering, hosts falling behind or at their cap, slow consumers. */
export function relayBanners(o: {
  view?: RelayView
  throttled?: HostRow[]
  capped?: HostRow[]
  consumers?: Consumer[]
  slowCutMs: number
  scope: 'overview' | 'hosts'
}): BannerSpec[] {
  const out: BannerSpec[] = []
  const q = o.view?.quorum
  if (q && q.health === 'down') {
    out.push({
      id: 'held',
      tone: 'err',
      title: `The firehose is held: ${q.answering.length} of ${q.members.length} members answering`,
      desc: q.leader ? 'the leader has no majority' : 'no leader',
      right: <Chip k="err">page</Chip>,
      open: true,
      body: (
        <>
          <p>
            Nothing is emitted past seq <span className="mono">{fmtNum(q.commit)}</span> until a majority holds the log. No event a consumer saw is lost: every emitted seq was on a majority
            first. Readers stay connected but pause at their in-flight cap, so PDSes buffer.
          </p>
          <p>
            Missing: {q.members.filter((m) => !q.answering.includes(m)).map((m) => <span key={m} className="mono">{m} </span>)}. <Link to="/admin/quorum">Quorum log</Link>
          </p>
        </>
      ),
    })
  } else if (q && q.health === 'degraded') {
    const missing = q.members.filter((m) => !q.answering.includes(m))
    const spare = q.answering.length - Math.floor(q.members.length / 2) - 1
    out.push({
      id: 'degraded',
      tone: 'warn',
      title: `${missing.join(', ')} ${missing.length === 1 ? 'is' : 'are'} not answering`,
      desc: `${q.answering.length} of ${q.members.length} members answering · epoch ${q.epoch}${q.leader ? ` · ${q.leader} leads` : ''}`,
      right: spare <= 0 ? <Chip k="warn">no spare</Chip> : undefined,
      body: (
        <p>
          The quorum is committing with {q.answering.length} of {q.members.length}.{' '}
          {spare <= 0 ? 'One more member down holds the firehose.' : `It can lose ${plural(spare, 'more member')} before the firehose is held.`} <Link to="/admin/quorum">Quorum log</Link>
        </p>
      ),
    })
  }
  for (const n of o.view?.nodes ?? []) {
    if (!n.stale || q?.members.includes(n.id)) continue
    out.push({ id: `stale-${n.id}`, tone: 'warn', title: `${n.id} didn't answer`, desc: n.error ?? 'its numbers are left out of the totals' })
  }

  const behind = (o.throttled ?? []).filter((h) => h.lagMs > 60_000)
  if (behind.length) {
    const h = behind[0]
    out.push({
      id: 'behind',
      tone: 'warn',
      title: behind.length === 1 ? `${h.host} is ${fmtMs(h.lagMs)} behind its own stream` : `${plural(behind.length, 'throttled host')} more than a minute behind`,
      desc: h.throttle != null ? `throttled to ${fmtNum(h.throttle)}/s` : `in the ${h.tier} tier`,
      right: behind.length > 1 ? `worst ${fmtMs(h.lagMs)}` : undefined,
      body: (
        <>
          <p>
            Throttled hosts are held back instead of dropped, so they fall behind until their PDS cuts the relay off (ConsumerTooSlow) and whatever it no longer holds is gone. Raise the
            limit or move the host to a roomier tier.
          </p>
          <p>{behind.slice(0, 6).map((b, i) => <span key={`${b.host}#${i}`}>{i > 0 && ' · '}{hostLink(b.host)} {fmtMs(b.lagMs)}</span>)}</p>
          <div className="cx-form-row">
            <button type="button" className="cx-btn sm" onClick={() => openPanel('host', h.host)}>
              Open {h.host}
            </button>
            {h.throttle != null && (
              <button type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('unthrottle', h)}>
                Lift its throttle…
              </button>
            )}
          </div>
        </>
      ),
    })
  }

  const atCap = (o.capped ?? []).filter((h) => h.maxAccounts > 0 && h.accounts >= h.maxAccounts && h.status !== 'banned' && h.status !== 'suspended')
  if (atCap.length)
    out.push({
      id: 'cap',
      tone: 'warn',
      title: `${plural(atCap.length, 'host')} at the account cap`,
      desc: atCap.slice(0, 3).map((h) => h.host).join(', ') + (atCap.length > 3 ? '…' : ''),
      right: 'policy',
      body: (
        <>
          <p>Past its cap a host's new accounts are created throttled, and stay that way until an operator lifts them. Raise a real PDS's cap from its page.</p>
          <p>
            {atCap.slice(0, 10).map((h, i) => (
              <span key={`${h.host}#${i}`}>
                {i > 0 && ' · '}
                {hostLink(h.host)} {fmtNum(h.accounts)}/{fmtNum(h.maxAccounts)}
              </span>
            ))}
          </p>
        </>
      ),
    })

  if (o.scope === 'overview') {
    const slow = (o.consumers ?? []).filter((c) => isSlow(c, o.slowCutMs)).sort((a, b) => b.lagMs - a.lagMs)
    if (slow.length) {
      const s = slow[0]
      out.push({
        id: 'slow',
        tone: 'warn',
        title: slow.length === 1 ? 'A consumer is falling behind' : `${slow.length} consumers are falling behind`,
        desc: (
          <>
            <span className="mono">#{s.id}</span> on {s.node} · {s.userAgent || 'no user agent'}
          </>
        ),
        right: `${fmtMs(s.lagMs)} behind`,
        body: (
          <p>
            It reads slower than the stream. At the slow-consumer cutoff ({fmtMs(o.slowCutMs)}) it's disconnected and can resume with its cursor. <Link to="/admin/consumers">Consumers</Link>
          </p>
        ),
      })
    }
  }
  return out
}

/** A tiny legend item. */
export const Lg = ({ color, dashed, children }: { color: string; dashed?: boolean; children: ReactNode }) => (
  <span>
    <i style={dashed ? { borderTop: `1.5px dashed var(--${color})`, background: 'none' } : { background: `var(--${color})` }} />
    {children}
  </span>
)
