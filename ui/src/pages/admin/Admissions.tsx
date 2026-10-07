import { useState, type ReactNode } from 'react'
import { Chip, Empty, Loaded, Meter, Panel, Src, TierTag } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import type { CrawlAdmission } from '../../lib/api'
import { ago, dt, fmtNum } from '../../lib/console/fmt'
import { useAdmissions } from '../../lib/console/queries'
import { SourceSelect, SourceTag, sourceOk } from './hostSource'

// Admission outcomes on the node answering (its last 500: requestCrawls, and discovery's when it
// leads), with each one's source, and today's new-host budget. Discovery shows the panel; Hosts
// shows a name's entries when its search finds no host.

const OUT: Record<CrawlAdmission['outcome'], ReactNode> = {
  admitted: <Chip k="ok">admitted</Chip>,
  'rate-limited': <Chip k="warn">429</Chip>,
  refused: <Chip k="err">refused</Chip>,
  banned: <Chip k="err">banned</Chip>,
}

export function AdmissionTable({ entries, maxHeight }: { entries: CrawlAdmission[]; maxHeight?: number }) {
  return (
    <div className="cx-tw" style={maxHeight ? { maxHeight } : undefined}>
      <table className="cx-t compact">
        <tbody>
          {entries.map((a, i) => (
            <tr key={`${a.atMs}-${a.host}-${i}`} data-open={a.outcome === 'admitted' ? `host:${a.host}` : undefined} onClick={a.outcome === 'admitted' ? () => openPanel('host', a.host) : undefined}>
              <td className="sm muted" title={dt(a.atMs)}>
                {ago(a.atMs)}
              </td>
              <td>{OUT[a.outcome] ?? <Chip k="idle">{a.outcome}</Chip>}</td>
              <td className="mono sm">{a.host}</td>
              <td>{a.tier && <TierTag t={a.tier} />}</td>
              <td>
                <SourceTag s={a.source} />
              </td>
              <td className="fill wrap sm t2">{a.reason}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function Admissions() {
  const l = useAdmissions()
  const d = l.data
  const [src, setSrc] = useState('')
  const keys = [...new Set((d?.entries ?? []).map((a) => a.source).filter((s) => s?.startsWith('bootstrap:')))]
  const shown = (d?.entries ?? []).filter((a) => sourceOk(src, a.source)).slice(0, 100)
  return (
    <Panel
      title="Admissions"
      src={<Src>hosts/admissions</Src>}
      right={
        d && (
          <span className="sm t2 nowrap" title="requestCrawl admissions today (UTC) against cluster.newHostsPerDay">
            today <Meter v={d.newHostsToday} max={d.newHostsPerDay} k={d.newHostsToday >= d.newHostsPerDay ? 'err' : d.newHostsToday > d.newHostsPerDay * 0.8 ? 'warn' : 'ok'} />{' '}
            <span className="mono">
              {fmtNum(d.newHostsToday)}/{fmtNum(d.newHostsPerDay)}
            </span>
          </span>
        )
      }
      foot={<span>This node's last 500 admissions, newest first. Allow rules, trusted domains and discovery (its own budget) don't spend the daily one.</span>}
    >
      <div className="cx-toolbar">
        <SourceSelect value={src} onChange={setSrc} keys={keys} />
      </div>
      <Loaded load={l}>{() => (shown.length ? <AdmissionTable entries={shown} maxHeight={320} /> : <Empty>{src ? 'No admission from this source in the last 500.' : 'No admission since this node started.'}</Empty>)}</Loaded>
    </Panel>
  )
}
