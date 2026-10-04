import { useState } from 'react'
import { Live, Tile } from '../components/relay'
import { ErrorNotice, Loading, Notice, Panel, Status } from '../components/ui'
import type { ClusterView } from '../lib/api'
import { fmtBytes, fmtNum, fmtSi } from '../lib/format'
import { useApi } from '../lib/useApi'

const POLL = 2000
const SLOTS = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6']

/** A node's color, by its position among node ids, so it keeps it while it lives. */
function colorOf(nodes: string[], id: string | null): string | undefined {
  if (!id) return undefined
  const i = nodes.indexOf(id)
  if (i < 0) return undefined
  if (nodes.length <= SLOTS.length) return `var(--${SLOTS[i]})`
  return `oklch(${i % 2 ? 0.6 : 0.74} 0.13 ${Math.round(160 + (i * 360) / nodes.length) % 360})`
}

export function Cluster() {
  const l = useApi<ClusterView>('cluster', undefined, POLL)
  const [focus, setFocus] = useState<string | null>(null)
  const c = l.data
  if (!c) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const cores = c.nodes.filter((n) => (n.role ?? 'core') === 'core')
  const ids = cores.map((n) => n.id).sort()
  const revs = [...new Set(cores.map((n) => n.rev))]
  const stale = c.nodes.filter((n) => n.stale)
  const unownedHost = c.hostShards.filter((x) => !x).length
  const unownedDid = c.didShards.filter((x) => !x).length
  const now = Date.now()
  const evIn = c.nodes.reduce((a, n) => a + n.eventsInPerSec, 0)
  const consumers = c.nodes.reduce((a, n) => a + n.consumers, 0)
  const healthy = c.nodes.filter((n) => !n.stale && n.reachable && ((n.role ?? 'core') !== 'core' || n.leaseValid)).length

  return (
    <>
      <div className="console-head">
        <h1>Cluster</h1>
        <Live at={l.at} error={l.error} every={POLL} />
      </div>
      <ErrorNotice error={l.error} />
      {stale.length > 0 && (
        <Notice kind="err">
          Not answering: {stale.map((n) => `${n.id} (${n.error ?? 'no answer'})`).join(', ')}. Its row shows dashes and the totals leave it out.
        </Notice>
      )}
      {revs.length > 1 && <Notice kind="warn">Mixed builds: {revs.join(', ')}. Fine during a rolling deploy, worth a look otherwise.</Notice>}
      {(unownedHost > 0 || unownedDid > 0) && (
        <Notice kind="err">
          {unownedHost} host shards and {unownedDid} DID shards have no owner. Their hosts aren't being read until a node takes the lease.
        </Notice>
      )}
      <div className="tiles">
        <Tile k="Nodes" v={<>{healthy}<small>of {c.nodes.length} healthy</small></>} sub={`${cores.length} core, ${c.nodes.length - cores.length} edge or replica`} tone={healthy < c.nodes.length ? 'bad' : undefined} />
        <Tile k="Host shards" v={fmtNum(c.hostShards.length)} sub={unownedHost ? `${unownedHost} unowned` : 'all owned'} tone={unownedHost ? 'bad' : undefined} />
        <Tile k="DID shards" v={fmtNum(c.didShards.length)} sub={unownedDid ? `${unownedDid} unowned` : 'all owned'} tone={unownedDid ? 'bad' : undefined} />
        <Tile k="Events in per second" v={fmtSi(evIn)} sub="sum over the cores" />
        <Tile k="Consumers" v={fmtNum(consumers)} sub="sum over every node" />
        <Tile k="Relay seq" v={<span className="mono">{fmtNum(c.lastSeq)}</span>} />
      </div>
      <Panel flush title="Nodes" desc="Cores read the hosts in their host shards and own sync state for their DID shards. Edges and replicas serve the merged stream. Each row is that node's own numbers.">
        <div className="table-wrap">
          <table className="data compact nodes">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Address</th>
                <th>Build</th>
                <th>Lease</th>
                <th className="num">Host shards</th>
                <th className="num">DID shards</th>
                <th className="num">Hosts</th>
                <th className="num">Consumers</th>
                <th className="num">In/s</th>
                <th className="num">Out/s</th>
                <th className="num">Stream seq</th>
                <th className="num">Durability lag</th>
                <th className="num">CPU</th>
                <th className="num">Memory</th>
              </tr>
            </thead>
            <tbody>
              {c.nodes.map((n) => {
                const ttl = Math.max(0, (n.leaseExpiresMs - now) / 1000)
                const core = (n.role ?? 'core') === 'core'
                const dash = <span className="muted">—</span>
                return (
                  <tr key={n.id} className={`${focus === n.id ? 'sel' : ''}${n.stale ? ' stale' : ''}`} onMouseEnter={() => setFocus(n.id)} onMouseLeave={() => setFocus(null)}>
                    <td>
                      <span className="node-id">
                        <span className="sw" style={{ background: colorOf(ids, n.id) }} />
                        <b>{n.id}</b>
                      </span>
                    </td>
                    <td>{n.role ?? 'core'}</td>
                    <td className="mono muted">{n.addr || dash}</td>
                    <td className="mono">
                      {n.version || dash} <span className="muted">{n.rev}</span>
                    </td>
                    <td>
                      {n.stale ? (
                        <Status kind="bad">{core && n.leaseValid ? 'not answering' : 'down'}</Status>
                      ) : !core ? (
                        <Status kind="ok">no lease (follows)</Status>
                      ) : !n.reachable ? (
                        <Status kind="bad">unreachable</Status>
                      ) : n.leaseValid ? (
                        <Status kind={ttl < 3 ? 'warn' : 'ok'}>valid, {ttl.toFixed(0)} s left</Status>
                      ) : (
                        <Status kind="bad">expired</Status>
                      )}
                    </td>
                    <td className="num">{core ? n.hostShards : dash}</td>
                    <td className="num">{core ? n.didShards : dash}</td>
                    <td className="num">{n.stale || !core ? dash : fmtNum(n.hosts)}</td>
                    <td className="num">{n.stale ? dash : n.consumers}</td>
                    <td className="num">{n.stale || !core ? dash : fmtSi(n.eventsInPerSec)}</td>
                    <td className="num">{n.stale ? dash : fmtSi(n.eventsOutPerSec)}</td>
                    <td className="num mono">{n.stale ? dash : fmtNum(n.streamSeq ?? 0)}</td>
                    <td className={`num${n.logDurabilityLagMs > 80 ? ' err-mid' : ''}`}>{n.stale || !core ? dash : `${n.logDurabilityLagMs.toFixed(0)} ms`}</td>
                    <td className="num">{n.stale ? dash : `${n.cpu.toFixed(1)} cores`}</td>
                    <td className="num">{n.stale ? dash : fmtBytes(n.memBytes)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Panel>
      <div className="grid2">
        <ShardMap title="Host shards" desc="Which node reads each shard's upstream hosts." shards={c.hostShards} ids={ids} cols={16} focus={focus} setFocus={setFocus} />
        <ShardMap title="DID shards" desc="Which node holds each shard's per-DID sync state." shards={c.didShards} ids={ids} cols={32} focus={focus} setFocus={setFocus} />
      </div>
    </>
  )
}

function ShardMap({
  title,
  desc,
  shards,
  ids,
  cols,
  focus,
  setFocus,
}: {
  title: string
  desc: string
  shards: (string | null)[]
  ids: string[]
  cols: number
  focus: string | null
  setFocus: (f: string | null) => void
}) {
  const [hover, setHover] = useState<number | null>(null)
  return (
    <Panel title={title} desc={desc}>
      <div className={`shardmap${focus ? ' focus' : ''}`} style={{ ['--cols' as string]: cols }} onMouseLeave={() => setHover(null)}>
        {shards.map((o, i) => (
          <span
            key={i}
            className={`shard${o ? '' : ' unowned'}${focus && o === focus ? ' hl' : ''}`}
            style={{ background: colorOf(ids, o) }}
            onMouseEnter={() => setHover(i)}
            title={`shard ${i}: ${o ?? 'unowned'}`}
          />
        ))}
      </div>
      <div className="inspect">{hover !== null ? `Shard ${hover}: ${shards[hover] ?? 'unowned'}` : 'Hover a shard, or a node to highlight its shards.'}</div>
      <div className="legend">
        {ids.map((id) => (
          <button key={id} type="button" onMouseEnter={() => setFocus(id)} onMouseLeave={() => setFocus(null)} onFocus={() => setFocus(id)} onBlur={() => setFocus(null)}>
            <span className="sw" style={{ background: colorOf(ids, id) }} />
            {id} <span className="count">{shards.filter((s) => s === id).length}</span>
          </button>
        ))}
      </div>
    </Panel>
  )
}
