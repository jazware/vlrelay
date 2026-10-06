import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { MISSING } from '../../lib/console/adminAdapter'
import { handleOf, setTailFilter, useFirehose, type FhEvent, type FhKind } from '../../lib/console/firehose'
import { clock, fmtBytes, fmtSi, seqS, shortDid } from '../../lib/console/fmt'
import { togglePaused, useLiveState } from '../../lib/console/live'
import { openDialog } from './dialogs'
import { Json, Src } from './kit'

// The firehose as this relay sends it, newest first: a sample of a few rows a second, or every
// frame that matches the filter (a DID, a handle or a collection), which follows one account at
// full rate. Scrolled down, new rows land above without moving what you're reading.

const KINDS: { k: FhKind; color: string }[] = [
  { k: 'commit', color: 'signal' },
  { k: 'identity', color: 'info' },
  { k: 'account', color: 'warn' },
  { k: 'sync', color: 'violet' },
]
const ROW_H = 20
const opClass = (a: string) => (a === 'create' ? 'op-c' : a === 'update' ? 'op-u' : 'op-d')

function Body({ e }: { e: FhEvent }) {
  const h = handleOf(e.did)
  const who = <span className="h">{h ?? shortDid(e.did)}</span>
  if (e.kind === 'commit') {
    const o = e.ops[0]
    if (!o)
      return (
        <>
          {who} <span className="muted">empty commit</span>
        </>
      )
    return (
      <>
        {who} <span className={opClass(o.action)}>{o.action}</span> {o.path.split('/')[0]}
        {e.ops.length > 1 && <span className="muted"> +{e.ops.length - 1}</span>}
      </>
    )
  }
  if (e.kind === 'identity')
    return (
      <>
        {who} identity · handle {e.handle ?? <span className="muted">none</span>}
      </>
    )
  if (e.kind === 'account')
    return (
      <>
        {who} active={String(e.active)}
        {e.status ? ` status=${e.status}` : ''}
      </>
    )
  return (
    <>
      {who} sync · rev {e.rev}
    </>
  )
}

function eventDialog(e: FhEvent) {
  openDialog((close) => (
    <div className="cx-dlg wide" role="dialog" aria-modal="true" aria-labelledby="cx-dlg-t">
      <div className="dh">
        <div className="ico" aria-hidden="true">
          #
        </div>
        <h2 id="cx-dlg-t">
          #{e.kind} · seq {seqS(e.seq)}
        </h2>
      </div>
      <div className="db" style={{ paddingLeft: 18 }}>
        <p className="muted sm" style={{ margin: 0 }}>
          Arrived {clock(e.at)} · {fmtBytes(e.frameBytes)} frame · {e.did}
        </p>
        <Json value={e.body} />
      </div>
      <div className="df">
        <span className="call">com.atproto.sync.subscribeRepos</span>
        <button type="button" className="cx-btn" onClick={close} autoFocus>
          Close
        </button>
      </div>
    </div>
  ))
}

export function LiveTail({ height = 300, max = 80 }: { height?: number; max?: number }) {
  const fh = useFirehose()
  const live = useLiveState()
  const [q, setQ] = useState('')
  const [kinds, setKinds] = useState<Set<FhKind>>(() => new Set(KINDS.map((k) => k.k)))
  const box = useRef<HTMLDivElement>(null)
  const lastTop = useRef<number | undefined>(undefined)
  const ql = q.trim().toLowerCase().replace(/^@/, '')

  const match = useMemo(() => {
    if (!ql) return undefined
    return (e: FhEvent) => {
      const h = handleOf(e.did) ?? ''
      return h.includes(ql) || e.did.includes(ql) || e.ops.some((o) => o.path.includes(ql))
    }
  }, [ql])
  useEffect(() => {
    setTailFilter(match)
    return () => setTailFilter(undefined)
  }, [match])

  const rows = useMemo(() => {
    const out: FhEvent[] = []
    for (let i = fh.events.length - 1; i >= 0 && out.length < max; i--) {
      const e = fh.events[i]
      if (!kinds.has(e.kind)) continue
      if (match && !match(e)) continue
      out.push(e)
    }
    return out
  }, [fh.events, kinds, match, max])

  // keep the reader's place: rows added on top push the scroll down by as much
  useLayoutEffect(() => {
    const el = box.current
    const top = rows[0]?.id
    if (el && lastTop.current !== undefined && top !== lastTop.current && el.scrollTop > 4) {
      const added = rows.findIndex((r) => r.id === lastTop.current)
      if (added > 0) el.scrollTop += added * ROW_H
    }
    lastTop.current = top
  }, [rows])

  const rejects = MISSING.find(([e]) => e.startsWith('GET ops/tail'))!
  return (
    <>
      <div className="cx-tailbar">
        <input className="cx-inp mono" placeholder="follow a DID, handle or collection" value={q} onChange={(e) => setQ(e.target.value)} spellCheck={false} aria-label="Filter events" />
        {KINDS.map(({ k, color }) => (
          <button
            key={k}
            type="button"
            className={`cx-tog${kinds.has(k) ? ' on' : ''}`}
            aria-pressed={kinds.has(k)}
            onClick={() =>
              setKinds((s) => {
                const n = new Set(s)
                if (n.has(k)) n.delete(k)
                else n.add(k)
                return n
              })
            }
          >
            <span className="cx-sw" style={{ background: `var(--${color})` }} />#{k}
          </button>
        ))}
        <button type="button" className="cx-tog" disabled title={`Rejected and held frames never reach the firehose. Needs ${rejects[0]}.`}>
          <span className="cx-sw" style={{ background: 'var(--err)' }} />
          rejects
        </button>
        <button type="button" className={`cx-tog${live.paused ? ' on' : ''}`} onClick={togglePaused} title="Pause or resume (space)">
          {live.paused ? '▶ resume' : '❚❚ pause'}
        </button>
      </div>
      <div className="cx-tail" ref={box} style={{ height }} role="log" aria-label="Firehose events">
        {rows.map((e) => (
          <div key={e.id} className={`cx-tl-row k-${e.kind}${e.at > Date.now() - 1500 ? ' new' : ''}`} onClick={() => eventDialog(e)}>
            <span className="ts">{clock(e.at)}</span>
            <span className="sq" title={String(e.seq)}>
              …{String(e.seq).slice(-7)}
            </span>
            <span className="kd">#{e.kind}</span>
            <span className="bd">
              <Body e={e} />
            </span>
            <span className="hs">{fmtBytes(e.frameBytes)}</span>
          </div>
        ))}
        {!rows.length && (
          <div className="cx-empty">
            {fh.status === 'open'
              ? fh.events.length
                ? 'Nothing matches the filter yet.'
                : 'Connected. Waiting for the next event.'
              : fh.status === 'connecting'
                ? 'Connecting to subscribeRepos…'
                : `The firehose isn't reachable from this page${fh.error ? ` (${fh.error})` : ''}. Retrying.`}
          </div>
        )}
      </div>
      <div className="cx-tailfoot">
        <span>
          {live.paused ? (
            <span className="s-acc">paused · {fh.held.toLocaleString()} rows held</span>
          ) : fh.status === 'open' ? (
            <>
              <span className="s-sig">●</span> {match ? 'following matches at full rate' : 'following the merged stream'}
            </>
          ) : (
            <span className="s-warn">▲ {fh.status === 'connecting' ? 'connecting' : 'reconnecting'}</span>
          )}
        </span>
        <span>
          {match ? `every match of ${fmtSi(fh.decodedRate)} decoded/s` : `sampled ~6 rows/s of ${fmtSi(fh.rate)} frames/s`}
          {fh.rate > fh.decodedRate * 1.05 && ` · decoding ${fmtSi(fh.decodedRate)}/s`}
        </span>
        <span className="r">
          <Src>com.atproto.sync.subscribeRepos</Src> <Src isNew>rejects + held frames</Src>
        </span>
      </div>
    </>
  )
}
