import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { TailFrame } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { handleOf, setTailFilter, useFirehose, type FhEvent, type FhKind } from '../../lib/console/firehose'
import { clock, fmtBytes, fmtSi, seqS, shortDid } from '../../lib/console/fmt'
import { togglePaused, useLiveState } from '../../lib/console/live'
import { openDialog } from './dialogs'
import { Json, Src } from './kit'

// The firehose as this relay sends it, newest first: a sample of a few rows a second, or every
// frame that matches the filter (a DID, a handle or a collection), which follows one account at
// full rate. "rejects" adds the frames this node read that never reached the stream (ops/tail),
// and a hostname follows that host's frames at full rate from the node's own record of them.
// Scrolled down, new rows land above without moving what you're reading.

const KINDS: { k: FhKind; color: string }[] = [
  { k: 'commit', color: 'signal' },
  { k: 'identity', color: 'info' },
  { k: 'account', color: 'warn' },
  { k: 'sync', color: 'violet' },
]
const ROW_H = 20
const TAIL_MS = 2000
const TAIL_KEEP = 200
/** A PDS hostname (or host:port on a dev network); a collection NSID has no port and is tried too, and answers nothing. */
const hostLike = (q: string) => !q.startsWith('did:') && !q.includes('/') && /^[a-z0-9-]+(\.[a-z0-9-]+)+(:\d+)?$/.test(q)
const TAIL_COLOR: Record<TailFrame['kind'], string> = { reject: 'err', held: 'warn', passed: 'signal' }

function tailDialog(f: TailFrame) {
  openDialog((close) => (
    <div className="cx-dlg wide" role="dialog" aria-modal="true" aria-labelledby="cx-dlg-t">
      <div className="dh">
        <div className="ico" aria-hidden="true">
          #
        </div>
        <h2 id="cx-dlg-t">
          {f.kind === 'passed' ? `#${f.event ?? 'event'} · seq ${seqS(f.seq)}` : `${f.kind} · ${f.reason ?? ''}`}
        </h2>
      </div>
      <div className="db" style={{ paddingLeft: 18 }}>
        <Json value={f} />
      </div>
      <div className="df">
        <span className="call">GET /admin/api/ops/tail</span>
        <button type="button" className="cx-btn" onClick={close} autoFocus>
          Close
        </button>
      </div>
    </div>
  ))
}

/** ops/tail while `on`: newest first, polled every 2 s from the newest seen. */
function useTail(on: boolean, host: string | undefined, rejects: boolean, paused: boolean) {
  const [rows, setRows] = useState<TailFrame[]>([])
  const [error, setError] = useState<string>()
  useEffect(() => {
    setRows([])
    setError(undefined)
    if (!on) return
    let since: number | undefined
    let live = true
    const tick = async () => {
      try {
        const fr = await A.tail({ host, rejects, sinceMs: since, limit: TAIL_KEEP })
        if (!live) return
        setError(undefined)
        const fresh = since === undefined ? fr : fr.filter((f) => f.atMs > since!)
        if (fr.length) since = Math.max(since ?? 0, fr[0].atMs)
        if (fresh.length) setRows((r) => [...fresh, ...r].slice(0, TAIL_KEEP))
      } catch (e) {
        if (live) setError(e instanceof Error ? e.message : String(e))
      }
    }
    tick()
    const id = paused ? undefined : setInterval(tick, TAIL_MS)
    return () => {
      live = false
      if (id) clearInterval(id)
    }
  }, [on, host, rejects, paused])
  return { rows, error }
}

type Row = { t: 'fh'; at: number; key: string; e: FhEvent } | { t: 'tail'; at: number; key: string; f: TailFrame }
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
  const [rejectsOn, setRejectsOn] = useState(false)
  const box = useRef<HTMLDivElement>(null)
  const lastTop = useRef<string | undefined>(undefined)
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

  const followHost = hostLike(ql) ? ql : undefined
  const tl = useTail(rejectsOn || !!followHost, followHost, rejectsOn, live.paused)
  // a hostname the node has frames for: the host's own frames replace the stream's
  const hostMode = !!followHost && tl.rows.length > 0

  const rows = useMemo(() => {
    const out: Row[] = []
    if (!hostMode)
      for (let i = fh.events.length - 1; i >= 0 && out.length < max; i--) {
        const e = fh.events[i]
        if (!kinds.has(e.kind)) continue
        if (match && !match(e)) continue
        out.push({ t: 'fh', at: e.at, key: `e${e.id}`, e })
      }
    for (const f of tl.rows) {
      if (f.kind === 'passed' && !hostMode) continue
      if (f.kind !== 'passed' && !rejectsOn) continue
      if (f.kind === 'passed' && f.event && !kinds.has(f.event as FhKind)) continue
      out.push({ t: 'tail', at: f.atMs, key: `t${f.atMs}/${f.did}/${f.upstreamSeq ?? f.seq ?? ''}/${f.kind}`, f })
    }
    return out.sort((a, b) => b.at - a.at).slice(0, max)
  }, [fh.events, kinds, match, max, tl.rows, hostMode, rejectsOn])

  // keep the reader's place: rows added on top push the scroll down by as much
  useLayoutEffect(() => {
    const el = box.current
    const top = rows[0]?.key
    if (el && lastTop.current !== undefined && top !== lastTop.current && el.scrollTop > 4) {
      const added = rows.findIndex((r) => r.key === lastTop.current)
      if (added > 0) el.scrollTop += added * ROW_H
    }
    lastTop.current = top
  }, [rows])

  return (
    <>
      <div className="cx-tailbar">
        <input className="cx-inp mono" placeholder="follow a DID, handle, collection or host" value={q} onChange={(e) => setQ(e.target.value)} spellCheck={false} aria-label="Filter events" />
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
        <button
          type="button"
          className={`cx-tog${rejectsOn ? ' on' : ''}`}
          aria-pressed={rejectsOn}
          onClick={() => setRejectsOn((v) => !v)}
          title="Frames this node read that never reached the firehose: rejected, or held (a throttled or deferred account)"
        >
          <span className="cx-sw" style={{ background: 'var(--err)' }} />
          rejects
        </button>
        <button type="button" className={`cx-tog${live.paused ? ' on' : ''}`} onClick={togglePaused} title="Pause or resume (space)">
          {live.paused ? '▶ resume' : '❚❚ pause'}
        </button>
      </div>
      <div className="cx-tail" ref={box} style={{ height }} role="log" aria-label="Firehose events">
        {rows.map((r) => {
          if (r.t === 'fh') {
            const e = r.e
            return (
              <div key={r.key} className={`cx-tl-row k-${e.kind}${e.at > Date.now() - 1500 ? ' new' : ''}`} onClick={() => eventDialog(e)}>
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
            )
          }
          const f = r.f
          const h = handleOf(f.did)
          return (
            <div key={r.key} className={`cx-tl-row k-${f.kind}${f.atMs > Date.now() - 1500 ? ' new' : ''}`} onClick={() => tailDialog(f)}>
              <span className="ts">{clock(f.atMs)}</span>
              <span className="sq" title={f.seq !== undefined ? String(f.seq) : 'never emitted'}>
                {f.seq !== undefined ? `…${String(f.seq).slice(-7)}` : '—'}
              </span>
              <span className="kd" style={f.kind === 'passed' ? undefined : { color: `var(--${TAIL_COLOR[f.kind]})` }}>
                {f.kind === 'passed' ? `#${f.event ?? 'event'}` : f.kind}
              </span>
              <span className="bd">
                <span className="h">{h ?? shortDid(f.did)}</span> {f.kind !== 'passed' && <span style={{ color: `var(--${TAIL_COLOR[f.kind]})` }}>{f.reason ?? ''}</span>}{' '}
                <span className="muted">{f.detail ?? (hostMode ? '' : f.host)}</span>
              </span>
              <span className="hs" title={f.upstreamSeq !== undefined ? `upstream seq ${f.upstreamSeq}` : undefined}>
                {f.upstreamSeq !== undefined ? `u…${String(f.upstreamSeq).slice(-5)}` : ''}
              </span>
            </div>
          )
        })}
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
              <span className="s-sig">●</span> {hostMode ? `following ${followHost} at full rate` : match ? 'following matches at full rate' : 'following the merged stream'}
              {rejectsOn && ' · with rejects'}
              {tl.error && <span className="s-warn"> · tail: {tl.error}</span>}
            </>
          ) : (
            <span className="s-warn">▲ {fh.status === 'connecting' ? 'connecting' : 'reconnecting'}</span>
          )}
        </span>
        <span>
          {hostMode ? `this node's frames from ${followHost}` : match ? `every match of ${fmtSi(fh.decodedRate)} decoded/s` : `sampled ~6 rows/s of ${fmtSi(fh.rate)} frames/s`}
          {fh.rate > fh.decodedRate * 1.05 && ` · decoding ${fmtSi(fh.decodedRate)}/s`}
        </span>
        <span className="r">
          <Src>com.atproto.sync.subscribeRepos</Src> <Src>ops/tail</Src>
        </span>
      </div>
    </>
  )
}
