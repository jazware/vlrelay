import { useEffect, useRef, type ReactNode } from 'react'
import { getAdminOperator, setAdminToken, type Case, type Consumer, type FullPolicyDoc, type HostList, type QuorumHistory, type QuorumView } from '../../lib/api'
import { releaseHeld } from '../../lib/console/firehose'
import { ago, clock, dur, fmtSi } from '../../lib/console/fmt'
import { getLive, togglePaused, toggleSources, useLiveState } from '../../lib/console/live'
import type { Optional } from '../../lib/console/adminAdapter'
import { applyChange, cached, feedLost, keys, refresh, resumeLive } from '../../lib/console/cache'
import { useChangeFeed, useFeed } from '../../lib/console/feed'
import { isSlow, seenEpochs, slowLagMs, useCapHosts, useConsumers, useOpenCases, useOverview, usePolicyFull, useQuorumHistory, useThrottledHosts } from '../../lib/console/queries'
import { useRelay, type RelayView } from '../../lib/console/relay'
import { setTheme, useAdminOperator, useAdminUnlock, useResolvedTheme } from '../../lib/hooks'
import { Link, navigate, usePath } from '../../lib/router'
import { closeDialog, DialogHost, isDialogOpen, openDialog } from './dialogs'
import { detailPath, Drawer } from './Drawer'
import { attention } from '../../pages/admin/relayUi'
import { epochEvents } from '../../pages/admin/quorumUi'
import { GLYPH, Jack, Kbd, Swatch } from './kit'
import { recentList } from './recent'
import { closePanel, openPanel, panelOf } from './nav'
import { isPaletteOpen, lookupProvider, Palette, registerPalette, setPaletteOpen, type PalItem } from './Palette'
import { SECTION, SECTIONS, TABBAR, type Section, type SectionId } from './sections'
import { Toasts } from './toast'

// The frame around every console page: the top bar with the carrier rule, the patch-panel rail
// (a tab bar on phones), the stale banner, and the hosts for the slide-over, palette, dialogs and
// toasts. Owns the keyboard.

/** Four PDS cords merging into one cobalt trunk that ends in the magenta carrier lamp. */
export const Mark = () => (
  <svg width="26" height="18" viewBox="0 0 26 18" aria-hidden="true">
    <g fill="none" stroke="var(--accent)" strokeWidth="1.6" strokeLinecap="round">
      <path d="M1 2C8 2 9 9 15 9" />
      <path d="M1 6.7C7 6.7 9 9 15 9" />
      <path d="M1 11.3C7 11.3 9 9 15 9" />
      <path d="M1 16C8 16 9 9 15 9" />
    </g>
    <path d="M15 9H20.5" stroke="var(--accent)" strokeWidth="3" strokeLinecap="round" />
    <circle cx="23" cy="9" r="2.6" fill="var(--signal)" />
  </svg>
)

export const ThemeIcon = () => (
  <svg width="15" height="15" viewBox="0 0 16 16" aria-hidden="true">
    <circle cx="8" cy="8" r="6.2" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <path d="M8 1.8 A6.2 6.2 0 0 1 8 14.2 Z" fill="currentColor" />
  </svg>
)

export function useThemeToggle() {
  const t = useResolvedTheme()
  return { theme: t, toggle: () => setTheme(t === 'dark' ? 'light' : 'dark') }
}

const lock = () => setAdminToken(null)

export function shortcutsDialog() {
  const rows: [string[], string][] = [
    [['⌘', 'K'], 'Command palette: hosts, DIDs, nodes, verbs like “ban …”'],
    ...SECTIONS.map((s): [string[], string] => [['g', s.key], s.label]),
    [['g', 'u'], 'The public page'],
    [['j', 'k'], 'Move through rows'],
    [['↵'], 'Open the row in a panel'],
    [['o'], 'Open the panel as a full page'],
    [['/'], 'Search on this page'],
    [['space'], 'Pause or resume live updates'],
    [['t'], 'Toggle light / dark'],
    [['esc'], 'Close the panel, dialog or full page'],
  ]
  openDialog((close) => (
    <div className="cx-dlg" role="dialog" aria-modal="true" aria-labelledby="cx-dlg-t">
      <div className="dh">
        <div className="ico" aria-hidden="true">
          ⌘
        </div>
        <h2 id="cx-dlg-t">Keyboard shortcuts</h2>
      </div>
      <div className="cx-keys">
        {rows.map(([k, d]) => (
          <span key={d} style={{ display: 'contents' }}>
            <span>
              <Kbd k={k} />
            </span>
            <span>{d}</span>
          </span>
        ))}
      </div>
      <div className="df">
        <button type="button" className="cx-btn" onClick={close} autoFocus>
          Close
        </button>
      </div>
    </div>
  ))
}

type Badge = { k: 'warn' | 'err' | 'plain'; t: string; title?: string }

function useBadges(): Partial<Record<SectionId, Badge>> {
  const { view } = useRelay()
  const cases = useOpenCases()
  const subs = useConsumers()
  const thr = useThrottledHosts()
  const pol = usePolicyFull()
  const cut = slowLagMs(pol.data)
  const slow = subs.data?.filter((c) => isSlow(c, cut)).length ?? 0
  const crit = cases.data?.some((c) => c.severity === 'critical')
  const nCases = cases.data?.length ?? 0
  const q = view?.quorum
  const down = q ? q.members.length - q.answering.length : (view?.nodes.filter((n) => n.stale).length ?? 0)
  const nThr = thr.data?.total ?? 0
  return {
    quorum: q?.health === 'down' ? { k: 'err', t: 'held', title: 'No quorum: the firehose is held' } : down ? { k: 'err', t: `${down} down` } : undefined,
    hosts: nThr ? { k: 'warn', t: `${nThr} thr`, title: 'Throttled hosts' } : undefined,
    consumers: slow ? { k: 'warn', t: `${slow} slow` } : undefined,
    moderation: nCases ? { k: crit ? 'err' : 'warn', t: String(nCases), title: 'Open cases' } : undefined,
  }
}

function Side({ current }: { current: Section }) {
  const badges = useBadges()
  const { view } = useRelay()
  const live = useLiveState()
  const operator = useAdminOperator()
  const self = view?.self ? view.byId.get(view.self) : view?.nodes[0]
  let group = ''
  return (
    <aside className="cx-side" aria-label="Sections">
      {SECTIONS.map((s) => {
        const head = s.group !== group ? s.group : ''
        group = s.group
        const b = badges[s.id]
        return (
          <div key={s.id} style={{ display: 'contents' }}>
            {head && <h6>{head}</h6>}
            <Link to={s.path} className={`cx-nav${current.id === s.id ? ' on' : ''}`} title={`${s.label} (g ${s.key})`} aria-current={current.id === s.id ? 'page' : undefined}>
              <Jack />
              {s.label}
              {b ? (
                <span className={`cx-badge ${b.k}`} title={b.title}>
                  {b.t}
                </span>
              ) : (
                <span className="k">g {s.key}</span>
              )}
            </Link>
          </div>
        )
      })}
      <div className="cx-sidefoot">
        {(view?.version || self?.version) && (
          <>
            vlRelay <span className="mono">{view?.version || self?.version}</span>
            {self?.rev && (
              <>
                {' '}
                · <span className="mono">{self.rev.slice(0, 8)}</span>
              </>
            )}
            <br />
          </>
        )}
        <Link to="/">Public page ↗</Link> ·{' '}
        <button type="button" className="cx-linklike" onClick={toggleSources} aria-pressed={live.showSources}>
          {live.showSources ? 'Hide' : 'Show'} data sources
        </button>
        <br />
        {operator ? (
          <>
            Signed in as <span className="mono">{operator}</span> by the proxy.
            <br />
          </>
        ) : (
          <>
            <button type="button" className="cx-linklike" onClick={lock}>
              Lock console
            </button>{' '}
            ·{' '}
          </>
        )}
        <button type="button" className="cx-linklike" onClick={shortcutsDialog}>
          shortcuts
        </button>
      </div>
    </aside>
  )
}

const FEED_TITLE = {
  live: 'Changes arrive on the admin change feed as they happen; rates poll every 2 s',
  reconnecting: 'The change feed dropped: every panel polls until it reconnects',
  polling: 'This relay has no change feed: every panel polls',
}

/** The one live indicator: the change feed (live, reconnecting, polling), unless the console is stale, paused or the firehose held. */
function StreamChip({ held }: { held: boolean }) {
  const live = useLiveState()
  const feed = useFeed()
  const ov = useOverview()
  const cls = live.stale ? ' stale' : held ? ' held' : live.paused ? ' paused' : feed.status === 'reconnecting' ? ' recon' : feed.status === 'polling' ? ' poll' : ''
  const text = live.stale ? 'not updating' : held ? 'firehose held' : live.paused ? 'paused' : feed.status
  const rate = ov.data ? (ov.data.streamEventsPerSec ?? ov.data.eventsOutPerSec) : undefined
  const det = live.stale ? `· last data ${live.lastOkAt ? dur(Date.now() - live.lastOkAt) : '—'} ago` : live.paused ? '· space to resume' : rate !== undefined ? `· ${fmtSi(rate)} ev/s` : ''
  const title = live.stale ? 'Retry now' : `${FEED_TITLE[feed.status]}${feed.why && feed.status === 'reconnecting' ? ` (${feed.why})` : ''}${feed.node && feed.status === 'live' ? `, served by ${feed.node}` : ''}. Click or press space to pause.`
  return (
    <button type="button" className={`cx-stream${cls}`} title={title} onClick={() => (live.stale ? refresh(keys.overview()) : togglePaused())}>
      <span className="dot" />
      <span>{text}</span>
      <span className="det muted mono">{det}</span>
    </button>
  )
}

const RECENT_GLYPH: Record<string, string> = { host: '⇄', node: '◆', epoch: '◇', consumer: '◆', case: '▤', acct: '@', rule: '§', flag: '⚑', dsource: '◇', ver: '▤' }

/** Keeps a shared poll running without re-rendering whatever mounts it. */
function Keep({ use }: { use: () => unknown }) {
  use()
  return null
}

/** ⌘K's inbox for an empty query: the banners' notices (each opening its row), then the details opened in this tab. */
function inboxItems(view: RelayView | undefined): PalItem[] {
  const qd = cached<Optional<QuorumView>>(keys.quorum())
  const events = view?.quorum ? epochEvents(qd?.supported ? qd.data : undefined, cached<QuorumHistory>(keys.quorumHistory())?.events ?? [], seenEpochs()) : []
  const hostList = (q: object) => cached<HostList>(keys.hosts(q))
  const att = attention({
    view,
    events,
    throttled: hostList({ status: 'throttled', sort: 'lag', desc: true, limit: 200 })?.hosts,
    capped: hostList({ flag: 'atCap', sort: 'accounts', desc: true, limit: 400 }),
    consumers: cached<Consumer[]>(keys.consumers()),
    slowCutMs: slowLagMs(cached<FullPolicyDoc>(keys.policyFull())),
    cases: cached<Case[]>(keys.cases('open')),
  }).map(
    (a): PalItem => ({
      group: 'Needs attention',
      inbox: true,
      glyph: <span className={`cx-g s-${a.tone}`}>{GLYPH[a.tone]}</span>,
      title: a.title,
      desc: a.desc,
      run: a.run,
    }),
  )
  const recent = recentList().map(
    (r): PalItem => ({
      group: 'Recent',
      inbox: true,
      glyph: RECENT_GLYPH[r.type] ?? '›',
      title: r.title,
      desc: `${/^[A-Z][a-z]/.test(r.kind) ? r.kind[0].toLowerCase() + r.kind.slice(1) : r.kind} · ${ago(r.at)}`,
      run: () => openPanel(r.type, r.id),
    }),
  )
  return [...att, ...recent]
}

/** The shell's own palette entries: sections, console actions, nodes, and the empty query's inbox. */
function useCorePalette() {
  const { view } = useRelay()
  const viewRef = useRef(view)
  viewRef.current = view
  const { toggle } = useThemeToggle()
  const toggleRef = useRef(toggle)
  toggleRef.current = toggle
  useEffect(() => {
    const offLookup = registerPalette(lookupProvider)
    const off = registerPalette({
      items: () => {
        const live = getLive()
        const goto: PalItem[] = [
          ...SECTIONS.map((s): PalItem => ({ group: 'Go to', glyph: '○', title: s.label, keys: ['g', s.key], always: true, run: () => navigate(s.path) })),
          { group: 'Go to', glyph: '○', title: 'Public page', desc: 'what anyone sees at /', keys: ['g', 'u'], run: () => navigate('/') },
        ]
        const acts: PalItem[] = [
          { group: 'Actions', title: live.paused ? 'Resume live updates' : 'Pause live updates', keys: ['space'], always: true, run: togglePaused },
          { group: 'Actions', title: 'Toggle light / dark', keys: ['t'], always: true, run: () => toggleRef.current() },
          { group: 'Actions', title: 'Keyboard shortcuts', keys: ['?'], always: true, run: shortcutsDialog },
          { group: 'Actions', title: live.showSources ? 'Hide data sources' : 'Show data sources', desc: 'which endpoint feeds each panel', run: toggleSources },
          ...(getAdminOperator() ? [] : [{ group: 'Actions', title: 'Lock console', desc: 'forget the admin token in this tab', run: lock }]),
        ]
        const nodes: PalItem[] = (viewRef.current?.nodes ?? []).map((n) => ({
          group: 'Nodes',
          glyph: <Swatch color={n.color} />,
          title: n.id,
          desc: `${n.role}${n.addr ? ` · ${n.addr}` : ''}`,
          run: () => {
            if (!location.pathname.startsWith(SECTION.quorum.path)) navigate(SECTION.quorum.path)
            openPanel('node', n.id)
          },
        }))
        return [...inboxItems(viewRef.current), ...goto, ...acts, ...nodes]
      },
    })
    return () => {
      off()
      offLookup()
    }
  }, [])
}

function useKeyboard(path: string) {
  const { toggle } = useThemeToggle()
  const kb = useRef(-1)
  const gAt = useRef(0)
  const toggleRef = useRef(toggle)
  toggleRef.current = toggle
  useEffect(() => {
    kb.current = -1
  }, [path])
  useEffect(() => {
    const rows = () => [...document.querySelectorAll<HTMLElement>('.cx-view [data-open]')].filter((r) => r.offsetParent !== null && !r.closest('[hidden]'))
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        setPaletteOpen(!isPaletteOpen())
        return
      }
      if (isPaletteOpen() || isDialogOpen()) return
      const t = e.target as HTMLElement
      const inField = !!t.closest?.('input,textarea,select,[contenteditable="true"]')
      if (e.key === 'Escape') {
        if (inField) return t.blur()
        const p = panelOf(new URLSearchParams(location.search))
        if (p) return closePanel()
        const m = location.pathname.match(/^(\/admin\/[^/]+)\/[^/]+\/[^/]+$/)
        if (m && document.querySelector('.cx-fullpage')) navigate(m[1])
        return
      }
      if (inField || e.metaKey || e.ctrlKey || e.altKey) return
      if (Date.now() - gAt.current < 1200) {
        gAt.current = 0
        if (e.key === 'u') {
          e.preventDefault()
          navigate('/')
          return
        }
        const s = SECTIONS.find((x) => x.key === e.key)
        if (s) {
          e.preventDefault()
          navigate(s.path)
        }
        return
      }
      switch (e.key) {
        case 'g':
          gAt.current = Date.now()
          return
        case '/': {
          e.preventDefault()
          const f = document.querySelector<HTMLInputElement>('.cx-view [data-search]')
          if (f) f.focus()
          else setPaletteOpen(true)
          return
        }
        case '?':
          return shortcutsDialog()
        case 't':
          return toggleRef.current()
        case ' ':
          if (t.closest?.('button,a,summary')) return
          e.preventDefault()
          return togglePaused()
        case 'o': {
          const p = panelOf(new URLSearchParams(location.search))
          const to = p && detailPath(p.type, p.id)
          if (to) navigate(to)
          return
        }
        case 'Enter': {
          if (t.closest?.('button,a,summary')) return
          const r = rows()[kb.current]
          if (r) r.click()
          return
        }
      }
      const down = e.key === 'j' || (e.key === 'ArrowDown' && kb.current >= 0)
      const up = e.key === 'k' || (e.key === 'ArrowUp' && kb.current >= 0)
      if (!down && !up) return
      const rs = rows()
      if (!rs.length) return
      e.preventDefault()
      rs.forEach((r) => r.classList.remove('kb'))
      kb.current = Math.max(0, Math.min(rs.length - 1, kb.current + (down ? 1 : -1)))
      const r = rs[kb.current]
      r.classList.add('kb')
      r.scrollIntoView({ block: 'nearest' })
      const open = r.dataset.open ?? ''
      const i = open.indexOf(':')
      if (panelOf(new URLSearchParams(location.search)) && i > 0 && !open.startsWith('row:')) openPanel(open.slice(0, i), open.slice(i + 1), { replace: true })
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [])
}

export function Shell({ section, crumbs, children }: { section: Section; crumbs?: ReactNode; children: ReactNode }) {
  const path = usePath()
  const live = useLiveState()
  const { view } = useRelay()
  const { theme, toggle } = useThemeToggle()
  const operator = useAdminOperator()
  const unlock = useAdminUnlock()
  useKeyboard(path)
  useCorePalette()
  useChangeFeed(unlock, { onChange: applyChange, onLost: feedLost })
  const wasPaused = useRef(live.paused)
  useEffect(() => {
    if (wasPaused.current && !live.paused) {
      releaseHeld()
      void resumeLive()
    }
    wasPaused.current = live.paused
  }, [live.paused])
  // a dialog belongs to the page it was opened on
  useEffect(() => closeDialog, [path])
  const held = view?.quorum?.health === 'down'
  const self = view?.self ? view.byId.get(view.self) : undefined
  const lead = view?.quorum?.leader
  return (
    <div className={`cx${live.stale ? ' is-stale' : ''}${held ? ' is-held' : ''}${live.paused ? ' is-paused' : ''}`} data-theme-resolved={theme}>
      <header className="cx-top">
        <Link to="/admin" className="cx-wordmark" aria-label="Console overview">
          <Mark />
          vlRelay<span className="where">operator · {location.host}</span>
        </Link>
        <nav className="cx-crumbs" aria-label="Breadcrumb">
          {crumbs ?? <b>{section.label}</b>}
        </nav>
        <div className="cx-spacer" />
        {self && !view?.single && (
          <div className="cx-via" title="Any member answers the console and asks the others for their numbers.">
            via <Swatch color={self.color} />
            <span className="mono">{self.id}</span>
            {lead === self.id && <span className="muted">(leader)</span>}
          </div>
        )}
        {operator && (
          <div className="cx-via" title="Signed in by the proxy in front of this node's admin listener">
            as <span className="mono">{operator}</span>
          </div>
        )}
        <StreamChip held={held} />
        <button type="button" className="cx-kbtn" onClick={() => setPaletteOpen(true)} title="Command palette (⌘K)">
          <svg width="13" height="13" viewBox="0 0 16 16" aria-hidden="true">
            <circle cx="7" cy="7" r="5" fill="none" stroke="currentColor" strokeWidth="1.6" />
            <path d="M11 11l3.5 3.5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
          </svg>
          <span className="lbl">Jump to host, DID, node…</span>
          <kbd>⌘K</kbd>
        </button>
        <button type="button" className="cx-iconbtn" onClick={toggle} title="Toggle theme (t)" aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
          <ThemeIcon />
        </button>
      </header>
      <Side current={section} />
      <main className="cx-main" id="cx-main">
        {live.stale && (
          <div className="cx-stalebar" role="status">
            <b>◌ Not updating.</b>
            <span>
              {live.lastOkAt ? (
                <>
                  Showing the relay as of <span className="mono">{clock(live.lastOkAt)}</span>.{' '}
                </>
              ) : (
                'Nothing loaded yet. '
              )}
              The relay keeps serving; the console lost <span className="mono">{self?.id ?? location.host}</span>
              {live.staleError ? ` (${live.staleError})` : ''}. Retrying every 2 s…
            </span>
          </div>
        )}
        <div className="cx-view" key={path}>
          {children}
        </div>
      </main>
      <nav className="cx-tabbar" aria-label="Sections">
        {TABBAR.map((id) => (
          <Link key={id} to={SECTION[id].path} className={section.id === id ? 'on' : undefined}>
            <Jack />
            {SECTION[id].short ?? SECTION[id].label}
          </Link>
        ))}
        <button type="button" onClick={() => setPaletteOpen(true)}>
          <Jack />
          More
        </button>
      </nav>
      <Drawer />
      <Palette />
      {/* the palette's inbox reads these when it opens */}
      <Keep use={useCapHosts} />
      {view?.quorum && <Keep use={useQuorumHistory} />}
      <DialogHost />
      <Toasts />
    </div>
  )
}
