import { Fragment, useEffect, useRef, useState, type JSX } from 'react'
import { ErrorNotice, Field, Notice, Spinner, Topbar } from '../components/ui'
import { useAdminToken } from '../lib/hooks'
import { Link, match, navigate } from '../lib/router'
import { api, setAdminToken } from '../lib/api'
import { useKey } from '../lib/useApi'
import { Overview } from './Overview'
import { Hosts } from './Hosts'
import { HostDetail } from './HostDetail'
import { Consumers } from './Consumers'
import { Cluster } from './Cluster'
import { Ops } from './Ops'
import { Rules } from './Rules'
import { Policy } from './Policy'
import { AccountDetail, Accounts } from './Accounts'
import { CaseDetail, Cases } from './Cases'
import { Quorum } from './Quorum'
import { Settings } from './Settings'
import { Tuning } from './Tuning'

/** `g` then the key jumps to the tab. Grouped by what an operator is doing. */
const TABS = [
  { to: '/admin', label: 'Overview', key: 'o', group: 'Traffic' },
  { to: '/admin/hosts', label: 'Hosts', key: 'h', group: 'Traffic' },
  { to: '/admin/consumers', label: 'Consumers', key: 's', group: 'Traffic' },
  { to: '/admin/cases', label: 'Cases', key: 'c', group: 'Moderation' },
  { to: '/admin/accounts', label: 'Accounts', key: 'a', group: 'Moderation' },
  { to: '/admin/rules', label: 'Domain rules', key: 'r', group: 'Moderation' },
  { to: '/admin/policy', label: 'Limits', key: 'p', group: 'Policy' },
  { to: '/admin/tuning', label: 'Tuning', key: 't', group: 'Policy' },
  { to: '/admin/cluster', label: 'Cluster', key: 'n', group: 'System' },
  { to: '/admin/quorum', label: 'Quorum', key: 'q', group: 'System' },
  { to: '/admin/ops', label: 'Operations', key: 'b', group: 'System' },
  { to: '/admin/settings', label: 'Settings', key: ',', group: 'System' },
]

export function AdminApp({ path }: { path: string }) {
  const token = useAdminToken()
  const p = path.replace(/\/+$/, '') || '/admin'
  const [help, setHelp] = useState(false)
  const pendingG = useRef(0)

  useKey(
    (e) => {
      if (e.key === '?') {
        setHelp((h) => !h)
        return
      }
      if (e.key === 'Escape') setHelp(false)
      if (e.key === '/') {
        const el = document.querySelector<HTMLInputElement>('[data-search]')
        if (el) {
          e.preventDefault()
          el.focus()
          el.select()
        }
        return
      }
      if (e.key === 'g') {
        pendingG.current = Date.now()
        return
      }
      if (Date.now() - pendingG.current < 1200) {
        pendingG.current = 0
        const t = TABS.find((t) => t.key === e.key)
        if (t) navigate(t.to)
      }
    },
    [],
  )

  useEffect(() => {
    const t = TABS.find((t) => (t.to === '/admin' ? p === '/admin' : p === t.to || p.startsWith(`${t.to}/`)))
    document.title = `${t?.label ?? 'Console'} · vlRelay`
  }, [p])

  if (!token) return <Login />
  let page: JSX.Element
  let m: Record<string, string> | null
  if (p === '/admin') page = <Overview />
  else if (p === '/admin/hosts') page = <Hosts />
  else if ((m = match('/admin/hosts/:host', p))) page = <HostDetail host={m.host} />
  else if (p === '/admin/consumers') page = <Consumers />
  else if (p === '/admin/cluster') page = <Cluster />
  else if (p === '/admin/ops') page = <Ops />
  else if (p === '/admin/rules') page = <Rules />
  else if (p === '/admin/policy') page = <Policy />
  else if (p === '/admin/tuning') page = <Tuning />
  else if (p === '/admin/quorum') page = <Quorum />
  else if (p === '/admin/settings') page = <Settings />
  else if (p === '/admin/accounts') page = <Accounts />
  else if ((m = match('/admin/accounts/:did', p))) page = <AccountDetail did={m.did} />
  else if (p === '/admin/cases') page = <Cases />
  else if ((m = match('/admin/cases/:id', p))) page = <CaseDetail id={Number(m.id)} />
  else page = <Notice kind="warn">There is no console page at {p}.</Notice>
  const current = (to: string) => (to === '/admin' ? p === '/admin' : p === to || p.startsWith(`${to}/`))
  return (
    <>
      <Topbar where="Relay console" console>
        <button type="button" className="btn sm quiet" onClick={() => setHelp((h) => !h)} aria-expanded={help}>
          Shortcuts <kbd>?</kbd>
        </button>
        <button type="button" className="btn sm" onClick={() => setAdminToken(null)}>
          Lock console
        </button>
      </Topbar>
      <main className="console">
        <nav className="tabs" aria-label="Console">
          {TABS.map((t, i) => (
            <Fragment key={t.to}>
              {t.group !== TABS[i - 1]?.group && (
                <span className="tab-group" aria-hidden="true">
                  {t.group}
                </span>
              )}
              <Link to={t.to} aria-current={current(t.to) ? 'page' : undefined} title={`${t.group}: g ${t.key}`}>
                {t.label}
              </Link>
            </Fragment>
          ))}
        </nav>
        {help && <Shortcuts onClose={() => setHelp(false)} />}
        {page}
      </main>
    </>
  )
}

function Shortcuts({ onClose }: { onClose: () => void }) {
  return (
    <section className="panel shortcuts" aria-label="Keyboard shortcuts">
      <header>
        <h2>Keyboard shortcuts</h2>
        <button type="button" className="btn sm quiet" onClick={onClose}>
          Close <kbd>Esc</kbd>
        </button>
      </header>
      <div className="body">
        <dl className="keys">
          {TABS.map((t) => (
            <div key={t.key}>
              <dt>
                <kbd>g</kbd> <kbd>{t.key}</kbd>
              </dt>
              <dd>{t.label}</dd>
            </div>
          ))}
          <div>
            <dt>
              <kbd>/</kbd>
            </dt>
            <dd>Search or filter on this page</dd>
          </div>
          <div>
            <dt>
              <kbd>j</kbd> <kbd>k</kbd> / <kbd>↓</kbd> <kbd>↑</kbd>
            </dt>
            <dd>Move through a table</dd>
          </div>
          <div>
            <dt>
              <kbd>↵</kbd>
            </dt>
            <dd>Open the selected row</dd>
          </div>
          <div>
            <dt>
              <kbd>Esc</kbd>
            </dt>
            <dd>Cancel a confirmation, leave a field</dd>
          </div>
        </dl>
      </div>
    </section>
  )
}

function Login() {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  return (
    <>
      <Topbar where="Relay console" console />
      <main className="signin">
        <div className="card">
          <div className="inner">
            <form
              onSubmit={async (e) => {
                e.preventDefault()
                setBusy(true)
                setError(undefined)
                try {
                  await api('cluster', { token: token.trim() })
                  setAdminToken(token.trim())
                } catch (err) {
                  setError(err)
                } finally {
                  setBusy(false)
                }
              }}
            >
              <h1>Relay console</h1>
              <p className="sub">Hosts, consumers, policy and moderation for this relay. The token stays in this tab only.</p>
              <ErrorNotice error={error} />
              <Field label="Admin token" hint="The relay's --admin-token (VLRELAY_ADMIN_TOKEN).">
                <input type="password" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" required autoFocus />
              </Field>
              <div className="row end">
                <button className="btn primary" disabled={busy}>
                  {busy && <Spinner />}
                  Unlock console
                </button>
              </div>
            </form>
          </div>
        </div>
      </main>
    </>
  )
}
