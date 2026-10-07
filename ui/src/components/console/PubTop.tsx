import { useEffect, useState } from 'react'
import { Link } from '../../lib/router'
import { Mark, ThemeIcon, useThemeToggle } from './Shell'

// The bar and footer the public page and the docs share.

let reachable: Promise<boolean> | null = null

/** Whether /admin answers from here: a proxy in front of a public relay usually hides it. Asked once per page load. */
export function useConsoleReachable() {
  const [ok, setOk] = useState(false)
  useEffect(() => {
    let live = true
    reachable ??= fetch('/admin', { method: 'HEAD' }).then(
      (r) => r.ok,
      () => false,
    )
    reachable.then((v) => live && setOk(v))
    return () => {
      live = false
    }
  }, [])
  return ok
}

export function PubTop({ here, consoleHere }: { here?: 'docs'; consoleHere: boolean }) {
  const { theme, toggle } = useThemeToggle()
  return (
    <header className="cx-pub-top">
      <Link to="/" className="cx-wordmark" aria-label="vlRelay">
        <Mark />
        vlRelay
      </Link>
      <Link to="/docs" className={`l${here === 'docs' ? ' on' : ' hide-sm'}`} aria-current={here === 'docs' ? 'page' : undefined}>
        Docs
      </Link>
      {consoleHere && (
        <Link to="/admin" className="l">
          Console
        </Link>
      )}
      <span className="cx-spacer" />
      <button type="button" className="cx-iconbtn" onClick={toggle} title="Toggle theme" aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
        <ThemeIcon />
      </button>
    </header>
  )
}

export function PubFoot({ version, consoleHere }: { version?: string; consoleHere: boolean }) {
  return (
    <footer className="cx-pubfoot">
      <span>
        vlRelay <span className="mono">{version ?? ''}</span>
      </span>
      <Link to="/docs">Docs</Link>
      <a href="/api/public/stats">Stats JSON</a>
      <a href="/xrpc/_health">Health</a>
      {consoleHere && <Link to="/admin">Operator console</Link>}
    </footer>
  )
}
