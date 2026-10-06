import { useMemo, useState } from 'react'
import { Tile } from '../components/relay'
import { Empty, ErrorNotice, Loading, Notice, Panel, Status } from '../components/ui'
import { ApiError, type ConfigEntry, type SettingsView } from '../lib/api'
import { Link } from '../lib/router'
import { useApi } from '../lib/useApi'
import './ops2.css'

const SOURCE_LABEL: Record<ConfigEntry['source'], string> = { flag: 'flag', env: 'env', default: 'default', unset: 'unset' }

export function Settings() {
  const l = useApi<SettingsView>('settings')
  const [q, setQ] = useState('')
  const [onlySet, setOnlySet] = useState(false)
  const rows = useMemo(() => {
    const e = l.data?.entries ?? []
    const needle = q.trim().toLowerCase()
    return e.filter(
      (x) =>
        (!onlySet || x.source === 'flag' || x.source === 'env') &&
        (!needle || x.flag.includes(needle) || (x.env ?? '').toLowerCase().includes(needle) || x.help.toLowerCase().includes(needle) || (x.value ?? '').toLowerCase().includes(needle)),
    )
  }, [l.data, q, onlySet])
  if (l.error instanceof ApiError && l.error.status === 404) return <Notice kind="info">{l.error.message}.</Notice>
  const s = l.data
  if (!s) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const count = (src: ConfigEntry['source']) => s.entries.filter((e) => e.source === src).length
  const secrets = s.entries.filter((e) => e.secret)

  return (
    <>
      <div className="console-head">
        <h1>Settings</h1>
        <span className="muted small">
          <span className="mono">{s.binary}</span> {s.version}
        </span>
      </div>
      <Notice kind="info">
        This node's process config, as it started: flags and environment take a restart to change. Limits, thresholds and budgets that change live are on{' '}
        <Link to="/admin/tuning">Tuning</Link> and <Link to="/admin/policy">Limits</Link>.
      </Notice>
      <div className="tiles">
        <Tile k="From flags" v={count('flag')} />
        <Tile k="From environment" v={count('env')} />
        <Tile k="Defaults" v={count('default')} />
        <Tile k="Unset" v={count('unset')} />
        <Tile k="Secrets set" v={<>{secrets.filter((e) => e.set).length}<small>of {secrets.length}</small></>} sub="values never leave the node" />
      </div>
      <Panel
        flush
        title="Effective config"
        actions={
          <>
            <input type="search" data-search placeholder="Filter flags, env, values" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Filter settings" />
            <label className="check">
              <input type="checkbox" checked={onlySet} onChange={(e) => setOnlySet(e.target.checked)} /> Only flags and env
            </label>
          </>
        }
      >
        {rows.length === 0 ? (
          <Empty title="Nothing matches">Clear the filter to see every flag.</Empty>
        ) : (
          <div className="table-wrap">
            <table className="data compact settings">
              <thead>
                <tr>
                  <th>Flag</th>
                  <th>Value</th>
                  <th>Source</th>
                  <th>Default</th>
                  <th>What it does</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((e) => (
                  <tr key={e.flag}>
                    <td>
                      <div className="mono">{e.flag}</div>
                      {e.env && <div className="mono muted small">{e.env}</div>}
                    </td>
                    <td className="val">
                      {e.secret ? (
                        <Status kind={e.set ? 'ok' : 'idle'}>{e.set ? 'set (hidden)' : 'not set'}</Status>
                      ) : e.value === null ? (
                        <span className="muted">—</span>
                      ) : (
                        <span className={`mono${e.default !== null && e.value !== e.default ? ' changed' : ''}`}>{e.value}</span>
                      )}
                    </td>
                    <td>
                      <span className={`src src-${e.source}`}>{SOURCE_LABEL[e.source]}</span>
                    </td>
                    <td className="mono muted">{e.default ?? '—'}</td>
                    <td className="help">{e.help || <span className="muted">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  )
}
