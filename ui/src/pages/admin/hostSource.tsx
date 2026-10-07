// A host's source (how the relay found it), shared by Hosts and its admission panel.

/** A host's source against the filter: `bootstrap` is any seed relay, `none` a host with no source recorded. */
export const sourceOk = (f: string, s: string | null | undefined) => !f || (f === 'none' ? !s : f === 'bootstrap' ? !!s?.startsWith('bootstrap:') : s === f)
/** The filter as `GET hosts?source=` takes it: `bootstrap:` is the prefix of every seed relay's key. */
export const sourceParam = (f: string) => (f === 'bootstrap' ? 'bootstrap:' : f || undefined)
export const SOURCES =['requestCrawl', 'bootstrap', 'plc', 'cli']

/** Where a host came from, short: "requestCrawl", "seed relay.example.com", "PLC export", "cli". */
export function SourceTag({ s }: { s: string | null | undefined }) {
  if (!s) return <span className="muted">—</span>
  if (s.startsWith('bootstrap:'))
    return (
      <span className="mono sm t2" title={s}>
        <span className="muted">seed </span>
        {s.slice('bootstrap:'.length)}
      </span>
    )
  return <span className="mono sm t2">{s === 'plc' ? 'PLC export' : s}</span>
}

/** The source filter's choices: the fixed kinds and every seed relay discovery reads. */
export function SourceSelect({ value, onChange, keys }: { value: string; onChange: (v: string) => void; keys: string[] }) {
  const relays = keys.filter((k) => k.startsWith('bootstrap:'))
  if (value && !SOURCES.includes(value) && value !== 'none' && !relays.includes(value)) relays.push(value)
  return (
    <select className="cx-inp" style={{ height: 28 }} value={value} onChange={(e) => onChange(e.target.value)} aria-label="Source">
      <option value="">any source</option>
      <option value="requestCrawl">requestCrawl</option>
      <option value="bootstrap">any seed relay</option>
      {relays.map((k) => (
        <option key={k} value={k}>
          seed {k.slice('bootstrap:'.length)}
        </option>
      ))}
      <option value="plc">PLC export</option>
      <option value="cli">cli (--host)</option>
      <option value="none">not recorded</option>
    </select>
  )
}
