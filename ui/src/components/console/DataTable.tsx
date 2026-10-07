import { useMemo, useState, type CSSProperties, type ReactNode } from 'react'
import { Empty } from './kit'
import { openPanel, panelParam, usePanel, type PanelRef } from './nav'

// The console's table: dense rows, sortable columns, and rows that open in the slide-over.
// Rows with `open` carry data-open="type:id", which is all the shell's j/k/Enter handling needs.

export type Col<T> = {
  id: string
  label: ReactNode
  title?: string
  /** Right-aligned (numbers). */
  r?: boolean
  /** Makes the header clickable; descending first. */
  sort?: (a: T, b: T) => number
  render: (row: T) => ReactNode
  className?: string
  style?: CSSProperties
  /** Takes the table's spare width (a name or a description), so the other columns keep to their content. */
  fill?: boolean
}

export function DataTable<T>({
  rows,
  cols,
  rowKey,
  open,
  onRow,
  sort: initial,
  compact,
  empty,
  dim,
  label,
  serverSort,
  fit,
}: {
  rows: T[]
  cols: Col<T>[]
  rowKey: (row: T) => string
  /** The detail a row opens in the slide-over. */
  open?: (row: T) => PanelRef | undefined
  /** Instead of `open`: a click handler (filters, focus). */
  onRow?: (row: T) => void
  sort?: { id: string; asc?: boolean }
  compact?: boolean
  empty?: ReactNode
  dim?: (row: T) => boolean
  label?: string
  /** The server sorts (and pages): rows come in order, a header click asks for another order. Columns with `serverSortable` get the click. */
  serverSort?: { id: string; asc: boolean; sortable: string[]; onSort: (s: { id: string; asc: boolean }) => void }
  /** Few, narrow columns: they keep to their content and an empty last column takes the rest. */
  fit?: boolean
}) {
  const [own, setOwn] = useState(initial)
  const sort = serverSort ? { id: serverSort.id, asc: serverSort.asc } : own
  const setSort = (s: { id: string; asc?: boolean }) => (serverSort ? serverSort.onSort({ id: s.id, asc: !!s.asc }) : setOwn(s))
  const panel = usePanel()
  const sorted = useMemo(() => {
    if (serverSort) return rows
    const c = own && cols.find((x) => x.id === own.id)
    if (!c?.sort) return rows
    const s = [...rows].sort(c.sort)
    return own?.asc ? s : s.reverse()
  }, [rows, cols, own, serverSort])
  // a list can name a thing twice (a host listed by two nodes); React keys can't
  const keys = useMemo(() => {
    const seen = new Map<string, number>()
    return sorted.map((r) => {
      const k = rowKey(r)
      const n = seen.get(k) ?? 0
      seen.set(k, n + 1)
      return n ? `${k}#${n}` : k
    })
  }, [sorted, rowKey])
  const sortable = (c: Col<T>) => (serverSort ? serverSort.sortable.includes(c.id) : !!c.sort)
  return (
    <div className="cx-tw">
      <table className={`cx-t${compact ? ' compact' : ''}`} aria-label={label}>
        <thead>
          <tr>
            {cols.map((c) => {
              const on = sort?.id === c.id
              return (
                <th
                  key={c.id}
                  className={`${c.r ? 'r ' : ''}${c.fill ? 'fill ' : ''}${sortable(c) ? 'sortable' : ''}`}
                  title={c.title}
                  aria-sort={on ? (sort?.asc ? 'ascending' : 'descending') : undefined}
                  onClick={sortable(c) ? () => setSort(on ? { id: c.id, asc: !sort?.asc } : { id: c.id, asc: c.id === 'host' }) : undefined}
                >
                  {c.label}
                  {on && (sort?.asc ? ' ↑' : ' ↓')}
                </th>
              )
            })}
            {fit && <th className="fill" aria-hidden="true" />}
          </tr>
        </thead>
        <tbody>
          {sorted.length === 0 && (
            <tr>
              <td colSpan={cols.length + (fit ? 1 : 0)}>{empty ?? <Empty>Nothing here.</Empty>}</td>
            </tr>
          )}
          {sorted.map((r, i) => {
            const o = open?.(r)
            const sel = o && panel && panel.type === o.type && panel.id === o.id
            return (
              <tr
                key={keys[i]}
                data-open={o ? panelParam(o.type, o.id) : onRow ? `row:${rowKey(r)}` : undefined}
                className={`${sel ? 'sel' : ''}${dim?.(r) ? ' dim' : ''}`}
                onClick={(e) => {
                  if ((e.target as HTMLElement).closest('button,a,input,select,textarea,label')) return
                  if (o) openPanel(o.type, o.id)
                  else onRow?.(r)
                }}
              >
                {cols.map((c) => (
                  <td key={c.id} className={`${c.r ? 'r ' : ''}${c.fill ? 'fill ' : ''}${c.className ?? ''}`} style={c.style}>
                    {c.render(r)}
                  </td>
                ))}
                {fit && <td className="fill" />}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
