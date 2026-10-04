import { useEffect, useRef } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { useResolvedTheme } from '../lib/hooks'

export type Series = { label: string; color: string /* css var name, e.g. "c1" */; dash?: boolean }

const PLOT_H = 170
const cssVar = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim()

/**
 * A live line chart: one y-axis, 2px lines, a crosshair with the values in
 * the legend below (uPlot's cursor legend is the hover layer).
 */
export function Chart({
  title,
  sub,
  series,
  data,
  fmt,
  height = PLOT_H,
}: {
  title: string
  sub?: string
  series: Series[]
  data: (number | null)[][]
  fmt: (v: number) => string
  /** Plot height in px (the legend comes on top of it). */
  height?: number
}) {
  const host = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)
  const theme = useResolvedTheme()
  const key = series.map((s) => s.label + s.color).join('|')
  const fmtRef = useRef(fmt)
  fmtRef.current = fmt

  useEffect(() => {
    const el = host.current
    if (!el) return
    const ink2 = cssVar('ink2')
    const grid = cssVar('rule2')
    const axis: uPlot.Axis = {
      stroke: ink2,
      grid: { stroke: grid, width: 1 },
      ticks: { show: false },
      font: '11px "JetBrains Mono", ui-monospace, monospace',
      gap: 6,
    }
    const opts: uPlot.Options = {
      width: el.clientWidth,
      height,
      padding: [8, 8, 0, 0],
      cursor: { points: { size: 8, width: 2 }, drag: { x: false, y: false } },
      legend: { show: true, live: true },
      scales: { x: { time: true }, y: { range: (_u, _min, max) => [0, max > 0 ? max * 1.12 : 1] } },
      axes: [
        { ...axis, space: 80, values: (_u, ts) => ts.map((t) => new Date(t * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })) },
        { ...axis, size: 66, values: (_u, vs) => vs.map((v) => (v == null ? '' : fmtRef.current(v))) },
      ],
      series: [
        { label: 'Time', value: (_u, t) => (t == null ? '—' : new Date(t * 1000).toLocaleTimeString([], { hour12: false })) },
        ...series.map((s) => ({
          label: s.label,
          stroke: cssVar(s.color),
          width: 2,
          dash: s.dash ? [5, 4] : undefined,
          points: { show: false },
          spanGaps: false,
          value: (_u: uPlot, v: number | null) => (v == null ? '—' : fmtRef.current(v)),
        })),
      ],
    }
    const u = new uPlot(opts, data as uPlot.AlignedData, el)
    plot.current = u
    const ro = new ResizeObserver(() => u.setSize({ width: el.clientWidth, height }))
    ro.observe(el)
    return () => {
      ro.disconnect()
      u.destroy()
      plot.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, theme, height])

  useEffect(() => {
    plot.current?.setData(data as uPlot.AlignedData)
  }, [data])

  const empty = data.length < 2 || data[0].length < 2
  return (
    <div className="chart">
      {title && <h3>{title}</h3>}
      {(title || sub) && <div className="sub">{sub}</div>}
      <div style={{ position: 'relative' }}>
        <div className="plot" ref={host} style={height === PLOT_H ? undefined : { minHeight: height }} />
        {empty && <div className="nodata">Collecting samples…</div>}
      </div>
    </div>
  )
}
