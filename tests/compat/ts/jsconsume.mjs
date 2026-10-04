// A Jetstream JSON subscriber: counts events by kind (commit ops by
// operation) and collects (did, rev) of commits, so they can be compared
// with what the relay emitted.
//
// node jsconsume.mjs --url ws://127.0.0.1:3460/subscribe --secs 60 [--cursor TIME_US] [--out file]
import { writeFileSync } from 'node:fs'

const args = {}
process.argv.slice(2).forEach((a, i, all) => {
  if (a.startsWith('--')) args[a.slice(2)] = all[i + 1]
})
const url = new URL(args.url)
if (args.cursor) url.searchParams.set('cursor', args.cursor)
const out = { url: String(url), kinds: {}, ops: {}, commits: 0, firstTimeUs: null, lastTimeUs: null, timeBackward: 0, closes: [] }
const revs = new Set()
const bump = (m, k) => (m[k] = (m[k] ?? 0) + 1)

// Jetstream restarts mid-run (run.sh), so reconnect from the last time_us seen
let ws
let stopping = false
const connect = () => {
  if (out.lastTimeUs !== null) url.searchParams.set('cursor', String(out.lastTimeUs))
  ws = new WebSocket(url)
  ws.onmessage = onMessage
  ws.onclose = (e) => {
    out.closes.push({ code: e.code, reason: e.reason })
    if (!stopping) setTimeout(connect, 500)
  }
  ws.onerror = () => {}
}
const onMessage = (m) => {
  const e = JSON.parse(m.data)
  bump(out.kinds, e.kind)
  if (out.lastTimeUs !== null && e.time_us < out.lastTimeUs) out.timeBackward++
  out.firstTimeUs ??= e.time_us
  out.lastTimeUs = e.time_us
  if (e.kind === 'commit') {
    bump(out.ops, e.commit.operation)
    const k = `${e.did} ${e.commit.rev}`
    if (!revs.has(k)) out.commits++
    revs.add(k)
  }
}
connect()

setTimeout(() => {
  stopping = true
  ws.close()
  if (args.out) writeFileSync(args.out, [...revs].join('\n') + '\n')
  console.log(JSON.stringify(out))
  process.exit(0)
}, Number(args.secs ?? 30) * 1000)
