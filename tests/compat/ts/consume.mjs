// @atproto/sync's Firehose (signature + MST proof checks) against a relay,
// plus a raw @atproto/xrpc-server Subscription with no lexicon validation, so
// a seq the validator refuses still shows up with its decoded type.
//
// node consume.mjs --service ws://127.0.0.1:3480 --plc http://127.0.0.1:3482 --secs 30 [--cursor N] [--raw]
import { IdResolver } from '@atproto/identity'
import { Firehose, parseAccount, parseCommitAuthenticated, parseIdentity, parseSync } from '@atproto/sync'
import { Subscription } from '@atproto/xrpc-server'

const args = Object.fromEntries(
  process.argv.slice(2).reduce((acc, a, i, all) => {
    if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1] && !all[i + 1].startsWith('--') ? all[i + 1] : true])
    return acc
  }, []),
)
const service = args.service
const secs = Number(args.secs ?? 30)
const cursor = args.cursor === undefined ? undefined : Number(args.cursor)

const out = { service, mode: args['coerce-seq'] ? 'coerce-seq' : args.raw ? 'raw' : 'firehose', events: {}, errors: {}, errorSamples: [], seqs: { first: null, last: null, unsafe: 0, types: {} }, perDidRevOrder: 0 }
const bump = (m, k) => (m[k] = (m[k] ?? 0) + 1)
const lastRev = new Map()
const noteSeq = (seq) => {
  bump(out.seqs.types, typeof seq)
  if (typeof seq === 'number' && !Number.isSafeInteger(seq)) out.seqs.unsafe++
  if (out.seqs.first === null) out.seqs.first = String(seq)
  out.seqs.last = String(seq)
}
const noteErr = (err) => {
  const k = `${err?.constructor?.name}: ${String(err?.message ?? err).slice(0, 160)}`
  bump(out.errors, k)
  if (out.errorSamples.length < 5) out.errorSamples.push({ name: err?.constructor?.name, message: String(err?.message ?? err).slice(0, 400), cause: String(err?.cause ?? err?.err ?? '').slice(0, 400) })
}

const makeResolver = () => {
  const idResolver = new IdResolver({ plcUrl: args.plc })
  // the identity package's fetch refuses http:// and IPs; the dev PLC is
  // both, so resolve DIDs with a plain fetch (signature checks unchanged)
  idResolver.did.resolveNoCheck = async (did) => {
    const r = await fetch(`${args.plc}/${did}`)
    return r.ok ? r.json() : null
  }
  return idResolver
}

let stop
if (args['coerce-seq']) {
  // Firehose's own checks with the seq narrowed to a Number first, to see
  // whether anything other than a seq above 2^53 fails
  const { com } = await import('./node_modules/@atproto/sync/dist/lexicons/index.js')
  const idResolver = makeResolver()
  const ac = new AbortController()
  stop = () => ac.abort()
  const sub = new Subscription({
    service,
    method: 'com.atproto.sync.subscribeRepos',
    signal: ac.signal,
    getParams: () => (cursor === undefined ? undefined : { cursor }),
    validate: (v) => {
      if (typeof v?.seq === 'bigint') v = { ...v, seq: Number(v.seq >> 10n) }
      const r = com.atproto.sync.subscribeRepos.$message.safeParse(v)
      if (!r.success) noteErr(new Error(`lexicon: ${r.reason}`))
      return r.success ? r.value : undefined
    },
  })
  ;(async () => {
    try {
      for await (const evt of sub) {
        try {
          const m = com.atproto.sync.subscribeRepos
          let parsed = []
          if (m.commit.$isTypeOf(evt)) parsed = await parseCommitAuthenticated(idResolver, evt)
          else if (m.sync.$isTypeOf(evt)) parsed = [await parseSync(evt)]
          else if (m.identity.$isTypeOf(evt)) parsed = [await parseIdentity(idResolver, evt, true)]
          else if (m.account.$isTypeOf(evt)) parsed = [parseAccount(evt)]
          for (const p of parsed.filter(Boolean)) {
            bump(out.events, p.event)
            noteSeq(p.seq)
          }
        } catch (err) {
          noteErr(err)
        }
      }
    } catch (err) {
      if (err?.name !== 'AbortError') noteErr(err)
    }
  })()
} else if (args.raw) {
  const ac = new AbortController()
  stop = () => ac.abort()
  const sub = new Subscription({
    service,
    method: 'com.atproto.sync.subscribeRepos',
    signal: ac.signal,
    getParams: () => (cursor === undefined ? undefined : { cursor }),
    validate: (v) => v,
  })
  ;(async () => {
    try {
      for await (const evt of sub) {
        bump(out.events, evt?.$type ?? 'unknown')
        noteSeq(evt?.seq)
      }
    } catch (err) {
      if (err?.name !== 'AbortError') noteErr(err)
    }
  })()
} else {
  const idResolver = makeResolver()
  const fh = new Firehose({
    service,
    idResolver,
    getCursor: () => cursor,
    handleEvent: (evt) => {
      bump(out.events, evt.event)
      noteSeq(evt.seq)
      if (evt.event === 'create' || evt.event === 'update' || evt.event === 'delete') {
        const prev = lastRev.get(evt.did)
        if (prev && evt.rev < prev) out.perDidRevOrder++
        lastRev.set(evt.did, evt.rev)
      }
    },
    onError: noteErr,
  })
  fh.start()
  stop = () => fh.destroy()
}

setTimeout(async () => {
  await stop()
  console.log(JSON.stringify(out, null, 2))
  process.exit(0)
}, secs * 1000)
