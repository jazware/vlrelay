import type { HostAction, HostRow } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import * as W from '../../lib/console/writes'
import { fmtNum } from '../../lib/console/fmt'
import { confirmAction, type ConfirmSpec } from './dialogs'

// The host actions (POST hosts/{host}/action), each behind a confirm that lists what happens and
// shows the exact call. Every action is audited on the host record by the relay; its answer (the
// host's row) goes into the cache at once (writes.ts).

/** The one-click account cap for a real PDS: above every independent PDS today, below what a trusted tier usually allows. */
export const BIG_HOST_CAP = 1_000_000

export type HostVerb = 'settier' | 'throttle' | 'unthrottle' | 'suspend' | 'ban' | 'unban' | 'reconnect' | 'raisecap' | 'tiercap' | 'unalias'

function spec(verb: HostVerb, h: HostRow, arg?: string): ConfirmSpec | undefined {
  const name = h.host
  const run = (a: HostAction) => W.hostAction(name, a)
  const call = (a: HostAction) => A.hostActionCall(name, a)
  switch (verb) {
    case 'settier': {
      if (!arg) return undefined
      const a: HostAction = { action: 'set-tier', tier: arg }
      return {
        tone: 'warn',
        primary: true,
        title: `Move ${name} to ${arg}?`,
        items: [
          `Its limits become the ${arg} tier's on every node within a few seconds.`,
          h.tier === 'throttled' && arg !== 'throttled' ? 'Its reader is released and catches up from its cursor.' : 'It overrides the tier until it is changed again.',
          ...(h.status === 'backpressure' ? ['It’s paused by the relay’s backpressure right now, which no tier changes: it resumes when the relay catches up.'] : []),
        ],
        action: 'Set tier',
        call: call(a),
        run: () => run(a),
        done: `${name} is now ${arg}`,
      }
    }
    case 'throttle':
      return {
        tone: 'warn',
        primary: true,
        title: `Throttle ${name}?`,
        items: ['Its reader is held at that rate: the PDS buffers instead of the relay dropping.', 'If it sends more for long, it falls behind and its PDS may cut the relay off.'],
        fields: [{ id: 'eps', label: 'Events per second', type: 'number', required: true, initial: h.throttle != null ? String(h.throttle) : '5' }],
        action: 'Throttle',
        call: (v) => call({ action: 'throttle', eventsPerSec: Number(v.eps) || 0 }),
        run: (v) => {
          const n = Number(v.eps)
          if (!(n >= 0)) return Promise.reject(new Error('Give a rate of 0 or more events per second.'))
          return run({ action: 'throttle', eventsPerSec: n })
        },
        done: `Throttled ${name}`,
      }
    case 'unthrottle': {
      const a: HostAction = { action: 'throttle', eventsPerSec: null }
      return { tone: 'warn', primary: true, title: `Lift the ${fmtNum(h.throttle ?? 0)}/s throttle on ${name}?`, items: ["Its tier's limits apply again."], action: 'Lift', call: call(a), run: () => run(a), done: 'Throttle lifted' }
    }
    case 'suspend':
      return {
        tone: 'err',
        title: `Suspend ${name}?`,
        items: ['Its socket closes now and its cursor is kept.', 'Unlike a ban, resuming it later loses nothing.'],
        fields: [{ id: 'reason', label: 'Reason (kept on the host record)', required: true }],
        word: name,
        action: 'Suspend',
        call: (v) => call({ action: 'suspend', reason: String(v.reason ?? '') }),
        run: (v) => run({ action: 'suspend', reason: String(v.reason).trim() }),
        done: `Suspended ${name}`,
      }
    case 'ban':
      return {
        tone: 'err',
        title: `Ban ${name}?`,
        items: ['Its socket closes and it is never connected again.', 'Its requestCrawl is refused until it is unbanned.', 'The accounts it hosts keep their state; their events stop.'],
        fields: [{ id: 'reason', label: 'Reason (kept on the host record)', required: true }],
        word: name,
        action: 'Ban',
        call: (v) => call({ action: 'ban', reason: String(v.reason ?? '') }),
        run: (v) => run({ action: 'ban', reason: String(v.reason).trim() }),
        done: `Banned ${name}`,
      }
    case 'unban': {
      const a: HostAction = { action: 'unban' }
      return {
        tone: 'warn',
        primary: true,
        title: `${h.status === 'suspended' ? 'Resume' : 'Unban'} ${name}?`,
        items: [`The relay redials it and resumes from upstream seq ${fmtNum(h.lastUpstreamSeq)}.`, 'A domain rule that bans it still wins.'],
        action: h.status === 'suspended' ? 'Resume' : 'Unban',
        call: call(a),
        run: () => run(a),
        done: `${h.status === 'suspended' ? 'Resumed' : 'Unbanned'} ${name}`,
      }
    }
    case 'reconnect': {
      const a: HostAction = { action: 'reconnect' }
      return {
        tone: 'warn',
        primary: true,
        title: `Reconnect ${name}?`,
        items: [`Closes its socket and resumes from upstream seq ${fmtNum(h.lastUpstreamSeq)}. Replays are deduplicated.`, `Sent to ${h.node || 'the node reading it'}.`],
        action: 'Reconnect',
        call: call(a),
        run: () => run(a),
        done: `Reconnecting ${name}`,
      }
    }
    case 'raisecap': {
      const a: HostAction = { action: 'set-account-limit', maxAccounts: BIG_HOST_CAP }
      return {
        tone: 'warn',
        primary: true,
        title: `Raise ${name}'s account cap to ${fmtNum(BIG_HOST_CAP)}?`,
        items: ["Replaces the tier's cap for this host only; its tier and event limits stay.", 'New accounts are admitted from now on. Accounts already created throttled stay throttled until lifted.'],
        action: 'Raise cap',
        call: call(a),
        run: () => run(a),
        done: `Raised ${name} to ${fmtNum(BIG_HOST_CAP)} accounts`,
      }
    }
    case 'unalias': {
      const a: HostAction = { action: 'unalias', pin: true }
      return {
        tone: 'warn',
        primary: true,
        title: `Read ${name} as its own PDS?`,
        items: [`Its socket opens again and resumes from its own cursor; whatever ${h.aliasOf ?? 'the other name'} already sent is deduplicated.`, 'The relay won’t mark it an alias again until an operator does.'],
        action: 'Not an alias',
        call: call(a),
        run: () => run(a),
        done: `${name} is its own host again`,
      }
    }
    case 'tiercap': {
      const a: HostAction = { action: 'set-account-limit', maxAccounts: null }
      return { tone: 'warn', primary: true, title: `Put ${name} back on its tier's account cap?`, items: ["Its own cap is dropped; the tier's applies."], action: 'Use tier cap', call: call(a), run: () => run(a), done: 'Back on the tier cap' }
    }
  }
}

/** Opens the confirm for a verb on a host; resolves true once it ran. */
export function hostActionDialog(verb: HostVerb, h: HostRow, arg?: string): Promise<boolean> {
  const s = spec(verb, h, arg)
  return s ? confirmAction(s) : Promise.resolve(false)
}
