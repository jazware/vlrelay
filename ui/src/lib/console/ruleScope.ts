import type { DomainRule } from '../api'

// Which domain rule decides a host, as the relay's lookup picks it (src/policy/rules.rs
// `Compiled::lookup`): the exact name, then the name without its port, then the longest
// `*.` suffix. A rule's `matches` counts the hosts it decides, so a broad rule's overridden
// hosts are the ones the more specific rules inside it decide.

/** How specifically `pattern` matches `host`: 0 when it doesn't; an exact name beats any wildcard, a longer wildcard a shorter one. */
export function specificity(pattern: string, host: string): number {
  const h = host.toLowerCase()
  if (pattern === h) return 2000
  const bare = h.split(':')[0]
  if (pattern === bare) return 1000
  if (!pattern.startsWith('*.')) return 0
  const base = pattern.slice(2)
  return bare === base || bare.endsWith(`.${base}`) ? base.length : 0
}

/** The most specific host a pattern names: the host an exact rule names, a wildcard's own domain. */
const nameOf = (p: string) => (p.startsWith('*.') ? p.slice(2) : p)

/** `narrow` takes every host it matches from `broad`: each one `broad` matches too, less specifically. */
export function overrides(narrow: DomainRule, broad: DomainRule): boolean {
  if (narrow.id === broad.id) return false
  const h = nameOf(narrow.pattern)
  const b = specificity(broad.pattern, h)
  return b > 0 && specificity(narrow.pattern, h) > b
}

/** The rules that take hosts from `r`, most hosts first. */
export const overriddenBy = (r: DomainRule, rules: DomainRule[]) => rules.filter((s) => overrides(s, r)).sort((a, b) => b.matches - a.matches || a.id - b.id)

/** The broader rules `r` wins over on every host it decides, the nearest first. */
export function overriddenRules(r: DomainRule, rules: DomainRule[]) {
  const h = nameOf(r.pattern)
  return rules.filter((b) => overrides(r, b)).sort((a, b) => specificity(b.pattern, h) - specificity(a.pattern, h))
}

/** The rule that would decide `host` if its winning rule `won` weren't there: the one it overrides there. */
export function overriddenOn(host: string, won: number, rules: DomainRule[]): DomainRule | undefined {
  let best: DomainRule | undefined
  let at = 0
  for (const r of rules) {
    const s = r.id === won ? 0 : specificity(r.pattern, host)
    if (s > at) [best, at] = [r, s]
  }
  return best
}
