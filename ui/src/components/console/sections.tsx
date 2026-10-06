// The console's information architecture: one entry per rail jack, in order. `key` is the letter
// after `g`; `aliases` are older paths that still land in the section (the pre-console pages
// keep their URLs until their sections are rebuilt).

export type SectionId = 'overview' | 'hosts' | 'consumers' | 'quorum' | 'store' | 'policy' | 'moderation' | 'settings'

export type Section = { id: SectionId; label: string; short?: string; key: string; group: '' | 'Traffic' | 'The log' | 'Rules' | 'System'; path: string; aliases?: string[] }

export const SECTIONS: Section[] = [
  { id: 'overview', label: 'Overview', key: 'o', group: '', path: '/admin' },
  { id: 'hosts', label: 'Hosts', key: 'h', group: 'Traffic', path: '/admin/hosts' },
  { id: 'consumers', label: 'Consumers', key: 'c', group: 'Traffic', path: '/admin/consumers' },
  { id: 'quorum', label: 'Quorum & cluster', short: 'Quorum', key: 'q', group: 'The log', path: '/admin/quorum', aliases: ['/admin/cluster', '/admin/ops'] },
  { id: 'store', label: 'Object store & cost', short: 'Store', key: 'b', group: 'The log', path: '/admin/store' },
  { id: 'policy', label: 'Policy', key: 'p', group: 'Rules', path: '/admin/policy', aliases: ['/admin/tuning'] },
  { id: 'moderation', label: 'Moderation', key: 'm', group: 'Rules', path: '/admin/moderation', aliases: ['/admin/cases', '/admin/accounts', '/admin/rules'] },
  { id: 'settings', label: 'Settings', key: 's', group: 'System', path: '/admin/settings' },
]

export const SECTION: Record<SectionId, Section> = Object.fromEntries(SECTIONS.map((x) => [x.id, x])) as Record<SectionId, Section>

/** The section a path belongs to (longest matching prefix, aliases included). */
export function sectionOf(path: string): Section {
  let best: Section = SECTION.overview
  let len = 0
  for (const sec of SECTIONS) {
    for (const p of [sec.path, ...(sec.aliases ?? [])]) {
      if (p === '/admin') continue
      if ((path === p || path.startsWith(`${p}/`)) && p.length > len) {
        best = sec
        len = p.length
      }
    }
  }
  return best
}

export const TABBAR: SectionId[] = ['overview', 'hosts', 'quorum', 'moderation']
