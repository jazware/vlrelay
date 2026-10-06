// Generated at build time from docs/ by docs-build/plugin.mjs.
declare module 'virtual:vlrelay-docs' {
  export type DocHeading = { id: string; text: string; level: number }
  export type DocMeta = {
    slug: string
    title: string
    section: string
    order: number
    summary: string
    status: 'stub' | 'draft' | 'ready'
    headings: DocHeading[]
  }
  export const nav: { name: string; pages: string[] }[]
  export const pages: DocMeta[]
  export const loaders: Record<string, () => Promise<{ default: string }>>
}
