import { defineConfig } from 'vocs'

export default defineConfig({
  title: 'tachyon-payments',
  description:
    'Private-retrieval (PIR) services for the Tachyon payment protocol on Zcash, built by Zcash Labs.',
  rootDir: '.',
  theme: {
    accentColor: {
      light: '#b8860b',
      dark: '#f4b728',
    },
  },
  socials: [
    {
      icon: 'github',
      link: 'https://github.com/zcashlabs/tachyon-payments',
    },
    {
      icon: 'x',
      link: 'https://x.com/zcashlabs',
    },
  ],
  topNav: [
    { text: 'Quickstart', link: '/quickstart' },
    { text: 'Wallet Tutorial', link: '/wallet-tutorial' },
    { text: 'Operator Guide', link: '/operator/building' },
    { text: 'Products', link: '/products/memo' },
    {
      text: 'Spec',
      link: 'https://github.com/zcashlabs/tachyon-payments/blob/master/docs/spec/spec-tachyon-payment-pir.md',
    },
  ],
  sidebar: [
    {
      text: 'Overview',
      items: [
        { text: 'Introduction', link: '/' },
        { text: 'Architecture', link: '/architecture' },
        { text: 'Local Regtest Quickstart', link: '/quickstart' },
        { text: 'Wallet Tutorial (zcash-devtool)', link: '/wallet-tutorial' },
      ],
    },
    {
      text: 'Operator Guide',
      items: [
        { text: 'Building & Testing', link: '/operator/building' },
        { text: 'Publisher Configuration', link: '/operator/publisher' },
        { text: 'Query Server Configuration', link: '/operator/query-server' },
        { text: 'TLS Edge & Routing', link: '/operator/tls-edge' },
        { text: 'Probe Client (CLI)', link: '/operator/cli' },
        { text: 'Observability & Alerts', link: '/operator/observability' },
      ],
    },
    {
      text: 'Products',
      items: [
        { text: 'Memo', link: '/products/memo' },
        { text: 'First-contact', link: '/products/first-contact' },
        { text: 'Tachygram', link: '/products/tachygram' },
        { text: 'Registry', link: '/products/registry' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'Shared Table & Wire Protocol', link: '/reference/shared' },
      ],
    },
  ],
})
