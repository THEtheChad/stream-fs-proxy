import { defineConfig, type UserConfig } from 'tsdown'

const config: UserConfig = defineConfig({
  // One entry per utility, so `syke/spool` loads only what it needs.
  entry: { index: 'src/index.ts', spool: 'src/spool/index.ts' },
  format: ['esm', 'cjs'],
  platform: 'node',
  target: 'node22',
  dts: true,
  clean: true,
})

// tsdown loads its config through a default export.
export default config
