import { defineConfig, type UserConfig } from 'tsdown'

const config: UserConfig = defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  platform: 'node',
  target: 'node22',
  dts: true,
  clean: true,
})

// tsdown loads its config through a default export.
export default config
