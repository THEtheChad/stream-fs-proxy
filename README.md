# stream-fs-proxy (retired)

stream-fs-proxy has been replaced by
[**syke**](https://github.com/THEtheChad/syke), published on npm as
[`@thethechad/syke`](https://www.npmjs.com/package/@thethechad/syke). The
`FSProxy` stream lives on there as `Spool`, rewritten in TypeScript with
bounded memory, disk limits and cleanup, alongside other stream utilities for
Node.js streams and Web Streams.

```sh
pnpm add @thethechad/syke
```

```js
import { Spool } from '@thethechad/syke/node'
```

The `stream-fs-proxy` package on npm is deprecated and this repository is
archived. The original 1.x code is at commit
[`67fb4ba`](https://github.com/THEtheChad/stream-fs-proxy/tree/67fb4ba).
