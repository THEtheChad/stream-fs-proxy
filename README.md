# stream-fs-proxy

A pass-through stream that never applies backpressure to its source. Whatever the
consumer isn't ready for is spilled to a temporary file and read back, a bounded
chunk at a time, as the consumer catches up.

Reach for it when:

1. **A network source would time out under backpressure.** You're piping an HTTP
   response, socket or upload into something slow (a database, a rate-limited API,
   a busy disk), and pausing the source long enough gets the connection dropped.
2. **You have many slow pipelines in parallel** and don't want each one's backlog
   held in memory.

Memory stays bounded by the stream's high-water mark plus `chunkSize` (and
`memoryThreshold`, if you set one), however far the consumer falls behind. Disk
usage grows only as large as the biggest backlog: once the consumer catches up,
the cache file is reused from the start.

## Install

```sh
pnpm add stream-fs-proxy
# or: npm install stream-fs-proxy
```

Requires Node.js 22 or later. Ships ESM, CommonJS and TypeScript types.

## Usage

```js
import { pipeline } from 'node:stream/promises'
import { FSProxy } from 'stream-fs-proxy'

const res = await fetch('https://example.com/large-dataset.ndjson')

await pipeline(res.body, new FSProxy(), slowDatabaseWriter)
```

CommonJS works too:

```js
const { FSProxy } = require('stream-fs-proxy')
```

Use `pipeline` rather than chained `.pipe()` calls so that an error anywhere in
the chain destroys every stream and the temporary file is removed.

## Options

`new FSProxy(options?)`

| Option                  | Default                     | Description                                                                                                  |
| ----------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `dir`                   | `os.tmpdir()`               | Directory for the cache file.                                                                                |
| `chunkSize`             | readable high-water mark    | Largest chunk, in bytes, read back from disk and pushed downstream.                                          |
| `memoryThreshold`       | `0`                         | Bytes to hold in memory before spilling to disk. Small bursts under this size never touch disk.             |
| `highWaterMark`, `readableHighWaterMark`, `writableHighWaterMark`, `signal` | Node defaults | Passed through to `stream.Duplex`. Aborting `signal` destroys the stream and removes the file. |

The stream is byte-oriented: strings are written as UTF-8 buffers, and object
mode isn't supported.

`proxy.filepath` holds the cache file's path once the stream first spills to disk
(it's `null` until then).

## How it works

- If the consumer is waiting and nothing is queued, a chunk is handed straight
  through without touching disk.
- Otherwise it goes to memory (up to `memoryThreshold`) or is appended to the
  cache file. Order is always preserved.
- The cache file is created lazily with mode `0600`, so a stream that never falls
  behind never creates one.
- The file is closed and deleted when the stream ends, errors or is destroyed.

## Prior art

[`fs-capacitor`](https://github.com/mike-marcacci/fs-capacitor) also buffers to a
temporary file, and supports multiple readers of the same buffered data. Use it if
you need to replay the stream; use `stream-fs-proxy` for a single pass-through
with bounded memory and file reuse.

## License

MIT
