# stream-fs-proxy

A pass-through stream that doesn't apply backpressure to its source. Whatever the
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
usage tracks the backlog, not the stream: space the consumer has already read is
reused before the cache file grows. By default it also stops spilling before the
disk drops below 10% free (see [Disk limits](#disk-limits)), and the file is
removed even if the process exits mid-stream.

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
| `minFreeSpace`          | `'10%'`                     | Free space to leave on `dir`'s filesystem: a percentage (`'10%'`), bytes, or `false` for no floor.           |
| `maxDiskBytes`          | no cap                      | Largest size the cache file may reach, in bytes.                                                             |
| `onLimit`               | `'backpressure'`            | What happens at a disk limit: `'backpressure'` pauses the source, `'error'` fails the stream.               |
| `highWaterMark`, `readableHighWaterMark`, `writableHighWaterMark`, `signal` | Node defaults | Passed through to `stream.Duplex`. Aborting `signal` destroys the stream and removes the file. |

The stream is byte-oriented: strings are written as UTF-8 buffers, and object
mode isn't supported.

`proxy.filepath` holds the cache file's path once the stream first spills to disk
(it's `null` until then).

## Disk limits

Two limits keep the cache file from filling the disk:

- **`minFreeSpace`** (default `'10%'`) checks the filesystem's real free space
  whenever the cache file is about to grow, and won't grow it past the floor.
  Because it reads live free space, every stream (and every other process) shares
  the same floor, so many parallel proxies can't oversubscribe the disk. If free
  space can't be read on a platform, the floor isn't enforced.
- **`maxDiskBytes`** caps the size of one stream's cache file.

At a limit, new data goes into any space the consumer has already read, so the
source resumes as soon as the consumer frees some room rather than after the
whole backlog drains.

If there's no room at all, `onLimit` decides:

- `'backpressure'` (default): the source is paused, as with an ordinary stream,
  until the consumer frees space or free space recovers. Data is never dropped.
- `'error'`: the stream fails with an `FSProxyLimitError`
  (`code: 'ERR_STREAM_FS_PROXY_LIMIT'`), for when a stalled source is worse than
  a failed one.

```js
import { FSProxy, FSProxyLimitError } from 'stream-fs-proxy'

new FSProxy({ maxDiskBytes: 2 * 1024 ** 3, minFreeSpace: '20%', onLimit: 'error' })
```

### Memory-backed temp directories

On some Linux systems `/tmp` is a `tmpfs`, which lives in RAM, so "spilling to
disk" there still uses memory. The first time a proxy spills into a tmpfs or
ramfs directory, it emits a process warning (`code: 'STREAM_FS_PROXY_TMPFS'`).
Pass a `dir` on a real disk to fix it.

## Cleanup on exit

The cache file is deleted when the stream ends, errors or is destroyed, and also
if the process exits mid-stream (`process.exit()` or the event loop emptying),
through a single shared `exit` listener.

Node doesn't emit `exit` when the process is killed by a signal it doesn't
handle, such as `SIGINT` or `SIGTERM`. If your app doesn't already handle those,
add a handler that exits normally so the cleanup runs:

```js
for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.once(signal, () => process.exit(1))
}
```

The library doesn't install signal handlers itself, since that would change how
your app shuts down. Nothing can clean up after `SIGKILL` or a crash of the OS;
files left that way are named `stream-fs-proxy-*` in the cache directory.

## How it works

- If the consumer is waiting and nothing is queued, a chunk is handed straight
  through without touching disk.
- Otherwise it goes to memory (up to `memoryThreshold`) or is appended to the
  cache file. Order is always preserved.
- The cache file is created lazily with mode `0600`, so a stream that never falls
  behind never creates one.
- The file holds unread data as a short list of regions. Reads take from the
  oldest region; writes extend the newest one or start a new region in space the
  consumer has already read (at least `chunkSize` bytes of it), and the file only
  grows when no such space exists.
- The file is closed and deleted when the stream ends, errors, is destroyed or
  the process exits.

## Prior art

[`fs-capacitor`](https://github.com/mike-marcacci/fs-capacitor) also buffers to a
temporary file, and supports multiple readers of the same buffered data. Use it if
you need to replay the stream. `stream-fs-proxy` is a single pass-through: it
skips the disk while the consumer keeps up, reuses space as data is read, and can
cap disk use, but it can't replay data that's already been read.

## License

MIT
