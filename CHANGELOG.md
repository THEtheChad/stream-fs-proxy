# Changelog

## syke 0.1.0 (unreleased)

First release of `syke`. Its first utility, `Spool`, is the successor to the
`stream-fs-proxy` package's `FSProxy`.

### Changes from stream-fs-proxy 1.x

- The package is `syke` and the class is `Spool`, a named export:
  `import { Spool } from 'syke'` or `const { Spool } = require('syke')`. In
  stream-fs-proxy 1.x the CommonJS build only exposed `FSProxy` as `.default`, so
  the documented `require()` usage threw.
- Requires Node.js 22 or later.
- Object mode is not supported (it never worked in 1.x).

### Fixed compared with stream-fs-proxy 1.x

- Memory is bounded. 1.x read the entire on-disk backlog into one buffer when the
  consumer resumed, so a 200 MB backlog became a single 200 MB chunk in memory.
- The final chunk could be lost or fail with `stream.push() after EOF` when the
  writable side finished while the reader was waiting for data.
- The spool file is closed and deleted when the stream errors, is destroyed or
  is aborted. 1.x left it in the temp directory, and never closed the file
  descriptor even on success.
- Write errors are passed to the stream instead of corrupting the write position.
- Short reads no longer push zero-filled bytes.
- File I/O no longer blocks the event loop.
- The spool file is created with mode `0600`.

### Added

- Options: `dir`, `chunkSize`, `memoryThreshold`, and the `signal` and
  high-water-mark options from `stream.Duplex`.
- Disk limits: `minFreeSpace` (default `'10%'`, checked against the filesystem's
  live free space), `maxDiskBytes`, and `onLimit` (`'backpressure'` or `'error'`,
  which throws `SpoolLimitError`).
- A one-time process warning when the spool directory is tmpfs or ramfs.
- Chunks pass straight through without touching disk when the consumer is keeping up.
- The spool file is created only when the stream first spills, and space the
  consumer has read is reused before the file grows, so disk use tracks the
  backlog rather than the whole stream.
- The spool file is deleted if the process exits mid-stream, or once a spool
  that was dropped without being ended or destroyed is garbage-collected.
- `Spool.from(source, options)` reads a Node readable, web `ReadableStream` or
  (async) iterable through a new spool. It overrides the inherited
  `Duplex.from()`, which returned a plain `Duplex` with no disk spooling.
- ESM and CommonJS builds with TypeScript types, importable from `syke` or
  `syke/spool`.

## stream-fs-proxy 1.0.5

Published to npm from code identical to 1.0.4; not tagged in git.
