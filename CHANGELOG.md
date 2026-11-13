# Changelog

## 2.0.0 (unreleased)

### Breaking

- `FSProxy` is now a named export: `import { FSProxy } from 'stream-fs-proxy'`
  or `const { FSProxy } = require('stream-fs-proxy')`. In 1.x the CommonJS build
  only exposed it as `.default`, so the documented `require()` usage threw.
- Requires Node.js 22 or later.
- Object mode is not supported (it never worked in 1.x).

### Fixed

- Memory is bounded. 1.x read the entire on-disk backlog into one buffer when the
  consumer resumed, so a 200 MB backlog became a single 200 MB chunk in memory.
- The final chunk could be lost or fail with `stream.push() after EOF` when the
  writable side finished while the reader was waiting for data.
- The cache file is closed and deleted when the stream errors, is destroyed or
  is aborted. 1.x left it in the temp directory, and never closed the file
  descriptor even on success.
- Write errors are passed to the stream instead of corrupting the write position.
- Short reads no longer push zero-filled bytes.
- File I/O no longer blocks the event loop.
- The cache file is created with mode `0600`.

### Added

- Options: `dir`, `chunkSize`, `memoryThreshold`, and the `signal` and
  high-water-mark options from `stream.Duplex`.
- Chunks pass straight through without touching disk when the consumer is keeping up.
- The cache file is created only when the stream first spills, and its space is
  reused once the backlog drains.
- ESM and CommonJS builds with TypeScript types.

## 1.0.5

Published to npm from code identical to 1.0.4; not tagged in git.
