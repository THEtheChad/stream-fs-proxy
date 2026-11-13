import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions, pipeline, Readable } from 'node:stream'
import type { ReadableStream } from 'node:stream/web'
import {
  type FreeSpaceFloor,
  type MinFreeSpace,
  freeSpaceBudget,
  parseMinFreeSpace,
  warnIfMemoryBacked,
} from './diskSpace.ts'
import { releaseFile, trackFile } from './fileRegistry.ts'

export interface SpoolOptions
  extends Pick<
    DuplexOptions,
    'highWaterMark' | 'readableHighWaterMark' | 'writableHighWaterMark' | 'signal'
  > {
  /** Directory for the temporary spool file. Defaults to `os.tmpdir()`. */
  dir?: string
  /**
   * Largest chunk, in bytes, read back from disk and pushed downstream.
   * Defaults to the readable high-water mark.
   */
  chunkSize?: number
  /**
   * Bytes to hold in memory before spilling to disk. Defaults to `0`, so
   * everything the consumer is not ready for goes straight to disk.
   */
  memoryThreshold?: number
  /**
   * Stop growing the spool file before free space on `dir`'s filesystem drops
   * below this: a percentage of the filesystem (`'10%'`), a number of bytes,
   * or `false` for no floor. Defaults to `'10%'`.
   */
  minFreeSpace?: MinFreeSpace
  /** Largest size, in bytes, the spool file may reach. Defaults to no cap. */
  maxDiskBytes?: number
  /**
   * What to do when a disk limit is reached. `'backpressure'` (the default)
   * pauses the source until the consumer frees space; `'error'` fails the
   * stream with an `SpoolLimitError`.
   */
  onLimit?: 'backpressure' | 'error'
}

/** The stream hit `maxDiskBytes` or `minFreeSpace` with `onLimit: 'error'`. */
/** Anything `Spool.from()` can read from. */
export type SpoolSource =
  | NodeJS.ReadableStream
  | ReadableStream
  | Iterable<Buffer | Uint8Array | string>
  | AsyncIterable<Buffer | Uint8Array | string>

export class SpoolLimitError extends Error {
  readonly code: 'ERR_SYKE_SPOOL_LIMIT' = 'ERR_SYKE_SPOOL_LIMIT'
  override name = 'SpoolLimitError'
}

/** A run of unread bytes in the spool file: [start, end). */
interface Extent {
  start: number
  end: number
}

/** Where the next write goes. */
interface WriteTarget {
  position: number
  length: number
}

/** How often to recheck free space while waiting on `minFreeSpace`. */
const FREE_SPACE_POLL_MS = 250

/**
 * A pass-through stream that doesn't apply backpressure to its source.
 * Whatever the consumer is not ready for is cached in a temporary file and
 * read back, `chunkSize` bytes at a time, as the consumer drains. Disk use can
 * be capped with `maxDiskBytes` and `minFreeSpace`.
 */
export class Spool extends Duplex {
  /** Path of the spool file. `null` until the stream first spills to disk. */
  filepath: string | null = null

  readonly #dir: string
  readonly #chunkSize: number
  readonly #memoryThreshold: number
  readonly #maxDiskBytes: number
  readonly #floor: FreeSpaceFloor
  readonly #onLimit: 'backpressure' | 'error'

  #file: Promise<FileHandle> | null = null
  #fileSize = 0
  // Unread bytes in stream order. Reads take from the first extent; writes
  // extend the last one or start a new one in free space, so space the reader
  // has finished with is reused before the file grows.
  #extents: Extent[] = []
  #reading = false
  #writing = false
  #wakeWriter: (() => void) | null = null
  #pending = new Set<Promise<unknown>>()

  #memory: Buffer[] = []
  #memoryBytes = 0

  #wantsData = false
  #writeEnded = false
  #readEnded = false

  /**
   * Reads `source` through a new Spool, so the source is never held back by
   * a slow consumer: `Spool.from(res).pipe(slowWriter)`. Accepts a Node
   * readable, a web `ReadableStream`, or an iterable or async iterable of
   * chunks. Errors and destruction propagate both ways, as with `pipeline`.
   *
   * Overrides the inherited `Duplex.from()`, which would return a plain
   * `Duplex` with no disk spilling.
   */
  static override from(source: SpoolSource, options?: SpoolOptions): Spool {
    const spool = new Spool(options)
    // Errors surface on `spool` itself; the callback only stops pipeline from
    // reporting them a second time.
    pipeline(toNodeReadable(source), spool, () => {})
    return spool
  }

  constructor({
    dir,
    chunkSize,
    memoryThreshold,
    minFreeSpace,
    maxDiskBytes,
    onLimit,
    ...options
  }: SpoolOptions = {}) {
    super({ ...options, objectMode: false, decodeStrings: true })
    this.#dir = dir ?? tmpdir()
    this.#chunkSize = positiveInt('chunkSize', chunkSize ?? this.readableHighWaterMark)
    this.#memoryThreshold = nonNegativeInt('memoryThreshold', memoryThreshold ?? 0)
    this.#maxDiskBytes =
      maxDiskBytes === undefined
        ? Number.POSITIVE_INFINITY
        : positiveInt('maxDiskBytes', maxDiskBytes)
    this.#floor = parseMinFreeSpace(minFreeSpace ?? '10%')
    if (onLimit !== undefined && onLimit !== 'backpressure' && onLimit !== 'error') {
      throw new RangeError(
        `syke: onLimit must be 'backpressure' or 'error', got ${String(onLimit)}`,
      )
    }
    this.#onLimit = onLimit ?? 'backpressure'
  }

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    done: (error?: Error | null) => void,
  ): void {
    if (this.#passThrough(chunk)) return done()

    // Memory only takes data while the disk is empty, so memory is always older.
    if (this.#diskEmpty() && this.#memoryBytes + chunk.length <= this.#memoryThreshold) {
      this.#memory.push(chunk)
      this.#memoryBytes += chunk.length
      done()
      this.#flush()
      return
    }

    this.#track(this.#spill(chunk)).then(() => {
      done()
      this.#flush()
    }, done)
  }

  override _final(done: (error?: Error | null) => void): void {
    this.#writeEnded = true
    this.#flush()
    done()
  }

  override _read(): void {
    this.#wantsData = true
    this.#flush()
  }

  override _destroy(error: Error | null, done: (error?: Error | null) => void): void {
    this.#memory = []
    this.#memoryBytes = 0
    this.#wakeWriter?.()
    this.#cleanup().then(
      () => done(error),
      (cleanupError: Error) => done(error ?? cleanupError),
    )
  }

  /** Hands `chunk` straight to a waiting consumer if nothing is queued ahead of it. */
  #passThrough(chunk: Buffer) {
    if (!this.#wantsData || !this.#diskEmpty() || this.#memory.length > 0) return false
    this.#wantsData = this.push(chunk)
    return true
  }

  /** No unread bytes on disk and no write in flight. */
  #diskEmpty() {
    return this.#extents.length === 0 && !this.#writing
  }

  async #spill(chunk: Buffer) {
    let rest = chunk
    while (rest.length > 0) {
      if (this.destroyed) return

      // Prefer space inside the file; only ask the filesystem when it must grow.
      let budget = 0
      let target = this.#place(rest.length, 0, false)
      if (!target && this.#fileSize < this.#maxDiskBytes) {
        budget = this.#floor ? await freeSpaceBudget(this.#dir, this.#floor) : Number.POSITIVE_INFINITY
        // State may have changed during the await, so place again from scratch.
        // Everything from here to `#writing = true` is synchronous.
        target = this.#place(rest.length, budget, false)
      }
      // At a limit, any free gap will do, however small.
      target ??= this.#place(rest.length, budget, true)

      if (!target) {
        // At a limit with nothing queued and the consumer waiting: hand it over.
        if (this.#passThrough(rest)) return
        if (this.#onLimit === 'error') throw new SpoolLimitError(this.#limitMessage(budget))
        await this.#waitForSpace()
        continue
      }

      const piece = rest.subarray(0, target.length)
      this.#writing = true
      try {
        const file = await this.#openFile()
        let offset = 0
        while (offset < piece.length) {
          const { bytesWritten } = await file.write(
            piece,
            offset,
            piece.length - offset,
            target.position + offset,
          )
          offset += bytesWritten
        }
      } finally {
        this.#writing = false
      }

      const end = target.position + piece.length
      const tail = this.#extents.at(-1)
      // The tail may have been fully read (and dropped) during the write.
      if (tail && tail.end === target.position) tail.end = end
      else this.#extents.push({ start: target.position, end })
      this.#fileSize = Math.max(this.#fileSize, end)
      rest = rest.subarray(piece.length)
      this.#flush()
    }
  }

  /**
   * Finds space for up to `want` bytes: right after the last extent if
   * possible, otherwise the first free gap. The file may grow by `growth`
   * bytes (capped by `maxDiskBytes`). Unless `anySize`, gaps smaller than
   * `chunkSize` are skipped to avoid fragmenting the file.
   */
  #place(want: number, growth: number, anySize: boolean): WriteTarget | null {
    const limit = Math.min(this.#maxDiskBytes, this.#fileSize + Math.max(0, growth))
    const tail = this.#extents.at(-1)
    const minGap = anySize ? 1 : Math.min(want, this.#chunkSize)

    let first: WriteTarget | null = null
    for (const gap of this.#gaps(limit)) {
      const length = Math.min(want, gap.end - gap.start)
      if (tail && gap.start === tail.end) return { position: gap.start, length }
      if (!first && gap.end - gap.start >= minGap) first = { position: gap.start, length }
    }
    return first
  }

  /** Free space in [0, limit), in file order. */
  #gaps(limit: number) {
    const gaps: Extent[] = []
    let cursor = 0
    for (const extent of [...this.#extents].sort((a, b) => a.start - b.start)) {
      if (extent.start > cursor) gaps.push({ start: cursor, end: Math.min(extent.start, limit) })
      cursor = Math.max(cursor, extent.end)
    }
    if (limit > cursor) gaps.push({ start: cursor, end: limit })
    return gaps.filter((gap) => gap.end > gap.start)
  }

  #limitMessage(budget: number) {
    if (this.#fileSize + Math.max(0, budget) >= this.#maxDiskBytes) {
      return `syke: spool file reached maxDiskBytes (${this.#maxDiskBytes} bytes)`
    }
    return `syke: spilling more would take free space on ${this.#dir} below minFreeSpace`
  }

  /** Resolves when the reader frees space, the consumer asks for data, or free space may have changed. */
  #waitForSpace() {
    return new Promise<void>((resolve) => {
      const wake = () => {
        clearTimeout(timer)
        this.#wakeWriter = null
        resolve()
      }
      const timer = setTimeout(wake, FREE_SPACE_POLL_MS)
      this.#wakeWriter = wake
    })
  }

  #openFile() {
    if (!this.#file) {
      const filepath = join(this.#dir, `syke-spool-${randomUUID()}`)
      this.filepath = filepath
      // `wx+` refuses to reuse an existing file; 0o600 keeps it private.
      this.#file = warnIfMemoryBacked(this.#dir)
        .then(() => open(filepath, 'wx+', 0o600))
        .then((file) => {
          trackFile(this, filepath, file)
          return file
        })
    }
    return this.#file
  }

  #flush() {
    if (this.#readEnded || this.destroyed) return
    this.#wakeWriter?.()
    if (this.#reading) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift() as Buffer
      this.#memoryBytes -= chunk.length
      this.#wantsData = this.push(chunk)
    }

    if (this.#memory.length > 0) return

    if (this.#extents.length > 0) {
      if (this.#wantsData) this.#readFromDisk()
      return
    }

    if (this.#writeEnded && this.#diskEmpty()) {
      this.#readEnded = true
      this.push(null)
    }
  }

  #readFromDisk() {
    this.#reading = true
    const head = this.#extents[0] as Extent
    const length = Math.min(this.#chunkSize, head.end - head.start)
    const position = head.start
    const read = this.#track(
      this.#openFile().then((file) => file.read(Buffer.allocUnsafe(length), 0, length, position)),
    )

    read.then(
      ({ bytesRead, buffer }) => {
        this.#reading = false
        if (this.destroyed) return
        if (bytesRead === 0) {
          this.destroy(new Error(`syke: spool file ended early (${this.filepath})`))
          return
        }

        head.start += bytesRead
        if (head.start === head.end) this.#extents.shift()
        this.#wantsData = this.push(bytesRead < length ? buffer.subarray(0, bytesRead) : buffer)
        this.#flush()
      },
      (error: Error) => {
        this.#reading = false
        this.destroy(error)
      },
    )
  }

  #track<T>(promise: Promise<T>) {
    this.#pending.add(promise)
    const untrack = () => this.#pending.delete(promise)
    promise.then(untrack, untrack)
    return promise
  }

  async #cleanup() {
    await Promise.allSettled(this.#pending)
    if (!this.#file) return

    const filepath = this.filepath as string
    const file = this.#file
    this.#file = null
    releaseFile(this, filepath)

    const opened = await Promise.allSettled([file])
    if (opened[0].status === 'fulfilled') await opened[0].value.close()
    await rm(filepath, { force: true })
  }
}

function toNodeReadable(source: SpoolSource): NodeJS.ReadableStream {
  if (typeof (source as NodeJS.ReadableStream).pipe === 'function') {
    return source as NodeJS.ReadableStream
  }
  if (typeof (source as ReadableStream).getReader === 'function') {
    return Readable.fromWeb(source as ReadableStream)
  }
  return Readable.from(source as Iterable<unknown> | AsyncIterable<unknown>, { objectMode: false })
}

function positiveInt(name: string, value: number) {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`syke: ${name} must be a positive integer, got ${value}`)
  }
  return value
}

function nonNegativeInt(name: string, value: number) {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`syke: ${name} must be a non-negative integer, got ${value}`)
  }
  return value
}
