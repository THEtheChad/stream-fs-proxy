import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions } from 'node:stream'
import {
  type FreeSpaceFloor,
  type MinFreeSpace,
  freeSpaceBudget,
  parseMinFreeSpace,
  warnIfMemoryBacked,
} from './diskSpace.ts'

export type { MinFreeSpace }

export interface FSProxyOptions
  extends Pick<
    DuplexOptions,
    'highWaterMark' | 'readableHighWaterMark' | 'writableHighWaterMark' | 'signal'
  > {
  /** Directory for the temporary cache file. Defaults to `os.tmpdir()`. */
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
   * Stop growing the cache file before free space on `dir`'s filesystem drops
   * below this: a percentage of the filesystem (`'10%'`), a number of bytes,
   * or `false` for no floor. Defaults to `'10%'`.
   */
  minFreeSpace?: MinFreeSpace
  /** Largest size, in bytes, the cache file may reach. Defaults to no cap. */
  maxDiskBytes?: number
  /**
   * What to do when a disk limit is reached. `'backpressure'` (the default)
   * pauses the source until the consumer frees space; `'error'` fails the
   * stream with an `FSProxyLimitError`.
   */
  onLimit?: 'backpressure' | 'error'
}

/** The stream hit `maxDiskBytes` or `minFreeSpace` with `onLimit: 'error'`. */
export class FSProxyLimitError extends Error {
  readonly code: 'ERR_STREAM_FS_PROXY_LIMIT' = 'ERR_STREAM_FS_PROXY_LIMIT'
  override name = 'FSProxyLimitError'
}

interface WriteTarget {
  region: 'head' | 'wrap'
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
export class FSProxy extends Duplex {
  /** Path of the cache file. `null` until the stream first spills to disk. */
  filepath: string | null = null

  readonly #dir: string
  readonly #chunkSize: number
  readonly #memoryThreshold: number
  readonly #maxDiskBytes: number
  readonly #floor: FreeSpaceFloor
  readonly #onLimit: 'backpressure' | 'error'

  #file: Promise<FileHandle> | null = null
  #fileSize = 0
  // The cache file is a ring buffer. Unread bytes are [headStart, headEnd),
  // followed by [0, wrapEnd) once writing has wrapped to the front of the file.
  #headStart = 0
  #headEnd = 0
  #wrapEnd = 0
  #wrapped = false
  #reading = false
  #writing = false
  #wakeWriter: (() => void) | null = null
  #pending = new Set<Promise<unknown>>()

  #memory: Buffer[] = []
  #memoryBytes = 0

  #wantsData = false
  #writeEnded = false
  #readEnded = false

  constructor({
    dir,
    chunkSize,
    memoryThreshold,
    minFreeSpace,
    maxDiskBytes,
    onLimit,
    ...options
  }: FSProxyOptions = {}) {
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
        `stream-fs-proxy: onLimit must be 'backpressure' or 'error', got ${String(onLimit)}`,
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
    return this.#headStart === this.#headEnd && this.#wrapEnd === 0 && !this.#writing
  }

  async #spill(chunk: Buffer) {
    let rest = chunk
    while (rest.length > 0) {
      if (this.destroyed) return

      const budget = await this.#growthBudget(rest.length)
      // Everything from here to `#writing = true` is synchronous, so the
      // target can't go stale before the write starts.
      this.#compact()
      const target = this.#target(rest.length, budget)

      if (!target) {
        // At a limit with nothing queued and the consumer waiting: hand it over.
        if (this.#passThrough(rest)) return
        if (this.#onLimit === 'error') throw new FSProxyLimitError(this.#limitMessage(budget))
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

      if (target.region === 'head') {
        this.#headEnd += piece.length
        this.#fileSize = Math.max(this.#fileSize, this.#headEnd)
      } else {
        this.#wrapEnd += piece.length
      }
      rest = rest.subarray(piece.length)
      this.#flush()
    }
  }

  /** Bytes the file may grow by under `minFreeSpace`; only checked when it would grow. */
  #growthBudget(want: number) {
    const grows = !this.#wrapped && this.#headEnd + want > this.#fileSize
    if (!grows || !this.#floor) return Promise.resolve(Number.POSITIVE_INFINITY)
    return freeSpaceBudget(this.#dir, this.#floor)
  }

  /** Where the next write of up to `want` bytes goes, or `null` if no space. */
  #target(want: number, budget: number): WriteTarget | null {
    if (this.#wrapped) {
      const room = this.#headStart - this.#wrapEnd
      if (room <= 0) return null
      return { region: 'wrap', position: this.#wrapEnd, length: Math.min(want, room) }
    }

    const end = Math.min(this.#maxDiskBytes, this.#fileSize + Math.max(0, budget))
    const room = end - this.#headEnd
    if (room > 0) {
      return { region: 'head', position: this.#headEnd, length: Math.min(want, room) }
    }

    // Out of room at the end of the file: wrap into space the reader has freed.
    if (this.#headStart > 0) {
      this.#wrapped = true
      this.#wrapEnd = 0
      return { region: 'wrap', position: 0, length: Math.min(want, this.#headStart) }
    }
    return null
  }

  #limitMessage(budget: number) {
    if (this.#maxDiskBytes <= this.#fileSize + Math.max(0, budget)) {
      return `stream-fs-proxy: cache file reached maxDiskBytes (${this.#maxDiskBytes} bytes)`
    }
    return `stream-fs-proxy: spilling more would take free space on ${this.#dir} below minFreeSpace`
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

  /** Once the head region is fully read, move the wrapped region (or the empty file) to the front. */
  #compact() {
    if (this.#reading || this.#writing || this.#headStart !== this.#headEnd) return
    this.#headStart = 0
    this.#headEnd = this.#wrapped ? this.#wrapEnd : 0
    this.#wrapEnd = 0
    this.#wrapped = false
  }

  #openFile() {
    if (!this.#file) {
      const filepath = join(this.#dir, `stream-fs-proxy-${randomUUID()}`)
      this.filepath = filepath
      // `wx+` refuses to reuse an existing file; 0o600 keeps it private.
      this.#file = warnIfMemoryBacked(this.#dir).then(() => open(filepath, 'wx+', 0o600))
    }
    return this.#file
  }

  #flush() {
    if (this.#readEnded || this.destroyed) return
    this.#compact()
    this.#wakeWriter?.()
    if (this.#reading) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift() as Buffer
      this.#memoryBytes -= chunk.length
      this.#wantsData = this.push(chunk)
    }

    if (this.#memory.length > 0) return

    if (this.#headEnd > this.#headStart) {
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
    const length = Math.min(this.#chunkSize, this.#headEnd - this.#headStart)
    const position = this.#headStart
    const read = this.#track(
      this.#openFile().then((file) => file.read(Buffer.allocUnsafe(length), 0, length, position)),
    )

    read.then(
      ({ bytesRead, buffer }) => {
        this.#reading = false
        if (this.destroyed) return
        if (bytesRead === 0) {
          this.destroy(new Error(`stream-fs-proxy: cache file ended early (${this.filepath})`))
          return
        }

        this.#headStart += bytesRead
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

    const opened = await Promise.allSettled([file])
    if (opened[0].status === 'fulfilled') await opened[0].value.close()
    await rm(filepath, { force: true })
  }
}

function positiveInt(name: string, value: number) {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`stream-fs-proxy: ${name} must be a positive integer, got ${value}`)
  }
  return value
}

function nonNegativeInt(name: string, value: number) {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`stream-fs-proxy: ${name} must be a non-negative integer, got ${value}`)
  }
  return value
}
