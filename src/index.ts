import { randomUUID } from 'node:crypto'
import { type FileHandle, open, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Duplex, type DuplexOptions } from 'node:stream'

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
}

/**
 * A pass-through stream that never applies backpressure to its source.
 * Whatever the consumer is not ready for is cached in a temporary file and
 * read back, `chunkSize` bytes at a time, as the consumer drains.
 */
export class FSProxy extends Duplex {
  /** Path of the cache file. `null` until the stream first spills to disk. */
  filepath: string | null = null

  readonly #dir: string
  readonly #chunkSize: number
  readonly #memoryThreshold: number

  #file: Promise<FileHandle> | null = null
  #readPos = 0
  #writePos = 0
  #reading = false
  #writing = false
  #pending = new Set<Promise<unknown>>()

  #memory: Buffer[] = []
  #memoryBytes = 0

  #wantsData = false
  #writeEnded = false
  #readEnded = false

  constructor({ dir, chunkSize, memoryThreshold, ...options }: FSProxyOptions = {}) {
    super({ ...options, objectMode: false, decodeStrings: true })
    this.#dir = dir ?? tmpdir()
    this.#chunkSize = positiveInt('chunkSize', chunkSize ?? this.readableHighWaterMark)
    this.#memoryThreshold = nonNegativeInt('memoryThreshold', memoryThreshold ?? 0)
  }

  override _write(chunk: Buffer, _encoding: BufferEncoding, done: (error?: Error | null) => void): void {
    // The consumer is waiting and nothing is ahead of this chunk: hand it over.
    if (this.#wantsData && this.#diskEmpty() && this.#memory.length === 0) {
      this.#wantsData = this.push(chunk)
      return done()
    }

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
    this.#cleanup().then(
      () => done(error),
      (cleanupError: Error) => done(error ?? cleanupError),
    )
  }

  /** No bytes waiting on disk and no write in flight. */
  #diskEmpty() {
    return this.#writePos === this.#readPos && !this.#writing
  }

  async #spill(chunk: Buffer) {
    this.#writing = true
    try {
      const file = await this.#openFile()
      let offset = 0
      while (offset < chunk.length) {
        const { bytesWritten } = await file.write(
          chunk,
          offset,
          chunk.length - offset,
          this.#writePos + offset,
        )
        offset += bytesWritten
      }
      this.#writePos += chunk.length
    } finally {
      this.#writing = false
    }
  }

  #openFile() {
    if (!this.#file) {
      const filepath = join(this.#dir, `stream-fs-proxy-${randomUUID()}`)
      this.filepath = filepath
      // `wx+` refuses to reuse an existing file; 0o600 keeps it private.
      this.#file = open(filepath, 'wx+', 0o600)
    }
    return this.#file
  }

  #flush() {
    if (this.#reading || this.#readEnded || this.destroyed) return

    while (this.#wantsData && this.#memory.length > 0) {
      const chunk = this.#memory.shift() as Buffer
      this.#memoryBytes -= chunk.length
      this.#wantsData = this.push(chunk)
    }

    if (this.#memory.length > 0) return

    if (this.#writePos > this.#readPos) {
      if (this.#wantsData) this.#readFromDisk()
      return
    }

    if (this.#writeEnded && !this.#writing) {
      this.#readEnded = true
      this.push(null)
    }
  }

  #readFromDisk() {
    this.#reading = true
    const length = Math.min(this.#chunkSize, this.#writePos - this.#readPos)
    const read = this.#track(
      this.#openFile().then((file) => file.read(Buffer.allocUnsafe(length), 0, length, this.#readPos)),
    )

    read.then(
      ({ bytesRead, buffer }) => {
        this.#reading = false
        if (this.destroyed) return
        if (bytesRead === 0) {
          this.destroy(new Error(`stream-fs-proxy: cache file ended early (${this.filepath})`))
          return
        }

        this.#readPos += bytesRead
        // Drained with no write in flight: reuse the file from the start so it
        // only ever grows to the largest backlog, not the whole stream.
        if (this.#readPos === this.#writePos && !this.#writing) {
          this.#readPos = 0
          this.#writePos = 0
        }

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
