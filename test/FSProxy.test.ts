import { randomBytes } from 'node:crypto'
import { existsSync } from 'node:fs'
import { mkdtemp, readdir, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { FSProxy } from '../src/index.ts'

let dir: string

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fs-proxy-test-'))
})

afterEach(async () => {
  await rm(dir, { recursive: true, force: true })
})

/** Emits `data` in chunks of the given sizes (cycled), as fast as it is pulled. */
function source(data: Buffer, sizes: number[] = [64 * 1024]) {
  let offset = 0
  let i = 0
  return new Readable({
    read() {
      if (offset >= data.length) return this.push(null)
      const size = sizes[i++ % sizes.length] as number
      this.push(data.subarray(offset, offset + size))
      offset += size
    },
  })
}

/** A consumer that records what it gets and can be paused. */
function sink() {
  const chunks: Buffer[] = []
  let release: (() => void) | null = null
  let paused: Promise<void> | null = null

  const stream = new Writable({
    highWaterMark: 1,
    write(chunk: Buffer, _encoding, done) {
      chunks.push(chunk)
      if (paused) paused.then(() => done())
      else done()
    },
  })

  return {
    stream,
    chunks,
    get received() {
      return Buffer.concat(chunks)
    },
    pause() {
      paused = new Promise((resolve) => {
        release = resolve
      })
    },
    resume() {
      paused = null
      release?.()
    },
  }
}

describe('FSProxy', () => {
  it('passes data through unchanged', async () => {
    const data = randomBytes(5 * 1024 * 1024)
    const out = sink()

    await pipeline(source(data, [1, 7000, 65536, 300000]), new FSProxy({ dir }), out.stream)

    expect(out.received.equals(data)).toBe(true)
  })

  it('accepts the whole source while the consumer is stalled, then replays it in bounded chunks', async () => {
    const data = randomBytes(8 * 1024 * 1024)
    const chunkSize = 32 * 1024
    const proxy = new FSProxy({ dir, chunkSize })
    const out = sink()
    out.pause()

    let peakBuffered = 0
    proxy.on('data', () => {
      peakBuffered = Math.max(peakBuffered, proxy.readableLength)
    })

    const done = pipeline(source(data), proxy, out.stream)
    // If the proxy applied backpressure, the source could never finish.
    await new Promise((resolve) => proxy.on('finish', resolve))
    expect(proxy.filepath).not.toBeNull()
    out.resume()
    await done

    expect(out.received.equals(data)).toBe(true)
    expect(Math.max(...out.chunks.map((c) => c.length))).toBeLessThanOrEqual(64 * 1024)
    expect(peakBuffered).toBeLessThanOrEqual(proxy.readableHighWaterMark + chunkSize)
  })

  it('removes the cache file once the stream finishes', async () => {
    const proxy = new FSProxy({ dir })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(1024 * 1024)), proxy, out.stream)
    await new Promise((resolve) => proxy.on('finish', resolve))

    expect(await readdir(dir)).toHaveLength(1)
    out.resume()
    await done
    await new Promise((resolve) => proxy.on('close', resolve))

    expect(existsSync(proxy.filepath as string)).toBe(false)
    expect(await readdir(dir)).toEqual([])
  })

  it('removes the cache file when the source errors', async () => {
    const proxy = new FSProxy({ dir })
    const out = sink()
    out.pause()
    let sent = 0
    const failing = new Readable({
      read() {
        if (sent++ < 10) this.push(randomBytes(64 * 1024))
        else this.destroy(new Error('socket reset'))
      },
    })

    await expect(pipeline(failing, proxy, out.stream)).rejects.toThrow('socket reset')
    await new Promise((resolve) => (proxy.closed ? resolve(null) : proxy.on('close', resolve)))

    expect(await readdir(dir)).toEqual([])
  })

  it('removes the cache file when aborted', async () => {
    const controller = new AbortController()
    const proxy = new FSProxy({ dir, signal: controller.signal })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(1024 * 1024)), proxy, out.stream)
    await new Promise((resolve) => proxy.on('finish', resolve))

    controller.abort()
    await expect(done).rejects.toThrow()
    await new Promise((resolve) => (proxy.closed ? resolve(null) : proxy.on('close', resolve)))

    expect(await readdir(dir)).toEqual([])
  })

  it('reports cache write errors instead of hanging', async () => {
    const proxy = new FSProxy({ dir: join(dir, 'missing') })
    const out = sink()
    out.pause()

    await expect(pipeline(source(randomBytes(1024 * 1024)), proxy, out.stream)).rejects.toThrow(
      /ENOENT/,
    )
  })

  it('keeps data under memoryThreshold off disk', async () => {
    const data = randomBytes(256 * 1024)
    const proxy = new FSProxy({ dir, memoryThreshold: 1024 * 1024 })
    const out = sink()
    out.pause()
    const done = pipeline(source(data, [10000]), proxy, out.stream)
    await new Promise((resolve) => proxy.on('finish', resolve))

    expect(proxy.filepath).toBeNull()
    expect(await readdir(dir)).toEqual([])
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('preserves order across memory and disk', async () => {
    const data = randomBytes(4 * 1024 * 1024)
    const proxy = new FSProxy({ dir, memoryThreshold: 100 * 1024, chunkSize: 9000 })
    const out = sink()

    // Stall and release the consumer repeatedly so data moves between
    // pass-through, memory and disk.
    let stalls = 0
    const timer = setInterval(() => {
      if (stalls++ % 2 === 0) out.pause()
      else out.resume()
    }, 2)

    try {
      await pipeline(source(data, [1, 4096, 50000, 130000]), proxy, out.stream)
    } finally {
      clearInterval(timer)
    }

    expect(proxy.filepath).not.toBeNull()
    expect(out.received.equals(data)).toBe(true)
  })

  it('reuses cache file space once the backlog drains', async () => {
    const backlog = 2 * 1024 * 1024
    const data = randomBytes(backlog * 3)
    const proxy = new FSProxy({ dir })
    const out = sink()
    const src = new Readable({ read() {} })
    const until = async (condition: () => boolean) => {
      while (!condition()) await new Promise((resolve) => setTimeout(resolve, 1))
    }

    const done = pipeline(src, proxy, out.stream)
    let peakFileSize = 0

    // Three rounds of: stall the consumer, spill a backlog, then drain it.
    for (let offset = 0; offset < data.length; offset += backlog) {
      out.pause()
      for (let i = offset; i < offset + backlog; i += 64 * 1024) {
        src.push(data.subarray(i, i + 64 * 1024))
      }
      await until(() => src.readableLength === 0 && proxy.writableLength === 0)
      peakFileSize = Math.max(peakFileSize, (await stat(proxy.filepath as string)).size)
      out.resume()
      await until(() => out.received.length === offset + backlog)
    }
    src.push(null)
    await done

    expect(out.received.equals(data)).toBe(true)
    expect(peakFileSize).toBeLessThanOrEqual(backlog)
  })

  it('creates the cache file readable only by the owner', async () => {
    if (process.platform === 'win32') return
    const proxy = new FSProxy({ dir })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(256 * 1024)), proxy, out.stream)
    await new Promise((resolve) => proxy.on('finish', resolve))

    const { mode } = await stat(proxy.filepath as string)
    expect(mode & 0o777).toBe(0o600)
    out.resume()
    await done
  })

  it('ends an empty stream without touching disk', async () => {
    const proxy = new FSProxy({ dir })
    const out = sink()
    await pipeline(Readable.from([]), proxy, out.stream)

    expect(out.received.length).toBe(0)
    expect(proxy.filepath).toBeNull()
  })

  it('accepts string writes', async () => {
    const proxy = new FSProxy({ dir })
    const out = sink()
    await pipeline(Readable.from(['héllo ', 'wörld'], { objectMode: false }), proxy, out.stream)

    expect(out.received.toString()).toBe('héllo wörld')
  })

  it('rejects invalid options', () => {
    expect(() => new FSProxy({ chunkSize: 0 })).toThrow(RangeError)
    expect(() => new FSProxy({ chunkSize: 1.5 })).toThrow(RangeError)
    expect(() => new FSProxy({ memoryThreshold: -1 })).toThrow(RangeError)
  })
})
