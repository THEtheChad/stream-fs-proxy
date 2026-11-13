import { execFileSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { existsSync } from 'node:fs'
import { readdir, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { Spool } from '../../src/index.ts'
import { closed, sink, source, until, useTempDir, watchFileSize } from '../helpers.ts'

const tmp = useTempDir()

describe('Spool', () => {
  it('passes data through unchanged', async () => {
    const data = randomBytes(5 * 1024 * 1024)
    const out = sink()

    await pipeline(source(data, [1, 7000, 65536, 300000]), new Spool({ dir: tmp.path }), out.stream)

    expect(out.received.equals(data)).toBe(true)
  })

  it('accepts the whole source while the consumer is stalled, then replays it in bounded chunks', async () => {
    const data = randomBytes(8 * 1024 * 1024)
    const chunkSize = 32 * 1024
    const spool = new Spool({ dir: tmp.path, chunkSize })
    const out = sink()
    out.pause()

    let peakBuffered = 0
    spool.on('data', () => {
      peakBuffered = Math.max(peakBuffered, spool.readableLength)
    })

    const done = pipeline(source(data), spool, out.stream)
    // If the spool applied backpressure, the source could never finish.
    await new Promise((resolve) => spool.on('finish', resolve))
    expect(spool.filepath).not.toBeNull()
    out.resume()
    await done

    expect(out.received.equals(data)).toBe(true)
    expect(Math.max(...out.chunks.map((c) => c.length))).toBeLessThanOrEqual(64 * 1024)
    expect(peakBuffered).toBeLessThanOrEqual(spool.readableHighWaterMark + chunkSize)
  })

  it('removes the cache file once the stream finishes', async () => {
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(1024 * 1024)), spool, out.stream)
    await new Promise((resolve) => spool.on('finish', resolve))

    expect(await readdir(tmp.path)).toHaveLength(1)
    out.resume()
    await done
    await closed(spool)

    expect(existsSync(spool.filepath as string)).toBe(false)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('removes the cache file when the source errors', async () => {
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    out.pause()
    let sent = 0
    const failing = new Readable({
      read() {
        if (sent++ < 10) this.push(randomBytes(64 * 1024))
        else this.destroy(new Error('socket reset'))
      },
    })

    await expect(pipeline(failing, spool, out.stream)).rejects.toThrow('socket reset')
    await closed(spool)

    expect(await readdir(tmp.path)).toEqual([])
  })

  it('removes the cache file when aborted', async () => {
    const controller = new AbortController()
    const spool = new Spool({ dir: tmp.path, signal: controller.signal })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(1024 * 1024)), spool, out.stream)
    await new Promise((resolve) => spool.on('finish', resolve))

    controller.abort()
    await expect(done).rejects.toThrow()
    await closed(spool)

    expect(await readdir(tmp.path)).toEqual([])
  })

  it('reports cache write errors instead of hanging', async () => {
    const spool = new Spool({ dir: join(tmp.path, 'missing') })
    const out = sink()
    out.pause()

    await expect(pipeline(source(randomBytes(1024 * 1024)), spool, out.stream)).rejects.toThrow(
      /ENOENT/,
    )
  })

  it('keeps data under memoryThreshold off disk', async () => {
    const data = randomBytes(256 * 1024)
    const spool = new Spool({ dir: tmp.path, memoryThreshold: 1024 * 1024 })
    const out = sink()
    out.pause()
    const done = pipeline(source(data, [10000]), spool, out.stream)
    await new Promise((resolve) => spool.on('finish', resolve))

    expect(spool.filepath).toBeNull()
    expect(await readdir(tmp.path)).toEqual([])
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('preserves order across memory and disk', async () => {
    const data = randomBytes(4 * 1024 * 1024)
    const spool = new Spool({ dir: tmp.path, memoryThreshold: 100 * 1024, chunkSize: 9000 })
    const out = sink()

    // Stall and release the consumer repeatedly so data moves between
    // pass-through, memory and disk.
    let stalls = 0
    const timer = setInterval(() => {
      if (stalls++ % 2 === 0) out.pause()
      else out.resume()
    }, 2)

    try {
      await pipeline(source(data, [1, 4096, 50000, 130000]), spool, out.stream)
    } finally {
      clearInterval(timer)
    }

    expect(spool.filepath).not.toBeNull()
    expect(out.received.equals(data)).toBe(true)
  })

  it('reuses cache file space once the backlog drains', async () => {
    const backlog = 2 * 1024 * 1024
    const data = randomBytes(backlog * 3)
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    const src = new Readable({ read() {} })
    const until = async (condition: () => boolean) => {
      while (!condition()) await new Promise((resolve) => setTimeout(resolve, 1))
    }

    const done = pipeline(src, spool, out.stream)
    let peakFileSize = 0

    // Three rounds of: stall the consumer, spill a backlog, then drain it.
    for (let offset = 0; offset < data.length; offset += backlog) {
      out.pause()
      for (let i = offset; i < offset + backlog; i += 64 * 1024) {
        src.push(data.subarray(i, i + 64 * 1024))
      }
      await until(() => src.readableLength === 0 && spool.writableLength === 0)
      peakFileSize = Math.max(peakFileSize, (await stat(spool.filepath as string)).size)
      out.resume()
      await until(() => out.received.length === offset + backlog)
    }
    src.push(null)
    await done

    expect(out.received.equals(data)).toBe(true)
    expect(peakFileSize).toBeLessThanOrEqual(backlog)
  })

  it('reuses space the consumer has read even when the backlog never drains', async () => {
    const MiB = 1024 * 1024
    const data = randomBytes(8 * MiB)
    const spool = new Spool({ dir: tmp.path })
    const src = new Readable({ read() {} })
    const out = sink()
    out.pause()
    const done = pipeline(src, spool, out.stream)
    const size = watchFileSize(spool)

    let pushed = 0
    let received = 0
    spool.on('data', (chunk: Buffer) => {
      received += chunk.length
    })
    const pushMiB = async () => {
      src.push(data.subarray(pushed, pushed + MiB))
      pushed += MiB
      await until(() => src.readableLength === 0 && spool.writableLength === 0)
    }

    // Keep 1–2 MiB queued the whole time: the consumer reads some, never all.
    await pushMiB()
    while (pushed < data.length) {
      await pushMiB()
      out.resume()
      await until(() => received >= pushed - 1.25 * MiB)
      out.pause()
    }
    src.push(null)
    out.resume()
    await done

    expect(out.received.equals(data)).toBe(true)
    // Reusing space only after a full drain would grow the file to all 8 MiB.
    expect(size.stop()).toBeLessThan(4 * MiB)
  })

  it('removes the cache file if the process exits mid-stream', () => {
    const entry = pathToFileURL(join(import.meta.dirname, '../../src/index.ts')).href
    const child = `
      import { statSync } from 'node:fs'
      import { Readable, Writable } from 'node:stream'
      import { Spool } from ${JSON.stringify(entry)}

      const spool = new Spool({ dir: ${JSON.stringify(tmp.path)} })
      const src = new Readable({ read() {} })
      // A consumer that never finishes a write, so everything spills.
      src.pipe(spool).pipe(new Writable({ highWaterMark: 1, write() {} }))
      src.push(Buffer.alloc(1024 * 1024))

      setInterval(() => {
        const size = spool.filepath && statSync(spool.filepath, { throwIfNoEntry: false })?.size
        if (size > 0) {
          console.log(spool.filepath)
          process.exit(0)
        }
      }, 5)
    `
    const output = execFileSync(process.execPath, ['--input-type=module', '-e', child], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 10_000,
    })

    expect(output.trim()).toMatch(/syke-spool-/)
    expect(existsSync(output.trim())).toBe(false)
  })

  it('removes the spool file when an abandoned spool is garbage-collected', () => {
    const entry = pathToFileURL(join(import.meta.dirname, '../../src/index.ts')).href
    const child = `
      import { existsSync } from 'node:fs'
      import { Readable } from 'node:stream'
      import { Spool } from ${JSON.stringify(entry)}

      // A spool with no consumer, dropped without being ended or destroyed.
      async function abandon() {
        const spool = new Spool({ dir: ${JSON.stringify(tmp.path)} })
        const src = new Readable({ read() {} })
        src.pipe(spool)
        src.push(Buffer.alloc(256 * 1024))
        while (!spool.filepath || !existsSync(spool.filepath) || spool.writableLength > 0) {
          await new Promise((resolve) => setTimeout(resolve, 5))
        }
        return spool.filepath
      }

      const filepath = await abandon()
      for (let i = 0; i < 100 && existsSync(filepath); i++) {
        globalThis.gc()
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
      // Report before exit, so the exit hook can't be what removed it.
      console.log(JSON.stringify({ filepath, exists: existsSync(filepath) }))
    `
    const output = execFileSync(
      process.execPath,
      ['--expose-gc', '--input-type=module', '-e', child],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 10_000 },
    )

    const result = JSON.parse(output)
    expect(result.filepath).toMatch(/syke-spool-/)
    expect(result.exists).toBe(false)
  })

  it('creates the cache file readable only by the owner', async () => {
    if (process.platform === 'win32') return
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    out.pause()
    const done = pipeline(source(randomBytes(256 * 1024)), spool, out.stream)
    await new Promise((resolve) => spool.on('finish', resolve))

    const { mode } = await stat(spool.filepath as string)
    expect(mode & 0o777).toBe(0o600)
    out.resume()
    await done
  })

  it('ends an empty stream without touching disk', async () => {
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    await pipeline(Readable.from([]), spool, out.stream)

    expect(out.received.length).toBe(0)
    expect(spool.filepath).toBeNull()
  })

  it('accepts string writes', async () => {
    const spool = new Spool({ dir: tmp.path })
    const out = sink()
    await pipeline(Readable.from(['héllo ', 'wörld'], { objectMode: false }), spool, out.stream)

    expect(out.received.toString()).toBe('héllo wörld')
  })

  it('rejects invalid options', () => {
    expect(() => new Spool({ chunkSize: 0 })).toThrow(RangeError)
    expect(() => new Spool({ chunkSize: 1.5 })).toThrow(RangeError)
    expect(() => new Spool({ memoryThreshold: -1 })).toThrow(RangeError)
  })
})
