import { randomBytes } from 'node:crypto'
import { readdirSync, statSync } from 'node:fs'
import { readdir, statfs } from 'node:fs/promises'
import { join } from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { FSProxy, FSProxyLimitError } from '../src/index.ts'
import { closed, fileSize, sink, sleep, source, until, useTempDir, watchFileSize } from './helpers.ts'

vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>()
  return { ...actual, statfs: vi.fn(actual.statfs) }
})

const tmp = useTempDir()
const MiB = 1024 * 1024
const EXT4 = 0xef53
const TMPFS = 0x01021994

/**
 * Fakes the filesystem under the test's temp dir: `free` bytes are available
 * before anything is written there, and shrink as the cache file grows.
 */
function fakeDisk({ total, free, type = EXT4 }: { total: number; free: number; type?: number }) {
  const disk = { free }
  vi.mocked(statfs).mockImplementation((async () => {
    const used = readdirSync(tmp.path).reduce((n, f) => n + statSync(join(tmp.path, f)).size, 0)
    const available = Math.max(0, disk.free - used)
    return { type, bsize: 1, blocks: total, bfree: available, bavail: available, files: 0, ffree: 0 }
  }) as never)
  return disk
}

beforeEach(() => {
  fakeDisk({ total: 1000 * MiB, free: 1000 * MiB })
})

describe('maxDiskBytes', () => {
  it('pauses the source once the cache file reaches the cap, then resumes', async () => {
    const cap = 256 * 1024
    const data = randomBytes(2 * MiB)
    const proxy = new FSProxy({ dir: tmp.path, maxDiskBytes: cap })
    const out = sink()
    out.pause()
    let finished = false
    proxy.on('finish', () => {
      finished = true
    })

    const done = pipeline(source(data), proxy, out.stream)
    await until(() => fileSize(proxy) === cap)
    await sleep(50)
    expect(fileSize(proxy)).toBe(cap)
    expect(finished).toBe(false)

    const size = watchFileSize(proxy)
    out.resume()
    await done

    expect(size.stop()).toBeLessThanOrEqual(cap)
    expect(out.received.equals(data)).toBe(true)
  })

  it('wraps around the cache file without reordering data', async () => {
    const cap = 100_000
    const data = randomBytes(4 * MiB)
    const proxy = new FSProxy({ dir: tmp.path, maxDiskBytes: cap, chunkSize: 9000 })
    const out = sink()
    const size = watchFileSize(proxy)

    let stalls = 0
    const timer = setInterval(() => {
      if (stalls++ % 2 === 0) out.pause()
      else out.resume()
    }, 2)

    try {
      await pipeline(source(data, [1, 4096, 50_000, 130_000]), proxy, out.stream)
    } finally {
      clearInterval(timer)
    }

    expect(size.stop()).toBeLessThanOrEqual(cap)
    expect(out.received.equals(data)).toBe(true)
  })

  it('handles a cap smaller than a single chunk', async () => {
    const data = randomBytes(MiB)
    const proxy = new FSProxy({ dir: tmp.path, maxDiskBytes: 1000, chunkSize: 300 })
    const out = sink()
    out.pause()

    const done = pipeline(source(data), proxy, out.stream)
    await until(() => fileSize(proxy) === 1000)
    out.resume()
    await done

    expect(out.received.equals(data)).toBe(true)
  })

  it("fails with FSProxyLimitError when onLimit is 'error'", async () => {
    const proxy = new FSProxy({ dir: tmp.path, maxDiskBytes: 256 * 1024, onLimit: 'error' })
    const out = sink()
    out.pause()

    const error = await pipeline(source(randomBytes(MiB)), proxy, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(FSProxyLimitError)
    expect(error).toMatchObject({ code: 'ERR_STREAM_FS_PROXY_LIMIT' })
    expect(error.message).toMatch(/maxDiskBytes/)

    await closed(proxy)
    expect(await readdir(tmp.path)).toEqual([])
  })
})

describe('minFreeSpace', () => {
  it('stops growing the cache file at a byte floor', async () => {
    fakeDisk({ total: 100 * MiB, free: 10 * MiB })
    const data = randomBytes(2 * MiB)
    const proxy = new FSProxy({ dir: tmp.path, minFreeSpace: 10 * MiB - 512 * 1024 })
    const out = sink()
    out.pause()

    const done = pipeline(source(data), proxy, out.stream)
    await until(() => fileSize(proxy) === 512 * 1024)
    await sleep(50)
    expect(fileSize(proxy)).toBe(512 * 1024)

    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('accepts a percentage of the filesystem', async () => {
    fakeDisk({ total: 100 * MiB, free: 10 * MiB + 512 * 1024 })
    const proxy = new FSProxy({ dir: tmp.path, minFreeSpace: '10%' })
    const out = sink()
    out.pause()

    const done = pipeline(source(randomBytes(2 * MiB)), proxy, out.stream)
    await until(() => fileSize(proxy) === 512 * 1024)
    await sleep(50)
    expect(fileSize(proxy)).toBe(512 * 1024)

    out.resume()
    await done
  })

  it("defaults to '10%'", async () => {
    fakeDisk({ total: 100 * MiB, free: 10 * MiB + 256 * 1024 })
    const proxy = new FSProxy({ dir: tmp.path })
    const out = sink()
    out.pause()

    const done = pipeline(source(randomBytes(2 * MiB)), proxy, out.stream)
    await until(() => fileSize(proxy) === 256 * 1024)
    await sleep(50)
    expect(fileSize(proxy)).toBe(256 * 1024)

    out.resume()
    await done
  })

  it('resumes spilling when free space recovers', async () => {
    const disk = fakeDisk({ total: 100 * MiB, free: 5 * MiB })
    const data = randomBytes(2 * MiB)
    const proxy = new FSProxy({ dir: tmp.path, minFreeSpace: '10%' })
    const out = sink()
    out.pause()

    const done = pipeline(source(data), proxy, out.stream)
    await sleep(100)
    expect(proxy.filepath).toBeNull()

    disk.free = 50 * MiB
    await until(() => proxy.writableFinished)
    expect(fileSize(proxy)).toBeGreaterThan(MiB)

    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('still delivers data when the disk is already below the floor', async () => {
    fakeDisk({ total: 100 * MiB, free: MiB })
    const data = randomBytes(2 * MiB)
    const proxy = new FSProxy({ dir: tmp.path })
    const out = sink()
    out.pause()

    const done = pipeline(source(data), proxy, out.stream)
    await sleep(100)
    out.resume()
    await done

    expect(proxy.filepath).toBeNull()
    expect(out.received.equals(data)).toBe(true)
  })

  it("fails with FSProxyLimitError when onLimit is 'error'", async () => {
    fakeDisk({ total: 100 * MiB, free: MiB })
    const proxy = new FSProxy({ dir: tmp.path, onLimit: 'error' })
    const out = sink()
    out.pause()

    const error = await pipeline(source(randomBytes(MiB)), proxy, out.stream).catch((e) => e)
    expect(error).toBeInstanceOf(FSProxyLimitError)
    expect(error.message).toMatch(/minFreeSpace/)
  })

  it('is ignored when set to false', async () => {
    fakeDisk({ total: 100 * MiB, free: 0 })
    const proxy = new FSProxy({ dir: tmp.path, minFreeSpace: false })
    const out = sink()
    out.pause()

    const done = pipeline(source(randomBytes(MiB)), proxy, out.stream)
    await until(() => fileSize(proxy) === MiB)
    out.resume()
    await done
  })

  it("is ignored when free space can't be read", async () => {
    vi.mocked(statfs).mockRejectedValue(Object.assign(new Error('nope'), { code: 'ENOSYS' }))
    const proxy = new FSProxy({ dir: tmp.path })
    const out = sink()
    out.pause()

    const done = pipeline(source(randomBytes(MiB)), proxy, out.stream)
    await until(() => fileSize(proxy) === MiB)
    out.resume()
    await done
  })
})

describe('memory-backed filesystems', () => {
  it.runIf(process.platform === 'linux')('warns once per directory', async () => {
    fakeDisk({ total: 1000 * MiB, free: 1000 * MiB, type: TMPFS })
    const warnings: Error[] = []
    const onWarning = (warning: Error) => warnings.push(warning)
    process.on('warning', onWarning)

    try {
      for (let i = 0; i < 2; i++) {
        const out = sink()
        out.pause()
        const proxy = new FSProxy({ dir: tmp.path })
        const done = pipeline(source(randomBytes(256 * 1024)), proxy, out.stream)
        await until(() => proxy.writableFinished)
        out.resume()
        await done
      }
      await sleep(10)
    } finally {
      process.off('warning', onWarning)
    }

    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toMatchObject({ code: 'STREAM_FS_PROXY_TMPFS' })
  })
})

describe('option validation', () => {
  it.each([
    { minFreeSpace: '150%' as const },
    { minFreeSpace: 'lots' as never },
    { minFreeSpace: -1 },
    { minFreeSpace: 1.5 },
    { maxDiskBytes: 0 },
    { maxDiskBytes: Number.POSITIVE_INFINITY },
    { onLimit: 'drop' as never },
  ])('rejects %o', (options) => {
    expect(() => new FSProxy(options)).toThrow(RangeError)
  })

  it('accepts an empty stream with limits set', async () => {
    const out = sink()
    await pipeline(Readable.from([]), new FSProxy({ maxDiskBytes: 1 }), out.stream)
    expect(out.received.length).toBe(0)
  })
})
