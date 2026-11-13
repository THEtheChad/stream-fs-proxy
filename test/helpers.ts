import { statSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'
import { afterEach, beforeEach } from 'vitest'
import type { FSProxy } from '../src/index.ts'

/** A fresh temp directory per test; read `.path` inside the test. */
export function useTempDir(): { readonly path: string } {
  const state = { path: '' }
  beforeEach(async () => {
    state.path = await mkdtemp(join(tmpdir(), 'fs-proxy-test-'))
  })
  afterEach(async () => {
    await rm(state.path, { recursive: true, force: true })
  })
  return state
}

/** Emits `data` in chunks of the given sizes (cycled), as fast as it is pulled. */
export function source(data: Buffer, sizes: number[] = [64 * 1024]): Readable {
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

export interface Sink {
  stream: Writable
  chunks: Buffer[]
  readonly received: Buffer
  pause(): void
  resume(): void
}

/** A consumer that records what it gets and can be paused. */
export function sink(): Sink {
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
      if (paused) return
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

export async function until(condition: () => boolean, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('until(): condition not met in time')
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

export function closed(proxy: FSProxy): Promise<void> {
  return new Promise((resolve) => (proxy.closed ? resolve() : proxy.once('close', () => resolve())))
}

export function fileSize(proxy: FSProxy): number {
  try {
    return proxy.filepath ? statSync(proxy.filepath).size : 0
  } catch {
    return 0
  }
}

/** Samples the cache file's size every millisecond; `stop()` returns the largest seen. */
export function watchFileSize(proxy: FSProxy): { stop(): number } {
  let peak = 0
  const timer = setInterval(() => {
    peak = Math.max(peak, fileSize(proxy))
  }, 1)
  return {
    stop() {
      clearInterval(timer)
      return Math.max(peak, fileSize(proxy))
    },
  }
}
