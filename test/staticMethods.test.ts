import { randomBytes } from 'node:crypto'
import { readdir } from 'node:fs/promises'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { describe, expect, it } from 'vitest'
import { FSProxy } from '../src/index.ts'
import { closed, sink, source, useTempDir } from './helpers.ts'

const tmp = useTempDir()

describe('FSProxy.from()', () => {
  it('returns an FSProxy, not the plain Duplex that Duplex.from() would', () => {
    expect(FSProxy.from([Buffer.from('x')])).toBeInstanceOf(FSProxy)
  })

  it('reads a Node readable without holding it back', async () => {
    const data = randomBytes(2 * 1024 * 1024)
    const proxy = FSProxy.from(source(data), { dir: tmp.path })
    const out = sink()
    out.pause()
    const done = pipeline(proxy, out.stream)

    // The source is fully accepted while the consumer is stalled.
    await new Promise((resolve) => proxy.on('finish', resolve))
    expect(proxy.filepath).not.toBeNull()
    out.resume()
    await done
    expect(out.received.equals(data)).toBe(true)
  })

  it('reads a web ReadableStream', async () => {
    const data = randomBytes(256 * 1024)
    const web = new ReadableStream({
      start(controller) {
        for (let i = 0; i < data.length; i += 10_000) controller.enqueue(data.subarray(i, i + 10_000))
        controller.close()
      },
    })
    const out = sink()
    await pipeline(FSProxy.from(web, { dir: tmp.path }), out.stream)
    expect(out.received.equals(data)).toBe(true)
  })

  it('reads sync and async iterables, including strings', async () => {
    async function* chunks() {
      yield 'héllo '
      yield Buffer.from('wörld')
    }
    const fromAsync = sink()
    const fromSync = sink()
    await pipeline(FSProxy.from(chunks(), { dir: tmp.path }), fromAsync.stream)
    await pipeline(FSProxy.from(['a', 'b', 'c'], { dir: tmp.path }), fromSync.stream)
    expect(fromAsync.received.toString()).toBe('héllo wörld')
    expect(fromSync.received.toString()).toBe('abc')
  })

  it('passes source errors to the proxy and cleans up', async () => {
    let sent = 0
    const failing = new Readable({
      read() {
        if (sent++ < 10) this.push(randomBytes(64 * 1024))
        else this.destroy(new Error('socket reset'))
      },
    })
    const proxy = FSProxy.from(failing, { dir: tmp.path })
    const out = sink()
    out.pause()

    await expect(pipeline(proxy, out.stream)).rejects.toThrow('socket reset')
    await closed(proxy)
    expect(await readdir(tmp.path)).toEqual([])
  })

  it('destroys the source when the proxy is destroyed', async () => {
    const src = new Readable({ read() {} })
    const proxy = FSProxy.from(src, { dir: tmp.path })
    proxy.destroy()
    await new Promise((resolve) => src.once('close', resolve))
    expect(src.destroyed).toBe(true)
  })
})

describe('FSProxy.toWeb()', () => {
  it('works as a TransformStream-shaped pair for pipeThrough()', async () => {
    const data = randomBytes(512 * 1024)
    const web = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let i = 0; i < data.length; i += 32_768) controller.enqueue(data.subarray(i, i + 32_768))
        controller.close()
      },
    })

    const chunks: Uint8Array[] = []
    await web
      .pipeThrough(FSProxy.toWeb(new FSProxy({ dir: tmp.path })))
      .pipeTo(new WritableStream({ write: (chunk) => void chunks.push(chunk) }))

    expect(Buffer.concat(chunks).equals(data)).toBe(true)
  })
})
