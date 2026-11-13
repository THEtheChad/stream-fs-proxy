import { statfs } from 'node:fs/promises'

/** Free space to leave on the cache file's filesystem: a percentage, bytes, or `false` for no floor. */
export type MinFreeSpace = `${number}%` | number | false

export type FreeSpaceFloor = { percent: number } | { bytes: number } | null

export function parseMinFreeSpace(value: MinFreeSpace): FreeSpaceFloor {
  if (value === false) return null

  if (typeof value === 'number') {
    if (!Number.isInteger(value) || value < 0) {
      throw new RangeError(
        `stream-fs-proxy: minFreeSpace must be a non-negative integer of bytes, a percentage like '10%', or false, got ${value}`,
      )
    }
    return { bytes: value }
  }

  const match = /^(\d+(?:\.\d+)?)%$/.exec(value)
  const percent = match ? Number(match[1]) : Number.NaN
  if (!(percent >= 0 && percent <= 100)) {
    throw new RangeError(
      `stream-fs-proxy: minFreeSpace must be a percentage between '0%' and '100%', got '${value}'`,
    )
  }
  return { percent }
}

/**
 * How many more bytes can be written to `dir` before free space drops below
 * `floor`. `Infinity` when there is no floor or free space can't be read.
 */
export async function freeSpaceBudget(dir: string, floor: FreeSpaceFloor): Promise<number> {
  if (!floor) return Number.POSITIVE_INFINITY
  try {
    const { bavail, blocks, bsize } = await statfs(dir)
    const reserve = 'bytes' in floor ? floor.bytes : (floor.percent / 100) * blocks * bsize
    return Math.floor(bavail * bsize - reserve)
  } catch {
    return Number.POSITIVE_INFINITY
  }
}

const TMPFS_MAGIC = 0x01021994
const RAMFS_MAGIC = 0x858458f6
const warnedDirs = new Set<string>()

/** Warns once per directory when "disk" is really memory (tmpfs/ramfs). */
export async function warnIfMemoryBacked(dir: string): Promise<void> {
  if (process.platform !== 'linux' || warnedDirs.has(dir)) return
  try {
    const { type } = await statfs(dir)
    if (type !== TMPFS_MAGIC && type !== RAMFS_MAGIC) return
  } catch {
    return
  }
  warnedDirs.add(dir)
  process.emitWarning(
    `${dir} is a memory-backed filesystem, so data stream-fs-proxy spills there still uses RAM. Pass a 'dir' on a real disk to keep memory bounded.`,
    { code: 'STREAM_FS_PROXY_TMPFS' },
  )
}
