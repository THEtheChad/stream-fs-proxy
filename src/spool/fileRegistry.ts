import { closeSync, unlinkSync } from 'node:fs'
import { type FileHandle, rm } from 'node:fs/promises'

/**
 * Spool files that haven't been cleaned up yet, by path. Holding the
 * FileHandle (not just its fd number) keeps garbage collection from closing
 * it behind our back, so the fd can't be reused by another file before we
 * close it.
 */
const openFiles = new Map<string, FileHandle>()
let exitHookInstalled = false

/** Removes the file of a spool that was garbage-collected without being destroyed. */
const abandoned = new FinalizationRegistry<string>((filepath) => {
  const file = openFiles.get(filepath)
  openFiles.delete(filepath)
  void Promise.resolve(file?.close())
    .catch(() => {})
    .then(() => rm(filepath, { force: true }))
    .catch(() => {})
})

/**
 * Tracks `filepath` until `releaseFile()` so it's deleted even if its stream
 * never is: when `owner` is garbage-collected, or when the process exits.
 * One shared `exit` listener covers every stream, so many concurrent spools
 * don't trip the max-listeners warning.
 */
export function trackFile(owner: object, filepath: string, file: FileHandle): void {
  if (!exitHookInstalled) {
    exitHookInstalled = true
    process.on('exit', removeAllSync)
  }
  openFiles.set(filepath, file)
  abandoned.register(owner, filepath, owner)
}

/** Stops tracking once the stream has closed and deleted the file itself. */
export function releaseFile(owner: object, filepath: string): void {
  openFiles.delete(filepath)
  abandoned.unregister(owner)
}

function removeAllSync() {
  for (const [filepath, file] of openFiles) {
    // Close first: Windows can't always delete a file that's still open.
    try {
      if (file.fd >= 0) closeSync(file.fd)
    } catch {}
    try {
      unlinkSync(filepath)
    } catch {}
  }
  openFiles.clear()
}
