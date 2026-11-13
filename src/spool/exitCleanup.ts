import { closeSync, unlinkSync } from 'node:fs'

/** Cache files that are still open, by path, with their file descriptors. */
const liveFiles = new Map<string, number>()
let hookInstalled = false

/**
 * Deletes `filepath` if the process exits before the stream cleans it up.
 * One shared `exit` listener covers every stream, so many concurrent
 * spools don't trip the max-listeners warning.
 */
export function removeOnExit(filepath: string, fd: number): void {
  if (!hookInstalled) {
    hookInstalled = true
    process.on('exit', removeAllSync)
  }
  liveFiles.set(filepath, fd)
}

export function forgetOnExit(filepath: string): void {
  liveFiles.delete(filepath)
}

function removeAllSync() {
  for (const [filepath, fd] of liveFiles) {
    // Close first: Windows can't delete a file that's still open.
    try {
      closeSync(fd)
    } catch {}
    try {
      unlinkSync(filepath)
    } catch {}
  }
  liveFiles.clear()
}
