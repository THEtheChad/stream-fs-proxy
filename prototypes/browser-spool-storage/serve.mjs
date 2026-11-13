// PROTOTYPE, throwaway. Serves index.html on localhost and, for `?auto` runs,
// writes the POSTed results to a file and exits.
//
//   node serve.mjs [port] [results.json]
//   then open http://localhost:<port>/?auto
import { readFile, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'

const port = Number(process.argv[2] ?? 8787)
const out = process.argv[3]
const page = new URL('./index.html', import.meta.url)

createServer(async (req, res) => {
  const { pathname } = new URL(req.url ?? '/', 'http://localhost')
  if (req.method === 'POST' && pathname === '/results') {
    let body = ''
    for await (const chunk of req) body += chunk
    res.end('ok')
    if (out) {
      await writeFile(out, body)
      console.log(`results written to ${out}`)
      setTimeout(() => process.exit(0), 100)
    } else {
      console.log(body)
    }
    return
  }
  if (pathname !== '/' && pathname !== '/index.html') {
    res.statusCode = 404
    return res.end()
  }
  res.setHeader('content-type', 'text/html; charset=utf-8')
  res.end(await readFile(page))
}).listen(port, '127.0.0.1', () => console.log(`http://localhost:${port}/`))
