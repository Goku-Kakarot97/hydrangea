const http2 = require('node:http2');
const http = require('node:http');
const zlib = require('node:zlib');
const fs = require('node:fs');
const crypto = require('node:crypto');
const mode = process.argv[2]; const port = parseInt(process.argv[3]);
const big = Buffer.alloc(8 * 1024 * 1024, 0x61);
function handler(req, res) {
  const url = new URL(req.url, 'http://x');
  const done = (code, body, headers = {}) => { res.writeHead(code, { 'content-type': 'text/plain', ...headers }); res.end(body); };
  if (url.pathname === '/hello') return done(200, 'hello from node ' + req.httpVersion);
  if (url.pathname === '/big') { res.writeHead(200, { 'content-length': big.length }); return res.end(big); }
  if (url.pathname === '/echo') { const chunks = []; req.on('data', c => chunks.push(c)); req.on('end', () => { const b = Buffer.concat(chunks); res.writeHead(200, { 'content-length': b.length, 'x-sha': crypto.createHash('sha256').update(b).digest('hex') }); res.end(b); }); return; }
  if (url.pathname === '/redirect') return done(302, '', { location: '/hello' });
  if (url.pathname === '/sse') { res.writeHead(200, { 'content-type': 'text/event-stream' }); let n = 0; const t = setInterval(() => { res.write('data: tick ' + (n++) + '\n\n'); if (n >= 5) { clearInterval(t); res.end(); } }, 50); res.on('close', () => clearInterval(t)); return; }
  if (url.pathname === '/slow') return setTimeout(() => done(200, 'slow done'), 1500);
  if (url.pathname === '/headers') { const h = {}; for (const k of Object.keys(req.headers)) h[k] = req.headers[k]; return done(200, JSON.stringify(h)); }
  if (url.pathname === '/gz') { const z = zlib.gzipSync(Buffer.from('hydrangea-'.repeat(100000))); res.writeHead(200, { 'content-encoding': 'gzip', 'content-type': 'text/plain', 'content-length': z.length }); return res.end(z); }
  if (url.pathname === '/deflate') { const z = zlib.deflateSync(Buffer.from('hydrangea-'.repeat(1000))); res.writeHead(200, { 'content-encoding': 'deflate', 'content-length': z.length }); return res.end(z); }
  if (url.pathname === '/ae') return done(200, String(req.headers['accept-encoding']));
  if (url.pathname === '/cookie') { res.writeHead(200, { 'set-cookie': ['session=abc; Path=/; HttpOnly', 'theme=dark; Path=/; Max-Age=3600'], 'content-type': 'text/plain' }); return res.end('cookies set'); }
  if (url.pathname === '/show') return done(200, 'cookie=' + req.headers['cookie']);
  if (url.pathname === '/reset') { req.stream.close(http2.constants.NGHTTP2_REFUSED_STREAM); return; }
  done(404, 'not found');
}
let server;
if (mode === 'h1') {
  server = http.createServer(handler);
} else if (mode === 'tls') {
  server = http2.createSecureServer({ key: fs.readFileSync(process.argv[4]), cert: fs.readFileSync(process.argv[5]), allowHTTP1: true }, handler);
} else {
  server = http2.createServer({ settings: { maxConcurrentStreams: 50 } }, handler);
}
server.listen(port, '127.0.0.1', () => console.log('READY'));
