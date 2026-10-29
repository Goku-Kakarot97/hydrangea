// A gRPC-Web client written against the protocol with Node's fetch (undici, HTTP/1.1): an independent reader of Hydrangea's chunked
// gRPC-Web responses and CORS preflight. Usage: node grpc-web-client.js port
const port = process.argv[2];
const base = `http://127.0.0.1:${port}/hydrangea.test.Echo/`;
let failures = 0;
const check = (what, ok, detail) => { console.log((ok ? '  ok   ' : '  FAIL ') + what + (ok ? '' : '  -> ' + JSON.stringify(detail))); if (!ok) failures++; };
const varint = n => { const b = []; while (n > 127) { b.push((n & 127) | 128); n >>>= 7; } b.push(n); return Buffer.from(b); };
const msg = (text, n = 0) => { const t = Buffer.from(text); const parts = [Buffer.from([0x0a]), varint(t.length), t]; if (n) parts.push(Buffer.from([0x10]), varint(n)); return Buffer.concat(parts); };
const frame = (payload, flag = 0) => { const h = Buffer.alloc(5); h[0] = flag; h.writeUInt32BE(payload.length, 1); return Buffer.concat([h, payload]); };
const readText = p => { let i = 0; while (i < p.length) { const tag = p[i++]; const wire = tag & 7; if (tag >> 3 === 1) { let len = 0, shift = 0; while (p[i] & 128) { len |= (p[i++] & 127) << shift; shift += 7; } len |= p[i++] << shift; return p.slice(i, i + len).toString(); } else { throw new Error('unexpected field'); } } return ''; };
async function call(method, body, headers = {}) {
  const res = await fetch(base + method, { method: 'POST', headers: { 'content-type': 'application/grpc-web+proto', 'x-grpc-web': '1', ...headers }, body });
  const buf = Buffer.from(await res.arrayBuffer());
  const messages = []; let trailers = {};
  for (let i = 0; i + 5 <= buf.length;) {
    const flag = buf[i], len = buf.readUInt32BE(i + 1), p = buf.slice(i + 5, i + 5 + len); i += 5 + len;
    if (flag & 0x80) for (const line of p.toString('latin1').split('\r\n').filter(Boolean)) { const k = line.indexOf(': '); trailers[line.slice(0, k)] = line.slice(k + 2); }
    else messages.push(readText(p));
  }
  return { res, messages, trailers };
}
(async () => {
  let r = await call('Unary', frame(msg('hello')));
  check('unary: status 200, content-type, one message, status 0', r.res.status === 200 && r.res.headers.get('content-type') === 'application/grpc-web+proto' && r.messages[0] === 'echo:hello' && r.trailers['grpc-status'] === '0', { s: r.res.status, m: r.messages, t: r.trailers });
  r = await call('Unary', frame(msg('FAIL:7:denied')));
  check('error status in the trailer frame', r.trailers['grpc-status'] === '7' && r.trailers['grpc-message'] === 'denied', r.trailers);
  r = await call('ServerStream', frame(msg('row', 20)));
  check('server streaming: 20 messages in order', r.messages.length === 20 && r.messages[19] === 'row-19' && r.trailers['grpc-status'] === '0', r.messages.length);
  r = await call('Unary', frame(msg('BIG:300000')));
  check('300 kB response survives chunking', r.trailers['grpc-status'] === '0', r.trailers);
  r = await call('Nope', frame(msg('x')));
  check('unknown method: Trailers-Only with UNIMPLEMENTED', r.res.headers.get('grpc-status') === '12', Object.fromEntries(r.res.headers));
  const pre = await fetch(base + 'Unary', { method: 'OPTIONS', headers: { origin: 'https://app.example', 'access-control-request-method': 'POST', 'access-control-request-headers': 'content-type,x-grpc-web' } });
  check('CORS preflight', pre.status === 204 && pre.headers.get('access-control-allow-origin') === 'https://app.example', Object.fromEntries(pre.headers));
  console.log(failures === 0 ? 'ALL OK' : failures + ' FAILED'); process.exit(failures ? 1 : 0);
})().catch(e => { console.log('ERROR', e); process.exit(1); });
