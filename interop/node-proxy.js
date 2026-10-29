// An independent forward proxy: Node's http server + net, CONNECT tunnels and absolute-form forwarding, Basic auth.
const http = require('node:http'); const net = require('node:net'); const { URL } = require('node:url');
const port = parseInt(process.argv[2]); const expected = process.argv[3] ? 'Basic ' + Buffer.from(process.argv[3]).toString('base64') : null;
let log = [];
function authorized(req) { return !expected || req.headers['proxy-authorization'] === expected; }
const server = http.createServer((req, res) => {
  log.push(req.method + ' ' + req.url);
  if (!authorized(req)) { res.writeHead(407, { 'Proxy-Authenticate': 'Basic realm="n"', 'Content-Length': 0 }); return res.end(); }
  const u = new URL(req.url);
  const headers = { ...req.headers }; delete headers['proxy-authorization']; delete headers['proxy-connection'];
  const up = http.request({ host: u.hostname, port: u.port || 80, path: u.pathname + u.search, method: req.method, headers }, r => { res.writeHead(r.statusCode, r.headers); r.pipe(res); });
  up.on('error', e => { res.writeHead(502); res.end(String(e)); });
  req.pipe(up);
});
server.on('connect', (req, clientSocket, head) => {
  log.push('CONNECT ' + req.url);
  if (!authorized(req)) { clientSocket.end('HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="n"\r\nContent-Length: 0\r\n\r\n'); return; }
  const [host, p] = req.url.split(':');
  const s = net.connect(parseInt(p), host, () => { clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n'); s.write(head); s.pipe(clientSocket); clientSocket.pipe(s); });
  s.on('error', () => clientSocket.destroy()); clientSocket.on('error', () => s.destroy());
});
server.on('request', () => {});
process.on('SIGUSR2', () => { console.log('LOG ' + JSON.stringify(log)); });
server.listen(port, '127.0.0.1', () => console.log('READY'));
