const http2 = require('node:http2');
const crypto = require('node:crypto');
const port = process.argv[2];
const session = http2.connect('http://127.0.0.1:' + port);
function maskedFrame(opcode, payload) {
  const mask = crypto.randomBytes(4); const body = Buffer.from(payload);
  const out = Buffer.alloc(6 + body.length); out[0] = 0x80 | opcode; out[1] = 0x80 | body.length; mask.copy(out, 2);
  for (let i = 0; i < body.length; i++) out[6 + i] = body[i] ^ mask[i & 3]; return out;
}
session.on('error', e => { console.log('session error', e.message); process.exit(1); });
session.once('remoteSettings', settings => {
  console.log('server enableConnectProtocol =', settings.enableConnectProtocol);
  const stream = session.request({ ':method': 'CONNECT', ':protocol': 'websocket', ':scheme': 'http', ':path': '/ws', ':authority': '127.0.0.1:' + port, 'sec-websocket-version': '13' });
  stream.on('response', headers => {
    console.log('response :status =', headers[':status']);
    stream.write(maskedFrame(1, 'hello'));
    stream.write(maskedFrame(2, 'binary!'));
  });
  let got = Buffer.alloc(0), step = 0;
  stream.on('data', chunk => {
    got = Buffer.concat([got, chunk]);
    while (got.length >= 2) {
      const len = got[1] & 0x7f; if (got.length < 2 + len) break;
      const op = got[0] & 0x0f; const text = got.subarray(2, 2 + len).toString();
      console.log('frame opcode=' + op + ' payload=' + text); got = got.subarray(2 + len); step++;
      if (step === 2) stream.write(maskedFrame(8, Buffer.from([0x03, 0xe8])));
      if (op === 8) { console.log('server answered with close; ending'); stream.end(); }
    }
  });
  stream.on('close', () => { console.log('stream closed, rst code =', stream.rstCode); session.close(); setTimeout(() => process.exit(0), 100); });
});
