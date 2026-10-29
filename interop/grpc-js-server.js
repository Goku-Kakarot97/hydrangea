// The echo.proto service on @grpc/grpc-js (an independent implementation), for Hydrangea's client to talk to. Usage: node grpc-js-server.js port
const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const path = require('node:path');
const def = loader.loadSync(path.join(__dirname, 'echo.proto'), { keepCase: true, longs: Number, bytes: Buffer, defaults: true });
const pkg = grpc.loadPackageDefinition(def).hydrangea.test;
let cancelled = 0;
const server = new grpc.Server({ 'grpc.max_receive_message_length': 16 << 20, 'grpc.max_send_message_length': 16 << 20 });
server.addService(pkg.Echo.service, {
  Unary(call, cb) {
    const t = call.request.text;
    if (t.startsWith('FAIL:')) { const p = t.split(':'); return cb({ code: Number(p[1]), details: p.slice(2).join(':') }); }
    if (t === 'THROW') throw new Error('secret internal detail');
    if (t === 'CANCELLED?') return cb(null, { text: String(cancelled) });
    if (t.startsWith('SLOW:')) { call.on('cancelled', () => { cancelled++; }); return setTimeout(() => cb(null, { text: 'slow done' }), Number(t.slice(5))); }
    if (t === 'META') { const trailers = new grpc.Metadata(); trailers.add('x-trailer', 'yes'); const v = call.metadata.get('x-test')[0]; return cb(null, { text: String(v) }, trailers); }
    if (t.startsWith('BIG:')) return cb(null, { text: 'big', blob: Buffer.alloc(Number(t.slice(4))) });
    cb(null, { text: 'echo:' + t, n: call.request.n, blob: call.request.blob });
  },
  ServerStream(call) {
    const t = call.request.text;
    if (t === 'ENDLESS') { let i = 0; const timer = setInterval(() => call.write({ text: 'tick', n: i++ }), 10); call.on('cancelled', () => { cancelled++; clearInterval(timer); }); return; }
    if (t.startsWith('FAILAFTER:')) { const k = Number(t.slice(10)); for (let i = 0; i < k; i++) call.write({ text: 'm', n: i }); return call.emit('error', { code: grpc.status.INTERNAL, details: 'failed after ' + k }); }
    for (let i = 0; i < Math.max(0, call.request.n); i++) call.write({ text: t + '-' + i, n: i, blob: call.request.blob });
    call.end();
  },
  ClientStream(call, cb) { const texts = []; call.on('data', m => texts.push(m.text)); call.on('end', () => cb(null, { text: texts.join(','), n: texts.length })); call.on('cancelled', () => cancelled++); },
  Bidi(call) { call.on('data', m => call.write({ text: 'echo:' + m.text, n: m.n, blob: m.blob })); call.on('end', () => call.end()); call.on('cancelled', () => cancelled++); },
});
server.bindAsync('127.0.0.1:' + process.argv[2], grpc.ServerCredentials.createInsecure(), (err, port) => { if (err) throw err; console.log('READY ' + port); });
