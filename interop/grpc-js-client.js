// @grpc/grpc-js (an independent gRPC implementation) as the client of a Hydrangea server, using echo.proto.
const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const path = require('node:path');
const def = loader.loadSync(path.join(__dirname, 'echo.proto'), { keepCase: true, longs: Number, bytes: Buffer, defaults: true });
const pkg = grpc.loadPackageDefinition(def).hydrangea.test;
const port = process.argv[2];
const client = new pkg.Echo('127.0.0.1:' + port, grpc.credentials.createInsecure(), { 'grpc.max_receive_message_length': 16 << 20, 'grpc.max_send_message_length': 16 << 20 });
let failures = 0;
const check = (what, ok, detail) => { console.log((ok ? '  ok   ' : '  FAIL ') + what + (ok ? '' : '  -> ' + JSON.stringify(detail))); if (!ok) failures++; };
const unary = (req, opts = {}, meta = new grpc.Metadata()) => new Promise(res => client.Unary(req, meta, opts, (err, reply, ) => res({ err, reply })));

(async () => {
  console.log('grpc-js ' + require('@grpc/grpc-js/package.json').version + ' -> Hydrangea');
  let r = await unary({ text: 'hello', n: 7, blob: Buffer.from([1, 2, 3]) });
  check('unary echo', !r.err && r.reply.text === 'echo:hello' && r.reply.n === 7 && Buffer.compare(r.reply.blob, Buffer.from([1, 2, 3])) === 0, r);
  r = await unary({ text: 'FAIL:5:no such thing, 100% sure é' });
  check('error status carries code and message', r.err && r.err.code === grpc.status.NOT_FOUND && r.err.details === 'no such thing, 100% sure é', r.err && [r.err.code, r.err.details]);
  r = await unary({ text: 'THROW' });
  check('handler exception is UNKNOWN without its message', r.err && r.err.code === grpc.status.UNKNOWN && !String(r.err.details).includes('secret'), r.err && [r.err.code, r.err.details]);
  const md = new grpc.Metadata(); md.add('x-test', 'from-js');
  const trailers = await new Promise(res => { const call = client.Unary({ text: 'META' }, md, (e, reply) => {}); call.on('status', s => res(s.metadata)); });
  r = await unary({ text: 'META' }, {}, md);
  check('request metadata reaches the handler', !r.err && r.reply.text === 'from-js', r);
  check('trailing metadata reaches the client', trailers.get('x-trailer')[0] === 'yes', trailers.getMap());
  r = await unary({ text: 'SLOW:3000' }, { deadline: Date.now() + 150 });
  check('deadline -> DEADLINE_EXCEEDED', r.err && r.err.code === grpc.status.DEADLINE_EXCEEDED, r.err && r.err.code);
  await new Promise(r => setTimeout(r, 300));
  r = await unary({ text: 'CANCELLED?' });
  check('the server saw that call cancelled', Number(r.reply.text) >= 1, r.reply);

  const rows = await new Promise((res, rej) => { const got = []; const s = client.ServerStream({ text: 'row', n: 5 }); s.on('data', m => got.push(m.text)); s.on('end', () => res(got)); s.on('error', rej); });
  check('server streaming', rows.length === 5 && rows[4] === 'row-4', rows);
  const failed = await new Promise(res => { const got = []; const s = client.ServerStream({ text: 'FAILAFTER:2' }); s.on('data', m => got.push(m.n)); s.on('error', e => res({ e, got })); });
  check('stream fails after two messages', failed.got.length === 2 && failed.e.code === grpc.status.INTERNAL && failed.e.details === 'failed after 2', [failed.got, failed.e.code, failed.e.details]);

  const cs = await new Promise((res, rej) => { const call = client.ClientStream((e, reply) => e ? rej(e) : res(reply)); ['a', 'b', 'c'].forEach(t => call.write({ text: t })); call.end(); });
  check('client streaming', cs.text === 'a,b,c' && cs.n === 3, cs);
  const echoes = await new Promise((res, rej) => { const got = []; const call = client.Bidi(); call.on('data', m => got.push(m.text)); call.on('end', () => res(got)); call.on('error', rej); for (let i = 0; i < 100; i++) call.write({ text: 'm' + i }); call.end(); });
  check('bidirectional streaming (100 round trips)', echoes.length === 100 && echoes[99] === 'echo:m99', echoes.length);

  r = await unary({ text: 'BIG:3000000' });
  check('3 MB response', !r.err && r.reply.blob.length === 3000000, r.err);
  r = await unary({ text: 'echo', blob: Buffer.alloc(5_000_000) });
  check('5 MB request -> RESOURCE_EXHAUSTED', r.err && r.err.code === grpc.status.RESOURCE_EXHAUSTED, r.err && [r.err.code, r.err.details]);
  const gz = new grpc.Client('127.0.0.1:' + port, grpc.credentials.createInsecure(), { 'grpc.default_compression_algorithm': 2 });
  const gzc = new pkg.Echo('127.0.0.1:' + port, grpc.credentials.createInsecure(), { 'grpc.default_compression_algorithm': 2 });
  r = await new Promise(res => gzc.Unary({ text: 'compressed', blob: Buffer.alloc(200000) }, (err, reply) => res({ err, reply })));
  check('gzip-compressed request', !r.err && r.reply.text === 'echo:compressed' && r.reply.blob.length === 200000, r.err);

  const before = Number((await unary({ text: 'CANCELLED?' })).reply.text);
  await new Promise(res => { const s = client.ServerStream({ text: 'ENDLESS' }); let n = 0; s.on('data', () => { if (++n === 3) { s.cancel(); } }); s.on('error', () => res()); });
  await new Promise(r => setTimeout(r, 300));
  const after = Number((await unary({ text: 'CANCELLED?' })).reply.text);
  check('cancelling a stream reaches the handler', after === before + 1, [before, after]);

  console.log(failures === 0 ? 'ALL OK' : failures + ' FAILED');
  process.exit(failures === 0 ? 0 : 1);
})();
