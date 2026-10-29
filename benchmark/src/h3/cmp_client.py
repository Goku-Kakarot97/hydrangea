"""Same-client HTTP/2 vs HTTP/3 comparison (needs aioquic and h2).
  cmp_client.py h2|h3 HOST PORT streams STREAMS rounds ROUNDS size BYTES   concurrent downloads of /download/BYTES on ONE connection, ROUNDS times
  cmp_client.py h2|h3 HOST PORT connect TRIALS                               fresh connection + one GET /plain, TRIALS times
Prints one JSON line. h2 is TLS 1.3 over TCP (ALPN h2) with 1 MiB stream and connection windows; h3 is aioquic with its default 1 MiB windows."""
import asyncio, json, ssl, sys, time

import h2.config, h2.connection, h2.events, h2.settings
from aioquic.asyncio import connect, QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration

WINDOW = 1 << 20


class H2Client(asyncio.Protocol):
    def __init__(self, host):
        self.host = host
        self.conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True, header_encoding="utf-8"))
        self.futs, self.got, self.transport = {}, {}, None
        self.ready = asyncio.get_event_loop().create_future()

    def connection_made(self, t):
        self.transport = t
        self.conn.initiate_connection()
        self.conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: WINDOW})
        self.conn.increment_flow_control_window(WINDOW - 65535)
        t.write(self.conn.data_to_send())
        self.ready.set_result(None)

    def data_received(self, data):
        for e in self.conn.receive_data(data):
            if isinstance(e, h2.events.DataReceived):
                self.got[e.stream_id] = self.got.get(e.stream_id, 0) + len(e.data)
                self.conn.acknowledge_received_data(e.flow_controlled_length, e.stream_id)
            elif isinstance(e, h2.events.StreamEnded):
                f = self.futs.get(e.stream_id)
                if f and not f.done(): f.set_result(self.got.get(e.stream_id, 0))
            elif isinstance(e, h2.events.ConnectionTerminated):
                for f in self.futs.values():
                    if not f.done(): f.set_exception(RuntimeError("goaway"))
        self.transport.write(self.conn.data_to_send())

    def connection_lost(self, exc):
        for f in self.futs.values():
            if not f.done(): f.set_exception(ConnectionError("lost"))

    def get(self, path):
        sid = self.conn.get_next_available_stream_id()
        self.conn.send_headers(sid, [(":method", "GET"), (":scheme", "https"), (":authority", self.host), (":path", path)], end_stream=True)
        f = asyncio.get_event_loop().create_future()
        self.futs[sid] = f
        self.transport.write(self.conn.data_to_send())
        return f


class H3Client(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.h3 = H3Connection(self._quic)
        self.futs, self.got = {}, {}

    def quic_event_received(self, ev):
        for e in self.h3.handle_event(ev):
            if isinstance(e, DataReceived):
                self.got[e.stream_id] = self.got.get(e.stream_id, 0) + len(e.data)
            if getattr(e, "stream_ended", False):
                f = self.futs.get(e.stream_id)
                if f and not f.done(): f.set_result(self.got.get(e.stream_id, 0))

    def get(self, path):
        sid = self._quic.get_next_available_stream_id()
        self.h3.send_headers(sid, [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"localhost"), (b":path", path.encode())], end_stream=True)
        f = asyncio.get_event_loop().create_future()
        self.futs[sid] = f
        self.transmit()
        return f


def _ctx():
    ctx = ssl.create_default_context()   # built once: loading the CA store per connection would add tens of ms to every h2 connect
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_alpn_protocols(["h2"])
    return ctx


class Session:
    CTX = _ctx()

    """One connection of either protocol with a common get(path) -> awaitable of the body size."""
    def __init__(self, mode, host, port):
        self.mode, self.host, self.port = mode, host, port

    async def __aenter__(self):
        if self.mode == "h2":
            ctx = Session.CTX
            loop = asyncio.get_event_loop()
            self.t, self.p = await loop.create_connection(lambda: H2Client(self.host), self.host, self.port, ssl=ctx, server_hostname="localhost")
            await self.p.ready
        else:
            cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, verify_mode=ssl.CERT_NONE, server_name="localhost", idle_timeout=60)
            self.cm = connect(self.host, self.port, configuration=cfg, create_protocol=H3Client)
            self.p = await self.cm.__aenter__()
            await self.p.wait_connected()
        return self

    async def __aexit__(self, *x):
        if self.mode == "h2": self.t.close()
        else: await self.cm.__aexit__(*x)

    def get(self, path):
        return self.p.get(path)


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


async def timed(s, path, out, size):
    t0 = time.perf_counter()
    n = await asyncio.wait_for(s.get(path), 120)
    assert n == size, (n, size)
    out.append((time.perf_counter() - t0) * 1000)


async def streams(mode, host, port, nstreams, rounds, size):
    lat, rounds_ms, t_all = [], [], time.perf_counter()
    async with Session(mode, host, port) as s:
        for _ in range(rounds):
            r0 = time.perf_counter()
            await asyncio.gather(*[timed(s, "/download/%d" % size, lat, size) for _ in range(nstreams)])
            rounds_ms.append((time.perf_counter() - r0) * 1000)
    total = time.perf_counter() - t_all
    return {"mode": mode, "streams": nstreams, "rounds": rounds, "bytes": size, "p50_ms": round(pct(lat, .5)), "p90_ms": round(pct(lat, .9)), "p99_ms": round(pct(lat, .99)),
            "max_ms": round(max(lat)), "round_mean_ms": round(sum(rounds_ms) / len(rounds_ms)), "MBps": round(nstreams * rounds * size / total / 1e6, 2)}


async def fresh(mode, host, port, trials):
    lat = []
    for _ in range(trials):
        t0 = time.perf_counter()
        try:
            async with Session(mode, host, port) as s:
                await asyncio.wait_for(s.get("/plain"), 30)
                lat.append((time.perf_counter() - t0) * 1000)   # up to the response; closing a QUIC connection waits out its draining period and is not part of "connect"
        except Exception:
            lat.append(30000)
    return {"mode": mode, "connect_trials": trials, "p50_ms": round(pct(lat, .5)), "p90_ms": round(pct(lat, .9)), "max_ms": round(max(lat))}


a = sys.argv
mode, host, port = a[1], a[2], int(a[3])
if a[4] == "streams":
    r = asyncio.run(streams(mode, host, port, int(a[5]), int(a[7]), int(a[9])))
else:
    r = asyncio.run(fresh(mode, host, port, int(a[5])))
print(json.dumps(r))
