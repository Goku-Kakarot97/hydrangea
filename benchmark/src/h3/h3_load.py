"""Closed-loop HTTP/3 load with aioquic: h3_load.py HOST PORT CONNS WINDOW SECONDS [PATH] -> prints completed requests (every response counts, status unchecked)."""
import asyncio, ssl, sys, time
from aioquic.asyncio import connect, QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import HeadersReceived, DataReceived
from aioquic.quic.configuration import QuicConfiguration

class P(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.h3 = H3Connection(self._quic); self.done = 0; self.waiter = None; self.path = "/plain"
    def quic_event_received(self, ev):
        for e in self.h3.handle_event(ev):
            if getattr(e, "stream_ended", False):
                self.done += 1
                self.fire()
    def fire(self):
        sid = self._quic.get_next_available_stream_id()
        self.h3.send_headers(sid, [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"localhost"), (b":path", self.path.encode())], end_stream=True)
        self.transmit()

async def one(host, port, window, secs, path, out):
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect(host, port, configuration=cfg, create_protocol=P) as c:
        c.path = path
        for _ in range(window): c.fire()
        t0 = time.time(); await asyncio.sleep(secs); out.append(c.done)

async def main():
    host = sys.argv[1]; port, conns, window, secs = map(int, sys.argv[2:6]); path = sys.argv[6] if len(sys.argv) > 6 else "/plain"
    out = []
    await asyncio.gather(*[one(host, port, window, secs, path, out) for _ in range(conns)])
    print(sum(out))
asyncio.run(main())
