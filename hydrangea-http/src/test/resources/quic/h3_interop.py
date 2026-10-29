"""HTTP/3 interop client for Http3InteropTest: aioquic's HTTP/3 stack talking to the Hydrangea server.
Usage: h3_interop.py PORT CASE   (prints OK <case> on success)."""
import asyncio, os, ssl, sys

from aioquic.asyncio import connect, QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated


class H3(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.h3 = H3Connection(self._quic)
        self.res = {}
        self.term = None

    def quic_event_received(self, event):
        if isinstance(event, ConnectionTerminated):
            self.term = event
            for r in self.res.values():
                if not r["fut"].done():
                    r["fut"].set_exception(RuntimeError("terminated %s %s" % (event.error_code, event.reason_phrase)))
        for e in self.h3.handle_event(event):
            r = self.res.get(getattr(e, "stream_id", -1))
            if r is None:
                continue
            if isinstance(e, HeadersReceived):
                r["headers"].extend(e.headers)
            elif isinstance(e, DataReceived):
                r["body"].extend(e.data)
            if getattr(e, "stream_ended", False) and not r["fut"].done():
                r["fut"].set_result(r)

    def request(self, method, path, headers=(), body=None, authority="localhost"):
        sid = self._quic.get_next_available_stream_id()
        fut = asyncio.get_event_loop().create_future()
        self.res[sid] = {"headers": [], "body": bytearray(), "fut": fut}
        self.h3.send_headers(sid, [(b":method", method.encode()), (b":scheme", b"https"), (b":authority", authority.encode()), (b":path", path.encode())]
                             + [(k.encode(), v.encode()) for k, v in headers], end_stream=body is None)
        if body is not None:
            self.h3.send_data(sid, body, end_stream=True)
        self.transmit()
        return fut


def status(r):
    return int(dict(r["headers"])[b":status"])


async def run(port, case):
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect("127.0.0.1", port, configuration=cfg, create_protocol=H3) as c:
        w = lambda f: asyncio.wait_for(f, 60)
        if case == "plain":
            r = await w(c.request("GET", "/plain"))
            assert status(r) == 200 and bytes(r["body"]) == b"hello", r
            assert dict(r["headers"])[b"content-length"] == b"5"
        elif case == "echo-small":
            r = await w(c.request("POST", "/echo", body=b"ping"))
            assert status(r) == 200 and bytes(r["body"]) == b"ping", r
        elif case == "echo-large":
            data = os.urandom(900_000)
            r = await w(c.request("POST", "/echo", body=data))
            assert status(r) == 200 and bytes(r["body"]) == data, (status(r), len(r["body"]))
        elif case == "big-response":
            r = await w(c.request("GET", "/big"))
            assert status(r) == 200 and len(r["body"]) == 3_000_000 and set(r["body"]) == {120}, len(r["body"])
        elif case == "concurrent":
            futs = [c.request("POST", "/echo", body=b"n%d" % i) for i in range(60)]
            for i, f in enumerate(futs):
                r = await w(f)
                assert bytes(r["body"]) == b"n%d" % i, (i, r["body"])
        elif case == "sequential":
            for i in range(250):   # past the initial stream limit: the server must keep granting streams
                r = await w(c.request("GET", "/plain"))
                assert status(r) == 200
        elif case == "not-found":
            r = await w(c.request("GET", "/nope"))
            assert status(r) == 404, r
        elif case == "headers":
            r = await w(c.request("GET", "/headers", headers=[("x-a", "1"), ("x-a", "2"), ("cookie", "a=1"), ("cookie", "b=2"), ("accept-encoding", "gzip, deflate, br"),
                                                               ("x-long", "v" * 2000)]))
            body = bytes(r["body"]).decode()
            assert status(r) == 200 and "cookie=a=1; b=2" in body and "host=localhost" in body and "x-long=" + "v" * 2000 in body and "x-a=1,2" in body, body[:300]
        elif case == "huge-headers":
            r = await w(c.request("GET", "/plain", headers=[("x-big%d" % i, "v" * 1200) for i in range(3)]))
            assert status(r) == 431, status(r)
        elif case == "too-large-body":
            r = await w(c.request("POST", "/echo", body=b"z" * 3_000_000))
            assert status(r) == 413, status(r)
        elif case == "handler-error":
            r = await w(c.request("GET", "/boom"))
            assert status(r) == 500, r
        elif case == "async":
            r = await w(c.request("GET", "/slow"))
            assert status(r) == 200 and bytes(r["body"]) == b"later", r
        else:
            raise SystemExit("unknown case " + case)
    print("OK", case)


asyncio.run(run(int(sys.argv[1]), sys.argv[2]))
