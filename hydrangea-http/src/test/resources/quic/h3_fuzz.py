"""Hostile HTTP/3 for Http3FuzzTest. Usage: h3_fuzz.py PORT SEED ROUNDS
Each round opens a real QUIC connection (ALPN h3) and writes random or mutated HTTP/3 frames, QPACK blocks and stream types on request and unidirectional
streams, with and without FIN. At the end a normal HTTP/3 request must still be answered. Prints OK on success."""
import asyncio, os, random, ssl, struct, sys

from aioquic.asyncio import connect, QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3_ALPN
from aioquic.h3.events import HeadersReceived
from aioquic.quic.configuration import QuicConfiguration


def vi(v):
    if v < 1 << 6: return bytes([v])
    if v < 1 << 14: return struct.pack(">H", 0x4000 | v)
    if v < 1 << 30: return struct.pack(">I", 0x80000000 | v)
    return struct.pack(">Q", 0xC000000000000000 | v)


def frame(t, payload):
    return vi(t) + vi(len(payload)) + payload


def lit(name, value):
    return bytes([0x20 | len(name)]) + name + bytes([len(value)]) + value


def block(r):
    """A QPACK field section, valid or nearly."""
    b = bytearray(b"\x00\x00")
    for part in r.sample([b"\xd1", b"\xd7", b"\xc1", b"\x50\x09localhost", lit(b"x-a", b"1"), lit(b"cookie", b"a=b"), b"\xc0|\x19", b"\x50\x05h\x00\x00\x00\x00",
                          b"\x5f\x1d\x03abc", b"\xff\x20", lit(b"X-Upper", b"v"), lit(b"connection", b"close"), lit(b"te", b"gzip"), b"\x27\x01\x61\x01\x62",
                          bytes([0x50, 0x82, 0xf1, 0xe3]), os.urandom(r.randint(1, 8))], r.randint(1, 8)):
        b += part
    if r.random() < .15:
        b[0] = r.getrandbits(8)
    return bytes(b)


def good_head(r):
    return frame(1, b"\x00\x00\xd1\xd7\xc1\x50\x09localhost")


def mutate(r, b):
    b = bytearray(b)
    for _ in range(r.randint(0, 3)):
        k = r.random()
        if k < .4 and b: b[r.randrange(len(b))] = r.getrandbits(8)
        elif k < .6 and b: b[r.randrange(len(b))] ^= 1 << r.randrange(8)
        elif k < .8 and b: del b[r.randrange(len(b)):]
        else: b += os.urandom(r.randint(1, 6))
    return bytes(b)


def request_bytes(r):
    parts = []
    for _ in range(r.randint(1, 5)):
        k = r.random()
        if k < .3: parts.append(good_head(r))
        elif k < .5: parts.append(frame(1, block(r)))
        elif k < .7: parts.append(frame(0, os.urandom(r.randint(0, 50))))
        elif k < .8: parts.append(frame(r.choice([2, 3, 4, 5, 6, 7, 8, 9, 0xd, 0x21, 0x40, 1 << 40, r.getrandbits(30)]), os.urandom(r.randint(0, 10))))
        elif k < .9: parts.append(vi(r.choice([0, 1])) + vi(r.choice([0, 1 << 30, (1 << 62) - 1, 100000])) + os.urandom(r.randint(0, 5)))
        else: parts.append(os.urandom(r.randint(1, 20)))
    return mutate(r, b"".join(parts)) if r.random() < .5 else b"".join(parts)


def uni_bytes(r):
    t = r.choice([0, 0, 0, 2, 3, 1, 0x21, 0x40 | 0x1f, r.getrandbits(8)])
    body = bytearray()
    if t == 0:
        settings = b"".join(vi(k) + vi(v) for k, v in r.sample([(1, 0), (6, 8192), (7, 0), (0x21, 1), (2, 0), (1, 4096), (5, 1), (r.getrandbits(8), r.getrandbits(8))], r.randint(0, 4)))
        body += frame(4, settings) if r.random() < .7 else b""
        for _ in range(r.randint(0, 3)):
            body += frame(r.choice([7, 0xd, 3, 4, 0, 1, 5, 0x21]), os.urandom(r.randint(0, 9)))
    elif t == 2:
        body += bytes([r.choice([0x20, 0x3f, 0xc0, 0x80, 0x00, 0x40])]) + os.urandom(r.randint(0, 6))
    elif t == 3:
        body += os.urandom(r.randint(0, 10))
    else:
        body += os.urandom(r.randint(0, 30))
    return vi(t) + mutate(r, bytes(body))


async def round_(port, r):
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect("127.0.0.1", port, configuration=cfg) as c:
        q = c._quic
        await c.wait_connected()
        for _ in range(r.randint(5, 25)):
            if r.random() < .3:
                sid = q.get_next_available_stream_id(is_unidirectional=True)
                q.send_stream_data(sid, uni_bytes(r), end_stream=r.random() < .1)
            else:
                sid = q.get_next_available_stream_id()
                data = request_bytes(r)
                if r.random() < .2 and len(data) > 1:
                    cut = r.randrange(1, len(data))
                    q.send_stream_data(sid, data[:cut], end_stream=False)
                    c.transmit()
                    await asyncio.sleep(0.003)
                    data = data[cut:]
                q.send_stream_data(sid, data, end_stream=r.random() < .8)
            if r.random() < .3:
                c.transmit()
        c.transmit()
        await asyncio.sleep(0.15)


class Normal(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.h3 = H3Connection(self._quic)
        self.fut = asyncio.get_event_loop().create_future()

    def quic_event_received(self, event):
        for e in self.h3.handle_event(event):
            if isinstance(e, HeadersReceived) and not self.fut.done():
                self.fut.set_result(dict(e.headers))


async def main(port, seed, rounds):
    r = random.Random(seed)
    for _ in range(rounds):
        try:
            await asyncio.wait_for(round_(port, r), 20)
        except (ConnectionError, asyncio.TimeoutError, OSError, AssertionError, ValueError):
            pass    # the server may close on a protocol error; that is a correct answer to many of these
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect("127.0.0.1", port, configuration=cfg, create_protocol=Normal) as c:
        sid = c._quic.get_next_available_stream_id()
        c.h3.send_headers(sid, [(b":method", b"GET"), (b":scheme", b"https"), (b":authority", b"localhost"), (b":path", b"/plain")], end_stream=True)
        c.transmit()
        h = await asyncio.wait_for(c.fut, 10)
        assert h[b":status"] == b"200", h
    print("OK")


asyncio.run(main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])))
