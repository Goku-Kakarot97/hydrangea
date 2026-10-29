"""Hostile 1-RTT frames for QuicFrame1RttFuzzTest. Usage: quic_fuzz_1rtt.py PORT SEED ROUNDS PACKETS
Each round completes a real handshake with aioquic, then injects packets that are correctly protected with the connection's own keys
but carry random or mutated frames. Finally a clean connection must still echo. Prints OK on success."""
import asyncio, os, random, ssl, struct, sys

from aioquic.asyncio import connect
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.tls import Epoch


def vi(v):
    if v < 1 << 6: return bytes([v])
    if v < 1 << 14: return struct.pack(">H", 0x4000 | v)
    if v < 1 << 30: return struct.pack(">I", 0x80000000 | v)
    return struct.pack(">Q", 0xC000000000000000 | v)


def rv(r):
    return vi(r.choice([0, 1, 2, 3, 7, 63, 64, 1000, 16383, 16384, (1 << 30) - 1, 1 << 30, (1 << 62) - 1, r.getrandbits(r.choice([4, 8, 16, 32, 62]))]))


def corpus(r):
    sid = r.choice([0, 4, 8, 1, 5, 2, 3, 6, 7, 400, rv(r)[0]])
    data = os.urandom(r.randint(0, 40))
    return [
        b"\x01", b"\x00" * r.randint(1, 20),
        b"\x02" + rv(r) + rv(r) + rv(r) + rv(r) + rv(r), b"\x03" + rv(r) * 7,
        b"\x04" + rv(r) + rv(r) + rv(r), b"\x05" + rv(r) + rv(r),
        b"\x06" + rv(r) + vi(len(data)) + data, b"\x07" + vi(len(data)) + data,
        bytes([r.choice([8, 9, 10, 11, 12, 13, 14, 15])]) + rv(r) + rv(r) + vi(len(data)) + data,
        b"\x08" + vi(sid) + data, b"\x0a" + vi(sid) + vi(len(data)) + data, b"\x0b" + vi(sid) + vi(len(data)) + data,
        b"\x0e" + vi(sid) + vi(r.randint(0, 5000)) + data, b"\x0f" + vi(sid) + vi(1 << 40) + b"x", b"\x0f" + vi(sid) + vi(0) + b"",
        b"\x10" + rv(r), b"\x11" + rv(r), b"\x12" + rv(r), b"\x13" + rv(r), b"\x14" + rv(r), b"\x15" + vi(sid) + rv(r), b"\x16" + rv(r), b"\x17" + rv(r),
        b"\x18" + rv(r) + rv(r) + bytes([r.choice([0, 1, 8, 20, 21, 255])]) + os.urandom(r.randint(0, 40)),
        b"\x19" + rv(r), b"\x1a" + os.urandom(8), b"\x1b" + os.urandom(8),
        b"\x1c" + rv(r) + rv(r) + rv(r) + b"why", b"\x1d" + rv(r) + rv(r), b"\x1e",
        os.urandom(r.randint(1, 60)),
    ]


def mutate(r, b):
    b = bytearray(b)
    for _ in range(r.randint(0, 3)):
        k = r.random()
        if k < .4 and b: b[r.randrange(len(b))] = r.getrandbits(8)
        elif k < .6 and b: b[r.randrange(len(b))] ^= 1 << r.randrange(8)
        elif k < .8 and b: del b[r.randrange(len(b)):]
        else: b += os.urandom(r.randint(1, 8))
    return bytes(b)


def inject(c, frames):
    q = c._quic
    crypto = q._cryptos[Epoch.ONE_RTT]
    q._packet_number += 1
    pn = q._packet_number
    header = bytes([0x43]) + q._peer_cid.cid + struct.pack(">I", pn & 0xFFFFFFFF)
    payload = frames + b"\x00" * max(0, 24 - len(frames))
    c._transport.sendto(crypto.send.encrypt_packet(header, payload, pn), q._network_paths[0].addr)


async def round_(port, r, packets):
    cfg = QuicConfiguration(is_client=True, alpn_protocols=["echo"], verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect("127.0.0.1", port, configuration=cfg) as c:
        await c.wait_connected()
        # some real traffic first so the server has streams, flow control and acks in play
        s = c._quic.get_next_available_stream_id()
        c._quic.send_stream_data(s, b"hello", end_stream=False)
        c.transmit()
        await asyncio.sleep(0.05)
        pool = corpus(r)
        for _ in range(packets):
            n = r.randint(1, 4)
            frames = b"".join(mutate(r, r.choice(pool)) if r.random() < .8 else r.choice(pool) for _ in range(n))
            try:
                inject(c, frames)
            except AssertionError:
                break   # the server closed and the client dropped its keys: this round is over
            if r.random() < .3:
                await asyncio.sleep(0.002)
            if r.random() < .1:
                pool = corpus(r)
        await asyncio.sleep(0.1)


async def main(port, seed, rounds, packets):
    r = random.Random(seed)
    for _ in range(rounds):
        try:
            await asyncio.wait_for(round_(port, r, packets), 20)
        except (ConnectionError, asyncio.TimeoutError, OSError):
            pass    # the server may close on a protocol violation; that is the correct answer to many of these
    cfg = QuicConfiguration(is_client=True, alpn_protocols=["echo"], verify_mode=ssl.CERT_NONE, server_name="localhost")
    async with connect("127.0.0.1", port, configuration=cfg) as c:
        sid = c._quic.get_next_available_stream_id()
        done = asyncio.get_event_loop().create_future()
        buf = bytearray()
        orig = c.quic_event_received

        def ev(e):
            if hasattr(e, "data") and getattr(e, "stream_id", None) == sid:
                buf.extend(e.data)
                if e.end_stream: done.set_result(bytes(buf))
        c.quic_event_received = ev
        c._quic.send_stream_data(sid, b"still alive", end_stream=True)
        c.transmit()
        assert await asyncio.wait_for(done, 10) == b"still alive"
    print("OK")


asyncio.run(main(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])))
