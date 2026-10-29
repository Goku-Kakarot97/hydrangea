"""Interop client for QuicInteropTest: an independent QUIC implementation (aioquic) talking to the Hydrangea echo server.
Usage: quic_interop.py PORT CASE   (prints OK <case> on success, exits non-zero otherwise)."""
import asyncio, os, random, socket, ssl, struct, sys

from aioquic.asyncio import connect, QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, StreamDataReceived


class Client(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.buf, self.done, self.term = {}, {}, None

    def quic_event_received(self, e):
        if isinstance(e, StreamDataReceived):
            self.buf.setdefault(e.stream_id, bytearray()).extend(e.data)
            if e.end_stream and e.stream_id in self.done:
                self.done[e.stream_id].set_result(bytes(self.buf[e.stream_id]))
        elif isinstance(e, ConnectionTerminated):
            self.term = e
            for f in self.done.values():
                if not f.done():
                    f.set_exception(RuntimeError("terminated %s %s" % (e.error_code, e.reason_phrase)))


def config(alpn="echo"):
    return QuicConfiguration(is_client=True, alpn_protocols=[alpn], verify_mode=ssl.CERT_NONE, server_name="localhost")


def start(c, size):
    sid = c._quic.get_next_available_stream_id()
    payload = os.urandom(size)
    fut = asyncio.get_event_loop().create_future()
    c.done[sid] = fut
    c._quic.send_stream_data(sid, payload, end_stream=True)
    return fut, payload


async def exchange(c, size, streams=1):
    pending = [start(c, size) for _ in range(streams)]
    c.transmit()
    for fut, payload in pending:
        got = await asyncio.wait_for(fut, 60)
        assert got == payload, "echo differs: %d vs %d bytes" % (len(got), len(payload))


def lossy(c, rate):
    rx = c._quic.receive_datagram

    def drop_in(data, addr, now):
        if random.random() >= rate:
            return rx(data, addr, now)

    c._quic.receive_datagram = drop_in
    tx = c._transport.sendto

    def drop_out(data, addr=None):
        if random.random() >= rate:
            return tx(data, addr)

    c._transport.sendto = drop_out


async def run(port, case):
    if case == "echo-small":
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            await exchange(c, 5)
    elif case == "echo-large":
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            await exchange(c, 2_000_000)
    elif case == "many-streams":
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            await exchange(c, 20_000, streams=60)   # more than the 50 left of the default 100 limit once finished streams are recycled
            await exchange(c, 20_000, streams=60)
    elif case == "loss":
        random.seed(5)
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            lossy(c, 0.10)
            await exchange(c, 400_000, streams=3)
    elif case == "key-update":
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            await exchange(c, 3000)
            for i in range(3):
                c._quic.request_key_update()
                c.transmit()
                await exchange(c, 30_000 + i)
    elif case == "server-close":
        async with connect("127.0.0.1", port, configuration=config(), create_protocol=Client) as c:
            sid = c._quic.get_next_available_stream_id()
            c._quic.send_stream_data(sid, b"CLOSE", end_stream=True)
            c.transmit()
            for _ in range(60):
                if c.term:
                    break
                await asyncio.sleep(0.1)
            assert c.term is not None and c.term.error_code == 42 and c.term.reason_phrase == "bye", c.term
    elif case == "alpn-mismatch":
        try:
            async with connect("127.0.0.1", port, configuration=config("nope"), create_protocol=Client) as c:
                await asyncio.sleep(1.5)
            raise AssertionError("connected with an ALPN the server does not speak")
        except ConnectionError:
            pass
    elif case == "version-negotiation":
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(3)
        dcid, scid = os.urandom(8), os.urandom(8)
        s.sendto(bytes([0xC3]) + struct.pack(">I", 0x1A2A3A4A) + bytes([8]) + dcid + bytes([8]) + scid + b"\0" * 1180, ("127.0.0.1", port))
        data, _ = s.recvfrom(2000)
        assert data[0] & 0x80 and data[1:5] == b"\0\0\0\0", data[:8].hex()
        assert data[6:14] == scid and data[15:23] == dcid and struct.unpack(">I", data[-4:])[0] == 1
    elif case == "retry-raw":
        # a first Initial without a token gets a Retry (long header, type 3, 16-byte tag); one with a forged token gets nothing
        def initial(token):
            dcid, scid = os.urandom(8), os.urandom(8)
            return dcid, scid, bytes([0xC0]) + struct.pack(">I", 1) + bytes([8]) + dcid + bytes([8]) + scid + bytes([len(token)]) + token + b"\x44\x00" + b"\0" * 1200
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2)
        dcid, scid, pkt = initial(b"")
        s.sendto(pkt, ("127.0.0.1", port))
        data, _ = s.recvfrom(2000)
        assert data[0] & 0xF0 == 0xF0 and data[1:5] == b"\0\0\0\1", data[:8].hex()
        assert data[6:6 + 8] == scid, "Retry must be addressed to the client's source id"
        assert len(data) < 120, "Retry must be small (no amplification)"
        _, _, pkt = initial(b"\x01" * 40)
        s.sendto(pkt, ("127.0.0.1", port))
        s.settimeout(1)
        try:
            s.recvfrom(2000)
            raise AssertionError("answered an Initial carrying a forged token")
        except socket.timeout:
            pass
    else:
        raise SystemExit("unknown case " + case)
    print("OK", case)


asyncio.run(run(int(sys.argv[1]), sys.argv[2]))
