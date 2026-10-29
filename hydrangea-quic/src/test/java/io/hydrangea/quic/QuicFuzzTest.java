package io.hydrangea.quic;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.fuzz.Fuzz;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hostile datagrams into the QUIC server: client Initials with valid packet protection (so the contents reach the frame and TLS parsers) whose frames
 * or ClientHello are mutated, and mutated whole datagrams (headers and ciphertext). The server may ignore or close; it must not log an internal error,
 * block its event loop, or stop answering a good handshake afterwards.
 */
class QuicFuzzTest {

    static EventLoopGroup group;
    static QuicServer server;
    static int port;
    static QuicServer retryServer;
    static int retryPort;
    static final List<LogRecord> LOGGED = new CopyOnWriteArrayList<>();
    static Handler handler;
    static DatagramSocket socket;

    @BeforeAll
    static void start(@TempDir Path dir) throws Exception {
        handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue() && r.getLoggerName() != null && r.getLoggerName().startsWith("io.hydrangea")) {
                    LOGGED.add(r);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger.getLogger("").addHandler(handler);
        Path ks = dir.resolve("k.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(), "-genkeypair", "-alias", "k", "-keyalg", "EC",
                "-groupname", "secp256r1", "-validity", "2", "-dname", "CN=localhost", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                "-keypass", "changeit").redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertEquals(0, p.waitFor());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        group = new EventLoopGroup(2);
        server = QuicServer.bind(group, new InetSocketAddress("127.0.0.1", 0),
                QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("echo")).withIdleTimeout(Duration.ofSeconds(1)), new QuicHandler() {
                });
        port = ((InetSocketAddress) server.localAddress()).getPort();
        retryServer = QuicServer.bind(group, new InetSocketAddress("127.0.0.1", 0),
                QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("echo")).withIdleTimeout(Duration.ofSeconds(1)).withRetry(true), new QuicHandler() {
                });
        retryPort = ((InetSocketAddress) retryServer.localAddress()).getPort();
        socket = new DatagramSocket();
        socket.setSoTimeout(15);
    }

    @AfterAll
    static void stop() {
        java.util.logging.Logger.getLogger("").removeHandler(handler);
        socket.close();
        server.close();
        retryServer.close();
        group.close();
    }

    // ---- building a client Initial ----

    private static final SecureRandom RANDOM = new SecureRandom();

    private static byte[] u16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static byte[] cat(byte[]... parts) {
        var o = new java.io.ByteArrayOutputStream();
        for (byte[] p : parts) o.writeBytes(p);
        return o.toByteArray();
    }

    private static byte[] ext(int type, byte[] body) {
        return cat(u16(type), u16(body.length), body);
    }

    static byte[] clientHello(byte[] scid) {
        byte[] pub = new byte[32];
        RANDOM.nextBytes(pub);
        pub[31] &= 0x7f;
        byte[] params = new QuicWire.Writer(64).varint(0x0f).varint(scid.length).bytes(scid).varint(0x04).varint(4).varint(1 << 20).toByteArray();
        byte[] extensions = cat(
                ext(0x2b, new byte[]{2, 3, 4}),
                ext(0x33, cat(u16(36), u16(0x1d), u16(32), pub)),
                ext(0x0d, cat(u16(4), u16(0x0403), u16(0x0804))),
                ext(0x0a, cat(u16(2), u16(0x1d))),
                ext(0x10, cat(u16(5), new byte[]{4, 'e', 'c', 'h', 'o'})),
                ext(0x00, cat(u16(12), new byte[]{0}, u16(9), "localhost".getBytes())),
                ext(0x39, params));
        byte[] random = new byte[32];
        RANDOM.nextBytes(random);
        byte[] body = cat(u16(0x0303), random, new byte[]{0}, u16(2), u16(0x1301), new byte[]{1, 0}, u16(extensions.length), extensions);
        return cat(new byte[]{1, (byte) (body.length >> 16), (byte) (body.length >> 8), (byte) body.length}, body);
    }

    static byte[] cryptoFrame(byte[] data) {
        return new QuicWire.Writer(data.length + 8).varint(0x06).varint(0).varint(data.length).bytes(data).toByteArray();
    }

    /** A protected client Initial datagram (padded to 1200 bytes) carrying {@code frames}. */
    static byte[] initial(byte[] dcid, byte[] scid, long pn, byte[] frames) {
        return initial(dcid, scid, new byte[0], pn, frames);
    }

    static byte[] initial(byte[] dcid, byte[] scid, byte[] token, long pn, byte[] frames) {
        int payload = Math.max(frames.length, 1200 - (1 + 4 + 1 + dcid.length + 1 + scid.length + 1 + token.length + 2 + 4 + 16));
        byte[] plain = new byte[payload];
        System.arraycopy(frames, 0, plain, 0, frames.length);   // the rest is PADDING (zero bytes)
        QuicWire.Writer h = new QuicWire.Writer(64);
        h.u8(0xc3).u32(1).u8(dcid.length).bytes(dcid).u8(scid.length).bytes(scid).varint(token.length).bytes(token).u16(0x4000 | (4 + payload + 16));
        int pnOffset = h.n;
        h.u32(pn);
        byte[] header = h.toByteArray();
        QuicCrypto.Keys keys = QuicCrypto.initialKeys(dcid)[0];
        byte[] ct = keys.seal(pn, header, plain, 0, plain.length);
        byte[] packet = cat(header, ct);
        byte[] mask = keys.mask(packet, pnOffset + 4);
        packet[0] ^= (byte) (mask[0] & 0x0f);
        for (int i = 0; i < 4; i++) packet[pnOffset + i] ^= mask[1 + i];
        return packet;
    }

    // ---- the exchange and its oracles ----

    private static void send(byte[] datagram) throws Exception {
        send(datagram, port);
    }

    private static void send(byte[] datagram, int port) throws Exception {
        int before = LOGGED.size();
        group.metrics().resetMaxBlockNanos();
        socket.send(new DatagramPacket(datagram, datagram.length, InetAddress.getLoopbackAddress(), port));
        byte[] sink = new byte[2048];
        long end = System.nanoTime() + 25_000_000L;
        while (System.nanoTime() < end) {
            try {
                socket.receive(new DatagramPacket(sink, sink.length));
            } catch (SocketTimeoutException quiet) {
                break;
            }
        }
        if (LOGGED.size() > before) {
            LogRecord r = LOGGED.get(before);
            throw new AssertionError("the server logged: " + r.getMessage() + " " + r.getThrown() + (r.getThrown() != null && r.getThrown().getStackTrace().length > 0
                    ? " at " + r.getThrown().getStackTrace()[0] : ""));
        }
        long ms = group.metrics().resetMaxBlockNanos() / 1_000_000;
        if (ms > 250) {
            throw new AssertionError("an event loop was blocked for " + ms + " ms");
        }
    }

    private static int iterations() {
        return Math.min(Fuzz.ITERATIONS, Integer.getInteger("fuzz.protocol.iterations", 400));
    }

    private static void check(List<Fuzz.Finding> findings) {
        assertTrue(findings.isEmpty(), () -> findings.size() + " finding(s):\n" + String.join("\n", findings.stream().map(Fuzz.Finding::toString).toList()));
    }

    private static byte[] randomCid() {
        byte[] b = new byte[8];
        RANDOM.nextBytes(b);
        return b;
    }

    /** A good handshake still gets a ServerHello, and the half-open connections of the fuzzing go away. */
    private static void assertHealthy() throws Exception {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (server.activeConnections() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(0, server.activeConnections(), "connections leaked");
        byte[] dcid = randomCid(), scid = randomCid();
        byte[] dg = initial(dcid, scid, 0, cryptoFrame(clientHello(scid)));
        socket.setSoTimeout(1000);
        socket.send(new DatagramPacket(dg, dg.length, InetAddress.getLoopbackAddress(), port));
        DatagramPacket reply = new DatagramPacket(new byte[2048], 2048);
        socket.receive(reply);
        assertTrue((reply.getData()[0] & 0x80) != 0 && reply.getLength() > 100, "no handshake flight for a good ClientHello");
        socket.setSoTimeout(15);
    }

    @Test
    void clientHelloContents() throws Exception {
        List<byte[]> corpus = new ArrayList<>();
        for (int i = 0; i < 4; i++) corpus.add(clientHello(randomCid()));
        check(Fuzz.run("quic ClientHello", corpus, in -> send(initial(randomCid(), randomCid(), 0, cryptoFrame(in))), Set.of(), 3000, iterations()));
        assertHealthy();
    }

    @Test
    void frames() throws Exception {
        byte[] ch = clientHello(randomCid());
        List<byte[]> corpus = new ArrayList<>(List.of(
                cryptoFrame(ch),
                new byte[]{0x01},                                                                  // PING
                new byte[]{0x02, 0, 0, 0, 0},                                                       // ACK of packet 0
                new byte[]{0x02, 5, 0, 1, 1, 0, 0},                                                 // ACK with a range
                new byte[]{0x1c, 0x0a, 0, 0},                                                       // CONNECTION_CLOSE
                new QuicWire.Writer(32).varint(0x06).varint(1_000_000).varint(5).bytes(new byte[5]).toByteArray(),   // CRYPTO far ahead
                new byte[]{0x08, 0, 3, 'a', 'b', 'c'},                                              // STREAM (not allowed in Initial)
                new byte[]{0x1e},                                                                  // HANDSHAKE_DONE (a client cannot send it)
                new byte[]{(byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0}));
        check(Fuzz.run("quic frames", corpus, in -> send(initial(randomCid(), randomCid(), 0, in)), Set.of(), 3000, iterations()));
        assertHealthy();
    }

    @Test
    void wholeDatagrams() throws Exception {
        byte[] scid = randomCid();
        List<byte[]> corpus = new ArrayList<>();
        for (int i = 0; i < 4; i++) corpus.add(initial(randomCid(), scid, i, cryptoFrame(clientHello(scid))));
        corpus.add(new byte[]{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30});   // a short header
        corpus.add(new QuicWire.Writer(64).u8(0x80).u32(0).u8(8).bytes(new byte[8]).u8(8).bytes(new byte[8]).u32(1).toByteArray());                       // Version Negotiation
        check(Fuzz.run("quic datagrams", corpus, QuicFuzzTest::send, Set.of(), 3000, iterations()));
        assertHealthy();
    }

    /** Address-validation tokens: a real one from a Retry, mutated; the server must refuse or accept it quietly, never fail. */
    @Test
    void retryTokens() throws Exception {
        byte[] odcid = randomCid(), scid = randomCid();
        socket.setSoTimeout(1000);
        byte[] first = initial(odcid, scid, 0, cryptoFrame(clientHello(scid)));
        socket.send(new DatagramPacket(first, first.length, InetAddress.getLoopbackAddress(), retryPort));
        DatagramPacket reply = new DatagramPacket(new byte[2048], 2048);
        try {
            socket.receive(reply);
        } catch (SocketTimeoutException e) {
            throw new AssertionError("no Retry; server logged: " + LOGGED.stream().map(r -> r.getMessage() + " " + r.getThrown()).toList(), e);
        } finally {
            socket.setSoTimeout(15);
        }
        byte[] retry = java.util.Arrays.copyOf(reply.getData(), reply.getLength());
        assertEquals(0xf0, retry[0] & 0xf0, "expected a Retry");
        int at = 1 + 4;
        at += 1 + (retry[at] & 0xff);                 // destination id
        int newIdLen = retry[at] & 0xff;
        byte[] newId = java.util.Arrays.copyOfRange(retry, at + 1, at + 1 + newIdLen);
        byte[] token = java.util.Arrays.copyOfRange(retry, at + 1 + newIdLen, retry.length - 16);
        assertTrue(token.length > 0);
        // the good token, a stale connection id, and mutations of both
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(token);
        corpus.add(new byte[0]);
        corpus.add(cat(token, new byte[]{1}));
        Fuzz.Target target = in -> {
            // token mutated; the Initial's own destination id is the one the Retry chose
            byte[] sc = randomCid();
            send(initial(newId, sc, in, 0, cryptoFrame(clientHello(sc))), retryPort);
        };
        check(Fuzz.run("quic retry token", corpus, target, Set.of(), 3000, iterations()));
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (retryServer.activeConnections() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(0, retryServer.activeConnections(), "connections leaked");
    }
}
