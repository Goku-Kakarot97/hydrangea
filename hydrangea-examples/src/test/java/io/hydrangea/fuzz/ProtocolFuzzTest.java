package io.hydrangea.fuzz;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http2.HpackEncoder;
import io.hydrangea.sse.SseEvent;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hostile bytes over real sockets into one server that speaks HTTP/1.1, h2c (prior knowledge and Upgrade), WebSocket, SSE and gRPC.
 * A server may close the connection or answer with an error; it must not log an internal failure (a RuntimeException escaping a parser is how
 * the stages report their own bugs), block its event loop, leak connections, or stop serving good requests afterwards.
 */
class ProtocolFuzzTest {

    private static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final String WS_KEY = "dGhlIHNhbXBsZSBub25jZQ==";

    static EventLoopGroup group;
    static Server server;
    static int port;
    static final List<LogRecord> LOGGED = new CopyOnWriteArrayList<>();
    static Handler logHandler;

    @BeforeAll
    static void start() throws Exception {
        logHandler = new Handler() {
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
        java.util.logging.Logger.getLogger("").addHandler(logHandler);
        group = new EventLoopGroup(2);
        WebSocketConfig ws = WebSocketConfig.defaults().allowAnyOrigin().withMaxMessageBytes(64 * 1024);
        HttpRouter router = new HttpRouter()
                .get("/plain", r -> HttpResponse.status(200).header("Content-Type", "text/plain").body("Hello".getBytes(StandardCharsets.UTF_8)))
                .post("/echo", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(r.body()))
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), ws, () -> new WebSocketListener() {
                    @Override
                    public void onText(WebSocket w, String t) {
                        w.sendText(t);
                    }

                    @Override
                    public void onBinary(WebSocket w, ByteBuffer d) {
                        w.sendBinary(d);
                    }
                }))
                .get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                    @Override
                    public void onOpen(io.hydrangea.sse.SseEmitter c) {
                        c.send(SseEvent.data("hi"));
                    }
                }));
        GrpcServer grpc = GrpcEchoExample.builder().fallback(router).build();
        server = grpc.bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        port = ((InetSocketAddress) server.localAddress()).getPort();
    }

    @AfterAll
    static void stop() {
        java.util.logging.Logger.getLogger("").removeHandler(logHandler);
        server.close();
        group.close();
    }

    // ---- building blocks ----

    static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    static byte[] text(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    static byte[] frame(int type, int flags, int sid, byte[] payload) {
        return ByteBuffer.allocate(9 + payload.length).put((byte) (payload.length >> 16)).put((byte) (payload.length >> 8)).put((byte) payload.length)
                .put((byte) type).put((byte) flags).putInt(sid).put(payload).array();
    }

    static byte[] hpack(String... nv) {
        HpackEncoder e = new HpackEncoder();
        for (int i = 0; i < nv.length; i += 2) {
            e.header(nv[i], nv[i + 1]);
        }
        return e.toByteArray();
    }

    static byte[] grpcMessage(Msg m) throws Exception {
        byte[] b = GrpcEchoExample.MSG.encode(m);
        return ByteBuffer.allocate(5 + b.length).put((byte) 0).putInt(b.length).put(b).array();
    }

    static byte[] wsFrame(int opcode, boolean fin, byte[] payload) {
        byte[] mask = {1, 2, 3, 4};
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write((fin ? 0x80 : 0) | opcode);
        if (payload.length < 126) {
            o.write(0x80 | payload.length);
        } else {
            o.write(0x80 | 126);
            o.write(payload.length >> 8);
            o.write(payload.length);
        }
        o.writeBytes(mask);
        for (int i = 0; i < payload.length; i++) {
            o.write(payload[i] ^ mask[i & 3]);
        }
        return o.toByteArray();
    }

    static String wsHandshake(String extra) {
        return "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + WS_KEY
                + "\r\nSec-WebSocket-Version: 13\r\n" + extra + "\r\n";
    }

    private static final byte[] SETTINGS = frame(4, 0, 0, new byte[0]);
    private static final byte[] SETTINGS_ACK = frame(4, 1, 0, new byte[0]);

    // ---- the exchange and its oracles ----

    /** Sends the input on a fresh connection, reads whatever comes back for a moment, closes, then checks what the server did. */
    static void exchange(byte[] input) throws Exception {
        int logged = LOGGED.size();
        group.metrics().resetMaxBlockNanos();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 2000);
            s.setSoTimeout(25);
            try {
                s.getOutputStream().write(input);
                s.getOutputStream().flush();
                InputStream in = s.getInputStream();
                byte[] sink = new byte[8192];
                long end = System.nanoTime() + 60_000_000L;
                while (System.nanoTime() < end) {
                    try {
                        if (in.read(sink) < 0) {
                            break;
                        }
                    } catch (SocketTimeoutException quiet) {
                        break;
                    }
                }
            } catch (IOException resetByServer) {
                // the server closed on us: an acceptable answer
            }
        }
        if (LOGGED.size() > logged) {
            LogRecord r = LOGGED.get(logged);
            String where = r.getThrown() == null ? "" : " " + r.getThrown() + " at " + (r.getThrown().getStackTrace().length > 0 ? r.getThrown().getStackTrace()[0] : "?");
            throw new AssertionError("the server logged: " + r.getMessage() + where);
        }
        long blockedMs = group.metrics().resetMaxBlockNanos() / 1_000_000;
        if (blockedMs > 250) {
            throw new AssertionError("an event loop was blocked for " + blockedMs + " ms");
        }
    }

    static final Set<Class<? extends Throwable>> NOTHING_ALLOWED = Set.of();

    private static void check(List<Fuzz.Finding> findings) {
        assertTrue(findings.isEmpty(), () -> findings.size() + " finding(s):\n" + String.join("\n", findings.stream().map(Fuzz.Finding::toString).toList()));
    }

    private static int iterations() {
        return Math.min(Fuzz.ITERATIONS, Integer.getInteger("fuzz.protocol.iterations", 500));
    }

    /** Good requests still work, and every connection the fuzzing opened is gone. */
    static void assertServerHealthy() throws Exception {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (server.activeConnections() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, server.activeConnections(), "connections leaked");
        try (Socket s = new Socket(InetAddress.getLoopbackAddress(), port)) {
            s.setSoTimeout(3000);
            s.getOutputStream().write(text("GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"));
            String all = new String(s.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
            assertTrue(all.startsWith("HTTP/1.1 200") && all.endsWith("Hello"), all);
        }
        HttpClient c = new HttpClient(group, HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)));
        try {
            assertEquals("Hello", c.send(ClientRequest.get("http://127.0.0.1:" + port + "/plain")).get(5, TimeUnit.SECONDS).bodyAsString());
        } finally {
            c.close();
        }
        try (io.hydrangea.grpc.GrpcChannel ch = io.hydrangea.grpc.GrpcChannel.forTarget("http://127.0.0.1:" + port).eventLoopGroup(group).build()) {
            assertEquals("echo:ok", ch.unary(GrpcEchoExample.UNARY, new Msg("ok"), io.hydrangea.grpc.CallOptions.DEFAULT).get(5, TimeUnit.SECONDS).text());
        }
    }

    // ---- the oracles themselves ----

    @Test
    void theFuzzerReportsWhatItShouldAndStaysQuietOtherwise() {
        List<byte[]> corpus = List.of(new byte[]{1, 2, 3});
        var found = Fuzz.run("self-test", corpus, in -> {
            if (in.length > 0 && (in[0] & 0xff) == 0xff) {
                throw new NullPointerException("planted");
            }
        }, Set.of(), 1000, 5000);
        assertEquals(1, found.size(), "one distinct planted failure expected: " + found);
        assertTrue(found.get(0).problem().contains("NullPointerException"));
        assertTrue(Fuzz.run("self-test", corpus, in -> {
            if (in.length > 100) {
                throw new IllegalArgumentException("documented");
            }
        }, Set.of(IllegalArgumentException.class), 1000, 2000).isEmpty());
        int before = LOGGED.size();
        System.getLogger("io.hydrangea.selftest").log(System.Logger.Level.WARNING, "planted warning");
        assertEquals(before + 1, LOGGED.size(), "the log oracle is blind");
        LOGGED.remove(LOGGED.size() - 1);
    }

    // ---- campaigns ----

    @Test
    void http1Requests() throws Exception {
        List<byte[]> corpus = new ArrayList<>(List.of(
                text("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n"),
                text("GET /plain?a=b%20c&d=%zz HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip, deflate\r\nCookie: a=b; c=d\r\n\r\n"),
                text("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"),
                text("POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5;ext=1\r\nhello\r\n0\r\nTrailer: v\r\n\r\n"),
                text("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
                text("POST /echo HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\nabc"),
                text("GET /plain HTTP/1.1\r\nHost: x\r\n\r\nGET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
                text("GET http://x/plain HTTP/1.1\r\nHost: x\r\n\r\n"),
                text("OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n"),
                text("GET /plain HTTP/1.0\r\n\r\n"),
                text("GET /plain HTTP/1.1\r\nHost: x\r\n X: folded\r\nA : space\r\n\r\n"),
                text("GET /events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\nLast-Event-ID: 7\r\n\r\n"),
                text("GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n"),
                text("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")));
        check(Fuzz.run("http/1.1", corpus, ProtocolFuzzTest::exchange, NOTHING_ALLOWED, 3000, iterations()));
        assertServerHealthy();
    }

    @Test
    void http2Frames() throws Exception {
        byte[] get = hpack(":method", "GET", ":scheme", "http", ":path", "/plain", ":authority", "x");
        byte[] post = hpack(":method", "POST", ":scheme", "http", ":path", "/echo", ":authority", "x", "content-length", "5");
        byte[] head = cat(PREFACE, SETTINGS, SETTINGS_ACK);
        List<byte[]> corpus = new ArrayList<>(List.of(
                cat(head, frame(1, 5, 1, get)),
                cat(head, frame(1, 4, 1, post), frame(0, 1, 1, text("hello"))),
                cat(head, frame(1, 0, 1, Fuzz.mutate(new Random(1), get, List.of(get))), frame(9, 4, 1, new byte[]{(byte) 0x88})),
                cat(head, frame(6, 0, 0, new byte[8]), frame(8, 0, 0, new byte[]{0, 0, 1, 0}), frame(2, 0, 3, new byte[]{0, 0, 0, 0, 16})),
                cat(head, frame(1, 5, 1, get), frame(3, 0, 1, new byte[]{0, 0, 0, 8}), frame(7, 0, 0, new byte[8])),
                cat(PREFACE, frame(4, 0, 0, new byte[]{0, 4, 0, 0, 0, 1, 0, 5, 0, 0, 0x40, 0, 0, 3, 0, 0, 0, 1})),
                cat(head, frame(1, 4, 1, hpack(":method", "POST", ":scheme", "http", ":path", "/echo", ":authority", "x")),
                        frame(0, 0, 1, new byte[3000]), frame(0, 1, 1, new byte[0])),
                cat(head, frame(5, 4, 1, new byte[]{0, 0, 0, 2}))));
        check(Fuzz.run("http/2 frames", corpus, ProtocolFuzzTest::exchange, NOTHING_ALLOWED, 3000, iterations()));
        assertServerHealthy();
    }

    @Test
    void grpcCalls() throws Exception {
        byte[] unary = hpack(":method", "POST", ":scheme", "http", ":path", "/hydrangea.test.Echo/Unary", ":authority", "x",
                "content-type", "application/grpc", "te", "trailers", "grpc-timeout", "5S", "grpc-encoding", "gzip", "grpc-accept-encoding", "gzip");
        byte[] stream = hpack(":method", "POST", ":scheme", "http", ":path", "/hydrangea.test.Echo/Bidi", ":authority", "x",
                "content-type", "application/grpc", "te", "trailers");
        byte[] reflection = hpack(":method", "POST", ":scheme", "http", ":path", "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo", ":authority", "x",
                "content-type", "application/grpc", "te", "trailers");
        byte[] head = cat(PREFACE, SETTINGS, SETTINGS_ACK);
        List<byte[]> corpus = new ArrayList<>(List.of(
                cat(head, frame(1, 4, 1, unary), frame(0, 1, 1, grpcMessage(new Msg("hi", 3, new byte[100])))),
                cat(head, frame(1, 4, 1, stream), frame(0, 0, 1, grpcMessage(new Msg("a"))), frame(0, 0, 1, grpcMessage(new Msg("b"))), frame(0, 1, 1, new byte[0])),
                cat(head, frame(1, 4, 1, reflection), frame(0, 1, 1, new byte[]{0, 0, 0, 0, 4, 0x3a, 0x02, 0x0a, 0x00})),
                cat(head, frame(1, 4, 1, hpack(":method", "POST", ":scheme", "http", ":path", "/grpc.health.v1.Health/Check", ":authority", "x",
                        "content-type", "application/grpc-web+proto")), frame(0, 1, 1, new byte[]{0, 0, 0, 0, 0})),
                text("POST /hydrangea.test.Echo/Unary HTTP/1.1\r\nHost: x\r\nContent-Type: application/grpc-web-text\r\nContent-Length: 12\r\n\r\nAAAAAAU=ZZZZ"),
                text("POST /hydrangea.test.Echo/Unary HTTP/1.1\r\nHost: x\r\nContent-Type: application/grpc-web+proto\r\nContent-Length: 5\r\n\r\n\0\0\0\0\0")));
        check(Fuzz.run("grpc", corpus, ProtocolFuzzTest::exchange, NOTHING_ALLOWED, 3000, iterations()));
        assertServerHealthy();
    }

    @Test
    void webSocketFrames() throws Exception {
        byte[] hs = text(wsHandshake(""));
        byte[] hsDeflate = text(wsHandshake("Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n"));
        List<byte[]> corpus = new ArrayList<>(List.of(
                cat(hs, wsFrame(1, true, text("hello"))),
                cat(hs, wsFrame(2, true, new byte[300]), wsFrame(9, true, text("p")), wsFrame(8, true, new byte[]{0x03, (byte) 0xe8})),
                cat(hs, wsFrame(1, false, text("he")), wsFrame(0, false, text("ll")), wsFrame(0, true, text("o"))),
                cat(hsDeflate, wsFrame(1, true, new byte[]{(byte) 0xf2, 0x48, (byte) 0xcd, (byte) 0xc9, (byte) 0xc9, 0x07, 0x00})),
                cat(hs, wsFrame(1, true, new byte[]{(byte) 0xc3, (byte) 0x28})),   // invalid UTF-8
                cat(hs, wsFrame(1, true, new byte[200]), wsFrame(8, true, new byte[]{0x03, (byte) 0xe8, 'b', 'y', 'e'}))));
        check(Fuzz.run("websocket", corpus, ProtocolFuzzTest::exchange, NOTHING_ALLOWED, 3000, iterations()));
        assertServerHealthy();
    }
}
