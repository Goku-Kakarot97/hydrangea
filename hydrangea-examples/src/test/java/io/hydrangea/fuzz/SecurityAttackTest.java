package io.hydrangea.fuzz;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.Options;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.fuzz.ProtocolFuzzTest.cat;
import static io.hydrangea.fuzz.ProtocolFuzzTest.frame;
import static io.hydrangea.fuzz.ProtocolFuzzTest.hpack;
import static io.hydrangea.fuzz.ProtocolFuzzTest.text;
import static io.hydrangea.fuzz.ProtocolFuzzTest.wsFrame;
import static io.hydrangea.fuzz.ProtocolFuzzTest.wsHandshake;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Attacks with a known shape, each checked against an invariant instead of a particular status code: a smuggled request never reaches the
 * handler, header values reaching handlers never carry control characters, limits answer before the memory is spent, and a WebSocket from another
 * origin or with a broken frame never becomes a session.
 */
class SecurityAttackTest {

    static EventLoopGroup group;
    static Server server;
    static int port;
    static final AtomicInteger ADMIN_HITS = new AtomicInteger();
    static final List<String> SEEN_HEADERS = new CopyOnWriteArrayList<>();
    static final AtomicInteger WS_SESSIONS = new AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        group = new EventLoopGroup(2);
        WebSocketConfig ws = WebSocketConfig.defaults().withMaxMessageBytes(1000);   // same-origin policy left at its default
        HttpRouter router = new HttpRouter()
                .get("/plain", r -> HttpResponse.ok("plain"))
                .get("/admin", r -> {
                    ADMIN_HITS.incrementAndGet();
                    return HttpResponse.ok("secret");
                })
                .post("/admin", r -> {
                    ADMIN_HITS.incrementAndGet();
                    return HttpResponse.ok("secret");
                })
                .get("/headers", r -> {
                    r.request().headers().forEach((n, v) -> SEEN_HEADERS.add(n + ": " + v));
                    SEEN_HEADERS.add("#path " + r.request().path());
                    return HttpResponse.ok("ok");
                })
                .post("/echo", r -> HttpResponse.ok(Integer.toString(r.body().length)))
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), ws, () -> new WebSocketListener() {
                    @Override
                    public void onOpen(WebSocket w) {
                        WS_SESSIONS.incrementAndGet();
                    }

                    @Override
                    public void onText(WebSocket w, String t) {
                        w.sendText(t);
                    }
                }));
        HttpConfig config = new HttpConfig(8 * 1024, 64 * 1024).withHttp2(io.hydrangea.http.Http2Config.defaults());
        server = HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null, config, router);
        port = ((InetSocketAddress) server.localAddress()).getPort();
    }

    @AfterAll
    static void stop() {
        server.close();
        group.close();
    }

    @BeforeEach
    void reset() {
        ADMIN_HITS.set(0);
        SEEN_HEADERS.clear();
        WS_SESSIONS.set(0);
    }

    /** Sends the bytes and returns everything the server said until it closed or stayed silent for {@code quietMs}. */
    static String talk(byte[] input, int quietMs) throws Exception {
        try (Socket s = new Socket(InetAddress.getLoopbackAddress(), port)) {
            s.setSoTimeout(quietMs);
            try {
                s.getOutputStream().write(input);
                s.getOutputStream().flush();
            } catch (IOException closedEarly) {
                // the server answered and closed before we finished writing: fine
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            long end = System.nanoTime() + 3_000_000_000L;
            while (System.nanoTime() < end) {
                try {
                    int n = s.getInputStream().read(buf);
                    if (n < 0) {
                        break;
                    }
                    out.write(buf, 0, n);
                } catch (SocketTimeoutException quiet) {
                    break;
                } catch (IOException reset) {
                    break;
                }
            }
            return out.toString(StandardCharsets.ISO_8859_1);
        }
    }

    static int responses(String wire) {
        int n = 0;
        for (int i = wire.indexOf("HTTP/1.1 "); i >= 0; i = wire.indexOf("HTTP/1.1 ", i + 1)) {
            n++;
        }
        return n;
    }

    private static final String SMUGGLED = "GET /admin HTTP/1.1\r\nHost: x\r\n\r\n";

    // ---- request smuggling (HTTP/1.1) ----

    @Test
    void ambiguousBodyLengthsNeverLetASecondRequestThrough() throws Exception {
        // control: an honest pipelined request does reach the handler, so a count of 0 below means something
        talk(text("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n" + SMUGGLED), 150);
        assertEquals(1, ADMIN_HITS.getAndSet(0), "the harness cannot see requests reaching the handler");
        String[][] vectors = {
                {"cl and te", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"te and cl", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"two content-lengths", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nContent-Length: 5\r\n\r\n" + SMUGGLED},
                {"content-length list", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0, 5\r\n\r\n" + SMUGGLED},
                {"space before colon", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding : chunked\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"tab value", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding:\tchunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"te xchunked", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: xchunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"te chunked, identity", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, identity\r\nContent-Length: 4\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"two te headers", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: x\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"te on http/1.0", "POST /echo HTTP/1.0\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n" + SMUGGLED},
                {"negative length", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n" + SMUGGLED},
                {"plus length", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\n" + SMUGGLED},
                {"hex length", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0x5\r\n\r\n" + SMUGGLED},
                {"overflowing length", "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 18446744073709551621\r\n\r\n" + SMUGGLED},
                {"bad chunk terminator", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX0\r\n\r\n" + SMUGGLED},
                {"signed chunk size", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n+5\r\nhello\r\n0\r\n\r\n" + SMUGGLED},
                {"chunk size overflow", "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff5\r\nhello\r\n0\r\n\r\n" + SMUGGLED},
        };
        List<String> failures = new java.util.ArrayList<>();
        for (String[] v : vectors) {
            ADMIN_HITS.set(0);
            String wire = talk(text(v[1]), 150);
            if (ADMIN_HITS.get() != 0) {
                failures.add(v[0] + ": the smuggled request reached the handler; answer was " + wire.replace("\r\n", "|"));
            }
        }
        assertTrue(failures.isEmpty(), () -> String.join("\n", failures));
    }

    @Test
    void malformedMessagesAreRefusedNotInterpreted() throws Exception {
        String[][] vectors = {
                {"bare LF line ends", "GET /plain HTTP/1.1\nHost: x\n\n"},
                {"bare CR in header", "GET /plain HTTP/1.1\r\nHost: x\rX: y\r\n\r\n"},
                {"NUL in value", "GET /plain HTTP/1.1\r\nHost: x\r\nX: a\0b\r\n\r\n"},
                {"header without colon", "GET /plain HTTP/1.1\r\nHost: x\r\nNoColon\r\n\r\n"},
                {"space in header name", "GET /plain HTTP/1.1\r\nHost: x\r\nA B: c\r\n\r\n"},
                {"obs-fold", "GET /plain HTTP/1.1\r\nHost: x\r\nX: a\r\n b\r\n\r\n"},
                {"no host", "GET /plain HTTP/1.1\r\n\r\n"},
                {"two hosts", "GET /plain HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n"},
                {"space in target", "GET /pl ain HTTP/1.1\r\nHost: x\r\n\r\n"},
                {"bad version", "GET /plain HTTP/1.9\r\nHost: x\r\n\r\n"},
        };
        List<String> failures = new java.util.ArrayList<>();
        for (String[] v : vectors) {
            String wire = talk(text(v[1] + SMUGGLED), 150);
            boolean served = wire.contains("HTTP/1.1 200");
            if (served || ADMIN_HITS.get() != 0) {
                failures.add(v[0] + ": accepted (" + wire.replace("\r\n", "|").substring(0, Math.min(60, wire.length())) + ")");
            }
            ADMIN_HITS.set(0);
        }
        assertTrue(failures.isEmpty(), () -> "lenient parsing:\n" + String.join("\n", failures));
    }

    // ---- limits answer before memory is spent ----

    @Test
    void anAnnouncedHugeBodyIsRefusedWithoutReadingIt() throws Exception {
        long t0 = System.nanoTime();
        String wire = talk(text("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 2000000000\r\n\r\nstart"), 500);
        assertTrue(wire.startsWith("HTTP/1.1 413") || wire.startsWith("HTTP/1.1 400"), wire);
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2500, "took too long to refuse");
    }

    @Test
    void hugeHeadersAndHeaderFloodsAreRefused() throws Exception {
        String big = "GET /plain HTTP/1.1\r\nHost: x\r\nX: " + "a".repeat(20_000) + "\r\n\r\n";
        assertTrue(talk(text(big), 300).matches("(?s)HTTP/1.1 4(31|14|00).*|^$"), "oversized header line");
        StringBuilder many = new StringBuilder("GET /plain HTTP/1.1\r\nHost: x\r\n");
        for (int i = 0; i < 5000; i++) {
            many.append("H").append(i).append(": v\r\n");
        }
        String wire = talk(text(many + "\r\n"), 300);
        assertFalse(wire.startsWith("HTTP/1.1 200"), "5000 headers accepted");
        String longLine = "GET /" + "a".repeat(20_000) + " HTTP/1.1\r\nHost: x\r\n\r\n";
        assertFalse(talk(text(longLine), 300).startsWith("HTTP/1.1 200"), "20 KB request line accepted");
        String chunky = "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n" + ("10000\r\n" + "a".repeat(65536) + "\r\n").repeat(3);
        assertFalse(talk(text(chunky), 300).startsWith("HTTP/1.1 200"), "chunked body past the limit accepted");
    }

    // ---- what handlers see ----

    @Test
    void handlersNeverSeeControlCharactersInHeadersOrPaths() throws Exception {
        talk(text("GET /headers HTTP/1.1\r\nHost: x\r\nX-A: v\u0001w\r\nConnection: close\r\n\r\n"), 150);
        byte[] preface = text("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        byte[] h2 = cat(preface, frame(4, 0, 0, new byte[0]),
                frame(1, 5, 1, hpack(":method", "GET", ":scheme", "http", ":path", "/headers", ":authority", "x", "x-a", "v\r\nInjected: 1")),
                frame(1, 5, 3, hpack(":method", "GET", ":scheme", "http", ":path", "/headers\r\nInjected: 1", ":authority", "x")),
                frame(1, 5, 5, hpack(":method", "GET", ":scheme", "http", ":path", "/headers", ":authority", "x", "x-b", "ok")));
        talk(h2, 200);
        for (String h : SEEN_HEADERS) {
            assertFalse(h.chars().anyMatch(c -> c < 0x20 && c != '\t' || c == 0x7f), "control character reached a handler: " + h.replace("\r", "\\r").replace("\n", "\\n"));
        }
    }

    @Test
    void http2RequestsBreakingTheHeaderRulesAreRefused() throws Exception {
        byte[] preface = text("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        byte[] base = cat(preface, frame(4, 0, 0, new byte[0]));
        byte[][] bad = {
                hpack(":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x", "Upper-Case", "v"),
                hpack(":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x", "transfer-encoding", "chunked"),
                hpack(":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x", "connection", "keep-alive"),
                hpack(":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x", "te", "gzip"),
                hpack("x-first", "1", ":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x"),
                hpack(":method", "GET", ":method", "POST", ":scheme", "http", ":path", "/admin", ":authority", "x"),
                hpack(":method", "GET", ":scheme", "http", ":authority", "x"),
                hpack(":method", "GET", ":scheme", "http", ":path", "", ":authority", "x"),
                hpack(":method", "GET", ":scheme", "http", ":path", "/admin", ":authority", "x", ":status", "200"),
                hpack(":method", "POST", ":scheme", "http", ":path", "/admin", ":authority", "x", "content-length", "5"),   // body will be 0 bytes: mismatch
        };
        for (int i = 0; i < bad.length; i++) {
            ADMIN_HITS.set(0);
            talk(cat(base, frame(1, 5, 1, bad[i])), 150);
            assertEquals(0, ADMIN_HITS.get(), "malformed h2 request " + i + " reached the handler");
        }
    }

    // ---- WebSocket ----

    @Test
    void aWebSocketFromAnotherOriginIsNotUpgraded() throws Exception {
        String wire = talk(text(wsHandshake("Origin: http://evil.example\r\n")), 200);
        assertTrue(wire.startsWith("HTTP/1.1 403"), wire);
        assertEquals(0, WS_SESSIONS.get());
    }

    @Test
    void brokenWebSocketFramesCloseTheSessionWithoutAnEcho() throws Exception {
        byte[] hs = text(wsHandshake("Origin: http://x\r\n"));
        Object[][] frames = {
                {"unmasked text", new byte[]{(byte) 0x81, 0x02, 'h', 'i'}},
                {"reserved bit", new byte[]{(byte) 0xc1, (byte) 0x80, 1, 2, 3, 4}},
                {"reserved opcode", wsFrame(3, true, text("x"))},
                {"control frame too long", wsFrame(9, true, new byte[126])},
                {"fragmented control frame", wsFrame(9, false, text("x"))},
                {"invalid utf-8", wsFrame(1, true, new byte[]{(byte) 0xc3, 0x28})},
                {"message over the limit", wsFrame(2, true, new byte[2000])},
                {"continuation without start", wsFrame(0, true, text("x"))},
                {"bad close code", wsFrame(8, true, new byte[]{0x03, (byte) 0xf0})},
        };
        for (Object[] f : frames) {
            String wire = talk(cat(hs, (byte[]) f[1], wsFrame(1, true, text("after"))), 200);
            assertFalse(wire.contains("after"), f[0] + ": the session carried on and echoed a later message");
        }
    }
}
