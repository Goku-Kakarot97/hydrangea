package io.hydrangea.websocket;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class WebSocketTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private static final String KEY = "dGhlIHNhbXBsZSBub25jZQ==";

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @BeforeEach
    void setUp() {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : cleanup) c.close();
        clientGroup.close();
        serverGroup.close();
    }

    /** Records every event; optionally echoes. */
    static class Recorder implements WebSocketListener {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final List<String> texts = Collections.synchronizedList(new ArrayList<>());
        final List<byte[]> binaries = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<WebSocket> opened = new CompletableFuture<>();
        final CompletableFuture<int[]> closedCode = new CompletableFuture<>();
        final CompletableFuture<String> closedReason = new CompletableFuture<>();
        final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger pongs = new AtomicInteger();
        final AtomicInteger pings = new AtomicInteger();
        final boolean echo;

        Recorder(boolean echo) {
            this.echo = echo;
        }

        @Override
        public void onOpen(WebSocket ws) {
            events.add("open");
            opened.complete(ws);
        }

        @Override
        public void onText(WebSocket ws, String text) {
            texts.add(text);
            if (echo) ws.sendText(text);
        }

        @Override
        public void onBinary(WebSocket ws, ByteBuffer data) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            binaries.add(b);
            if (echo) ws.sendBinary(ByteBuffer.wrap(b));
        }

        @Override
        public void onPing(WebSocket ws, ByteBuffer data) {
            pings.incrementAndGet();
        }

        @Override
        public void onPong(WebSocket ws, ByteBuffer data) {
            pongs.incrementAndGet();
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            errors.add(error);
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            events.add("close:" + code);
            closedReason.complete(reason);
            closedCode.complete(new int[]{code});
        }

        int awaitClose() throws Exception {
            return closedCode.get(10, TimeUnit.SECONDS)[0];
        }
    }

    private Server start(WebSocketConfig cfg, Supplier<? extends WebSocketListener> listener) throws IOException {
        return start(Options.defaults(), null, cfg, listener);
    }

    private Server start(Options options, SSLContext tls, WebSocketConfig cfg, Supplier<? extends WebSocketListener> listener) throws IOException {
        HttpRouter router = new HttpRouter()
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), cfg, listener))
                .get("/plain", r -> HttpResponse.ok("just http"));
        Server s = HttpServer.bind(serverGroup, ANY, options, tls, HttpConfig.defaults(), router);
        cleanup.add(s);
        return s;
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- raw protocol helpers ----

    record Frame(int op, boolean fin, byte[] payload, boolean rsv1) {
    }

    static final class Raw implements AutoCloseable {
        final Socket s;
        final InputStream in;
        final String head;

        Raw(int port, String extraHeaders) throws IOException {
            s = new Socket("127.0.0.1", port);
            s.setSoTimeout(5000);
            in = s.getInputStream();
            s.getOutputStream().write(("GET /ws HTTP/1.1\r\nHost: 127.0.0.1:" + port + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: " + KEY + "\r\nSec-WebSocket-Version: 13\r\n" + extraHeaders + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            head = readHead();
            assertTrue(head.startsWith("HTTP/1.1 101"), head);
            assertTrue(head.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), head);
        }

        String readHead() throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) >= 0) {
                b.write(c);
                byte[] a = b.toByteArray();
                int n = a.length;
                if (n >= 4 && a[n - 4] == '\r' && a[n - 3] == '\n' && a[n - 2] == '\r' && a[n - 1] == '\n') break;
            }
            return b.toString(StandardCharsets.ISO_8859_1);
        }

        /** A client frame with mask key 37 fa 21 3d (the RFC example key). */
        static byte[] frame(int firstByte, byte[] payload) {
            byte[] key = {0x37, (byte) 0xfa, 0x21, 0x3d};
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            o.write(firstByte);
            int n = payload.length;
            if (n < 126) o.write(0x80 | n);
            else if (n < 65536) { o.write(0x80 | 126); o.write(n >> 8); o.write(n); }
            else { o.write(0x80 | 127); for (int i = 7; i >= 0; i--) o.write((int) ((long) n >> (8 * i))); }
            o.writeBytes(key);
            for (int i = 0; i < n; i++) o.write(payload[i] ^ key[i & 3]);
            return o.toByteArray();
        }

        void send(byte[] raw) throws IOException {
            s.getOutputStream().write(raw);
            s.getOutputStream().flush();
        }

        Frame read() throws IOException {
            int b0 = in.read();
            if (b0 < 0) return null;
            int b1 = in.read();
            assertEquals(0, b1 & 0x80, "server frames are never masked");
            long len = b1 & 0x7F;
            if (len == 126) len = in.read() << 8 | in.read();
            else if (len == 127) { len = 0; for (int i = 0; i < 8; i++) len = len << 8 | in.read(); }
            byte[] p = in.readNBytes((int) len);
            return new Frame(b0 & 0x0F, (b0 & 0x80) != 0, p, (b0 & 0x40) != 0);
        }

        /** Reads until the server hangs up; returns the status of the last Close frame seen, or -1. */
        int closeStatusThenEof() throws IOException {
            int status = -1;
            Frame f;
            while ((f = read()) != null) {
                if (f.op() == 8) status = f.payload().length >= 2 ? (f.payload()[0] & 0xFF) << 8 | (f.payload()[1] & 0xFF) : 1005;
            }
            return status;
        }

        @Override
        public void close() throws IOException {
            s.close();
        }
    }

    // ---- RFC 6455 vectors ----

    @Test
    void acceptKeyMatchesTheRfcExample() {
        assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", WebSocketUpgrade.acceptKey(KEY));
    }

    @Test
    void rfcExampleFrames() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        try (Raw raw = new Raw(port(server), "")) {
            // RFC 6455 §5.7: masked text "Hello" -> echoed unmasked: 81 05 48 65 6c 6c 6f
            raw.send(new byte[]{(byte) 0x81, (byte) 0x85, 0x37, (byte) 0xfa, 0x21, 0x3d, 0x7f, (byte) 0x9f, 0x4d, 0x51, 0x58});
            Frame f = raw.read();
            assertEquals(1, f.op());
            assertEquals("Hello", new String(f.payload()));
            // masked ping "Hello" -> pong "Hello"
            raw.send(new byte[]{(byte) 0x89, (byte) 0x85, 0x37, (byte) 0xfa, 0x21, 0x3d, 0x7f, (byte) 0x9f, 0x4d, 0x51, 0x58});
            f = raw.read();
            assertEquals(10, f.op());
            assertEquals("Hello", new String(f.payload()));
            // fragmented "Hel" + "lo" (01 83 .. / 80 82 ..)
            raw.send(new byte[]{0x01, (byte) 0x83, 0x37, (byte) 0xfa, 0x21, 0x3d, 0x7f, (byte) 0x9f, 0x4d});
            raw.send(new byte[]{(byte) 0x80, (byte) 0x82, 0x37, (byte) 0xfa, 0x21, 0x3d, 0x5b, (byte) 0x95});
            f = raw.read();
            assertEquals("Hello", new String(f.payload()));
            // 256 and 65536 byte binary messages use the 16-bit and 64-bit length forms
            for (int size : new int[]{125, 126, 256, 65535, 65536, 70000}) {
                byte[] data = new byte[size];
                new java.util.Random(size).nextBytes(data);
                raw.send(Raw.frame(0x82, data));
                f = raw.read();
                assertEquals(2, f.op());
                assertArrayEquals(data, f.payload(), "size " + size);
            }
            // close handshake: the reply echoes the status code
            raw.send(Raw.frame(0x88, new byte[]{0x03, (byte) 0xe8, 'b', 'y', 'e'}));
            assertEquals(1000, raw.closeStatusThenEof());
        }
    }

    // ---- violations ----

    @Test
    void protocolViolationsCloseWithTheRightStatus() throws Exception {
        record Case(String name, byte[] bytes, int status) {
        }
        byte[] big = new byte[126];
        List<Case> cases = List.of(
                new Case("unmasked client frame", new byte[]{(byte) 0x81, 0x01, 'x'}, 1002),
                new Case("RSV1 set", Raw.frame(0xC1, utf8("x")), 1002),
                new Case("RSV2 set", Raw.frame(0xA1, utf8("x")), 1002),
                new Case("reserved opcode 3", Raw.frame(0x83, utf8("x")), 1002),
                new Case("reserved opcode 11", Raw.frame(0x8B, utf8("x")), 1002),
                new Case("ping longer than 125", Raw.frame(0x89, big), 1002),
                new Case("fragmented ping", Raw.frame(0x09, utf8("x")), 1002),
                new Case("fragmented close", Raw.frame(0x08, new byte[]{0x03, (byte) 0xe8}), 1002),
                new Case("continuation without start", Raw.frame(0x80, utf8("x")), 1002),
                new Case("new data frame inside a fragmented message", concat(Raw.frame(0x01, utf8("a")), Raw.frame(0x81, utf8("b"))), 1002),
                new Case("non-minimal 16-bit length", new byte[]{(byte) 0x81, (byte) (0x80 | 126), 0, 5, 1, 2, 3, 4, 'h', 'e', 'l', 'l', 'o'}, 1002),
                new Case("64-bit length with top bit", new byte[]{(byte) 0x82, (byte) (0x80 | 127), (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4}, 1002),
                new Case("invalid UTF-8 text", Raw.frame(0x81, new byte[]{(byte) 0xff, (byte) 0xfe}), 1007),
                new Case("invalid UTF-8 across fragments", concat(Raw.frame(0x01, new byte[]{(byte) 0xce}), Raw.frame(0x80, new byte[]{'x'})), 1007),
                new Case("close with a 1-byte payload", Raw.frame(0x88, new byte[]{0x03}), 1002),
                new Case("close code 1005 on the wire", Raw.frame(0x88, new byte[]{0x03, (byte) 0xed}), 1002),
                new Case("close code 999", Raw.frame(0x88, new byte[]{0x03, (byte) 0xe7}), 1002),
                new Case("close code 1004", Raw.frame(0x88, new byte[]{0x03, (byte) 0xec}), 1002),
                new Case("invalid UTF-8 close reason", Raw.frame(0x88, new byte[]{0x03, (byte) 0xe8, (byte) 0xff}), 1007),
                new Case("message over the limit", Raw.frame(0x82, new byte[300]), 1009));
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(256), () -> new Recorder(true));
        for (Case c : cases) {
            try (Raw raw = new Raw(port(server), "")) {
                raw.send(c.bytes());
                assertEquals(c.status(), raw.closeStatusThenEof(), c.name());
            }
        }
    }

    @Test
    void invalidUtf8IsRejectedAtTheBadFragmentNotAtTheEnd() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x01, utf8("fine so far ")));         // text, not final
            raw.send(Raw.frame(0x00, new byte[]{(byte) 0xff}));      // continuation, not final, invalid byte
            // no FIN frame is ever sent: the server must not wait for one
            assertEquals(1007, raw.closeStatusThenEof());
        }
        try (Raw raw = new Raw(port(server), "")) { // a character split across fragments is fine
            byte[] euro = utf8("€");                                  // e2 82 ac
            raw.send(Raw.frame(0x01, new byte[]{euro[0]}));
            raw.send(Raw.frame(0x00, new byte[]{euro[1]}));
            raw.send(Raw.frame(0x80, new byte[]{euro[2]}));
            Frame f = raw.read();
            assertEquals("€", new String(f.payload(), StandardCharsets.UTF_8));
        }
        try (Raw raw = new Raw(port(server), "")) { // ...but a truncated one at the end is not
            raw.send(Raw.frame(0x01, new byte[]{utf8("€")[0]}));
            raw.send(Raw.frame(0x80, new byte[]{utf8("€")[1]}));
            assertEquals(1007, raw.closeStatusThenEof());
        }
    }

    @Test
    void invalidUtf8InsideAFrameThatIsStillArrivingClosesImmediately() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        try (Raw raw = new Raw(port(server), "")) {
            byte[] payload = new byte[100];
            java.util.Arrays.fill(payload, (byte) 'a');
            payload[10] = (byte) 0xff; // bad byte early; the remaining 80 bytes are never sent
            byte[] frame = Raw.frame(0x81, payload);
            raw.send(java.util.Arrays.copyOf(frame, 6 + 20)); // header + mask + first 20 payload bytes only
            assertEquals(1007, raw.closeStatusThenEof());
        }
        try (Raw raw = new Raw(port(server), "")) { // valid text trickling in byte by byte still works, masks stay aligned
            byte[] frame = Raw.frame(0x81, utf8("Việt Nam ✓ xin chào"));
            for (byte b : frame) {
                raw.send(new byte[]{b});
                Thread.sleep(2);
            }
            assertEquals("Việt Nam ✓ xin chào", new String(raw.read().payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void totalFragmentedSizeIsLimitedToo() throws Exception {
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(256), () -> new Recorder(true));
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x02, new byte[200]));
            raw.send(Raw.frame(0x00, new byte[200]));
            assertEquals(1009, raw.closeStatusThenEof());
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    @Test
    void abnormalDisconnectIsReportedAs1006() throws Exception {
        Recorder rec = new Recorder(true);
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        Raw raw = new Raw(port(server), "");
        raw.close(); // no Close frame
        assertEquals(1006, rec.awaitClose());
    }

    @Test
    void listenerExceptionBecomes1011() throws Exception {
        Recorder rec = new Recorder(false) {
            @Override
            public void onText(WebSocket ws, String text) {
                throw new IllegalStateException("app bug");
            }
        };
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x81, utf8("boom")));
            assertEquals(1011, raw.closeStatusThenEof());
        }
        assertEquals(1011, rec.awaitClose());
        assertEquals("app bug", rec.errors.get(0).getMessage());
    }

    // ---- handshake validation ----

    private static String request(int port, String path, String method, String headers) throws IOException {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(5000);
            s.getOutputStream().write((method + " " + path + " HTTP/1.1\r\nHost: 127.0.0.1:" + port + "\r\nConnection: close\r\n" + headers + "\r\n")
                    .getBytes(StandardCharsets.ISO_8859_1));
            InputStream in = s.getInputStream();
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) >= 0) { // the head first: a 101 keeps the connection open, so never read to EOF there
                b.write(c);
                byte[] a = b.toByteArray();
                int n = a.length;
                if (n >= 4 && a[n - 4] == '\r' && a[n - 3] == '\n' && a[n - 2] == '\r' && a[n - 1] == '\n') break;
            }
            String head = b.toString(StandardCharsets.ISO_8859_1);
            if (head.startsWith("HTTP/1.1 101")) {
                return head;
            }
            return head + new String(in.readAllBytes(), StandardCharsets.ISO_8859_1); // error responses end with the connection
        }
    }

    @Test
    void handshakeRequestsAreValidated() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        int p = port(server);
        String ok = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + KEY + "\r\n";
        assertTrue(request(p, "/ws", "GET", ok).startsWith("HTTP/1.1 101"));
        assertTrue(request(p, "/ws", "GET", "").startsWith("HTTP/1.1 400"), "plain GET to a websocket route");
        String wrongVersion = request(p, "/ws", "GET", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 8\r\nSec-WebSocket-Key: " + KEY + "\r\n");
        assertTrue(wrongVersion.startsWith("HTTP/1.1 426") && wrongVersion.contains("Sec-WebSocket-Version: 13"), wrongVersion);
        assertTrue(request(p, "/ws", "GET", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n").startsWith("HTTP/1.1 400"), "no key");
        assertTrue(request(p, "/ws", "GET", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: c2hvcnQ=\r\n").startsWith("HTTP/1.1 400"), "key is not 16 bytes");
        assertTrue(request(p, "/ws", "GET", "Upgrade: websocket\r\nConnection: keep-alive\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + KEY + "\r\n").startsWith("HTTP/1.1 400"), "Connection lacks Upgrade");
        assertTrue(request(p, "/ws", "GET", ok + "Sec-WebSocket-Key: " + KEY + "\r\n").startsWith("HTTP/1.1 400"), "duplicate key header");
        assertTrue(request(p, "/ws", "GET", ok + "Origin: http://evil.example\r\n").startsWith("HTTP/1.1 403"), "cross-site origin");
        assertTrue(request(p, "/ws", "GET", ok + "Origin: null\r\n").startsWith("HTTP/1.1 403"));
        assertTrue(request(p, "/ws", "GET", ok + "Origin: http://127.0.0.1:" + p + "\r\n").startsWith("HTTP/1.1 101"), "same origin is fine");
        assertTrue(request(p, "/plain", "GET", "").contains("just http"), "normal routes keep working");
    }

    @Test
    void anyOriginCanBeAllowedExplicitly() throws Exception {
        Server server = start(WebSocketConfig.defaults().allowAnyOrigin(), () -> new Recorder(true));
        String ok = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + KEY + "\r\nOrigin: http://anywhere.example\r\n";
        assertTrue(request(port(server), "/ws", "GET", ok).startsWith("HTTP/1.1 101"));
    }

    @Test
    void subprotocolNegotiationPrefersTheServersOrder() throws Exception {
        Server server = start(WebSocketConfig.defaults().withSubprotocols("v2", "v1"), () -> new Recorder(true));
        String base = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + KEY + "\r\n";
        assertTrue(request(port(server), "/ws", "GET", base + "Sec-WebSocket-Protocol: v1, v2\r\n").contains("Sec-WebSocket-Protocol: v2"));
        assertFalse(request(port(server), "/ws", "GET", base + "Sec-WebSocket-Protocol: chat\r\n").contains("Sec-WebSocket-Protocol"));
    }

    // ---- interoperability: the JDK's independent client ----

    private static final class JdkListener implements java.net.http.WebSocket.Listener {
        final List<String> texts = Collections.synchronizedList(new ArrayList<>());
        final List<byte[]> binaries = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger pongs = new AtomicInteger();
        final CompletableFuture<Object[]> closed = new CompletableFuture<>();
        private final StringBuilder partial = new StringBuilder();
        private final ByteArrayOutputStream partialBin = new ByteArrayOutputStream();

        @Override
        public CompletionStage<?> onText(java.net.http.WebSocket ws, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                texts.add(partial.toString());
                partial.setLength(0);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(java.net.http.WebSocket ws, ByteBuffer data, boolean last) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            partialBin.writeBytes(b);
            if (last) {
                binaries.add(partialBin.toByteArray());
                partialBin.reset();
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(java.net.http.WebSocket ws, ByteBuffer message) {
            pongs.incrementAndGet();
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(java.net.http.WebSocket ws, int statusCode, String reason) {
            closed.complete(new Object[]{statusCode, reason});
            return null;
        }
    }

    @Test
    void jdkClientTalksToOurServer() throws Exception {
        Recorder rec = new Recorder(true);
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        JdkListener l = new JdkListener();
        var ws = java.net.http.HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port(server) + "/ws"), l).get(10, TimeUnit.SECONDS);
        ws.sendText("héllo wörld ✓", true).get(5, TimeUnit.SECONDS);
        byte[] bin = new byte[100_000];
        new java.util.Random(3).nextBytes(bin);
        ws.sendBinary(ByteBuffer.wrap(bin), true).get(5, TimeUnit.SECONDS);
        ws.sendText("frag-", false).get(5, TimeUnit.SECONDS);
        ws.sendText("men", false).get(5, TimeUnit.SECONDS);
        ws.sendText("ted", true).get(5, TimeUnit.SECONDS);
        ws.sendPing(ByteBuffer.wrap(utf8("p"))).get(5, TimeUnit.SECONDS);
        for (int i = 0; i < 50 && (l.texts.size() < 2 || l.binaries.isEmpty() || l.pongs.get() < 1); i++) Thread.sleep(100);
        assertEquals(List.of("héllo wörld ✓", "frag-mented"), l.texts);
        assertArrayEquals(bin, l.binaries.get(0));
        assertEquals(1, l.pongs.get());
        ws.sendClose(4001, "going away").get(5, TimeUnit.SECONDS);
        Object[] closed = l.closed.get(10, TimeUnit.SECONDS);
        assertEquals(4001, closed[0]);
        assertEquals("", closed[1], "our server echoes the status code only; the reason is not required in the reply");
        assertEquals(4001, rec.awaitClose());
        assertEquals("going away", rec.closedReason.get(5, TimeUnit.SECONDS));
    }

    // ---- Hydrangea client <-> Hydrangea server ----

    private CompletableFuture<WebSocket> connect(Server server, Recorder listener, WebSocketConfig cfg) {
        return WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", listener, cfg);
    }

    @Test
    void ourClientEchoTextBinaryAndLargeMessages() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        Recorder client = new Recorder(false);
        WebSocket ws = connect(server, client, WebSocketConfig.defaults()).get(10, TimeUnit.SECONDS);
        client.opened.get(5, TimeUnit.SECONDS);
        assertTrue(ws.isOpen());
        ws.sendText("Việt Nam ✓");
        byte[] big = new byte[900_000];
        new java.util.Random(9).nextBytes(big);
        ws.sendBinary(ByteBuffer.wrap(big));
        for (int i = 0; i < 100 && (client.texts.isEmpty() || client.binaries.isEmpty()); i++) Thread.sleep(50);
        assertEquals(List.of("Việt Nam ✓"), client.texts);
        assertArrayEquals(big, client.binaries.get(0));
        ws.sendPing(ByteBuffer.wrap(utf8("x")));
        for (int i = 0; i < 100 && client.pongs.get() == 0; i++) Thread.sleep(20);
        assertEquals(1, client.pongs.get());
    }

    @Test
    void connectionsMessagesBytesAndCloseCodesAreCountedOnBothSides() throws Exception {
        io.hydrangea.CaptureSink server = new io.hydrangea.CaptureSink();
        io.hydrangea.CaptureSink client = new io.hydrangea.CaptureSink();
        serverGroup.metrics().bindTo(server);
        clientGroup.metrics().bindTo(client);
        Recorder serverSide = new Recorder(true);
        Server s = start(WebSocketConfig.defaults(), () -> serverSide);
        Recorder rec = new Recorder(false);
        WebSocket ws = connect(s, rec, WebSocketConfig.defaults()).get(10, TimeUnit.SECONDS);
        rec.opened.get(5, TimeUnit.SECONDS);
        io.hydrangea.CaptureSink.eventually(() -> server.get("websocket.connections{side,server}") == 1 && client.get("websocket.connections{side,client}") == 1, "both ends open");
        for (int i = 0; i < 3; i++) {
            ws.sendText("hello");   // 5 bytes each
        }
        io.hydrangea.CaptureSink.eventually(() -> rec.texts.size() == 3, "the echoes");
        assertEquals(3, client.get("websocket.messages{side,client,direction,sent}"));
        assertEquals(3, client.get("websocket.messages{side,client,direction,received}"));
        assertEquals(15, client.get("websocket.bytes{side,client,direction,sent}"));
        assertEquals(3, server.get("websocket.messages{side,server,direction,received}"));
        assertEquals(3, server.get("websocket.messages{side,server,direction,sent}"));
        assertEquals(15, server.get("websocket.bytes{side,server,direction,received}"));
        ws.close(1000, "done").get(10, TimeUnit.SECONDS);
        rec.awaitClose();
        serverSide.awaitClose();
        io.hydrangea.CaptureSink.eventually(() -> server.get("websocket.connections{side,server}") == 0 && client.get("websocket.connections{side,client}") == 0, "both ends gone");
        assertEquals(1, server.get("websocket.closes{side,server,code,1000}"));
        assertEquals(1, client.get("websocket.closes{side,client,code,1000}"));
    }

    @Test
    void closeHandshakeInitiatedByTheClient() throws Exception {
        Recorder serverSide = new Recorder(true);
        Server server = start(WebSocketConfig.defaults(), () -> serverSide);
        Recorder client = new Recorder(false);
        WebSocket ws = connect(server, client, WebSocketConfig.defaults()).get(10, TimeUnit.SECONDS);
        ws.close(1000, "done").get(10, TimeUnit.SECONDS);
        assertEquals(1000, client.awaitClose());
        assertEquals(1000, serverSide.awaitClose());
        assertEquals("done", serverSide.closedReason.get(5, TimeUnit.SECONDS));
        assertFalse(ws.isOpen());
        assertInstanceOf(ClosedChannelException.class, failure(ws.sendText("late")));
        assertEquals(List.of("open", "close:1000"), client.events, "exactly one open and one close");
    }

    @Test
    void closeHandshakeInitiatedByTheServer() throws Exception {
        Recorder serverSide = new Recorder(false) {
            @Override
            public void onOpen(WebSocket ws) {
                super.onOpen(ws);
                ws.close(1001, "server restarting");
            }
        };
        Server server = start(WebSocketConfig.defaults(), () -> serverSide);
        Recorder client = new Recorder(false);
        connect(server, client, WebSocketConfig.defaults()).get(10, TimeUnit.SECONDS);
        assertEquals(1001, client.awaitClose());
        assertEquals("server restarting", client.closedReason.get(5, TimeUnit.SECONDS));
        assertEquals(1001, serverSide.awaitClose());
    }

    @Test
    void closeWithoutAnswerEndsAfterTheCloseTimeoutWithCode1006() throws Exception {
        // a "server" that completes the handshake and then goes silent
        var ss = new java.net.ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        Thread t = Thread.startVirtualThread(() -> {
            try (Socket s = ss.accept()) {
                var in = s.getInputStream();
                ByteArrayOutputStream head = new ByteArrayOutputStream();
                while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) head.write(in.read());
                String key = head.toString(StandardCharsets.ISO_8859_1).lines().filter(x -> x.startsWith("Sec-WebSocket-Key")).findFirst().get().split(": ")[1];
                s.getOutputStream().write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                        + WebSocketUpgrade.acceptKey(key) + "\r\n\r\n").getBytes());
                while (in.read() >= 0) { /* never answer */ }
            } catch (IOException ignored) { }
        });
        Recorder client = new Recorder(false);
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + ss.getLocalPort() + "/", client,
                WebSocketConfig.defaults().withCloseTimeout(Duration.ofMillis(300))).get(10, TimeUnit.SECONDS);
        long t0 = System.nanoTime();
        ws.close(1000, "bye").get(10, TimeUnit.SECONDS);
        assertEquals(1006, client.awaitClose(), "no Close frame came back");
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 3000);
    }

    @Test
    void subprotocolIsReportedOnBothSides() throws Exception {
        Server server = start(WebSocketConfig.defaults().withSubprotocols("v2"), () -> new Recorder(true));
        Recorder client = new Recorder(false);
        WebSocket ws = connect(server, client, WebSocketConfig.defaults().withSubprotocols("v1", "v2")).get(10, TimeUnit.SECONDS);
        assertEquals("v2", ws.subprotocol());
        Recorder none = new Recorder(false);
        assertNull(connect(server, none, WebSocketConfig.defaults().withSubprotocols("other")).get(10, TimeUnit.SECONDS).subprotocol());
    }

    @Test
    void handshakeRefusalsSurfaceAsExceptionsWithTheStatus() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        // 404: no such route
        var notFound = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/nothing", new Recorder(false));
        var e1 = assertInstanceOf(WebSocketHandshakeException.class, failure(notFound));
        assertEquals(404, e1.status());
        // 403: origin policy
        var forbidden = connect(server, new Recorder(false), WebSocketConfig.defaults().withHeaders(Map.of("Origin", "http://evil.example")));
        assertEquals(403, ((WebSocketHandshakeException) failure(forbidden)).status());
        // a normal HTTP route answers 400 to a websocket handshake
        var plain = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/plain", new Recorder(false));
        assertEquals(200, ((WebSocketHandshakeException) failure(plain)).status());
        // connection refused
        int dead;
        try (var tmp = new java.net.ServerSocket(0)) {
            dead = tmp.getLocalPort();
        }
        assertInstanceOf(IOException.class, failure(WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + dead + "/", new Recorder(false))));
    }

    @Test
    void clientRejectsBadUrlsAndReservedHeaders() {
        assertInstanceOf(IllegalArgumentException.class, failure(WebSocketClient.connect(clientGroup, "http://x/", new Recorder(false))));
        assertInstanceOf(IllegalArgumentException.class, failure(WebSocketClient.connect(clientGroup, "ws://user:pw@x/", new Recorder(false))));
        assertInstanceOf(IllegalArgumentException.class, failure(WebSocketClient.connect(clientGroup, "ws://127.0.0.1:1/", new Recorder(false),
                WebSocketConfig.defaults().withHeaders(Map.of("Sec-WebSocket-Key", "x")))));
        assertInstanceOf(IllegalArgumentException.class, failure(WebSocketClient.connect(clientGroup, "ws://127.0.0.1:1/", new Recorder(false),
                WebSocketConfig.defaults().withHeaders(Map.of("X-A", "a\r\nInjected: 1")))));
    }

    @Test
    void handshakeTimeoutAgainstAServerThatNeverAnswers() throws Exception {
        var ss = new java.net.ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        Thread.startVirtualThread(() -> {
            try (Socket s = ss.accept()) {
                s.getInputStream().readAllBytes();
            } catch (IOException ignored) { }
        });
        var f = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + ss.getLocalPort() + "/", new Recorder(false),
                WebSocketConfig.defaults().withHandshakeTimeout(Duration.ofMillis(300)));
        assertInstanceOf(java.net.SocketTimeoutException.class, failure(f));
    }

    @Test
    void sendingTooMuchIsRejectedLocallyAndReceivingTooMuchCloses1009() throws Exception {
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(1000), () -> new Recorder(true));
        Recorder client = new Recorder(false);
        WebSocket ws = connect(server, client, WebSocketConfig.defaults().withMaxMessageBytes(5000)).get(10, TimeUnit.SECONDS);
        assertThrows(IllegalArgumentException.class, () -> ws.sendBinary(ByteBuffer.allocate(5001)));
        ws.sendBinary(ByteBuffer.allocate(2000)); // fine for us, too big for the server
        assertEquals(1009, client.awaitClose());
    }

    @Test
    void manyMessagesFromSeveralThreadsKeepPerThreadOrder() throws Exception {
        Server server = start(WebSocketConfig.defaults(), () -> new Recorder(true));
        Recorder client = new Recorder(false);
        WebSocket ws = connect(server, client, WebSocketConfig.defaults()).get(10, TimeUnit.SECONDS);
        int threads = 4, per = 5000;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            ts.add(Thread.startVirtualThread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < per; i++) ws.sendText(id + ":" + i);
            }));
        }
        go.countDown();
        for (int i = 0; i < 300 && client.texts.size() < threads * per; i++) Thread.sleep(50);
        assertEquals(threads * per, client.texts.size());
        int[] next = new int[threads];
        for (String s : new ArrayList<>(client.texts)) {
            String[] p = s.split(":");
            int id = Integer.parseInt(p[0]);
            assertEquals(next[id]++, Integer.parseInt(p[1]), "messages of one sender must stay in order");
        }
    }

    // ---- keep-alive ----

    @Test
    void serverPingsAreAnsweredAndSilentPeersAreDropped() throws Exception {
        Recorder serverSide = new Recorder(false);
        Server server = start(WebSocketConfig.defaults().withPingInterval(Duration.ofMillis(100)), () -> serverSide);
        JdkListener l = new JdkListener();
        java.net.http.HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + port(server) + "/ws"), l).get(10, TimeUnit.SECONDS);
        for (int i = 0; i < 50 && serverSide.pongs.get() < 3; i++) Thread.sleep(50);
        assertTrue(serverSide.pongs.get() >= 3, "the JDK client answers pings automatically, pongs=" + serverSide.pongs.get());
        assertFalse(serverSide.closedCode.isDone());

        // a peer that never reads or answers is dropped after two intervals without traffic
        Recorder silentSide = new Recorder(false);
        Server server2 = start(WebSocketConfig.defaults().withPingInterval(Duration.ofMillis(100)), () -> silentSide);
        try (Raw raw = new Raw(port(server2), "")) {
            assertEquals(1006, silentSide.awaitClose());
            assertFalse(silentSide.errors.isEmpty());
            assertInstanceOf(java.net.SocketTimeoutException.class, silentSide.errors.get(0));
        }
    }

    // ---- TLS ----

    private record Tls(SSLContext server, SSLContext client) {
    }

    private static Tls tls(Path dir) throws Exception {
        Path ks = dir.resolve("t.p12");
        Process pr = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "t", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost",
                "-ext", "san=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit")
                .redirectErrorStream(true).start();
        pr.getInputStream().readAllBytes();
        assertEquals(0, pr.waitFor());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(store);
        SSLContext s = SSLContext.getInstance("TLS");
        s.init(kmf.getKeyManagers(), null, null);
        SSLContext c = SSLContext.getInstance("TLS");
        c.init(null, tmf.getTrustManagers(), null);
        return new Tls(s, c);
    }

    @Test
    void secureWebSocketWithOurClientAndWithTheJdkClient(@TempDir Path dir) throws Exception {
        Tls t = tls(dir);
        Server server = start(Options.defaults(), t.server(), WebSocketConfig.defaults(), () -> new Recorder(true));
        int p = port(server);
        Recorder client = new Recorder(false);
        WebSocket ws = WebSocketClient.connect(clientGroup, "wss://localhost:" + p + "/ws", client,
                WebSocketConfig.defaults().withSslContext(t.client())).get(10, TimeUnit.SECONDS);
        byte[] big = new byte[200_000];
        new java.util.Random(5).nextBytes(big);
        ws.sendText("over tls");
        ws.sendBinary(ByteBuffer.wrap(big));
        for (int i = 0; i < 100 && (client.texts.isEmpty() || client.binaries.isEmpty()); i++) Thread.sleep(50);
        assertEquals("over tls", client.texts.get(0));
        assertArrayEquals(big, client.binaries.get(0));
        ws.close().get(10, TimeUnit.SECONDS);
        assertEquals(1000, client.awaitClose());

        JdkListener l = new JdkListener();
        var jdk = java.net.http.HttpClient.newBuilder().sslContext(t.client()).build().newWebSocketBuilder()
                .buildAsync(URI.create("wss://localhost:" + p + "/ws"), l).get(10, TimeUnit.SECONDS);
        jdk.sendText("jdk over tls", true).get(5, TimeUnit.SECONDS);
        for (int i = 0; i < 100 && l.texts.isEmpty(); i++) Thread.sleep(50);
        assertEquals(List.of("jdk over tls"), l.texts);

        // untrusted certificate: the default trust store does not know it
        assertInstanceOf(IOException.class, failure(WebSocketClient.connect(clientGroup, "wss://localhost:" + p + "/ws", new Recorder(false))));
    }

    private static Throwable failure(CompletableFuture<?> f) {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        }
        throw new AssertionError("expected failure");
    }
}
