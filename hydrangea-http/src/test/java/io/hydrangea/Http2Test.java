package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpHandler;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http2.HpackDecoder;
import io.hydrangea.http2.HpackEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HTTP/2 against independent peers: a raw-frame peer written here (so every rule can be provoked precisely), and the JDK's own HTTP/2
 * client over TLS/ALPN. The h2spec conformance suite was run separately (147 tests, see the README).
 */
class Http2Test {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private static final int DATA = 0, HEADERS = 1, PRIORITY = 2, RST_STREAM = 3, SETTINGS = 4, PING = 6, GOAWAY = 7, WINDOW_UPDATE = 8, CONTINUATION = 9;
    private static final int END_STREAM = 1, END_HEADERS = 4, ACK = 1;
    private static final int PROTOCOL_ERROR = 1, REFUSED_STREAM = 7, ENHANCE_YOUR_CALM = 11, STREAM_CLOSED = 5;

    private EventLoopGroup group;
    private final List<CompletableFuture<HttpResponse>> pending = new ArrayList<>();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    // ---- a minimal frame-level HTTP/2 peer ----

    record Frame(int type, int flags, int sid, byte[] payload) {
        int errorCode() {
            return type == RST_STREAM ? ByteBuffer.wrap(payload).getInt() : ByteBuffer.wrap(payload).getInt(4);
        }
    }

    private static final Frame EOF = new Frame(-1, 0, 0, new byte[0]);

    static final class Peer implements AutoCloseable {
        final Socket socket;
        final OutputStream out;
        final BlockingQueue<Frame> frames = new LinkedBlockingQueue<>();
        final HpackEncoder enc = new HpackEncoder();
        final HpackDecoder dec = new HpackDecoder(4096, 1 << 20);
        final java.util.Map<Integer, Long> serverSettings = new java.util.HashMap<>();

        Peer(Socket socket, boolean sendPreface) throws IOException {
            this(socket, sendPreface, false);
        }

        /** @param skipClientPreface true when this peer plays the server: the 24-byte client preface precedes the first frame */
        Peer(Socket socket, boolean sendPreface, boolean skipClientPreface) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            Thread reader = new Thread(() -> {
                try {
                    if (skipClientPreface) {
                        in.readFully(new byte[24]);
                    }
                    while (true) {
                        byte[] h = new byte[9];
                        in.readFully(h);
                        int len = (h[0] & 0xff) << 16 | (h[1] & 0xff) << 8 | (h[2] & 0xff);
                        byte[] payload = new byte[len];
                        in.readFully(payload);
                        frames.add(new Frame(h[3] & 0xff, h[4] & 0xff, ByteBuffer.wrap(h).getInt(5) & 0x7fffffff, payload));
                    }
                } catch (IOException e) {
                    frames.add(EOF);
                }
            });
            reader.setDaemon(true);
            reader.start();
            if (sendPreface) {
                out.write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            }
        }

        static Peer connect(int port) throws IOException {
            return new Peer(new Socket("127.0.0.1", port), true);
        }

        void send(int type, int flags, int sid, byte[] payload) throws IOException {
            ByteBuffer b = ByteBuffer.allocate(9 + payload.length);
            b.put((byte) (payload.length >>> 16)).put((byte) (payload.length >>> 8)).put((byte) payload.length).put((byte) type).put((byte) flags)
                    .putInt(sid).put(payload);
            out.write(b.array());
            out.flush();
        }

        /** SETTINGS with (id, value) pairs. */
        void settings(int... idValue) throws IOException {
            ByteBuffer b = ByteBuffer.allocate(idValue.length * 3);
            for (int i = 0; i < idValue.length; i += 2) {
                b.putShort((short) idValue[i]).putInt(idValue[i + 1]);
            }
            send(SETTINGS, 0, 0, b.array());
        }

        void windowUpdate(int sid, int inc) throws IOException {
            send(WINDOW_UPDATE, 0, sid, ByteBuffer.allocate(4).putInt(inc).array());
        }

        void ping(byte[] payload) throws IOException {
            send(6, 1, 0, payload);   // PING with the ACK flag
        }

        void rst(int sid, int code) throws IOException {
            send(RST_STREAM, 0, sid, ByteBuffer.allocate(4).putInt(code).array());
        }

        /** HEADERS carrying name, value pairs (names are sent exactly as given). */
        void headers(int sid, boolean endStream, boolean endHeaders, String... nv) throws IOException {
            enc.reset();
            for (int i = 0; i < nv.length; i += 2) {
                enc.header(nv[i], nv[i + 1]);
            }
            send(HEADERS, (endStream ? END_STREAM : 0) | (endHeaders ? END_HEADERS : 0), sid, enc.toByteArray());
        }

        void get(int sid, String path) throws IOException {
            headers(sid, true, true, ":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", path);
        }

        void post(int sid, String path, boolean endStream, String... extra) throws IOException {
            String[] base = {":method", "POST", ":scheme", "http", ":authority", "localhost", ":path", path};
            String[] all = Arrays.copyOf(base, base.length + extra.length);
            System.arraycopy(extra, 0, all, base.length, extra.length);
            headers(sid, endStream, true, all);
        }

        void data(int sid, boolean end, byte[] d) throws IOException {
            send(DATA, end ? END_STREAM : 0, sid, d);
        }

        /** Completes the connection set-up: reads the server's SETTINGS (and acknowledges them) and the acknowledgement of ours. */
        Peer handshake(int... mySettings) throws IOException {
            settings(mySettings);
            boolean sawSettings = false, sawAck = false;
            while (!(sawSettings && sawAck)) {
                Frame f = next();
                if (f.type() == SETTINGS && (f.flags() & ACK) != 0) {
                    sawAck = true;
                } else if (f.type() == SETTINGS) {
                    sawSettings = true;
                    ByteBuffer sb = ByteBuffer.wrap(f.payload());
                    while (sb.remaining() >= 6) {
                        serverSettings.put((int) sb.getShort() & 0xffff, sb.getInt() & 0xffffffffL);
                    }
                    send(SETTINGS, ACK, 0, new byte[0]);
                }
            }
            return this;
        }

        Frame next() {
            return next(5000);
        }

        Frame next(long ms) {
            try {
                Frame f = frames.poll(ms, TimeUnit.MILLISECONDS);
                assertNotNull(f, "no frame within " + ms + " ms");
                return f;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        Frame pollOrNull(long ms) {
            try {
                return frames.poll(ms, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        /** The next frame that is not WINDOW_UPDATE or SETTINGS bookkeeping. */
        Frame nextInteresting() {
            for (;;) {
                Frame f = next();
                if (f.type() != WINDOW_UPDATE && f.type() != SETTINGS) {
                    return f;
                }
            }
        }

        /** Reads one complete response: status and body. Fails on anything unexpected. */
        Response response(int sid) throws Exception {
            Response r = new Response();
            for (;;) {
                Frame f = nextInteresting();
                if (f.type() == HEADERS) {
                    assertEquals(sid, f.sid());
                    byte[] block = f.payload();
                    List<String> hs = new ArrayList<>();
                    dec.decode(block, (n, v) -> hs.add(n + ": " + v));
                    for (String h : hs) {
                        if (h.startsWith(":status: ")) {
                            r.status = Integer.parseInt(h.substring(9));
                        }
                    }
                    r.headers.addAll(hs);
                } else if (f.type() == DATA) {
                    r.body.write(f.payload(), 0, f.payload().length);
                } else if (f.type() == RST_STREAM && f.sid() == sid) {
                    r.reset = f.errorCode();
                } else if (f.type() == RST_STREAM && f.errorCode() == 0) {
                    continue;   // the NO_ERROR reset that follows an early answer on another stream
                } else if (f.type() == EOF.type()) {
                    fail("connection closed before the response of stream " + sid + " ended");
                } else {
                    fail("unexpected frame type " + f.type());
                }
                boolean ended = (f.type() == HEADERS || f.type() == DATA) && (f.flags() & END_STREAM) != 0;
                if (ended || r.reset >= 0) {
                    return r;
                }
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    static final class Response {
        int status;
        int reset = -1;
        final List<String> headers = new ArrayList<>();
        final java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();

        String text() {
            return body.toString(StandardCharsets.UTF_8);
        }
    }

    private Server server(HttpConfig cfg, HttpHandler handler) throws IOException {
        return HttpServer.bind(group, ANY, Options.defaults(), null, cfg, handler);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private HttpHandler handler() {
        HttpRouter router = new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version()))
                .get("/users/{id}", r -> HttpResponse.ok("user " + r.pathVariable("id") + " q=" + r.param("q", "-")))
                .post("/echo", r -> HttpResponse.status(200).body(r.body()))
                .get("/big/{n}", r -> HttpResponse.status(200).body(new byte[r.pathInt("n")]));
        return request -> {
            if (request.path().equals("/slow")) {
                CompletableFuture<HttpResponse> f = new CompletableFuture<>();
                synchronized (pending) {
                    pending.add(f);
                }
                return f;
            }
            return router.handle(request);
        };
    }

    private HttpConfig h2() {
        return HttpConfig.defaults().withHttp2(Http2Config.defaults());
    }

    // ---- the protocol works ----

    @Test
    void servesRequestsOverCleartextHttp2() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/hello");
            Response r = p.response(1);
            assertEquals(200, r.status);
            assertEquals("hello over HTTP/2", r.text());
            assertTrue(r.headers.stream().anyMatch(h -> h.startsWith("content-length: 17")), r.headers.toString());

            p.get(3, "/users/42?q=hi");                      // routing, path variables and query work as with HTTP/1.1
            assertEquals("user 42 q=hi", p.response(3).text());

            p.post(5, "/echo", false, "content-length", "11");  // a body in two DATA frames
            p.data(5, false, "hello ".getBytes());
            p.data(5, true, "world".getBytes());
            assertEquals("hello world", p.response(5).text());

            p.get(7, "/nothing");
            assertEquals(404, p.response(7).status);
        }
    }

    @Test
    void http11StillWorksOnTheSamePort() throws Exception {
        try (Server server = server(h2(), handler())) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            var res = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/hello")).build(), BodyHandlers.ofString());
            assertEquals("hello over HTTP/1.1", res.body());
        }
    }

    @Test
    void http2IsOffUnlessEnabled() throws Exception {
        try (Server server = server(HttpConfig.defaults(), handler()); Socket s = new Socket("127.0.0.1", port(server))) {
            s.setSoTimeout(3000);
            s.getOutputStream().write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            String answer = new String(s.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
            assertTrue(answer.startsWith("HTTP/1.1 "), "treated as a (bad) HTTP/1.1 request, got: " + answer);
        }
    }

    @Test
    void manyStreamsInParallelOnOneConnection() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            for (int i = 0; i < 50; i++) {
                p.get(1 + 2 * i, "/users/" + i);
            }
            int seen = 0;
            while (seen < 50) {
                Frame f = p.nextInteresting();
                if (f.type() == DATA) {
                    assertTrue(new String(f.payload()).startsWith("user "));
                    seen++;
                }
            }
        }
    }

    @Test
    void jdkHttp2ClientOverTlsNegotiatesH2(@TempDir Path dir) throws Exception {
        SSLContext[] ctx = TestTls.tls(dir);
        try (Server server = HttpServer.bind(group, ANY, Options.defaults(), ctx[0], h2(), handler())) {
            HttpClient client = HttpClient.newBuilder().sslContext(ctx[1]).version(HttpClient.Version.HTTP_2).build();
            String base = "https://localhost:" + port(server);
            var hello = client.send(HttpRequest.newBuilder(URI.create(base + "/hello")).build(), BodyHandlers.ofString());
            assertEquals(HttpClient.Version.HTTP_2, hello.version());
            assertEquals("hello over HTTP/2", hello.body());

            byte[] payload = new byte[400_000];
            new Random(3).nextBytes(payload);
            var echo = client.send(HttpRequest.newBuilder(URI.create(base + "/echo")).POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(),
                    BodyHandlers.ofByteArray());
            assertArrayEquals(payload, echo.body());

            var big = client.send(HttpRequest.newBuilder(URI.create(base + "/big/3000000")).build(), BodyHandlers.ofByteArray());
            assertEquals(3_000_000, big.body().length);

            List<CompletableFuture<java.net.http.HttpResponse<String>>> all = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                all.add(client.sendAsync(HttpRequest.newBuilder(URI.create(base + "/users/" + i)).build(), BodyHandlers.ofString()));
            }
            for (int i = 0; i < 40; i++) {
                assertEquals("user " + i + " q=-", all.get(i).get(10, TimeUnit.SECONDS).body());
            }
        }
    }

    @Test
    void aClientThatSendsGarbageAfterAlpnH2GetsAnHttp2Error(@TempDir Path dir) throws Exception {
        SSLContext[] ctx = TestTls.tls(dir);
        try (Server server = HttpServer.bind(group, ANY, Options.defaults(), ctx[0], h2(), handler())) {
            SSLSocket s = (SSLSocket) ctx[1].getSocketFactory().createSocket("localhost", port(server));
            SSLParameters params = s.getSSLParameters();
            params.setApplicationProtocols(new String[]{"h2"});
            s.setSSLParameters(params);
            s.startHandshake();
            assertEquals("h2", s.getApplicationProtocol());
            try (Peer p = new Peer(s, false)) {
                s.getOutputStream().write("GET / HTTP/1.1\r\n\r\n".getBytes());
                Frame sawGoaway = null;
                for (int i = 0; i < 6 && sawGoaway == null; i++) {
                    Frame f = p.next();
                    if (f.type() == GOAWAY) {
                        sawGoaway = f;
                    } else if (f.type() == EOF.type()) {
                        break;
                    }
                }
                assertNotNull(sawGoaway, "an HTTP/2 connection must answer a bad preface with GOAWAY, not an HTTP/1.1 error page");
                assertEquals(PROTOCOL_ERROR, sawGoaway.errorCode());
            }
        }
    }

    // ---- flow control ----

    @Test
    void theServerStopsAtTheClientsStreamWindow() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake(4, 100)) { // INITIAL_WINDOW_SIZE = 100
            p.get(1, "/big/1000");
            long got = 0;
            Frame f;
            p.next(); // HEADERS
            while ((f = p.pollOrNull(400)) != null) {
                if (f.type() == DATA) {
                    got += f.payload().length;
                }
            }
            assertEquals(100, got, "no more than the window may be sent");
            p.windowUpdate(1, 400);
            got = 0;
            while ((f = p.pollOrNull(400)) != null) {
                if (f.type() == DATA) {
                    got += f.payload().length;
                }
            }
            assertEquals(400, got);
            p.windowUpdate(1, 10_000);
            got = 0;
            boolean end = false;
            while (!end && (f = p.pollOrNull(1000)) != null) {
                if (f.type() == DATA) {
                    got += f.payload().length;
                    end = (f.flags() & END_STREAM) != 0;
                }
            }
            assertEquals(500, got);
            assertTrue(end);
        }
    }

    @Test
    void theServerStopsAtTheConnectionWindow() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake(4, 1 << 20)) {
            p.get(1, "/big/200000");   // the stream window allows it, the connection window (65535) does not
            long got = 0;
            Frame f;
            while ((f = p.pollOrNull(500)) != null) {
                if (f.type() == DATA) {
                    got += f.payload().length;
                }
            }
            assertEquals(65535, got);
            p.windowUpdate(0, 200_000);
            boolean end = false;
            while (!end && (f = p.pollOrNull(1000)) != null) {
                if (f.type() == DATA) {
                    got += f.payload().length;
                    end = (f.flags() & END_STREAM) != 0;
                }
            }
            assertEquals(200_000, got);
        }
    }

    @Test
    void theServerCreditsTheClientSoALargeUploadKeepsFlowing() throws Exception {
        HttpConfig cfg = new HttpConfig(16 * 1024, 8 * 1024 * 1024).withHttp2(Http2Config.defaults());
        try (Server server = server(cfg, handler()); Peer p = Peer.connect(port(server)).handshake()) {
            byte[] chunk = new byte[16384];
            new Random(1).nextBytes(chunk);
            p.post(1, "/echo", false);
            int total = 3 * 1024 * 1024;     // three times the 1 MiB window the server announced: it must be credited on the way
            int sent = 0;
            long window = 1 << 20;           // both the stream and the connection window the server announced
            while (sent < total) {
                while (window < chunk.length) {
                    Frame f = p.next();
                    if (f.type() == WINDOW_UPDATE) {
                        window += ByteBuffer.wrap(f.payload()).getInt();  // stream and connection credits arrive together; the smaller bound is the same here
                    }
                }
                p.data(1, false, chunk);
                sent += chunk.length;
                window -= chunk.length;
            }
            p.data(1, true, new byte[0]);
            long got = 0;
            for (;;) {   // the echo comes back, and we credit the server's sending side
                Frame f = p.next();
                if (f.type() == DATA) {
                    got += f.payload().length;
                    p.windowUpdate(1, f.payload().length);
                    p.windowUpdate(0, f.payload().length);
                    if ((f.flags() & END_STREAM) != 0) {
                        break;
                    }
                }
            }
            assertEquals(total, got);
        }
    }

    @Test
    void aFrameLargerThanTheStreamWindowIsAFlowControlError() throws Exception {
        Http2Config tiny = new Http2Config(100, 1000, 65535, 16384, 4096, 200, 2000);   // 1000-byte stream windows
        try (Server server = server(HttpConfig.defaults().withHttp2(tiny), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/echo", false);
            p.data(1, false, new byte[5000]);
            Frame f = p.nextInteresting();
            assertEquals(RST_STREAM, f.type());
            assertEquals(3, f.errorCode(), "FLOW_CONTROL_ERROR");
        }
    }

    // ---- rules that must be enforced ----

    @Test
    void malformedRequestsAreResetWithProtocolError() throws Exception {
        String[][] bad = {
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", "Upper", "case"},       // upper case field name
                {":method", "GET", ":scheme", "http", ":authority", "x"},                                          // no :path
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", "connection", "close"}, // connection-specific
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", ":method", "GET"},     // duplicate pseudo-header
                {":method", "GET", ":scheme", "http", ":authority", "x", "te", "gzip", ":path", "/hello"},         // pseudo-header after a regular one
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", "te", "gzip"},         // te other than trailers
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", ":status", "200"},     // response pseudo-header
                {":method", "GET", ":scheme", "http", ":authority", "x", ":path", ""},                              // empty :path
        };
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            int sid = 1;
            for (String[] headers : bad) {
                p.headers(sid, true, true, headers);
                Response r = p.response(sid);
                assertEquals(PROTOCOL_ERROR, r.reset, "should be reset: " + Arrays.toString(headers));
                sid += 2;
            }
            p.get(sid, "/hello");  // the connection itself is fine afterwards
            assertEquals(200, p.response(sid).status);
        }
    }

    @Test
    void contentLengthMustMatchTheBody() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/echo", false, "content-length", "10");
            p.data(1, true, new byte[5]);
            assertEquals(PROTOCOL_ERROR, p.response(1).reset);
            p.post(3, "/echo", false, "content-length", "3");
            p.data(3, false, new byte[5]);
            assertEquals(PROTOCOL_ERROR, p.response(3).reset);
        }
    }

    @Test
    void tooManyConcurrentStreamsAreRefused() throws Exception {
        Http2Config two = Http2Config.defaults().withMaxConcurrentStreams(2);
        try (Server server = server(HttpConfig.defaults().withHttp2(two), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/slow");
            p.get(3, "/slow");
            p.get(5, "/slow");
            Frame f = p.nextInteresting();
            assertEquals(RST_STREAM, f.type());
            assertEquals(5, f.sid());
            assertEquals(REFUSED_STREAM, f.errorCode());
            synchronized (pending) {
                pending.forEach(x -> x.complete(HttpResponse.ok("late")));
            }
            assertEquals("late", p.response(1).text());
            assertEquals("late", p.response(3).text());
            p.get(7, "/hello");   // capacity is back
            assertEquals(200, p.response(7).status);
        }
    }

    @Test
    void oversizedHeadersGet431() throws Exception {
        HttpConfig cfg = new HttpConfig(2000, 1000).withHttp2(Http2Config.defaults());
        try (Server server = server(cfg, handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.headers(1, true, true, ":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello", "x-big", "a".repeat(5000));
            Response r = p.response(1);
            assertEquals(431, r.status);
            p.get(3, "/hello");   // the HPACK state stayed in step: the next request decodes fine
            assertEquals(200, p.response(3).status);
        }
    }

    @Test
    void anEndlessHeaderBlockIsCutOff() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.headers(1, true, false, ":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hello");
            byte[] filler = new byte[16384];
            Arrays.fill(filler, (byte) 'a');
            try {
                for (int i = 0; i < 40; i++) {
                    p.send(CONTINUATION, 0, 1, filler);   // never END_HEADERS
                }
            } catch (IOException serverGaveUp) {
                // fine: the server already closed
            }
            Frame f;
            do {
                f = p.next();
            } while (f.type() != GOAWAY && f.type() != EOF.type());
            if (f.type() == GOAWAY) {
                assertEquals(ENHANCE_YOUR_CALM, f.errorCode());
            }
        }
    }

    @Test
    void rapidResetsAreCutOff() throws Exception {
        Http2Config strict = Http2Config.defaults().withLimits(50, 5000);
        try (Server server = server(HttpConfig.defaults().withHttp2(strict), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            try {
                for (int i = 0; i < 300; i++) {
                    int sid = 1 + 2 * i;
                    p.get(sid, "/slow");
                    p.rst(sid, 8); // CANCEL
                }
            } catch (IOException serverGaveUp) {
                // fine
            }
            Frame f;
            do {
                f = p.next();
            } while (f.type() != GOAWAY && f.type() != EOF.type());
            assertEquals(GOAWAY, f.type());
            assertEquals(ENHANCE_YOUR_CALM, f.errorCode());
        }
    }

    @Test
    void aFloodOfPingsIsCutOff() throws Exception {
        Http2Config strict = Http2Config.defaults().withLimits(100, 100);
        try (Server server = server(HttpConfig.defaults().withHttp2(strict), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            try {
                for (int i = 0; i < 1000; i++) {
                    p.send(PING, 0, 0, new byte[8]);
                }
            } catch (IOException serverGaveUp) {
                // fine
            }
            Frame f;
            do {
                f = p.next();
            } while (f.type() != GOAWAY && f.type() != EOF.type());
            assertEquals(GOAWAY, f.type());
            assertEquals(ENHANCE_YOUR_CALM, f.errorCode());
        }
    }

    @Test
    void pingsAreAnsweredAndUnknownFramesIgnored() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            byte[] payload = {1, 2, 3, 4, 5, 6, 7, 8};
            p.send(PING, 0, 0, payload);
            p.send(0xEE, 0, 0, new byte[]{9, 9, 9});    // a frame type nobody defined
            Frame f = p.nextInteresting();
            assertEquals(PING, f.type());
            assertEquals(ACK, f.flags());
            assertArrayEquals(payload, f.payload());
            p.get(1, "/hello");
            assertEquals(200, p.response(1).status);
        }
    }

    @Test
    void anOversizedUploadIsAnsweredEarlyAndTheStreamReset() throws Exception {
        HttpConfig cfg = new HttpConfig(16 * 1024, 1000).withHttp2(Http2Config.defaults());
        try (Server server = server(cfg, handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/echo", false);
            p.data(1, false, new byte[2000]);
            List<Frame> got = new ArrayList<>();
            Frame f;
            while ((f = p.pollOrNull(500)) != null) {
                got.add(f);
            }
            assertTrue(got.stream().anyMatch(x -> x.type() == HEADERS && x.sid() == 1), "a 413 response");
            Frame reset = got.stream().filter(x -> x.type() == RST_STREAM).findFirst().orElseThrow();
            assertEquals(0, reset.errorCode(), "NO_ERROR: tells the client to stop sending");
            p.get(3, "/hello");
            assertEquals(200, p.response(3).status);
        }
    }

    @Test
    void framesOnAClosedStreamAreAnError() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/hello");
            p.response(1);
            p.data(1, false, new byte[1]);
            Frame f = p.nextInteresting();
            assertEquals(RST_STREAM, f.type());
            assertEquals(STREAM_CLOSED, f.errorCode());
        }
    }

    @Test
    void theClientPrefaceIsChecked() throws Exception {
        try (Server server = server(h2(), handler()); Peer p = new Peer(new Socket("127.0.0.1", port(server)), false)) {
            p.out.write("PRI * HTTP/2.0\r\n\r\nSX\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            Frame f;
            do {
                f = p.next();
            } while (f.type() != GOAWAY && f.type() != EOF.type());
            if (f.type() == GOAWAY) {
                assertEquals(PROTOCOL_ERROR, f.errorCode());
            }
        }
    }

    // ---- lifecycle ----

    @Test
    void gracefulShutdownSendsGoawayAndLetsRunningStreamsFinish() throws Exception {
        Server server = server(h2(), handler());
        try (Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/slow");
            Thread.sleep(200);
            CompletableFuture<Void> done = server.shutdownGracefully(Duration.ofSeconds(5));
            Frame f = p.nextInteresting();
            assertEquals(GOAWAY, f.type());
            assertEquals(0, f.errorCode());
            assertFalse(done.isDone(), "a stream is still running");
            synchronized (pending) {
                pending.forEach(x -> x.complete(HttpResponse.ok("finished")));
            }
            assertEquals("finished", p.response(1).text());
            done.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void anIdleConnectionIsClosed() throws Exception {
        HttpConfig cfg = h2().withKeepAliveTimeout(Duration.ofMillis(300));
        try (Server server = server(cfg, handler()); Peer p = Peer.connect(port(server)).handshake()) {
            Frame f;
            do {
                f = p.next();
            } while (f.type() != GOAWAY && f.type() != EOF.type());
            assertTrue(f.type() == GOAWAY || f.type() == EOF.type());
        }
    }
}
