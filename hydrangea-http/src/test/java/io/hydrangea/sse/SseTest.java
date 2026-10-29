package io.hydrangea.sse;

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
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SseTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup group;
    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : cleanup) c.close();
        group.close();
    }

    private Server start(SseConfig config, Supplier<? extends SseListener> listener) throws IOException {
        return start(Options.defaults(), null, config, listener);
    }

    private Server start(Options options, SSLContext tls, SseConfig config, Supplier<? extends SseListener> listener) throws IOException {
        HttpRouter router = new HttpRouter()
                .get("/events", r -> SseResponse.accept(r.request(), config, listener))
                .post("/events", r -> SseResponse.accept(r.request(), config, listener))
                .get("/plain", r -> HttpResponse.ok("plain"));
        Server s = HttpServer.bind(group, ANY, options, tls, HttpConfig.defaults(), router);
        cleanup.add(s);
        return s;
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    /** Listener that records lifecycle and lets the test drive the emitter. */
    static class Probe implements SseListener {
        final CompletableFuture<SseEmitter> opened = new CompletableFuture<>();
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final AtomicInteger closeCalls = new AtomicInteger();
        volatile String lastEventIdSeen;

        @Override
        public void onOpen(SseEmitter e) {
            lastEventIdSeen = e.lastEventId();
            opened.complete(e);
        }

        @Override
        public void onClose(SseEmitter e) {
            closeCalls.incrementAndGet();
            closed.complete(null);
        }

        SseEmitter emitter() throws Exception {
            return opened.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void serverAndClientCountConnectionsAndEvents() throws Exception {
        io.hydrangea.CaptureSink sink = new io.hydrangea.CaptureSink();
        group.metrics().bindTo(sink);
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        io.hydrangea.http.HttpClient http = new io.hydrangea.http.HttpClient(group, io.hydrangea.http.HttpClientConfig.defaults());
        cleanup.add(http);
        java.util.concurrent.atomic.AtomicInteger got = new java.util.concurrent.atomic.AtomicInteger();
        SseSubscription sub = SseClient.subscribe(http, "http://127.0.0.1:" + port(server) + "/events", new SseClientListener() {
            @Override
            public void onMessage(SseMessage m) {
                got.incrementAndGet();
            }
        });
        SseEmitter emitter = probe.emitter();
        io.hydrangea.CaptureSink.eventually(() -> sink.get("sse.connections{side,server}") == 1, "the server to count the stream");
        for (int i = 0; i < 3; i++) {
            emitter.send(SseEvent.data("tick " + i));
        }
        io.hydrangea.CaptureSink.eventually(() -> got.get() == 3, "three events at the client");
        assertEquals(3, sink.get("sse.events{side,server,direction,sent}"));
        assertEquals(3, sink.get("sse.events{side,client,direction,received}"));
        sub.close();
        io.hydrangea.CaptureSink.eventually(() -> sink.get("sse.connections{side,server}") == 0, "the stream to leave");
    }

    // ---- a raw client that decodes chunked framing itself ----

    static final class RawSse implements AutoCloseable {
        final Socket s;
        final InputStream in;
        String head;

        RawSse(int port, String version, String extraHeaders) throws IOException {
            s = new Socket("127.0.0.1", port);
            s.setSoTimeout(5000);
            in = s.getInputStream();
            s.getOutputStream().write(("GET /events " + version + "\r\nHost: 127.0.0.1:" + port + "\r\n" + extraHeaders + "\r\n")
                    .getBytes(StandardCharsets.ISO_8859_1));
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) >= 0) {
                b.write(c);
                byte[] a = b.toByteArray();
                int n = a.length;
                if (n >= 4 && a[n - 4] == '\r' && a[n - 3] == '\n' && a[n - 2] == '\r' && a[n - 1] == '\n') break;
            }
            head = b.toString(StandardCharsets.ISO_8859_1);
        }

        boolean chunked() {
            return head.toLowerCase().contains("transfer-encoding: chunked");
        }

        /** Next HTTP chunk's payload, or null at the terminating 0-chunk / EOF. */
        byte[] readChunk() throws IOException {
            StringBuilder line = new StringBuilder();
            int c;
            while ((c = in.read()) >= 0 && c != '\n') {
                if (c != '\r') line.append((char) c);
            }
            if (c < 0) return null;
            int size = Integer.parseInt(line.toString().trim(), 16);
            if (size == 0) {
                in.readNBytes(2); // final CRLF
                return null;
            }
            byte[] payload = in.readNBytes(size);
            assertEquals('\r', in.read());
            assertEquals('\n', in.read());
            return payload;
        }

        /** Text of the next chunk (each SSE event is exactly one chunk); null at end of stream. */
        String readEventText() throws IOException {
            byte[] b = readChunk();
            return b == null ? null : new String(b, StandardCharsets.UTF_8);
        }

        /** Raw bytes until EOF (for streams that are not chunked). */
        String readToEof() throws IOException {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        @Override
        public void close() throws IOException {
            s.close();
        }
    }

    // ---- wire format ----

    @Test
    void encodingOfEveryField() {
        assertEquals("data: hi\n\n", enc(SseEvent.data("hi")));
        assertEquals("event: price\nid: 7\ndata: 42\n\n", enc(SseEvent.data("42").event("price").id("7")));
        assertEquals("retry: 3000\ndata: x\n\n", enc(SseEvent.data("x").retry(Duration.ofSeconds(3))));
        assertEquals(": keep-alive\n\n", enc(SseEvent.comment("keep-alive")));
        assertEquals("data: line1\ndata: line2\ndata: line3\ndata: line4\n\n", enc(SseEvent.data("line1\nline2\r\nline3\rline4")));
        assertEquals("data: a\ndata: \ndata: b\n\n", enc(SseEvent.data("a\n\nb")), "an empty line in the data is kept as an empty data field");
        assertEquals("data: \ndata: \n\n", enc(SseEvent.data("\n")), "data that is just a newline");
        assertEquals("data: \n\n", enc(SseEvent.data("")));
        assertEquals("data: Việt ✓ 🎉\n\n", enc(SseEvent.data("Việt ✓ 🎉")));
        assertEquals(":\n:  two\n\n", enc(SseEvent.comment("\n two")), "separator space + the comment's own leading space");
    }

    @Test
    void fieldInjectionIsImpossible() {
        // newlines in data become more data lines, never new fields
        assertEquals("data: a\ndata: event: evil\ndata: id: 1\n\n", enc(SseEvent.data("a\nevent: evil\nid: 1")));
        assertEquals("data: a\ndata: data: b\n\n", enc(SseEvent.data("a\rdata: b")));
        // and the single-line fields refuse line breaks outright
        assertThrows(IllegalArgumentException.class, () -> SseEvent.data("x").event("a\nretry: 1"));
        assertThrows(IllegalArgumentException.class, () -> SseEvent.data("x").event("a\rb"));
        assertThrows(IllegalArgumentException.class, () -> SseEvent.data("x").id("1\n\ndata: forged"));
        assertThrows(IllegalArgumentException.class, () -> SseEvent.data("x").id("a\0b"));
        assertThrows(IllegalArgumentException.class, () -> SseEvent.data("x").retry(Duration.ofMillis(-1)));
        // a comment cannot break out either
        assertEquals(": a\n: data: forged\n\n", enc(SseEvent.comment("a\ndata: forged")));
    }

    private static String enc(SseEvent e) {
        return new String(e.encode(), StandardCharsets.UTF_8);
    }

    // ---- over a real connection ----

    @Test
    void responseHeadAndChunkedFraming() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "")) {
            assertTrue(c.head.startsWith("HTTP/1.1 200 OK"), c.head);
            assertTrue(c.head.contains("Content-Type: text/event-stream; charset=utf-8"), c.head);
            assertTrue(c.head.contains("Cache-Control: no-cache"), c.head);
            assertTrue(c.chunked(), c.head);
            assertFalse(c.head.toLowerCase().contains("content-length"), "a stream has no length");
            assertTrue(c.head.contains("Connection: close"));
            SseEmitter e = probe.emitter();
            e.send(SseEvent.data("one").id("1")).get(5, TimeUnit.SECONDS);
            e.send("two\nlines").get(5, TimeUnit.SECONDS);
            e.send(SseEvent.data("three").event("custom")).get(5, TimeUnit.SECONDS);
            assertEquals("id: 1\ndata: one\n\n", c.readEventText());
            assertEquals("data: two\ndata: lines\n\n", c.readEventText());
            assertEquals("event: custom\ndata: three\n\n", c.readEventText());
        }
    }

    @Test
    void serverSideCloseEndsTheStreamWithATerminatingChunk() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "")) {
            SseEmitter e = probe.emitter();
            e.send("last");
            e.close();
            e.close(); // idempotent
            assertEquals("data: last\n\n", c.readEventText());
            assertNull(c.readChunk(), "0-chunk ends the body");
            assertEquals(-1, c.in.read(), "then the server closes the connection");
        }
        probe.closed.get(5, TimeUnit.SECONDS);
        assertEquals(1, probe.closeCalls.get());
        assertFalse(probe.emitter().isOpen());
        assertInstanceOf(ClosedChannelException.class, failure(probe.emitter().send("late")));
    }

    @Test
    void clientDisconnectIsDetected() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        RawSse c = new RawSse(port(server), "HTTP/1.1", "");
        probe.emitter();
        c.close();
        probe.closed.get(5, TimeUnit.SECONDS);
        assertEquals(1, probe.closeCalls.get(), "onClose exactly once");
        assertFalse(probe.emitter().isOpen());
    }

    @Test
    void lastEventIdAndRetryHint() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults().withRetry(Duration.ofSeconds(5)), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "Last-Event-ID: 41\r\n")) {
            probe.emitter().send(SseEvent.data("resumed").id("42"));
            assertEquals("retry: 5000\n\n", c.readEventText(), "the retry hint comes first");
            assertEquals("id: 42\ndata: resumed\n\n", c.readEventText());
            assertEquals("41", probe.lastEventIdSeen);
        }
    }

    @Test
    void heartbeatKeepsAQuietStreamAlive() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults().withHeartbeat(Duration.ofMillis(100)), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "")) {
            assertEquals(": keep-alive\n\n", c.readEventText());
            assertEquals(": keep-alive\n\n", c.readEventText());
            probe.emitter().send("real");
            String next = c.readEventText();
            while (next.startsWith(":")) next = c.readEventText(); // heartbeats may interleave
            assertEquals("data: real\n\n", next);
        }
    }

    @Test
    void heartbeatLetsAnIdleTimeoutServerKeepStreamsOpenButNotDeadOnes() throws Exception {
        Probe probe = new Probe();
        Server server = start(Options.defaults().withIdleTimeout(Duration.ofMillis(400)), null,
                SseConfig.defaults().withHeartbeat(Duration.ofMillis(100)), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "")) {
            Thread.sleep(1200); // three idle timeouts without application events
            assertTrue(probe.emitter().isOpen(), "heartbeats count as activity");
            assertEquals(": keep-alive\n\n", c.readEventText());
        }
    }

    @Test
    void http10ClientGetsAnUnchunkedCloseDelimitedStream() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.0", "")) {
            assertFalse(c.chunked(), c.head);
            SseEmitter e = probe.emitter();
            e.send("plain one");
            e.send("plain two");
            e.close();
            assertEquals("data: plain one\n\ndata: plain two\n\n", c.readToEof());
        }
    }

    @Test
    void onlyGetIsAccepted() throws Exception {
        Server server = start(SseConfig.defaults(), Probe::new);
        try (Socket s = new Socket("127.0.0.1", port(server))) {
            s.setSoTimeout(5000);
            s.getOutputStream().write(("POST /events HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").getBytes());
            String resp = new String(s.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
            assertTrue(resp.startsWith("HTTP/1.1 405") && resp.contains("Allow: GET"), resp);
        }
    }

    // ---- misbehaving clients ----

    @Test
    void aClientThatNeverReadsIsDisconnectedInsteadOfBufferedForever() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults().withMaxPendingBytes(256 * 1024).withHeartbeat(null), () -> probe);
        RawSse c = new RawSse(port(server), "HTTP/1.1", ""); // reads nothing after the head
        SseEmitter e = probe.emitter();
        String chunk = "x".repeat(16 * 1024);
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (int i = 0; i < 4000 && e.isOpen(); i++) { // up to 64 MB offered
            sends.add(e.send(chunk));
            if (i % 50 == 0) Thread.sleep(2);
        }
        probe.closed.get(10, TimeUnit.SECONDS);
        assertFalse(e.isOpen());
        long failed = sends.stream().filter(CompletableFuture::isCompletedExceptionally).count();
        assertTrue(failed > 0, "later sends must fail once the client is cut off");
        assertEquals(1, probe.closeCalls.get());
        c.close();
    }

    @Test
    void bytesSentByTheClientAfterTheRequestAreIgnored() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        try (RawSse c = new RawSse(port(server), "HTTP/1.1", "")) {
            c.s.getOutputStream().write("GET /plain HTTP/1.1\r\nHost: x\r\n\r\nrandom junk".getBytes());
            probe.emitter().send("still works");
            assertEquals("data: still works\n\n", c.readEventText());
        }
    }

    // ---- concurrency ----

    @Test
    void broadcastFromManyThreadsToManyClientsKeepsPerThreadOrder() throws Exception {
        Set<SseEmitter> subscribers = ConcurrentHashMap.newKeySet();
        CountDownLatch connected = new CountDownLatch(20);
        Server server = start(SseConfig.defaults(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                subscribers.add(e);
                connected.countDown();
            }

            @Override
            public void onClose(SseEmitter e) {
                subscribers.remove(e);
            }
        });
        List<RawSse> clients = new ArrayList<>();
        for (int i = 0; i < 20; i++) clients.add(new RawSse(port(server), "HTTP/1.1", ""));
        assertTrue(connected.await(5, TimeUnit.SECONDS));
        int threads = 4, per = 100;
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
                for (int i = 0; i < per; i++) {
                    for (SseEmitter e : subscribers) e.send(id + ":" + i);
                }
            }));
        }
        go.countDown();
        for (RawSse c : clients) {
            int[] next = new int[threads];
            for (int n = 0; n < threads * per; n++) {
                String ev = c.readEventText();
                assertNotNull(ev);
                assertTrue(ev.startsWith("data: "));
                String[] p = ev.substring(6, ev.length() - 2).split(":");
                int id = Integer.parseInt(p[0]);
                assertEquals(next[id]++, Integer.parseInt(p[1]), "per-sender order must be kept for each client");
            }
        }
        for (RawSse c : clients) c.close();
        for (int i = 0; i < 100 && !subscribers.isEmpty(); i++) Thread.sleep(50);
        assertTrue(subscribers.isEmpty(), "every disconnect was noticed");
    }

    // ---- interop with the JDK's independent HTTP client ----

    @Test
    void jdkClientReadsTheStreamLineByLine() throws Exception {
        Probe probe = new Probe();
        Server server = start(SseConfig.defaults(), () -> probe);
        var client = java.net.http.HttpClient.newHttpClient();
        var req = java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/events")).build();
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch gotAll = new CountDownLatch(1);
        var response = client.sendAsync(req, java.net.http.HttpResponse.BodyHandlers.ofLines()).thenAccept(r -> {
            assertEquals(200, r.statusCode());
            assertEquals("text/event-stream; charset=utf-8", r.headers().firstValue("content-type").orElse(""));
            r.body().forEach(l -> {
                lines.add(l);
                if (l.equals("data: bye")) gotAll.countDown();
            });
        });
        SseEmitter e = probe.emitter();
        e.send(SseEvent.data("héllo").event("greeting").id("1"));
        e.send("a\nb");
        e.send("bye");
        assertTrue(gotAll.await(10, TimeUnit.SECONDS));
        assertEquals(List.of("event: greeting", "id: 1", "data: héllo", "", "data: a", "data: b", "", "data: bye"), lines.subList(0, 8));
        e.close();
        response.get(10, TimeUnit.SECONDS); // the body ends when the stream does
    }

    // ---- TLS ----

    @Test
    void streamOverHttps(@TempDir Path dir) throws Exception {
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
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        SSLContext clientCtx = SSLContext.getInstance("TLS");
        clientCtx.init(null, tmf.getTrustManagers(), null);

        Probe probe = new Probe();
        Server server = start(Options.defaults(), serverCtx, SseConfig.defaults(), () -> probe);
        var client = java.net.http.HttpClient.newBuilder().sslContext(clientCtx).build();
        var req = java.net.http.HttpRequest.newBuilder(URI.create("https://localhost:" + port(server) + "/events")).build();
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        var done = client.sendAsync(req, java.net.http.HttpResponse.BodyHandlers.ofLines()).thenAccept(r -> r.body().forEach(lines::add));
        SseEmitter e = probe.emitter();
        e.send("secret one");
        e.send(SseEvent.data("x".repeat(100_000)).event("big"));
        e.send("secret two");
        e.close(); // must flush everything and end the chunked body cleanly even though TLS is buffering
        done.get(15, TimeUnit.SECONDS);
        assertEquals("data: secret one", lines.get(0));
        assertTrue(lines.contains("data: secret two"), "the last event must not be lost when closing over TLS");
        assertEquals(100_000 + 6, lines.stream().filter(l -> l.startsWith("data: x")).findFirst().orElseThrow().length());
    }

    // ---- regression: ordinary routes unaffected ----

    @Test
    void normalRoutesStillKeepAliveNextToStreams() throws Exception {
        Server server = start(SseConfig.defaults(), Probe::new);
        try (Socket s = new Socket("127.0.0.1", port(server))) {
            s.setSoTimeout(5000);
            for (int i = 0; i < 3; i++) {
                s.getOutputStream().write("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n".getBytes());
                byte[] buf = new byte[512];
                int n = s.getInputStream().read(buf);
                String resp = new String(buf, 0, n, StandardCharsets.ISO_8859_1);
                assertTrue(resp.startsWith("HTTP/1.1 200") && resp.endsWith("plain"), resp);
            }
        }
        assertEquals(1, group.metrics().snapshot().connectionsOpened());
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
