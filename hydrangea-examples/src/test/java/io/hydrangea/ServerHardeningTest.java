package io.hydrangea;

import io.hydrangea.http.AccessLog;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.HttpServerStage;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.sse.SseClient;
import io.hydrangea.sse.SseClientConfig;
import io.hydrangea.sse.SseClientListener;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;
import io.hydrangea.tls.TlsStage;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;
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
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ServerHardeningTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup group;
    private EventLoopGroup clientGroup;
    private final List<AutoCloseable> cleanup = new ArrayList<>();
    private final List<CompletableFuture<HttpResponse>> pending = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger handled = new AtomicInteger();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : cleanup) c.close();
        pending.forEach(f -> f.complete(HttpResponse.ok("released at teardown")));
        clientGroup.close();
        group.close();
    }

    // ---- fixtures ----

    private HttpRouter router() {
        return new HttpRouter()
                .get("/fast", r -> {
                    handled.incrementAndGet();
                    return HttpResponse.ok("fast");
                })
                .post("/post", r -> HttpResponse.status(201).text("created"))
                .getAsync("/slow", r -> {          // answers after 600 ms
                    handled.incrementAndGet();
                    var f = new CompletableFuture<HttpResponse>();
                    Thread.startVirtualThread(() -> {
                        try {
                            Thread.sleep(600);
                        } catch (InterruptedException ignored) { }
                        f.complete(HttpResponse.ok("slow done"));
                    });
                    return f;
                })
                .getAsync("/never", r -> {         // answers only when the test ends
                    handled.incrementAndGet();
                    var f = new CompletableFuture<HttpResponse>();
                    pending.add(f);
                    return f;
                })
                .postStream("/sink", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    long[] n = {0};
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer d, BodyControl c) {
                            n[0] += d.remaining();
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("got " + n[0]));
                        }
                    });
                    return done;
                })
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), WebSocketConfig.defaults(), () -> new WebSocketListener() {
                    @Override
                    public void onText(WebSocket ws, String t) {
                        ws.sendText(t);
                    }
                }))
                .get("/events", r -> SseResponse.accept(r.request(), io.hydrangea.sse.SseConfig.defaults().withHeartbeat(null), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        e.send("hello");
                    }
                }));
    }

    private Server start(Options options, HttpConfig cfg) throws IOException {
        Server s = HttpServer.bind(group, ANY, options, null, cfg, router());
        cleanup.add(s);
        return s;
    }

    private Server start(HttpConfig cfg) throws IOException {
        return start(Options.defaults(), cfg);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    static final class Raw implements AutoCloseable {
        final Socket s;
        final InputStream in;

        Raw(int port) throws IOException {
            s = new Socket("127.0.0.1", port);
            s.setSoTimeout(8000);
            in = s.getInputStream();
        }

        void send(String text) throws IOException {
            s.getOutputStream().write(text.getBytes(StandardCharsets.ISO_8859_1));
            s.getOutputStream().flush();
        }

        /** The head of a response only (for HEAD requests, whose Content-Length describes a body that is not sent). */
        String readHeadOnly() throws IOException {
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

        /** One response (head + Content-Length body), or null at EOF. */
        String readResponse() throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            int c;
            while ((c = in.read()) >= 0) {
                b.write(c);
                byte[] a = b.toByteArray();
                int n = a.length;
                if (n >= 4 && a[n - 4] == '\r' && a[n - 3] == '\n' && a[n - 2] == '\r' && a[n - 1] == '\n') break;
            }
            if (c < 0 && b.size() == 0) return null;
            String head = b.toString(StandardCharsets.ISO_8859_1);
            int i = head.toLowerCase().indexOf("content-length:");
            int len = i < 0 ? 0 : Integer.parseInt(head.substring(i + 15, head.indexOf('\r', i)).trim());
            return head + new String(in.readNBytes(len), StandardCharsets.ISO_8859_1);
        }

        /** True if the server closes the connection (EOF or reset) within the timeout. */
        boolean closedWithin(int millis) throws IOException {
            s.setSoTimeout(millis);
            try {
                return in.read() < 0;
            } catch (SocketTimeoutException stillOpen) {
                return false;
            } catch (IOException reset) {
                return true;
            } finally {
                s.setSoTimeout(8000);
            }
        }

        @Override
        public void close() throws IOException {
            s.close();
        }
    }

    private static String get(String path, String extra) {
        return "GET " + path + " HTTP/1.1\r\nHost: x\r\n" + extra + "\r\n";
    }

    private static boolean eventually(int millis, java.util.function.BooleanSupplier cond) throws InterruptedException {
        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(10);
        }
        return cond.getAsBoolean();
    }

    // ================= graceful shutdown =================

    @Test
    void idleKeepAliveConnectionsAreClosedAtOnceAndNewOnesAreRefused() throws Exception {
        Server server = start(HttpConfig.defaults());
        List<Raw> conns = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Raw r = new Raw(port(server));
            r.send(get("/fast", ""));
            assertTrue(r.readResponse().endsWith("fast"));
            conns.add(r);
        }
        assertEquals(5, server.activeConnections());
        int port = port(server); // after shutdown there is no listening socket to ask
        long t0 = System.nanoTime();
        server.shutdownGracefully(Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 1500, "nothing was in progress: no reason to wait");
        for (Raw r : conns) {
            assertTrue(r.closedWithin(2000), "the idle keep-alive connection is closed");
            r.close();
        }
        assertEquals(0, server.activeConnections());
        assertThrows(IOException.class, () -> new Socket("127.0.0.1", port).close(), "the port is released: new clients are refused");
    }

    @Test
    void aRequestInProgressIsAnsweredWithConnectionCloseThenTheConnectionEnds() throws Exception {
        Server server = start(HttpConfig.defaults());
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/slow", ""));
            Thread.sleep(150);
            assertEquals(1, handled.get());
            long t0 = System.nanoTime();
            CompletableFuture<Void> done = server.shutdownGracefully(Duration.ofSeconds(5));
            assertFalse(done.isDone(), "a request is still running");
            String resp = raw.readResponse();
            assertTrue(resp.startsWith("HTTP/1.1 200") && resp.endsWith("slow done"), resp);
            assertTrue(resp.contains("Connection: close"), "the client is told this connection ends: " + resp);
            assertTrue(raw.closedWithin(2000));
            done.get(5, TimeUnit.SECONDS);
            long waitedMs = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            assertTrue(waitedMs >= 300 && waitedMs < 3000, "waited for the request, not for the timeout: " + waitedMs + " ms");
        }
    }

    @Test
    void pipelinedRequestsAfterTheDrainBeganAreNotServed() throws Exception {
        Server server = start(HttpConfig.defaults());
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/slow", "") + get("/fast", ""));
            Thread.sleep(150);
            CompletableFuture<Void> done = server.shutdownGracefully(Duration.ofSeconds(5));
            assertTrue(raw.readResponse().endsWith("slow done"));
            assertNull(raw.readResponse(), "no second response: the connection ended after the first");
            done.get(5, TimeUnit.SECONDS);
        }
        assertEquals(1, handled.get(), "the second request was never handed to the application");
    }

    @Test
    void whatIgnoresTheDrainIsClosedByForceWhenTheTimeoutEnds() throws Exception {
        Server server = start(HttpConfig.defaults());
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/never", ""));
            Thread.sleep(150);
            long t0 = System.nanoTime();
            server.shutdownGracefully(Duration.ofMillis(500)).get(5, TimeUnit.SECONDS);
            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            assertTrue(ms >= 400 && ms < 2500, "completes at the deadline: " + ms + " ms");
            assertTrue(raw.closedWithin(2000));
        }
        assertEquals(0, server.activeConnections());
    }

    @Test
    void shutdownWithoutConnectionsCompletesAtOnceAndIsIdempotent() throws Exception {
        Server server = start(HttpConfig.defaults());
        var f1 = server.shutdownGracefully(Duration.ofSeconds(1));
        assertTrue(f1.isDone());
        assertSame(f1, server.shutdownGracefully(Duration.ofSeconds(1)));
    }

    @Test
    void rawHandlersAreToldToDrainAndMayCloseThemselves() throws Exception {
        AtomicInteger drains = new AtomicInteger();
        Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            @Override
            public void onDrain(Connection c) {
                drains.incrementAndGet();
                c.write(ByteBuffer.wrap("bye\n".getBytes()));
                c.closeAfterFlush();
            }
        });
        cleanup.add(server);
        Server stubborn = Server.bind(group, ANY, Options.defaults(), () -> new Handler() { }); // the default onDrain does nothing
        cleanup.add(stubborn);
        try (Raw a = new Raw(port(server)); Raw b = new Raw(port(stubborn))) {
            assertTrue(eventually(2000, () -> server.activeConnections() == 1 && stubborn.activeConnections() == 1));
            long t0 = System.nanoTime();
            server.shutdownGracefully(Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
            assertEquals(1, drains.get());
            assertEquals("bye", new String(a.in.readNBytes(3)), "the handler said goodbye and closed");
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 1500);
            long t1 = System.nanoTime();
            stubborn.shutdownGracefully(Duration.ofMillis(400)).get(5, TimeUnit.SECONDS);
            assertTrue(Duration.ofNanos(System.nanoTime() - t1).toMillis() >= 300, "this one only ended at the deadline");
            assertTrue(b.closedWithin(2000));
        }
    }

    @Test
    void webSocketsAreToldGoingAwayAndStubbornPeersAreForced() throws Exception {
        Server server = start(HttpConfig.defaults());
        CompletableFuture<int[]> closedWith = new CompletableFuture<>();
        WebSocket polite = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() {
            @Override
            public void onClose(WebSocket ws, int code, String reason) {
                closedWith.complete(new int[]{code});
            }
        }).get(10, TimeUnit.SECONDS);
        // a peer that completes the handshake and then never reads or answers
        Raw silent = new Raw(port(server));
        silent.send(get("/ws", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
        assertTrue(silent.readResponse().startsWith("HTTP/1.1 101"));
        assertTrue(eventually(2000, () -> server.activeConnections() == 2));
        long t0 = System.nanoTime();
        server.shutdownGracefully(Duration.ofMillis(800)).get(10, TimeUnit.SECONDS);
        assertEquals(1001, closedWith.get(5, TimeUnit.SECONDS)[0], "a polite client sees 'going away' and the handshake completes");
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() >= 700, "the silent peer held the shutdown until the deadline");
        assertEquals(0, server.activeConnections());
        silent.close();
        assertTrue(polite != null);
    }

    @Test
    void serverSentEventStreamsEndCleanlyAndClientsKnowToReconnect() throws Exception {
        Server server = start(HttpConfig.defaults());
        CompletableFuture<Boolean> willReconnect = new CompletableFuture<>();
        var http = new HttpClient(clientGroup, HttpClientConfig.defaults());
        cleanup.add(http);
        CountDownLatch gotHello = new CountDownLatch(1);
        var sub = SseClient.subscribe(http, "http://127.0.0.1:" + port(server) + "/events", SseClientConfig.defaults().withDefaultRetry(Duration.ofSeconds(30)),
                new SseClientListener() {
                    @Override
                    public void onMessage(io.hydrangea.sse.SseMessage m) {
                        gotHello.countDown();
                    }

                    @Override
                    public void onError(Throwable error, boolean reconnect) {
                        willReconnect.complete(reconnect);
                    }
                });
        assertTrue(gotHello.await(5, TimeUnit.SECONDS));
        server.shutdownGracefully(Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
        assertTrue(willReconnect.get(5, TimeUnit.SECONDS), "the stream ended and the client will try again, as EventSource does");
        sub.close();
    }

    @Test
    void anUploadInProgressCompletesBeforeTheConnectionEnds() throws Exception {
        Server server = start(HttpConfig.defaults());
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /sink HTTP/1.1\r\nHost: x\r\nContent-Length: 2000\r\n\r\n" + "a".repeat(1000));
            Thread.sleep(200);
            CompletableFuture<Void> done = server.shutdownGracefully(Duration.ofSeconds(5));
            Thread.sleep(100);
            assertFalse(done.isDone());
            raw.send("b".repeat(1000));
            String resp = raw.readResponse();
            assertTrue(resp.endsWith("got 2000") && resp.contains("Connection: close"), resp);
            done.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void connectionsArrivingWhileShutdownStartsAreAllAccountedFor() throws Exception {
        Server server = start(HttpConfig.defaults());
        int port = port(server);
        AtomicInteger connected = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> clients = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            clients.add(Thread.startVirtualThread(() -> {
                try {
                    go.await();
                    for (int i = 0; i < 200; i++) {
                        try (Socket s = new Socket("127.0.0.1", port)) {
                            s.setSoTimeout(2000);
                            s.getOutputStream().write(get("/fast", "").getBytes());
                            s.getInputStream().read(new byte[256]);
                            connected.incrementAndGet();
                        } catch (IOException refused) {
                            return; // the port is closed: shutdown has begun
                        }
                    }
                } catch (InterruptedException ignored) { }
            }));
        }
        go.countDown();
        assertTrue(eventually(5000, () -> connected.get() > 50));
        server.shutdownGracefully(Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);
        for (Thread t : clients) t.join(10_000);
        assertEquals(0, server.activeConnections(), "no connection slipped through the accounting");
        assertTrue(eventually(3000, () -> group.metrics().snapshot().activeConnections() == 0));
    }

    // ================= timeouts =================

    @Test
    void aSlowlorisHeadIsCutOffByTheDeadlineEvenWhileBytesKeepArriving() throws Exception {
        Server server = start(HttpConfig.defaults().withHeaderTimeout(Duration.ofMillis(500)));
        try (Raw raw = new Raw(port(server))) {
            long t0 = System.nanoTime();
            raw.send("GET /fast HTTP/1.1\r\nHost: x\r\n");
            AtomicInteger sent = new AtomicInteger();
            Thread dribble = Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0; i < 40; i++) { // one more header byte every 100 ms: an inactivity timer would never fire
                        raw.send("X");
                        sent.incrementAndGet();
                        Thread.sleep(100);
                    }
                } catch (Exception serverHungUp) { }
            });
            String resp = raw.readResponse();
            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            assertTrue(resp.startsWith("HTTP/1.1 408"), resp);
            assertTrue(ms >= 400 && ms < 1500, "cut off at the deadline, not when the dribble stopped: " + ms + " ms");
            assertTrue(sent.get() < 20);
            dribble.join(5000);
        }
        assertEquals(1, group.metrics().snapshot().httpTimeouts());
        assertTrue(eventually(2000, () -> server.activeConnections() == 0));
    }

    @Test
    void silentAndIdleConnectionsAreClosedByTheKeepAliveTimeout() throws Exception {
        Server server = start(HttpConfig.defaults().withKeepAliveTimeout(Duration.ofMillis(300)));
        try (Raw silent = new Raw(port(server)); Raw used = new Raw(port(server))) {
            used.send(get("/fast", ""));
            assertTrue(used.readResponse().endsWith("fast"));
            long t0 = System.nanoTime();
            assertTrue(silent.closedWithin(2000), "never sent a byte: closed silently");
            assertTrue(used.closedWithin(2000), "idle between requests: closed");
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 1500);
        }
        assertEquals(2, group.metrics().snapshot().httpTimeouts());
    }

    @Test
    void theDeadlinesDoNotApplyWhileTheApplicationIsBusy() throws Exception {
        Server server = start(HttpConfig.defaults().withKeepAliveTimeout(Duration.ofMillis(200)).withHeaderTimeout(Duration.ofMillis(200)).withBodyTimeout(Duration.ofMillis(200)));
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/slow", "")); // takes 600 ms: three times every deadline
            String resp = raw.readResponse();
            assertTrue(resp.endsWith("slow done"), resp);
            raw.send(get("/fast", ""));  // and the connection is usable again, with a fresh keep-alive period
            assertTrue(raw.readResponse().endsWith("fast"));
        }
    }

    @Test
    void aSlowBufferedBodyIsCutOffButAStreamedOneIsNot() throws Exception {
        Server server = start(HttpConfig.defaults().withBodyTimeout(Duration.ofMillis(400)));
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /post HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nstart");
            Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0; i < 20; i++) {
                        Thread.sleep(100);
                        raw.send("x");
                    }
                } catch (Exception ignored) { }
            });
            assertTrue(raw.readResponse().startsWith("HTTP/1.1 408"));
        }
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /sink HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n\r\n");
            for (int i = 0; i < 12; i++) { // 1.2 s in total: three times the body timeout
                Thread.sleep(100);
                raw.send("y");
            }
            assertTrue(raw.readResponse().endsWith("got 12"), "streaming routes manage their own pace");
        }
    }

    @Test
    void protocolSwitchesAndStreamsAreNotCutOffByHttpDeadlines() throws Exception {
        Server server = start(HttpConfig.defaults().withKeepAliveTimeout(Duration.ofMillis(250)).withHeaderTimeout(Duration.ofMillis(250)));
        CompletableFuture<String> echoed = new CompletableFuture<>();
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() {
            @Override
            public void onText(WebSocket w, String t) {
                echoed.complete(t);
            }
        }).get(10, TimeUnit.SECONDS);
        Thread.sleep(900); // far longer than any HTTP deadline
        assertTrue(ws.isOpen());
        ws.sendText("still here");
        assertEquals("still here", echoed.get(5, TimeUnit.SECONDS));

        var http = new HttpClient(clientGroup, HttpClientConfig.defaults());
        cleanup.add(http);
        CountDownLatch hello = new CountDownLatch(1);
        var sub = SseClient.subscribe(http, "http://127.0.0.1:" + port(server) + "/events", new SseClientListener() {
            @Override
            public void onMessage(io.hydrangea.sse.SseMessage m) {
                hello.countDown();
            }
        });
        assertTrue(hello.await(5, TimeUnit.SECONDS));
        Thread.sleep(900);
        assertFalse(sub.isClosed());
        sub.close();
    }

    @Test
    void aRejectionIsDeliveredEvenWhileTheClientKeepsSendingTheRejectedBody() throws Exception {
        Server server = start(HttpConfig.defaults()); // buffered limit 1 MiB
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /post HTTP/1.1\r\nHost: x\r\nContent-Length: 50000000\r\n\r\n");
            Thread writer = Thread.startVirtualThread(() -> {
                try {
                    byte[] chunk = new byte[64 * 1024];
                    for (int i = 0; i < 60; i++) raw.s.getOutputStream().write(chunk); // ~4 MB offered, none of it wanted
                } catch (IOException ignored) { }
            });
            Thread.sleep(600); // a client that is busy sending reads the answer late: by then an abrupt close would have reset the connection
            String resp = raw.readResponse(); // must be the 413, not "connection reset"
            assertTrue(resp.startsWith("HTTP/1.1 413"), resp);
            writer.join(10_000);
        }
    }

    @Test
    void aLingeringCloseSaysGoodbyeWithAFinThenKeepsReadingUntilThePeerIsDone() throws Exception {
        // the handler answers once and asks for a lingering close while the client still has plenty to send
        Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            boolean said;

            @Override
            public void onRead(Connection c, ByteBuffer data) {
                if (!said) {
                    said = true;
                    c.write(ByteBuffer.wrap("bye".getBytes()));
                    c.closeAfterFlush(Duration.ofSeconds(5));
                }
            }
        });
        cleanup.add(server);
        long before = group.metrics().snapshot().bytesRead();
        try (Raw raw = new Raw(port(server))) {
            raw.send("x");
            assertEquals("bye", new String(raw.in.readNBytes(3)));
            assertEquals(-1, raw.in.read(), "a clean FIN right after our last byte, not a reset");
            byte[] chunk = new byte[64 * 1024];
            for (int i = 0; i < 40; i++) {            // 2.5 MB sent after the server asked to close: all of it must be accepted
                raw.s.getOutputStream().write(chunk);
            }
            raw.s.shutdownOutput();                   // the peer is done: now the server may let go
            assertTrue(eventually(5000, () -> group.metrics().snapshot().bytesRead() - before >= 40L * 64 * 1024 + 1),
                    "the server kept reading and discarding; it saw " + (group.metrics().snapshot().bytesRead() - before));
            assertTrue(eventually(5000, () -> server.activeConnections() == 0), "and closed as soon as the peer finished");
        }
    }

    @Test
    void aTlsHandshakeThatNeverCompletesIsDropped(@TempDir Path dir) throws Exception {
        SSLContext ctx = serverContext(dir);
        Server server = Server.bind(group, ANY, Options.defaults(), () -> Pipeline.of(
                TlsStage.server(ctx).handshakeTimeout(Duration.ofMillis(400)), new HttpServerStage(HttpConfig.defaults(), HttpRouterHolder.ROUTER)));
        cleanup.add(server);
        try (Raw raw = new Raw(port(server))) { // connects, sends nothing
            long t0 = System.nanoTime();
            assertTrue(raw.closedWithin(3000));
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 2000);
        }
    }

    private static final class HttpRouterHolder {
        static final HttpRouter ROUTER = new HttpRouter().get("/x", r -> HttpResponse.ok("x"));
    }

    private static SSLContext serverContext(Path dir) throws Exception {
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
        SSLContext s = SSLContext.getInstance("TLS");
        s.init(kmf.getKeyManagers(), null, null);
        return s;
    }

    // ================= connection limits =================

    @Test
    void theTotalConnectionLimitTurnsExcessClientsAwayAndFreesSlotsWhenTheyLeave() throws Exception {
        Server server = start(Options.defaults().withMaxConnections(3), HttpConfig.defaults());
        List<Raw> inside = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Raw r = new Raw(port(server));
            r.send(get("/fast", ""));
            assertTrue(r.readResponse().endsWith("fast"));
            inside.add(r);
        }
        for (int i = 0; i < 3; i++) {
            try (Raw extra = new Raw(port(server))) {
                assertTrue(extra.closedWithin(2000), "over the limit: closed at once");
            }
        }
        assertEquals(3, group.metrics().snapshot().connectionsRejected());
        inside.get(0).close();
        assertTrue(eventually(3000, () -> server.activeConnections() == 2));
        try (Raw again = new Raw(port(server))) {
            again.send(get("/fast", ""));
            assertTrue(again.readResponse().endsWith("fast"), "a slot was freed");
        }
        inside.forEach(r -> { try { r.close(); } catch (IOException ignored) { } });
    }

    @Test
    void thePerAddressLimitStopsOneHostAndItsAccountingDoesNotLeak() throws Exception {
        Server server = start(Options.defaults().withMaxConnectionsPerAddress(2), HttpConfig.defaults());
        try (Raw a = new Raw(port(server)); Raw b = new Raw(port(server))) {
            a.send(get("/fast", ""));
            b.send(get("/fast", ""));
            assertTrue(a.readResponse().endsWith("fast") && b.readResponse().endsWith("fast"));
            try (Raw third = new Raw(port(server))) {
                assertTrue(third.closedWithin(2000), "a third connection from the same address");
            }
        }
        assertTrue(eventually(3000, () -> server.activeConnections() == 0));
        // 150 connections one after the other from the same address: the counter must go back to zero each time
        for (int i = 0; i < 150; i++) {
            try (Raw r = new Raw(port(server))) {
                r.send(get("/fast", "Connection: close\r\n"));
                assertTrue(r.readResponse().endsWith("fast"), "connection " + i);
            }
        }
        assertEquals(1, group.metrics().snapshot().connectionsRejected());
    }

    // ================= access log and metrics =================

    @Test
    void everyResponseIsLoggedWithWhatTheOperatorNeeds() throws Exception {
        List<AccessLog.Entry> entries = Collections.synchronizedList(new ArrayList<>());
        Server server = start(HttpConfig.defaults().withAccessLog(entries::add).withHeaderTimeout(Duration.ofMillis(300)));
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/fast", "User-Agent: probe/1.0\r\n"));
            raw.readResponse();
            raw.send(get("/missing", ""));
            raw.readResponse();
            raw.send("POST /post HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc");
            raw.readResponse();
            raw.send("HEAD /fast HTTP/1.1\r\nHost: x\r\n\r\n");
            raw.readHeadOnly();
            raw.send(get("/slow", ""));
            raw.readResponse();
        }
        try (Raw bad = new Raw(port(server))) { // malformed
            bad.send("BROKEN\r\n\r\n");
            bad.readResponse();
        }
        try (Raw slow = new Raw(port(server))) { // times out
            slow.send("GET /fast HTTP/1.1\r\nHost: x\r\n");
            slow.readResponse();
        }
        try (Raw up = new Raw(port(server))) { // streamed upload
            up.send("POST /sink HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nConnection: close\r\n\r\nwxyz");
            up.readResponse();
        }
        assertTrue(eventually(3000, () -> entries.size() >= 8), "entries: " + entries);
        var byTarget = new java.util.HashMap<String, AccessLog.Entry>();
        synchronized (entries) {
            for (var e : entries) byTarget.put(e.method() + " " + e.target() + " " + e.status(), e);
        }
        AccessLog.Entry ok = byTarget.get("GET /fast 200");
        assertNotNull(ok, byTarget.keySet().toString());
        assertEquals(4, ok.responseBytes());
        assertEquals("probe/1.0", ok.userAgent());
        assertEquals("HTTP/1.1", ok.version());
        assertNotNull(ok.remote());
        assertNotNull(byTarget.get("GET /missing 404"));
        assertEquals(7, byTarget.get("POST /post 201").responseBytes());
        assertEquals(0, byTarget.get("HEAD /fast 200").responseBytes(), "HEAD sends no body");
        assertTrue(byTarget.get("GET /slow 200").duration().toMillis() >= 500, "duration covers the handler: " + byTarget.get("GET /slow 200").duration());
        assertNotNull(byTarget.get("null null 400"), "a request that never parsed is still logged: " + byTarget.keySet());
        assertTrue(byTarget.keySet().stream().anyMatch(k -> k.endsWith(" 408")), byTarget.keySet().toString());
        assertEquals(5, byTarget.get("POST /sink 200").responseBytes(), "a streamed upload is logged too (the body \"got 4\")");
    }

    @Test
    void theLoggerBackedAccessLogNeutralisesClientControlledText() throws Exception {
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        java.util.logging.Logger jul = java.util.logging.Logger.getLogger("test.access");
        jul.setUseParentHandlers(false);
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord r) {
                lines.add(r.getMessage());
            }

            public void flush() { }

            public void close() { }
        };
        jul.addHandler(capture);
        cleanup.add(() -> jul.removeHandler(capture));
        Server server = start(HttpConfig.defaults().withAccessLog(AccessLog.toLogger("test.access")));
        try (Raw raw = new Raw(port(server))) {
            raw.send(get("/fast", "User-Agent: evil\" 200 0 \"forged\\agent\r\n"));
            raw.readResponse();
        }
        assertTrue(eventually(2000, () -> !lines.isEmpty()));
        String line = lines.get(0);
        assertTrue(line.contains("GET /fast HTTP/1.1"), line);
        assertTrue(line.contains("\\\"forged\\\\agent"), "quotes and backslashes are escaped so the line cannot be forged: " + line);
        assertEquals(1, line.lines().count());
    }

    @Test
    void metricsCountHttpAndExportAsPrometheusText() throws Exception {
        Server server = start(HttpConfig.defaults());
        try (Raw raw = new Raw(port(server))) {
            for (int i = 0; i < 3; i++) {
                raw.send(get("/fast", ""));
                raw.readResponse();
            }
            raw.send(get("/missing", ""));
            raw.readResponse();
        }
        Metrics.Snapshot m = group.metrics().snapshot();
        assertEquals(4, m.httpRequests());
        assertEquals(3, m.http2xx());
        assertEquals(1, m.http4xx());
        String text = m.toPrometheus("hydrangea");
        assertTrue(text.contains("# TYPE hydrangea_connections_active gauge"), text);
        assertTrue(text.contains("hydrangea_http_requests_total 4"), text);
        assertTrue(text.contains("hydrangea_http_responses_total{class=\"2xx\"} 3"), text);
        assertTrue(text.contains("hydrangea_http_responses_total{class=\"4xx\"} 1"), text);
        // every sample line is "name{labels} number" with a legal metric name, and every sample has a TYPE
        var types = new java.util.HashSet<String>();
        for (String line : text.split("\n")) {
            if (line.startsWith("# TYPE ")) {
                types.add(line.split(" ")[2]);
            } else if (!line.startsWith("#") && !line.isBlank()) {
                String name = line.substring(0, line.indexOf(line.contains("{") ? '{' : ' '));
                assertTrue(name.matches("[a-zA-Z_:][a-zA-Z0-9_:]*"), line);
                assertTrue(types.contains(name), "no TYPE line for " + name);
                Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
            }
        }
    }
}
