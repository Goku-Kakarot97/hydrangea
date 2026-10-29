package io.hydrangea;

import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.HttpStatusException;
import io.hydrangea.http.RequestBodyStream;
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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpUploadServerTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup group;
    private final List<AutoCloseable> cleanup = new ArrayList<>();

    // observations made by the routes
    private final List<String> bodyEvents = java.util.Collections.synchronizedList(new ArrayList<>());
    private final CompletableFuture<Throwable> bodyError = new CompletableFuture<>();
    private final AtomicInteger bodyErrors = new AtomicInteger();
    private final AtomicInteger bodyCompletes = new AtomicInteger();
    private final AtomicLong limitedSeen = new AtomicLong();
    private final AtomicReference<BodyControl> pausedControl = new AtomicReference<>();
    private final AtomicReference<RequestBodyStream> lateStream = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : cleanup) c.close();
        group.close();
    }

    private static String hex(byte[] b) {
        return HexFormat.of().formatHex(b);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private HttpRouter router() {
        return new HttpRouter()
                // hashes whatever arrives; keeps nothing
                .postStream("/upload", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    MessageDigest md = sha256();
                    long[] n = {0};
                    long declared = body.contentLength();
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer data, BodyControl c) {
                            n[0] += data.remaining();
                            md.update(data);
                        }

                        @Override
                        public void onComplete() {
                            bodyCompletes.incrementAndGet();
                            done.complete(HttpResponse.ok("sha=" + hex(md.digest()) + " bytes=" + n[0] + " declared=" + declared));
                        }

                        @Override
                        public void onError(Throwable t) {
                            bodyErrors.incrementAndGet();
                            bodyError.complete(t);
                            done.completeExceptionally(t);
                        }
                    });
                    return done;
                })
                // answers 413 as soon as more than 1000 bytes arrived, while the client is still sending
                .postStream("/limited", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    long[] n = {0};
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer data, BodyControl c) {
                            n[0] += data.remaining();
                            limitedSeen.set(n[0]);
                            if (n[0] > 1000) done.complete(HttpResponse.status(413).text("too big"));
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("small enough"));
                        }

                        @Override
                        public void onError(Throwable t) {
                            bodyErrors.incrementAndGet();
                            bodyError.complete(t);
                        }
                    });
                    return done;
                })
                // refuses before reading anything
                .postStream("/reject", (r, body) -> {
                    throw new HttpStatusException(403, "not for you");
                })
                // counts, but pauses after the first piece until the test resumes it
                .postStream("/pausing", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    long[] n = {0};
                    boolean[] paused = {false};
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer data, BodyControl c) {
                            n[0] += data.remaining();
                            bodyEvents.add("data:" + n[0]);
                            if (!paused[0]) {
                                paused[0] = true;
                                pausedControl.set(c);
                                c.pause();
                            }
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("bytes=" + n[0]));
                        }

                        @Override
                        public void onError(Throwable t) {
                            bodyErrors.incrementAndGet();
                            bodyError.complete(t);
                        }
                    });
                    return done;
                })
                .postStream("/throwing-listener", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer data, BodyControl c) {
                            throw new IllegalStateException("listener bug");
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("never reached normally"));
                        }
                    });
                    return done;
                })
                .postStream("/failing", (r, body) -> {
                    body.listen(new BodyListener() { });
                    return CompletableFuture.failedFuture(new IllegalStateException("handler bug"));
                })
                .postStream("/ignore", (r, body) -> CompletableFuture.completedFuture(HttpResponse.ok("ignored the body"))) // no listener: body is read and thrown away
                .postStream("/late-listen", (r, body) -> {
                    lateStream.set(body);
                    body.listen(new BodyListener() { });
                    return CompletableFuture.completedFuture(HttpResponse.ok("ok"));
                })
                .routeStream("GET", "/nobody", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    body.listen(new BodyListener() {
                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("no body, length " + body.contentLength()));
                        }
                    });
                    return done;
                })
                .post("/buffered", r -> HttpResponse.ok("buffered " + r.body().length))
                .get("/plain", r -> HttpResponse.ok("plain"));
    }

    private Server start(Options options, SSLContext tls) throws IOException {
        // a tiny buffered-body limit: streaming routes must not be subject to it
        Server s = HttpServer.bind(group, ANY, options, tls, new HttpConfig(16 * 1024, 1024), router());
        cleanup.add(s);
        return s;
    }

    private Server start() throws IOException {
        return start(Options.defaults(), null);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    // ---- raw HTTP helpers ----

    static final class Raw implements AutoCloseable {
        final Socket s;
        final InputStream in;
        final OutputStream out;

        Raw(int port) throws IOException {
            s = new Socket("127.0.0.1", port);
            s.setSoTimeout(10_000);
            in = s.getInputStream();
            out = s.getOutputStream();
        }

        void send(String text) throws IOException {
            out.write(text.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
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
            return c < 0 && b.size() == 0 ? null : b.toString(StandardCharsets.ISO_8859_1);
        }

        /** Reads one response (head + Content-Length body); returns head + body text. */
        String readResponse() throws IOException {
            String head = readHead();
            if (head == null) return null;
            int i = head.toLowerCase().indexOf("content-length:");
            int len = i < 0 ? 0 : Integer.parseInt(head.substring(i + 15, head.indexOf('\r', i)).trim());
            return head + new String(in.readNBytes(len), StandardCharsets.ISO_8859_1);
        }

        @Override
        public void close() throws IOException {
            s.close();
        }
    }

    private static String post(String path, String extraHeaders, long length) {
        return "POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Length: " + length + "\r\n" + extraHeaders + "\r\n";
    }

    // ---- bodies beyond the buffered limit ----

    @Test
    void largeUploadsFromTheJdkClientAreHashedAsTheyArrive() throws Exception {
        Server server = start();
        int size = 24 * 1024 * 1024;
        byte[] data = new byte[size];
        new java.util.Random(5).nextBytes(data);
        var client = java.net.http.HttpClient.newHttpClient();
        String url = "http://127.0.0.1:" + port(server) + "/upload";
        String expected = hex(sha256().digest(data));

        var fixed = client.send(java.net.http.HttpRequest.newBuilder(URI.create(url))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(data)).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals("sha=" + expected + " bytes=" + size + " declared=" + size, fixed.body(), "Content-Length upload, 24000x the buffered limit");

        var chunked = client.send(java.net.http.HttpRequest.newBuilder(URI.create(url))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofInputStream(() -> new java.io.ByteArrayInputStream(data))).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals("sha=" + expected + " bytes=" + size + " declared=-1", chunked.body(), "chunked upload");
    }

    @Test
    void chunkedBodyWithExtensionsAndTrailersIsDeliveredExactly() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /upload HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
            raw.send("5;ext=1\r\nhello\r\n");
            Thread.sleep(30);
            raw.send("6\r\n world\r\n0\r\nTrailer-X: y\r\n\r\n");
            String resp = raw.readResponse();
            assertTrue(resp.startsWith("HTTP/1.1 200"), resp);
            assertTrue(resp.endsWith("sha=" + hex(sha256().digest("hello world".getBytes())) + " bytes=11 declared=-1"), resp);
        }
    }

    @Test
    void bufferedRoutesKeepTheirLimitOnTheSameServer() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/buffered", "Connection: close\r\n", 5000) + "x".repeat(5000));
            assertTrue(raw.readResponse().startsWith("HTTP/1.1 413"));
        }
    }

    @Test
    void keepAliveAndPipeliningWorkAcrossStreamedRequests() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/upload", "", 3) + "abc");
            assertTrue(raw.readResponse().contains("bytes=3"));
            raw.send(post("/upload", "", 3) + "def" + "GET /plain HTTP/1.1\r\nHost: x\r\n\r\n"); // the next request rides along
            assertTrue(raw.readResponse().contains("bytes=3"));
            assertTrue(raw.readResponse().endsWith("plain"));
            raw.send(post("/upload", "", 0)); // empty body on a stream route
            assertTrue(raw.readResponse().contains("bytes=0"));
        }
        assertEquals(1, group.metrics().snapshot().connectionsOpened());
    }

    @Test
    void aStreamRouteAlsoServesRequestsWithoutABody() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send("GET /nobody HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            assertTrue(raw.readResponse().endsWith("no body, length 0"));
        }
    }

    @Test
    void routeThatIgnoresTheBodyStillGetsItDrainedAndKeepsTheConnection() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/ignore", "", 100_000) + "z".repeat(100_000));
            assertTrue(raw.readResponse().endsWith("ignored the body"));
            raw.send("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n");
            assertTrue(raw.readResponse().endsWith("plain"), "the discarded body did not desynchronise the connection");
        }
    }

    @Test
    void aSuccessfulAnswerSentBeforeTheBodyEndedStillKeepsTheConnectionAlive() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/ignore", "", 200_000) + "a".repeat(1000));   // the route answers at once, the body is still arriving
            String early = raw.readResponse();
            assertTrue(early.startsWith("HTTP/1.1 200") && early.endsWith("ignored the body"), early);
            assertFalse(early.contains("Connection: close"), "a success is not a reason to give up the connection: " + early);
            Thread.sleep(200);                                             // definitely mid-body when the answer went out
            raw.send("b".repeat(199_000));                                 // the rest of the body, within the drain budget
            raw.send("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n");             // and the next request on the same connection
            assertTrue(raw.readResponse().endsWith("plain"), "the connection was kept and is in sync");
        }
        assertEquals(1, group.metrics().snapshot().connectionsOpened());
    }

    @Test
    void listenMustBeCalledInsideTheRoute() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/late-listen", "Connection: close\r\n", 3) + "abc");
            raw.readResponse();
        }
        assertThrows(IllegalStateException.class, () -> lateStream.get().listen(new BodyListener() { }));
    }

    // ---- Expect: 100-continue ----

    @Test
    void expectContinueIsAnsweredOnlyForRequestsTheRouteAccepts() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/upload", "Expect: 100-continue\r\n", 4));
            assertEquals("HTTP/1.1 100 Continue\r\n\r\n", raw.readHead(), "accepted: go ahead");
            raw.send("data");
            assertTrue(raw.readResponse().startsWith("HTTP/1.1 200"));
        }
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/reject", "Expect: 100-continue\r\nConnection: close\r\n", 1_000_000));
            String resp = raw.readResponse();
            assertTrue(resp.startsWith("HTTP/1.1 403"), "refused straight away, without 100 Continue: " + resp);
            assertTrue(resp.endsWith("not for you"));
            assertNull(raw.readHead(), "and the connection is closed (no body will follow)");
        }
    }

    // ---- early responses ----

    @Test
    void anEarlyRejectIsDeliveredEvenWhileTheClientKeepsSendingTheBody() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            int size = 3 * 1024 * 1024;
            raw.send(post("/limited", "", size));
            Thread writer = Thread.startVirtualThread(() -> {
                try {
                    byte[] chunk = new byte[16 * 1024];
                    for (int sent = 0; sent < size; sent += chunk.length) raw.out.write(chunk);
                    raw.out.flush();
                } catch (IOException ignored) { }
            });
            String resp = raw.readResponse(); // must not die with "connection reset" even though 3 MB were still on the wire
            assertTrue(resp.startsWith("HTTP/1.1 413"), resp);
            assertTrue(resp.contains("Connection: close"));
            writer.join(10_000);
            assertNull(raw.readHead(), "the server closes after draining");
        }
        assertEquals(1, bodyErrors.get(), "the body listener is told the body will not complete");
        assertTrue(bodyError.get(5, TimeUnit.SECONDS).getMessage().contains("response was sent before"));
    }

    @Test
    void drainingIsBoundedSoANeverEndingUploadIsCutOff() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/limited", "", 2_000_000_000L));
            byte[] chunk = new byte[64 * 1024];
            AtomicLong sent = new AtomicLong();
            Thread writer = Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0; i < 20_000; i++) { // far beyond the 8 MiB drain budget
                        raw.out.write(chunk);
                        sent.addAndGet(chunk.length);
                    }
                } catch (IOException cut) { /* the server hung up on us: expected */ }
            });
            writer.join(30_000);
            assertFalse(writer.isAlive());
            assertTrue(sent.get() < 200L * 1024 * 1024, "the server must stop reading after the drain budget; client managed to send " + sent.get());
        }
        for (int i = 0; i < 100 && group.metrics().snapshot().activeConnections() > 0; i++) Thread.sleep(50);
        assertEquals(0, group.metrics().snapshot().activeConnections());
    }

    // ---- flow control ----

    @Test
    void pausingTheBodyStopsDeliveryAndSlowsTheSender() throws Exception {
        Server server = start();
        int total = 40 * 1024 * 1024;
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/pausing", "", total));
            AtomicLong sent = new AtomicLong();
            Thread writer = Thread.startVirtualThread(() -> {
                try {
                    byte[] chunk = new byte[64 * 1024];
                    for (long i = 0; i < total; i += chunk.length) {
                        raw.out.write(chunk);
                        sent.addAndGet(chunk.length);
                    }
                } catch (IOException ignored) { }
            });
            for (int i = 0; i < 100 && pausedControl.get() == null; i++) Thread.sleep(20);
            assertNotNull(pausedControl.get(), "the route paused");
            Thread.sleep(800);
            int eventsWhilePaused = bodyEvents.size();
            long sentWhilePaused = sent.get();
            Thread.sleep(500);
            assertEquals(eventsWhilePaused, bodyEvents.size(), "nothing is delivered while paused");
            assertTrue(sentWhilePaused < total / 2, "TCP backpressure must have throttled the sender; it sent " + sentWhilePaused + " of " + total);
            pausedControl.get().resume();
            String resp = raw.readResponse();
            assertTrue(resp.endsWith("bytes=" + total), resp);
            writer.join(10_000);
        }
    }

    // ---- failures ----

    @Test
    void clientDisconnectMidBodyIsReportedToTheListenerOnce() throws Exception {
        Server server = start();
        Raw raw = new Raw(port(server));
        raw.send(post("/upload", "", 1_000_000) + "only a little");
        Thread.sleep(200);
        raw.close();
        assertTrue(bodyError.get(5, TimeUnit.SECONDS).getMessage().contains("closed before the request body"));
        Thread.sleep(200);
        assertEquals(1, bodyErrors.get());
        assertEquals(0, bodyCompletes.get());
        for (int i = 0; i < 100 && group.metrics().snapshot().activeConnections() > 0; i++) Thread.sleep(20);
        assertEquals(0, group.metrics().snapshot().activeConnections());
    }

    @Test
    void aThrowingListenerAndAFailingHandlerBothBecome500() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/throwing-listener", "", 10) + "0123456789");
            String resp = raw.readResponse();
            assertTrue(resp.startsWith("HTTP/1.1 500"), resp);
        }
        try (Raw raw = new Raw(port(server))) {
            raw.send(post("/failing", "", 3) + "abc");
            assertTrue(raw.readResponse().startsWith("HTTP/1.1 500"));
        }
        try (Raw raw = new Raw(port(server))) { // the server is still healthy
            raw.send("GET /plain HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            assertTrue(raw.readResponse().endsWith("plain"));
        }
    }

    @Test
    void malformedChunkingMidUploadAbortsTheListener() throws Exception {
        Server server = start();
        try (Raw raw = new Raw(port(server))) {
            raw.send("POST /upload HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\nZZ\r\n");
            assertTrue(raw.readResponse().startsWith("HTTP/1.1 400"));
        }
        assertTrue(bodyError.get(5, TimeUnit.SECONDS) instanceof IOException);
    }

    // ---- TLS ----

    @Test
    void uploadOverHttps(@TempDir Path dir) throws Exception {
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

        Server server = start(Options.defaults(), serverCtx);
        byte[] data = new byte[5 * 1024 * 1024];
        new java.util.Random(8).nextBytes(data);
        var client = java.net.http.HttpClient.newBuilder().sslContext(clientCtx).build();
        var resp = client.send(java.net.http.HttpRequest.newBuilder(URI.create("https://localhost:" + port(server) + "/upload"))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(data)).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals("sha=" + hex(sha256().digest(data)) + " bytes=" + data.length + " declared=" + data.length, resp.body());
    }
}
