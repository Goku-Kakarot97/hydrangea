package io.hydrangea;

import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.BodySink;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.HttpStatusException;
import io.hydrangea.http.RequestBody;
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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpUploadClientTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private final List<AutoCloseable> cleanup = new ArrayList<>();
    private final AtomicReference<BodyControl> pausedControl = new AtomicReference<>();
    private final AtomicInteger serverBodyErrors = new AtomicInteger();
    private final CompletableFuture<Throwable> serverBodyError = new CompletableFuture<>();

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

    private HttpClient client(HttpClientConfig cfg) {
        HttpClient c = new HttpClient(clientGroup, cfg);
        cleanup.add(c);
        return c;
    }

    private HttpClient client() {
        return client(HttpClientConfig.defaults().withRequestTimeout(Duration.ofSeconds(60)));
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

    private static ClientResponse ok(CompletableFuture<ClientResponse> f) throws Exception {
        return f.get(60, TimeUnit.SECONDS);
    }

    private static Throwable failure(CompletableFuture<?> f) {
        try {
            f.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        }
        throw new AssertionError("expected the request to fail");
    }

    // ---- a Hydrangea server whose routes consume the body as a stream ----

    private Server uploadServer(SSLContext tls) throws IOException {
        HttpRouter router = new HttpRouter()
                .routeStream("PUT", "/upload", this::hashing)
                .postStream("/upload", this::hashing)
                .postStream("/pausing", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    long[] n = {0};
                    boolean[] paused = {false};
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer d, BodyControl c) {
                            n[0] += d.remaining();
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
                            serverBodyErrors.incrementAndGet();
                            serverBodyError.complete(t);
                        }
                    });
                    return done;
                })
                .postStream("/reject", (r, body) -> {
                    throw new HttpStatusException(403, "no uploads for you");
                })
                .routeStream("PUT", "/moved", (r, body) -> CompletableFuture.completedFuture(
                        HttpResponse.status(307).header("Location", "/upload")))
                .postStream("/see-other", (r, body) -> CompletableFuture.completedFuture(
                        HttpResponse.status(303).header("Location", "/plain")))
                .get("/plain", r -> HttpResponse.ok("plain " + r.body().length));
        Server s = HttpServer.bind(serverGroup, ANY, Options.defaults(), tls, new HttpConfig(16 * 1024, 1024), router);
        cleanup.add(s);
        return s;
    }

    private Server uploadServer() throws IOException {
        return uploadServer(null);
    }

    private java.util.concurrent.CompletionStage<HttpResponse> hashing(io.hydrangea.http.RouteRequest r, io.hydrangea.http.RequestBodyStream body) {
        var done = new CompletableFuture<HttpResponse>();
        MessageDigest md = sha256();
        long[] n = {0};
        long declared = body.contentLength();
        body.listen(new BodyListener() {
            @Override
            public void onData(ByteBuffer d, BodyControl c) {
                n[0] += d.remaining();
                md.update(d);
            }

            @Override
            public void onComplete() {
                done.complete(HttpResponse.ok("sha=" + hex(md.digest()) + " bytes=" + n[0] + " declared=" + declared));
            }

            @Override
            public void onError(Throwable t) {
                serverBodyErrors.incrementAndGet();
                serverBodyError.complete(t);
            }
        });
        return done;
    }

    private static String url(Server s, String path) {
        return "http://127.0.0.1:" + ((InetSocketAddress) s.localAddress()).getPort() + path;
    }

    private static Path randomFile(Path dir, String name, int size, long seed) throws IOException {
        Path p = dir.resolve(name);
        byte[] buf = new byte[1024 * 1024];
        java.util.Random rnd = new java.util.Random(seed);
        try (OutputStream out = Files.newOutputStream(p)) {
            for (int written = 0; written < size; written += buf.length) {
                rnd.nextBytes(buf);
                out.write(buf, 0, Math.min(buf.length, size - written));
            }
        }
        return p;
    }

    private static String sha(Path p) throws Exception {
        MessageDigest md = sha256();
        try (InputStream in = Files.newInputStream(p)) {
            byte[] b = new byte[65536];
            for (int n; (n = in.read(b)) > 0; ) md.update(b, 0, n);
        }
        return hex(md.digest());
    }

    // ---- raw scripted server ----

    private final class Raw implements AutoCloseable {
        final ServerSocket ss;
        final AtomicInteger accepted = new AtomicInteger();
        final java.util.concurrent.ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        Raw(java.util.function.Consumer<Socket> script) throws IOException {
            ss = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
            pool.execute(() -> {
                try {
                    for (;;) {
                        Socket s = ss.accept();
                        accepted.incrementAndGet();
                        pool.execute(() -> {
                            try (s) {
                                script.accept(s);
                            } catch (Exception ignored) { }
                        });
                    }
                } catch (IOException closed) { }
            });
            cleanup.add(this);
        }

        String url(String path) {
            return "http://127.0.0.1:" + ss.getLocalPort() + path;
        }

        @Override
        public void close() throws IOException {
            ss.close();
            pool.shutdownNow();
        }
    }

    private static String readHead(Socket s) throws IOException {
        InputStream in = s.getInputStream();
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

    private static void write(Socket s, String data) throws IOException {
        s.getOutputStream().write(data.getBytes(StandardCharsets.ISO_8859_1));
        s.getOutputStream().flush();
    }

    private static long headerValue(String head, String name) {
        for (String line : head.split("\r\n")) {
            if (line.toLowerCase().startsWith(name.toLowerCase() + ":")) return Long.parseLong(line.substring(name.length() + 1).trim());
        }
        return -1;
    }

    // ---- uploads ----

    @Test
    void aLargeFileIsStreamedToTheServerAndArrivesIntact(@TempDir Path dir) throws Exception {
        Server server = uploadServer();
        Path file = randomFile(dir, "big.bin", 48 * 1024 * 1024, 1);
        long t0 = System.nanoTime();
        ClientResponse r = ok(client().send(ClientRequest.put(url(server, "/upload")).body(RequestBody.file(file))));
        assertEquals(200, r.status());
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " declared=" + Files.size(file), r.bodyAsString(),
                "48 MB, 48000x the server's buffered-body limit, byte-exact");
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toSeconds() < 30);
    }

    @Test
    void anUnknownLengthStreamIsSentChunked(@TempDir Path dir) throws Exception {
        Server server = uploadServer();
        Path file = randomFile(dir, "f.bin", 5 * 1024 * 1024, 2);
        ClientResponse r = ok(client().send(ClientRequest.post(url(server, "/upload"))
                .body(RequestBody.stream(() -> {
                    try {
                        return Files.newInputStream(file);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }, -1))));
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " declared=-1", r.bodyAsString());
    }

    @Test
    void producerBodyWithBackpressureCanBeGeneratedOnTheFly() throws Exception {
        Server server = uploadServer();
        MessageDigest expected = sha256();
        long total = 20L * 1024 * 1024;
        RequestBody body = RequestBody.producer(total, false, sink -> {
            byte[] chunk = new byte[50_000];
            long sent = 0;
            long seed = 0;
            while (sent < total) {
                int n = (int) Math.min(chunk.length, total - sent);
                for (int i = 0; i < n; i++) chunk[i] = (byte) ((seed + i) * 7);
                seed += n;
                expected.update(chunk, 0, n);
                sink.whenWritable().get();
                sink.write(ByteBuffer.wrap(chunk, 0, n)).get();
                sent += n;
            }
        }); // returning from produce() finishes the body
        ClientResponse r = ok(client().send(ClientRequest.post(url(server, "/upload")).body(body)));
        assertEquals("sha=" + hex(expected.digest()) + " bytes=" + total + " declared=" + total, r.bodyAsString());
    }

    @Test
    void headersAdvertiseLengthOrChunking(@TempDir Path dir) throws Exception {
        List<String> heads = new ArrayList<>();
        try (Raw raw = new Raw(s -> {
            try {
                String head = readHead(s);
                synchronized (heads) { heads.add(head); }
                long len = headerValue(head, "Content-Length");
                if (len >= 0) {
                    s.getInputStream().readNBytes((int) len);
                } else { // chunked: read until the terminating chunk
                    ByteArrayOutputStream b = new ByteArrayOutputStream();
                    while (!b.toString(StandardCharsets.ISO_8859_1).endsWith("0\r\n\r\n")) b.write(s.getInputStream().read());
                }
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            } catch (Exception ignored) { }
        })) {
            Path file = randomFile(dir, "a.bin", 100_000, 3);
            ok(client().send(ClientRequest.put(raw.url("/")).body(RequestBody.file(file))));
            ok(client().send(ClientRequest.put(raw.url("/")).body(RequestBody.stream(() -> new java.io.ByteArrayInputStream(new byte[300]), -1))));
            Path empty = Files.createFile(dir.resolve("empty"));
            ok(client().send(ClientRequest.put(raw.url("/")).body(RequestBody.file(empty))));
            synchronized (heads) {
                assertEquals(100_000, headerValue(heads.get(0), "Content-Length"));
                assertFalse(heads.get(0).toLowerCase().contains("transfer-encoding"));
                assertTrue(heads.get(1).toLowerCase().contains("transfer-encoding: chunked"));
                assertEquals(-1, headerValue(heads.get(1), "Content-Length"));
                assertEquals(0, headerValue(heads.get(2), "Content-Length"), "an empty file is a zero-length body");
            }
        }
    }

    @Test
    void aBodyThatDisagreesWithItsDeclaredLengthFailsTheRequest() throws Exception {
        Server server = uploadServer();
        HttpClient c = client();
        var tooShort = c.send(ClientRequest.post(url(server, "/upload")).body(RequestBody.producer(1000, false, sink -> {
            sink.write(ByteBuffer.wrap(new byte[400])).get();
        })));
        assertTrue(failure(tooShort).getMessage().contains("ended after 400 of the declared 1000"), failure(tooShort).getMessage());
        var tooLong = c.send(ClientRequest.post(url(server, "/upload")).body(RequestBody.producer(10, false, sink -> {
            sink.write(ByteBuffer.wrap(new byte[50])).get();
        })));
        assertTrue(failure(tooLong).getMessage().contains("longer than the declared 10") || failure(tooLong) instanceof IOException);
        // the broken connections were discarded; the client still works
        assertEquals(200, ok(c.send(ClientRequest.post(url(server, "/upload")).body("fine"))).status());
        assertTrue(serverBodyErrors.get() >= 1, "the server saw the truncated bodies as aborted");
    }

    @Test
    void aSourceThatEndsEarlyOrThrowsFailsTheRequest(@TempDir Path dir) throws Exception {
        Server server = uploadServer();
        HttpClient c = client();
        var shortSource = c.send(ClientRequest.post(url(server, "/upload"))
                .body(RequestBody.stream(() -> new java.io.ByteArrayInputStream(new byte[10]), 100)));
        assertTrue(failure(shortSource).getMessage().contains("ended after 10 of the declared 100"));
        var throwing = c.send(ClientRequest.post(url(server, "/upload")).body(RequestBody.producer(-1, false, sink -> {
            sink.write(ByteBuffer.wrap(new byte[5])).get();
            throw new IllegalStateException("producer bug");
        })));
        Throwable t = failure(throwing);
        assertInstanceOf(IOException.class, t);
        assertEquals("producer bug", t.getCause().getMessage());
        Path file = randomFile(dir, "shrinks.bin", 1000, 4);
        RequestBody body = RequestBody.file(file);
        Files.write(file, new byte[10]); // the file changes after the request was built
        assertTrue(failure(c.send(ClientRequest.post(url(server, "/upload")).body(body))).getMessage().contains("changed size"));
    }

    // ---- backpressure ----

    @Test
    void aSlowReceiverStopsTheProducerInsteadOfFillingMemory() throws Exception {
        Server server = uploadServer();
        long total = 160L * 1024 * 1024;
        AtomicLong produced = new AtomicLong();
        CompletableFuture<ClientResponse> response = client().send(ClientRequest.post(url(server, "/pausing")).body(
                RequestBody.producer(total, false, sink -> {
                    byte[] chunk = new byte[64 * 1024];
                    while (produced.get() < total) {
                        sink.whenWritable().get();   // the flow control under test
                        sink.write(ByteBuffer.wrap(chunk)).get();
                        produced.addAndGet(chunk.length);
                    }
                })));
        for (int i = 0; i < 100 && pausedControl.get() == null; i++) Thread.sleep(20);
        assertNotNull(pausedControl.get(), "the server paused its reading");
        Thread.sleep(1000); // plenty of time to produce far more than the kernel and client buffers hold, if nothing stopped it
        long whilePaused = produced.get();
        Thread.sleep(500);
        assertEquals(whilePaused, produced.get(), "the producer is blocked, not just slow");
        assertTrue(whilePaused < 48L * 1024 * 1024, "only socket buffers' worth may be in flight; produced " + whilePaused / 1024 / 1024 + " MB of " + total / 1024 / 1024);
        pausedControl.get().resume();
        assertEquals("bytes=" + total, ok(response).bodyAsString());
    }

    // ---- Expect: 100-continue ----

    @Test
    void theBodyIsHeldBackUntilTheServerSaysContinue(@TempDir Path dir) throws Exception {
        AtomicLong bytesBeforeContinue = new AtomicLong(-1);
        try (Raw raw = new Raw(s -> {
            try {
                String head = readHead(s);
                assertTrue(head.toLowerCase().contains("expect: 100-continue"), head);
                Thread.sleep(400); // a client that does not wait would have sent the body by now
                bytesBeforeContinue.set(s.getInputStream().available());
                write(s, "HTTP/1.1 100 Continue\r\n\r\n");
                long len = headerValue(head, "Content-Length");
                byte[] body = s.getInputStream().readNBytes((int) len);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: " + String.valueOf(body.length).length() + "\r\n\r\n" + body.length);
            } catch (Exception ignored) { }
        })) {
            Path file = randomFile(dir, "b.bin", 200_000, 5);
            ClientResponse r = ok(client().send(ClientRequest.put(raw.url("/")).expectContinue().body(RequestBody.file(file))));
            assertEquals("200000", r.bodyAsString(), "the whole body arrived after the 100");
            assertEquals(0, bytesBeforeContinue.get(), "nothing was sent before the go-ahead");
        }
    }

    @Test
    void aRejectedExpectContinueNeverSendsTheBody(@TempDir Path dir) throws Exception {
        Server server = uploadServer();
        AtomicBoolean started = new AtomicBoolean();
        RequestBody spy = new RequestBody() {
            public long contentLength() { return 10_000_000; }
            public void start(BodySink sink) { started.set(true); }
        };
        HttpClient c = client();
        ClientResponse r = ok(c.send(ClientRequest.post(url(server, "/reject")).expectContinue().body(spy)));
        assertEquals(403, r.status());
        assertEquals("no uploads for you", r.bodyAsString());
        assertFalse(started.get(), "the 10 MB body was never even requested from the source");
        // the connection had an incomplete request on it, so it must not be reused
        assertEquals(200, ok(c.send(ClientRequest.get(url(server, "/plain")))).status());
        assertEquals(2, serverGroup.metrics().snapshot().connectionsOpened());
    }

    @Test
    void ifTheServerNeverAnswersContinueTheBodyIsSentAfterAboutASecond() throws Exception {
        AtomicLong headAt = new AtomicLong();
        AtomicLong bodyAt = new AtomicLong();
        try (Raw raw = new Raw(s -> {
            try {
                String head = readHead(s);
                headAt.set(System.nanoTime());
                s.getInputStream().readNBytes((int) headerValue(head, "Content-Length")); // an old server that ignores Expect
                bodyAt.set(System.nanoTime());
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            } catch (Exception ignored) { }
        })) {
            ClientResponse r = ok(client().send(ClientRequest.put(raw.url("/")).expectContinue()
                    .body(RequestBody.producer(5, true, sink -> sink.write(ByteBuffer.wrap("hello".getBytes())).get()))));
            assertEquals("ok", r.bodyAsString());
            long waitedMs = (bodyAt.get() - headAt.get()) / 1_000_000;
            assertTrue(waitedMs >= 800 && waitedMs < 3000, "waited about 1 s for the go-ahead before sending anyway: " + waitedMs + " ms");
        }
    }

    // ---- the server answers before the upload is finished ----

    @Test
    void anEarlyAnswerIsReturnedAndTheUploadIsAbandoned(@TempDir Path dir) throws Exception {
        AtomicInteger conns = new AtomicInteger();
        try (Raw raw = new Raw(s -> {
            try {
                conns.incrementAndGet();
                readHead(s);
                write(s, "HTTP/1.1 401 Unauthorized\r\nContent-Length: 6\r\n\r\ndenied"); // before reading any body
                long end = System.currentTimeMillis() + 3000;
                while (System.currentTimeMillis() < end && s.getInputStream().read(new byte[65536]) >= 0) { /* drain politely */ }
            } catch (Exception ignored) { }
        })) {
            Path file = randomFile(dir, "huge.bin", 30 * 1024 * 1024, 6);
            HttpClient c = client();
            ClientResponse r = ok(c.send(ClientRequest.put(raw.url("/")).body(RequestBody.file(file))));
            assertEquals(401, r.status());
            assertEquals("denied", r.bodyAsString());
            // the half-sent connection must not be reused
            ok(c.send(ClientRequest.get(raw.url("/"))));
            assertEquals(2, raw.accepted.get());
        }
    }

    // ---- retry, redirect, cancel, timeout ----

    @Test
    void aReplayableUploadIsRetriedAfterAStaleConnectionButAOneShotSourceIsNot(@TempDir Path dir) throws Exception {
        AtomicInteger conn = new AtomicInteger();
        List<String> received = java.util.Collections.synchronizedList(new ArrayList<>());
        try (Raw raw = new Raw(s -> {
            try {
                int id = conn.incrementAndGet();
                for (;;) {
                    String head = readHead(s);
                    if (head == null) return;
                    byte[] body = s.getInputStream().readNBytes((int) Math.max(0, headerValue(head, "Content-Length")));
                    if (id == 1 && received.size() >= 1) return; // 2nd request on the first connection: hang up unanswered
                    received.add(id + ":" + hex(sha256().digest(body)).substring(0, 12));
                    write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                }
            } catch (Exception ignored) { }
        })) {
            Path file = randomFile(dir, "r.bin", 300_000, 7);
            String fileSha = sha(file).substring(0, 12);
            HttpClient c = client();
            ok(c.send(ClientRequest.put(raw.url("/")).body(RequestBody.file(file))));            // connection 1
            ok(c.send(ClientRequest.put(raw.url("/")).body(RequestBody.file(file))));            // reuse fails -> retried on connection 2
            assertEquals(List.of("1:" + fileSha, "2:" + fileSha), received, "the retry resent the whole file");

            // fresh scenario with a one-shot source: no retry, so the stale reuse is a failure
            conn.set(0);
            received.clear();
            HttpClient c2 = client();
            ok(c2.send(ClientRequest.put(raw.url("/")).body(RequestBody.file(file))));
            var once = c2.send(ClientRequest.put(raw.url("/")).body(RequestBody.streamOnce(() -> {
                try {
                    return Files.newInputStream(file);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }, Files.size(file))));
            assertInstanceOf(IOException.class, failure(once));
        }
    }

    @Test
    void redirectsResendAReplayableBodyAndRefuseToGuessWithAOneShotOne(@TempDir Path dir) throws Exception {
        Server server = uploadServer();
        Path file = randomFile(dir, "m.bin", 2 * 1024 * 1024, 8);
        HttpClient c = client();
        ClientResponse resent = ok(c.send(ClientRequest.put(url(server, "/moved")).body(RequestBody.file(file))));
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " declared=" + Files.size(file), resent.bodyAsString(),
                "307 re-sent the file to the new location");
        ClientResponse oneShot = ok(c.send(ClientRequest.put(url(server, "/moved")).body(
                RequestBody.streamOnce(() -> new java.io.ByteArrayInputStream(new byte[1000]), 1000))));
        assertEquals(307, oneShot.status(), "a body that cannot be re-sent: the redirect is returned, not followed");
        ClientResponse seeOther = ok(c.send(ClientRequest.post(url(server, "/see-other")).body(RequestBody.file(file))));
        assertEquals("plain 0", seeOther.bodyAsString(), "303 turns it into a GET without a body");
    }

    @Test
    void cancellingMidUploadClosesTheConnectionAndStopsTheProducer() throws Exception {
        Server server = uploadServer();
        AtomicLong produced = new AtomicLong();
        CompletableFuture<Throwable> producerSawFailure = new CompletableFuture<>();
        var f = client().send(ClientRequest.post(url(server, "/upload")).body(RequestBody.producer(1L << 40, false, sink -> {
            byte[] chunk = new byte[64 * 1024];
            try {
                while (true) {
                    sink.whenWritable().get();
                    sink.write(ByteBuffer.wrap(chunk)).get();
                    produced.addAndGet(chunk.length);
                }
            } catch (Exception e) {
                producerSawFailure.complete(e);
            }
        })));
        for (int i = 0; i < 200 && produced.get() < 4 * 1024 * 1024; i++) Thread.sleep(20);
        assertTrue(produced.get() >= 4 * 1024 * 1024, "the upload was under way");
        assertTrue(f.cancel(true));
        assertNotNull(producerSawFailure.get(10, TimeUnit.SECONDS), "the producer is told to stop");
        assertTrue(serverBodyError.get(10, TimeUnit.SECONDS).getMessage().contains("closed before the request body"), "the server saw the connection drop");
    }

    @Test
    void theRequestTimeoutAlsoCoversASlowUpload() throws Exception {
        Server server = uploadServer();
        CompletableFuture<Throwable> producerSawFailure = new CompletableFuture<>();
        var f = client().send(ClientRequest.post(url(server, "/upload")).timeout(Duration.ofMillis(600)).body(RequestBody.producer(-1, false, sink -> {
            try {
                while (true) {
                    sink.write(ByteBuffer.wrap(new byte[100])).get();
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                producerSawFailure.complete(e);
            }
        })));
        assertInstanceOf(SocketTimeoutException.class, failure(f));
        assertNotNull(producerSawFailure.get(10, TimeUnit.SECONDS));
    }

    // ---- interop and TLS ----

    @Test
    void uploadsAreUnderstoodByTheJdkServer(@TempDir Path dir) throws Exception {
        var jdk = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jdk.createContext("/in", ex -> {
            MessageDigest md = sha256();
            long n = 0;
            try (InputStream in = ex.getRequestBody()) {
                byte[] b = new byte[65536];
                for (int r; (r = in.read(b)) > 0; n += r) md.update(b, 0, r);
            }
            byte[] out = ("sha=" + hex(md.digest()) + " bytes=" + n + " te=" + ex.getRequestHeaders().getFirst("Transfer-Encoding")).getBytes();
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        jdk.start();
        cleanup.add(() -> jdk.stop(0));
        String base = "http://127.0.0.1:" + jdk.getAddress().getPort() + "/in";
        Path file = randomFile(dir, "j.bin", 3 * 1024 * 1024, 9);
        HttpClient c = client();
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " te=null", ok(c.send(ClientRequest.put(base).body(RequestBody.file(file)))).bodyAsString());
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " te=chunked", ok(c.send(ClientRequest.put(base).body(
                RequestBody.stream(() -> {
                    try {
                        return Files.newInputStream(file);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }, -1)))).bodyAsString());
    }

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

        Server server = uploadServer(serverCtx);
        Path file = randomFile(dir, "tls.bin", 8 * 1024 * 1024, 10);
        HttpClient c = client(HttpClientConfig.defaults().withSslContext(clientCtx).withRequestTimeout(Duration.ofSeconds(60)));
        ClientResponse r = ok(c.send(ClientRequest.put("https://localhost:" + ((InetSocketAddress) server.localAddress()).getPort() + "/upload")
                .expectContinue().body(RequestBody.file(file))));
        assertEquals("sha=" + sha(file) + " bytes=" + Files.size(file) + " declared=" + Files.size(file), r.bodyAsString());
    }

    @Test
    void requestBodyValidation() {
        assertThrows(IllegalArgumentException.class, () -> RequestBody.producer(-5, false, s -> { }));
        assertEquals(3, RequestBody.text("abc").contentLength());
        assertTrue(RequestBody.bytes(new byte[1]).isReplayable());
        assertFalse(RequestBody.streamOnce(() -> null, 0).isReplayable());
    }

    private static final class AtomicBoolean {
        private volatile boolean v;

        void set(boolean b) {
            v = b;
        }

        boolean get() {
            return v;
        }
    }
}
