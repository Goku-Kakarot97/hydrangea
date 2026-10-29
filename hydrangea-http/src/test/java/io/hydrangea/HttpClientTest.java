package io.hydrangea;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class HttpClientTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
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

    private HttpClient client(HttpClientConfig cfg) {
        HttpClient c = new HttpClient(clientGroup, cfg);
        cleanup.add(c);
        return c;
    }

    private HttpClient client() {
        return client(HttpClientConfig.defaults());
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private static Throwable failure(CompletableFuture<?> f) {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("expected the request to fail but: " + e, e);
        }
        throw new AssertionError("expected the request to fail but it succeeded");
    }

    private static ClientResponse ok(CompletableFuture<ClientResponse> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    /** Raw scripted server: one virtual thread per connection runs {@code script}. */
    private final class Raw implements AutoCloseable {
        final ServerSocket ss;
        final AtomicInteger accepted = new AtomicInteger();
        final java.util.concurrent.ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        Raw(Consumer<Socket> script) throws IOException {
            ss = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
            pool.execute(() -> {
                try {
                    for (;;) {
                        Socket s = ss.accept();
                        accepted.incrementAndGet();
                        pool.execute(() -> {
                            try (s) {
                                script.accept(s);
                            } catch (Exception ignored) {
                                // client went away
                            }
                        });
                    }
                } catch (IOException closed) {
                    // server closed
                }
            });
            cleanup.add(this);
        }

        int port() {
            return ss.getLocalPort();
        }

        String url(String path) {
            return "http://127.0.0.1:" + port() + path;
        }

        @Override
        public void close() throws IOException {
            ss.close();
            pool.shutdownNow();
        }
    }

    /** Reads one request (head + Content-Length body); returns the head, or null on EOF. */
    private static String readRequest(Socket s) throws IOException {
        InputStream in = s.getInputStream();
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) >= 0) {
            head.write(b);
            byte[] h = head.toByteArray();
            int n = h.length;
            if (n >= 4 && h[n - 4] == '\r' && h[n - 3] == '\n' && h[n - 2] == '\r' && h[n - 1] == '\n') {
                String text = new String(h, StandardCharsets.ISO_8859_1);
                int i = text.toLowerCase().indexOf("content-length:");
                if (i >= 0) {
                    int len = Integer.parseInt(text.substring(i + 15, text.indexOf('\r', i)).trim());
                    in.readNBytes(len);
                }
                return text;
            }
        }
        return null;
    }

    private static void write(Socket s, String data) throws IOException {
        OutputStream out = s.getOutputStream();
        out.write(data.getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    // ---- basics against the Hydrangea server ----

    private Server routerServer(HttpRouter router) throws IOException {
        Server s = HttpServer.bind(serverGroup, ANY, router);
        cleanup.add(s);
        return s;
    }

    @Test
    void getPostHeadersAndStatus() throws Exception {
        Server server = routerServer(new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hi " + r.param("n", "?")).header("X-Seen-UA", String.valueOf(r.header("User-Agent"))))
                .post("/echo", r -> HttpResponse.status(201).header("X-Len", r.header("Content-Length")).body(r.body()))
                .get("/missing-header", r -> HttpResponse.ok(String.valueOf(r.header("X-Custom")))));
        HttpClient c = client();
        String base = "http://127.0.0.1:" + port(server);

        ClientResponse get = ok(c.get(base + "/hello?n=Sinh"));
        assertEquals(200, get.status());
        assertEquals("hi Sinh", get.bodyAsString());
        assertEquals("Hydrangea/0.1", get.header("x-seen-ua"));
        assertEquals("OK", get.reason());
        assertTrue(get.isSuccessful());

        ClientResponse post = ok(c.send(ClientRequest.post(base + "/echo").body("xin chào")));
        assertEquals(201, post.status());
        assertEquals("xin chào", post.bodyAsString());
        assertEquals("9", post.header("x-len"), "Content-Length is bytes, not chars");

        assertEquals("v", ok(c.send(ClientRequest.get(base + "/missing-header").header("X-Custom", "v"))).bodyAsString());
        assertEquals(404, ok(c.get(base + "/nope")).status());
    }

    @Test
    void keepAliveReusesOneConnection() throws Exception {
        Server server = routerServer(new HttpRouter().get("/n", r -> HttpResponse.ok("x")));
        HttpClient c = client();
        for (int i = 0; i < 6; i++) {
            assertEquals("x", ok(c.get("http://127.0.0.1:" + port(server) + "/n")).bodyAsString());
        }
        assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened());
    }

    @Test
    void concurrencyIsCappedPerHostAndQueuedRequestsComplete() throws Exception {
        var scheduler = Executors.newScheduledThreadPool(1);
        cleanup.add(scheduler::shutdownNow);
        Server server = routerServer(new HttpRouter().getAsync("/slow", r -> {
            var f = new CompletableFuture<HttpResponse>();
            scheduler.schedule(() -> f.complete(HttpResponse.ok("done")), 50, TimeUnit.MILLISECONDS);
            return f;
        }));
        HttpClient c = client(HttpClientConfig.defaults().withMaxConnectionsPerHost(3));
        List<CompletableFuture<ClientResponse>> fs = new ArrayList<>();
        for (int i = 0; i < 30; i++) fs.add(c.get("http://127.0.0.1:" + port(server) + "/slow"));
        for (var f : fs) assertEquals("done", ok(f).bodyAsString());
        long opened = serverGroup.metrics().snapshot().connectionsOpened();
        assertTrue(opened <= 3, "never more than 3 connections, opened=" + opened);
    }

    @Test
    void largeBodiesBothWays() throws Exception {
        byte[] data = new byte[5 * 1024 * 1024];
        new java.util.Random(7).nextBytes(data);
        Server server = HttpServer.bind(serverGroup, ANY, Options.defaults(), null, new HttpConfig(16 * 1024, 8 * 1024 * 1024),
                new HttpRouter().post("/echo", r -> HttpResponse.status(200).body(r.body())));
        cleanup.add(server);
        HttpClient c = client(HttpClientConfig.defaults().withMaxResponseBytes(8 * 1024 * 1024));
        ClientResponse r = ok(c.send(ClientRequest.post("http://127.0.0.1:" + port(server) + "/echo").body(data)));
        assertArrayEquals(data, r.body());
    }

    // ---- interoperability with an independent server (JDK) ----

    @Test
    void worksAgainstJdkHttpServerIncludingChunkedResponses() throws Exception {
        var jdk = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jdk.createContext("/chunked", ex -> {
            ex.sendResponseHeaders(200, 0); // length 0 => Transfer-Encoding: chunked
            try (OutputStream o = ex.getResponseBody()) {
                o.write("chunk-".getBytes());
                o.flush();
                o.write("ok".getBytes());
            }
        });
        jdk.createContext("/echo", ex -> {
            byte[] in = ex.getRequestBody().readAllBytes();
            ex.sendResponseHeaders(200, in.length);
            try (OutputStream o = ex.getResponseBody()) {
                o.write(in);
            }
        });
        jdk.start();
        cleanup.add(() -> jdk.stop(0));
        HttpClient c = client();
        String base = "http://127.0.0.1:" + jdk.getAddress().getPort();
        assertEquals("chunk-ok", ok(c.get(base + "/chunked")).bodyAsString());
        assertEquals("ping", ok(c.send(ClientRequest.post(base + "/echo").body("ping"))).bodyAsString());
        assertEquals("chunk-ok", ok(c.get(base + "/chunked")).bodyAsString(), "connection reusable after a chunked response");
    }

    // ---- response framing ----

    @Test
    void chunkedResponseWithExtensionsAndTrailers() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;ext=1\r\nWiki\r\n");
                Thread.sleep(30);
                write(s, "5\r\npedia\r\n0\r\nTrailer-X: y\r\n\r\n");
                readRequest(s); // keep the connection open for reuse checks
            } catch (Exception ignored) { }
        })) {
            assertEquals("Wikipedia", ok(client().get(raw.url("/"))).bodyAsString());
        }
    }

    @Test
    void closeDelimitedBodyEndsAtEofAndConnectionIsNotReused() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.0 200 OK\r\n\r\nuntil-close");
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            assertEquals("until-close", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals("until-close", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals(2, raw.accepted.get());
        }
    }

    @Test
    void headResponseHasNoBodyAndConnectionStaysReusable() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                while (readRequest(s) != null) {
                    write(s, "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n"); // HEAD: length but no body
                }
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            ClientResponse r = ok(c.send(ClientRequest.head(raw.url("/"))));
            assertEquals(0, r.body().length);
            assertEquals("1000", r.header("content-length"));
            ok(c.send(ClientRequest.head(raw.url("/"))));
            assertEquals(1, raw.accepted.get());
        }
    }

    @Test
    void noBodyStatusesAndInterimResponses() throws Exception {
        AtomicInteger n = new AtomicInteger();
        try (Raw raw = new Raw(s -> {
            try {
                while (readRequest(s) != null) {
                    switch (n.getAndIncrement()) {
                        case 0 -> write(s, "HTTP/1.1 204 No Content\r\nContent-Length: 5\r\n\r\n");
                        case 1 -> write(s, "HTTP/1.1 304 Not Modified\r\n\r\n");
                        default -> write(s, "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                    }
                }
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            assertEquals(204, ok(c.get(raw.url("/"))).status());
            assertEquals(304, ok(c.get(raw.url("/"))).status());
            ClientResponse r = ok(c.get(raw.url("/")));
            assertEquals(200, r.status());
            assertEquals("ok", r.bodyAsString());
            assertEquals(1, raw.accepted.get());
        }
    }

    // ---- hostile or broken servers ----

    @Test
    void malformedResponsesFailTheRequestWithIoException() throws Exception {
        String[] bad = {
                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                "garbage\r\n\r\n",
                "HTTP/2 200 OK\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nabcd",
                "HTTP/1.1 200 OK\nContent-Length: 0\n\n",
                "HTTP/1.1 200 OK\r\nBad Header : v\r\n\r\n",
                "HTTP/1.1 200 OK\r\n folded: v\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n",
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\n",
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
                "HTTP/1.1 200 OK\r\nX: " + "a".repeat(20_000),
                "HTTP/1.1 101 Switching Protocols\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc", // then EOF
        };
        for (String response : bad) {
            try (Raw raw = new Raw(s -> {
                try {
                    readRequest(s);
                    write(s, response);
                } catch (Exception ignored) { }
            })) {
                Throwable t = failure(client().get(raw.url("/")));
                assertInstanceOf(IOException.class, t, "for response: " + response.replace("\r", "\\r").replace("\n", "\\n"));
            }
        }
    }

    @Test
    void responseSizeLimitIsEnforcedForFixedAndChunkedBodies() throws Exception {
        HttpClient c = client(HttpClientConfig.defaults().withMaxResponseBytes(100));
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 101\r\n\r\n" + "x".repeat(101));
            } catch (Exception ignored) { }
        })) {
            assertInstanceOf(IOException.class, failure(c.get(raw.url("/"))));
        }
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n65\r\n" + "x".repeat(101) + "\r\n0\r\n\r\n");
            } catch (Exception ignored) { }
        })) {
            assertInstanceOf(IOException.class, failure(c.get(raw.url("/"))));
        }
        try (Raw raw = new Raw(s -> { // endless close-delimited body
            try {
                readRequest(s);
                write(s, "HTTP/1.0 200 OK\r\n\r\n");
                for (int i = 0; i < 1000; i++) write(s, "y".repeat(100));
            } catch (Exception ignored) { }
        })) {
            assertInstanceOf(IOException.class, failure(c.get(raw.url("/"))));
        }
    }

    @Test
    void unsolicitedDataOnIdleConnectionDropsIt() throws Exception {
        AtomicInteger n = new AtomicInteger();
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                Thread.sleep(100);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nextra"); // nobody asked
                if (n.incrementAndGet() == 1) Thread.sleep(300);
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            assertEquals("ok", ok(c.get(raw.url("/"))).bodyAsString());
            Thread.sleep(250);
            assertEquals("ok", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals(2, raw.accepted.get(), "the poisoned connection must not be reused");
        }
    }

    @Test
    void extraBytesAfterAResponseMakeTheConnectionNonReusable() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokEXTRA-SMUGGLED-BYTES");
                readRequest(s);
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            assertEquals("ok", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals("ok", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals(2, raw.accepted.get());
        }
    }

    // ---- timeouts, cancellation, connect errors, close ----

    @Test
    void requestTimeoutFailsAndClosesTheConnection() throws Exception {
        CountDownLatch serverSawClose = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                if (s.getInputStream().read() < 0) serverSawClose.countDown(); // blocks until the client hangs up
            } catch (Exception ignored) { }
        })) {
            long t0 = System.nanoTime();
            Throwable t = failure(client().send(ClientRequest.get(raw.url("/")).timeout(Duration.ofMillis(300))));
            assertInstanceOf(SocketTimeoutException.class, t);
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 3000);
            assertTrue(serverSawClose.await(5, TimeUnit.SECONDS), "a timed-out connection must be closed, not pooled");
        }
    }

    @Test
    void cancellingTheFutureClosesTheConnection() throws Exception {
        CountDownLatch requestSeen = new CountDownLatch(1);
        CountDownLatch serverSawClose = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                requestSeen.countDown();
                if (s.getInputStream().read() < 0) serverSawClose.countDown();
            } catch (Exception ignored) { }
        })) {
            var f = client().get(raw.url("/"));
            assertTrue(requestSeen.await(5, TimeUnit.SECONDS));
            assertTrue(f.cancel(true));
            assertTrue(serverSawClose.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void queuedRequestTimesOutWithoutNeedingAConnection() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                Thread.sleep(2000);
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client(HttpClientConfig.defaults().withMaxConnectionsPerHost(1));
            var first = c.send(ClientRequest.get(raw.url("/")).timeout(Duration.ofSeconds(5)));
            var queued = c.send(ClientRequest.get(raw.url("/")).timeout(Duration.ofMillis(200)));
            assertInstanceOf(SocketTimeoutException.class, failure(queued));
            first.cancel(true);
            assertEquals(1, raw.accepted.get(), "the queued request never opened a second connection");
        }
    }

    @Test
    void connectionRefusedFailsAndFreesTheSlot() throws Exception {
        int dead;
        try (ServerSocket tmp = new ServerSocket(0)) {
            dead = tmp.getLocalPort();
        }
        HttpClient c = client(HttpClientConfig.defaults().withMaxConnectionsPerHost(1));
        for (int i = 0; i < 3; i++) { // the single slot must be released each time, or the 2nd request would hang
            assertInstanceOf(IOException.class, failure(c.get("http://127.0.0.1:" + dead + "/")));
        }
    }

    @Test
    void unknownHostFails() {
        assertInstanceOf(IOException.class, failure(client().get("http://no-such-host.invalid/")));
    }

    @Test
    void closingTheClientFailsPendingAndFutureRequests() throws Exception {
        CountDownLatch requestSeen = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequest(s);
                requestSeen.countDown();
                Thread.sleep(3000);
            } catch (Exception ignored) { }
        })) {
            HttpClient c = new HttpClient(clientGroup, HttpClientConfig.defaults());
            var inflight = c.get(raw.url("/"));
            assertTrue(requestSeen.await(5, TimeUnit.SECONDS));
            c.close();
            assertInstanceOf(IOException.class, failure(inflight));
            assertInstanceOf(IOException.class, failure(c.get(raw.url("/"))));
        }
    }

    // ---- stale pooled connections ----

    /** First connection: answers request 1, then swallows request 2 and hangs up (the "server closed an idle connection" race). */
    private Raw flakyServer() throws IOException {
        AtomicInteger conn = new AtomicInteger();
        return new Raw(s -> {
            try {
                int id = conn.incrementAndGet();
                readRequest(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nr" + id + "a");
                if (id == 1) {
                    readRequest(s); // request 2 arrives on the reused connection...
                    return;         // ...and we close without answering
                }
                while (readRequest(s) != null) write(s, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nr" + id + "b");
            } catch (Exception ignored) { }
        });
    }

    @Test
    void idempotentRequestIsRetriedOnceOnAFreshConnection() throws Exception {
        try (Raw raw = flakyServer()) {
            HttpClient c = client();
            assertEquals("r1a", ok(c.get(raw.url("/"))).bodyAsString());
            assertEquals("r2a", ok(c.get(raw.url("/"))).bodyAsString(), "answered by the second connection: retried transparently");
            assertEquals(2, raw.accepted.get());
        }
    }

    @Test
    void nonIdempotentRequestIsNotRetried() throws Exception {
        try (Raw raw = flakyServer()) {
            HttpClient c = client();
            assertEquals("r1a", ok(c.send(ClientRequest.post(raw.url("/")).body("x"))).bodyAsString());
            assertInstanceOf(IOException.class, failure(c.send(ClientRequest.post(raw.url("/")).body("x"))));
            assertEquals(1, raw.accepted.get());
        }
    }

    // ---- metrics ----

    @Test
    void clientCountsRequestsByClassRedirectsErrorsAndTimesThem() throws Exception {
        CaptureSink sink = new CaptureSink();
        clientGroup.metrics().bindTo(sink);
        Server server = routerServer(new HttpRouter()
                .get("/ok", r -> HttpResponse.ok("fine"))
                .get("/missing", r -> HttpResponse.status(404).text("no"))
                .get("/boom", r -> HttpResponse.status(503).text("down"))
                .get("/go", r -> HttpResponse.status(302).header("Location", "/ok")));
        String base = "http://127.0.0.1:" + port(server);
        HttpClient c = client();
        ok(c.get(base + "/ok"));
        ok(c.get(base + "/ok"));
        assertEquals(404, ok(c.get(base + "/missing")).status());
        assertEquals(503, ok(c.get(base + "/boom")).status());
        assertEquals("fine", ok(c.get(base + "/go")).bodyAsString());
        int dead;
        try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) {
            dead = ss.getLocalPort();
        }
        failure(c.get("http://127.0.0.1:" + dead + "/ok"));   // nobody listens there

        CaptureSink.eventually(() -> sink.get("http.client.active") == 0, "every request to be finished");
        assertEquals(3, sink.get("http.client.requests{class,2xx}"), "two direct, one after the redirect");
        assertEquals(1, sink.get("http.client.requests{class,4xx}"));
        assertEquals(1, sink.get("http.client.requests{class,5xx}"));
        assertEquals(0, sink.get("http.client.requests{class,3xx}"), "a followed redirect is not an answer");
        assertEquals(1, sink.get("http.client.requests{class,error}"));
        assertEquals(1, sink.get("http.client.redirects"));
        assertEquals(6, sink.get("http.client.duration.count"));
        assertTrue(sink.get("http.client.duration.sum") > 0);
    }

    // ---- redirects ----

    @Test
    void redirects() throws Exception {
        Server server = routerServer(new HttpRouter()
                .get("/start", r -> HttpResponse.status(302).header("Location", "/target?x=1"))
                .get("/target", r -> HttpResponse.ok("target:" + r.method() + ":" + r.param("x", "-")))
                .post("/post301", r -> HttpResponse.status(301).header("Location", "/target"))
                .post("/post307", r -> HttpResponse.status(307).header("Location", "/echo"))
                .post("/echo", r -> HttpResponse.ok(r.method() + ":" + r.bodyAsString()))
                .post("/post303", r -> HttpResponse.status(303).header("Location", "/target"))
                .get("/loop", r -> HttpResponse.status(302).header("Location", "/loop"))
                .get("/bad-location", r -> HttpResponse.status(302).header("Location", "ftp://nope/"))
                .get("/chain/{n}", r -> r.pathInt("n") == 0 ? HttpResponse.ok("end")
                        : HttpResponse.status(302).header("Location", "/chain/" + (r.pathInt("n") - 1))));
        String base = "http://127.0.0.1:" + port(server);
        HttpClient c = client();

        ClientResponse r = ok(c.get(base + "/start"));
        assertEquals("target:GET:1", r.bodyAsString());
        assertEquals(base + "/target?x=1", r.uri().toString(), "uri() is the final URL");
        assertEquals("target:GET:-", ok(c.send(ClientRequest.post(base + "/post301").body("data"))).bodyAsString(), "301 turns POST into GET");
        assertEquals("target:GET:-", ok(c.send(ClientRequest.post(base + "/post303").body("data"))).bodyAsString());
        assertEquals("POST:data", ok(c.send(ClientRequest.post(base + "/post307").body("data"))).bodyAsString(), "307 keeps method and body");
        assertEquals("end", ok(c.get(base + "/chain/5")).bodyAsString());
        assertTrue(failure(c.get(base + "/chain/6")).getMessage().contains("too many redirects"));
        assertTrue(failure(c.get(base + "/loop")).getMessage().contains("too many redirects"));
        assertEquals(302, ok(c.get(base + "/bad-location")).status(), "unsupported scheme: the 3xx is returned as is");
        assertEquals(302, ok(client(HttpClientConfig.defaults().withMaxRedirects(0)).get(base + "/start")).status(), "following disabled");
    }

    @Test
    void credentialsAreNotForwardedToAnotherOrigin() throws Exception {
        Server other = routerServer(new HttpRouter().get("/who", r -> HttpResponse.ok("auth=" + r.header("Authorization") + " x=" + r.header("X-Keep"))));
        Server first = routerServer(new HttpRouter()
                .get("/same", r -> HttpResponse.status(302).header("Location", "/who"))
                .get("/who", r -> HttpResponse.ok("auth=" + r.header("Authorization") + " x=" + r.header("X-Keep")))
                .get("/hop", r -> HttpResponse.status(302).header("Location", "http://localhost:" + port(other) + "/who")));
        HttpClient c = client();
        ClientRequest req = ClientRequest.get("http://127.0.0.1:" + port(first) + "/same").header("Authorization", "Bearer secret").header("X-Keep", "1");
        assertEquals("auth=Bearer secret x=1", ok(c.send(req)).bodyAsString(), "same origin keeps credentials");
        ClientRequest hop = ClientRequest.get("http://127.0.0.1:" + port(first) + "/hop").header("Authorization", "Bearer secret").header("X-Keep", "1");
        assertEquals("auth=null x=1", ok(c.send(hop)).bodyAsString(), "different host: Authorization dropped, other headers kept");
    }

    // ---- request validation ----

    @Test
    void requestValidation() {
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("ftp://host/x"));
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("/relative"));
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("http://user:pw@host/"));
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.of("GE T", "http://h/"));
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("http://h/").header("X", "a\r\nInjected: 1"));
        assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("http://h/").header("Bad Name", "v"));
        for (String managed : new String[]{"Host", "Content-Length", "Transfer-Encoding", "Connection"}) {
            assertThrows(IllegalArgumentException.class, () -> ClientRequest.get("http://h/").header(managed, "x"));
        }
    }

    @Test
    void nonAsciiTargetMustBePercentEncoded() throws Exception {
        Server server = routerServer(new HttpRouter().get("/users/{name}", r -> HttpResponse.ok(r.pathVariable("name"))));
        HttpClient c = client();
        assertEquals("Việt", ok(c.get("http://127.0.0.1:" + port(server) + "/users/Vi%E1%BB%87t")).bodyAsString());
        assertInstanceOf(IllegalArgumentException.class, failure(c.get("http://127.0.0.1:" + port(server) + "/users/Việt")));
    }

    // ---- HTTPS ----

    private record Tls(SSLContext server, SSLContext client) {
    }

    private static Tls tls(Path dir, String san) throws Exception {
        Path ks = dir.resolve("t.p12");
        Process pr = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "t", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=test",
                "-ext", "san=" + san, "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit")
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

    private Server httpsServer(SSLContext ctx) throws IOException {
        Server s = HttpServer.bind(serverGroup, ANY, Options.defaults(), ctx, HttpConfig.defaults(),
                new HttpRouter().get("/secure", r -> HttpResponse.ok("secret")).post("/echo", r -> HttpResponse.status(200).body(r.body())));
        cleanup.add(s);
        return s;
    }

    @Test
    void httpsWithTrustedCertificate(@TempDir Path dir) throws Exception {
        Tls t = tls(dir, "dns:localhost,ip:127.0.0.1");
        Server server = httpsServer(t.server());
        HttpClient c = client(HttpClientConfig.defaults().withSslContext(t.client()));
        assertEquals("secret", ok(c.get("https://localhost:" + port(server) + "/secure")).bodyAsString());
        assertEquals("secret", ok(c.get("https://127.0.0.1:" + port(server) + "/secure")).bodyAsString());
        byte[] big = new byte[300_000];
        new java.util.Random(1).nextBytes(big);
        assertArrayEquals(big, ok(c.send(ClientRequest.post("https://localhost:" + port(server) + "/echo").body(big))).body());
        assertEquals(2, serverGroup.metrics().snapshot().connectionsOpened(), "localhost and 127.0.0.1 are separate pools: one connection each");
    }

    @Test
    void httpsRejectsUntrustedAndMismatchedCertificates(@TempDir Path dir) throws Exception {
        Tls t = tls(dir, "dns:localhost,ip:127.0.0.1");
        Server server = httpsServer(t.server());
        // default trust store does not know the self-signed certificate
        Throwable untrusted = failure(client().get("https://localhost:" + port(server) + "/secure"));
        assertInstanceOf(IOException.class, untrusted);
        // trusted certificate but for another host name
        Path other = Files.createDirectory(dir.resolve("other"));
        Tls wrongName = tls(other, "dns:other.example");
        Server server2 = httpsServer(wrongName.server());
        HttpClient strict = client(HttpClientConfig.defaults().withSslContext(wrongName.client()));
        Throwable mismatch = failure(strict.get("https://localhost:" + port(server2) + "/secure"));
        assertInstanceOf(IOException.class, mismatch, "hostname verification must reject the certificate");
    }

    private long plainServerConnections(Server plain) {
        // `plain` and `secure` share serverGroup; only the secure server's single connection may exist
        return serverGroup.metrics().snapshot().connectionsOpened() - 1;
    }

    @Test
    void httpsDowngradeRedirectIsNotFollowed(@TempDir Path dir) throws Exception {
        Tls t = tls(dir, "dns:localhost,ip:127.0.0.1");
        Server plain = routerServer(new HttpRouter().get("/x", r -> HttpResponse.ok("plain")));
        Server secure = HttpServer.bind(serverGroup, ANY, Options.defaults(), t.server(), HttpConfig.defaults(),
                new HttpRouter().get("/go", r -> HttpResponse.status(302).header("Location", "http://127.0.0.1:" + port(plain) + "/x")));
        cleanup.add(secure);
        HttpClient c = client(HttpClientConfig.defaults().withSslContext(t.client()));
        ClientResponse r = ok(c.get("https://localhost:" + port(secure) + "/go"));
        assertEquals(302, r.status(), "never silently leave TLS");
        assertEquals(0, plainServerConnections(plain), "the plain-http target must never have been contacted");
    }
}
