package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.BodySink;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpHandler;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RequestBody;
import io.hydrangea.http.StreamHandle;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** The HTTP/2 client against Hydrangea's own HTTP/2 server (cleartext and TLS); protocol corner cases are in Http2ClientProtocolTest. */
class Http2ClientTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private final List<CompletableFuture<HttpResponse>> pending = new ArrayList<>();
    private final AtomicReference<SseEmitter> emitter = new AtomicReference<>();
    private final CountDownLatch sseOpen = new CountDownLatch(1);
    private final CountDownLatch sseClosed = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        clientGroup.close();
        serverGroup.close();
    }

    private HttpRouter router() {
        return new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version()))
                .get("/users/{id}", r -> HttpResponse.ok("user " + r.pathVariable("id") + " q=" + r.param("q", "-")))
                .post("/echo", r -> HttpResponse.status(200).body(r.body()))
                .get("/big/{n}", r -> HttpResponse.status(200).body(new byte[r.pathInt("n")]))
                .get("/redirect", r -> HttpResponse.status(302).header("Location", "/hello").text(""))
                .get("/headers", r -> HttpResponse.ok(r.request().headers().toString()))
                .get("/slow", r -> {
                    throw new UnsupportedOperationException("handled by the wrapper");
                })
                .get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        emitter.set(e);
                        sseOpen.countDown();
                    }

                    @Override
                    public void onClose(SseEmitter e) {
                        sseClosed.countDown();
                    }
                }))
                .postStream("/sink", (r, body) -> {
                    CompletableFuture<HttpResponse> done = new CompletableFuture<>();
                    AtomicLong n = new AtomicLong();
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer d, BodyControl c) {
                            n.addAndGet(d.remaining());
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok(Long.toString(n.get())));
                        }
                    });
                    return done;
                });
    }

    private HttpHandler handler() {
        HttpRouter router = router();
        return new HttpHandler() {
            @Override
            public java.util.concurrent.CompletionStage<HttpResponse> handle(io.hydrangea.http.HttpRequest request) throws Exception {
                if (request.path().equals("/slow")) {
                    CompletableFuture<HttpResponse> f = new CompletableFuture<>();
                    synchronized (pending) {
                        pending.add(f);
                    }
                    return f;
                }
                return router.handle(request);
            }

            @Override
            public boolean wantsStreamingBody(io.hydrangea.http.HttpRequest head) {
                return router.wantsStreamingBody(head);
            }

            @Override
            public java.util.concurrent.CompletionStage<HttpResponse> handleStreaming(io.hydrangea.http.HttpRequest head,
                                                                                   io.hydrangea.http.RequestBodyStream body) throws Exception {
                return router.handleStreaming(head, body);
            }
        };
    }

    private HttpConfig serverConfig(boolean http2) {
        HttpConfig c = new HttpConfig(16 * 1024, 32 * 1024 * 1024);
        return http2 ? c.withHttp2(Http2Config.defaults()) : c;
    }

    private Server cleartextServer(boolean http2) throws Exception {
        return HttpServer.bind(serverGroup, ANY, Options.defaults(), null, serverConfig(http2), handler());
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private HttpClient client(HttpClientConfig cfg) {
        return new HttpClient(clientGroup, cfg);
    }

    private static HttpClientConfig h2c() {
        return HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
    }

    private int pendingCount() {
        synchronized (pending) {
            return (int) pending.stream().filter(f -> !f.isDone()).count();
        }
    }

    private void completePending(String body) {
        synchronized (pending) {
            pending.forEach(f -> f.complete(HttpResponse.ok(body)));
        }
    }

    // ---- negotiation ----

    @Test
    void speaksHttp2OverCleartextWithPriorKnowledge() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            assertEquals("hello over HTTP/2", c.get("http://127.0.0.1:" + port(server) + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
        }
    }

    @Test
    void cleartextStaysHttp11WithoutPriorKnowledge() throws Exception {
        HttpClientConfig cfg = HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults());
        try (Server server = cleartextServer(true); HttpClient c = client(cfg)) {
            assertEquals("hello over HTTP/1.1", c.get("http://127.0.0.1:" + port(server) + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
        }
    }

    @Test
    void negotiatesH2ThroughAlpn(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server server = HttpServer.bind(serverGroup, ANY, Options.defaults(), tls[0], serverConfig(true), handler());
             HttpClient c = client(HttpClientConfig.defaults().withSslContext(tls[1]).withHttp2(Http2ClientConfig.defaults()))) {
            assertEquals("hello over HTTP/2", c.get("https://localhost:" + port(server) + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
        }
    }

    @Test
    void fallsBackToHttp11WhenTheServerDoesNotOfferH2(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server server = HttpServer.bind(serverGroup, ANY, Options.defaults(), tls[0], serverConfig(false), handler());
             HttpClient c = client(HttpClientConfig.defaults().withSslContext(tls[1]).withHttp2(Http2ClientConfig.defaults()))) {
            String base = "https://localhost:" + port(server);
            for (int i = 0; i < 5; i++) {
                assertEquals("hello over HTTP/1.1", c.get(base + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
            }
            // a burst: the first connection learned that the host is HTTP/1.1, so the rest may open more connections
            List<CompletableFuture<ClientResponse>> burst = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                burst.add(c.get(base + "/users/" + i));
            }
            for (int i = 0; i < 20; i++) {
                assertEquals("user " + i + " q=-", burst.get(i).get(10, TimeUnit.SECONDS).bodyAsString());
            }
        }
    }

    @Test
    void aBurstOfRequestsSharesOneConnection(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server server = HttpServer.bind(serverGroup, ANY, Options.defaults(), tls[0], serverConfig(true), handler());
             HttpClient c = client(HttpClientConfig.defaults().withSslContext(tls[1]).withHttp2(Http2ClientConfig.defaults()))) {
            String base = "https://localhost:" + port(server);
            List<CompletableFuture<ClientResponse>> all = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                all.add(c.get(base + "/users/" + i + "?q=x"));
            }
            for (int i = 0; i < 300; i++) {
                assertEquals("user " + i + " q=x", all.get(i).get(20, TimeUnit.SECONDS).bodyAsString());
            }
            assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened(), "all 300 requests multiplexed on one connection");
        }
    }

    // ---- bodies ----

    @Test
    void bigBodiesGoBothWays() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c().withMaxResponseBytes(64 * 1024 * 1024))) {
            String base = "http://127.0.0.1:" + port(server);
            byte[] payload = new byte[6 * 1024 * 1024];
            new Random(5).nextBytes(payload);
            ClientResponse echo = c.send(ClientRequest.post(base + "/echo").body(payload)).get(30, TimeUnit.SECONDS);
            assertArrayEquals(payload, echo.body());
            assertEquals(10_000_000, c.get(base + "/big/10000000").get(30, TimeUnit.SECONDS).body().length);
        }
    }

    @Test
    void aStreamedUploadHonoursBackpressure() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            long total = 40L * 1024 * 1024;
            byte[] chunk = new byte[64 * 1024];
            AtomicLong maxWhenWrittenWhileUnwritable = new AtomicLong();
            RequestBody body = RequestBody.producer(total, false, sink -> {
                long sent = 0;
                while (sent < total) {
                    sink.whenWritable().get(30, TimeUnit.SECONDS);
                    sink.write(ByteBuffer.wrap(chunk));
                    sent += chunk.length;
                }
                sink.finish();
            });
            ClientResponse r = c.send(ClientRequest.post("http://127.0.0.1:" + port(server) + "/sink").body(body)).get(60, TimeUnit.SECONDS);
            assertEquals(200, r.status());
            assertEquals(Long.toString(total), r.bodyAsString());
        }
    }

    @Test
    void aStreamedUploadOfUnknownLengthIsEndedByTheClient() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            RequestBody body = RequestBody.producer(-1, false, sink -> {
                for (int i = 0; i < 10; i++) {
                    sink.write(ByteBuffer.wrap(new byte[1000])).get();
                }
                sink.finish();
            });
            ClientResponse r = c.send(ClientRequest.post("http://127.0.0.1:" + port(server) + "/sink").body(body)).get(20, TimeUnit.SECONDS);
            assertEquals("10000", r.bodyAsString());
        }
    }

    @Test
    void aPausedDownloadStopsDeliveringAndResumesIntact() throws Exception {
        try (Server server = cleartextServer(true);
             HttpClient c = client(h2c().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true).withWindows(65535, 1 << 20)))) {
            AtomicLong received = new AtomicLong();
            AtomicLong atPause = new AtomicLong(-1);
            AtomicReference<BodyControl> control = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            StreamHandle h = c.stream(ClientRequest.get("http://127.0.0.1:" + port(server) + "/big/3000000"), new BodyListener() {
                boolean first = true;

                @Override
                public void onData(ByteBuffer d, BodyControl ctl) {
                    received.addAndGet(d.remaining());
                    control.set(ctl);
                    if (first) {
                        first = false;
                        atPause.set(received.get());
                        ctl.pause();
                    }
                }

                @Override
                public void onComplete() {
                    done.countDown();
                }

                @Override
                public void onError(Throwable e) {
                    done.countDown();
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (atPause.get() < 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(500);
            assertEquals(atPause.get(), received.get(), "no onData while paused");
            assertFalse(h.completion().isDone());
            control.get().resume();
            assertTrue(done.await(20, TimeUnit.SECONDS));
            assertEquals(3_000_000, received.get());
            h.completion().get(5, TimeUnit.SECONDS);
        }
    }

    // ---- behaviours ----

    @Test
    void followsRedirects() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            ClientResponse r = c.get("http://127.0.0.1:" + port(server) + "/redirect").get(10, TimeUnit.SECONDS);
            assertEquals(200, r.status());
            assertEquals("hello over HTTP/2", r.bodyAsString());
        }
    }

    @Test
    void sendsLowerCaseHeadersAndNoConnectionSpecificOnes() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            ClientResponse r = c.send(ClientRequest.get("http://127.0.0.1:" + port(server) + "/headers")
                    .header("X-Custom", "yes").header("Accept-Language", "vi")).get(10, TimeUnit.SECONDS);
            String seen = r.bodyAsString();
            assertTrue(seen.contains("x-custom"), "names go out in lower case: " + seen);
            assertFalse(seen.contains("X-Custom"), seen);
            assertTrue(seen.contains("user-agent") && seen.contains("accept-language"), seen);
            assertFalse(seen.contains("connection") || seen.contains("keep-alive") || seen.contains("transfer-encoding"), seen);
        }
    }

    @Test
    void cancellingOneRequestLeavesTheOthersAlone() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            String base = "http://127.0.0.1:" + port(server);
            CompletableFuture<ClientResponse> a = c.get(base + "/slow");
            CompletableFuture<ClientResponse> b = c.get(base + "/slow");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (pendingCount() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(2, pendingCount());
            a.cancel(true);
            Thread.sleep(200);
            completePending("done");
            assertEquals("done", b.get(10, TimeUnit.SECONDS).bodyAsString());
            assertTrue(a.isCancelled());
            assertEquals("hello over HTTP/2", c.get(base + "/hello").get(5, TimeUnit.SECONDS).bodyAsString(), "the connection is still usable");
            assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened());
        }
    }

    @Test
    void aTimeoutOnlyAffectsItsOwnRequest() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            String base = "http://127.0.0.1:" + port(server);
            CompletableFuture<ClientResponse> slowOk = c.get(base + "/slow");
            CompletableFuture<ClientResponse> shortTimeout = c.send(ClientRequest.get(base + "/slow").timeout(Duration.ofMillis(300)));
            ExecutionException e = assertThrows(ExecutionException.class, () -> shortTimeout.get(5, TimeUnit.SECONDS));
            assertInstanceOf(java.net.SocketTimeoutException.class, e.getCause());
            completePending("fine");
            assertEquals("fine", slowOk.get(10, TimeUnit.SECONDS).bodyAsString());
        }
    }

    @Test
    void respectsTheLimitOfStreamsPerConnection() throws Exception {
        HttpClientConfig cfg = h2c().withMaxConnectionsPerHost(1).withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true).withMaxStreamsPerConnection(2));
        try (Server server = cleartextServer(true); HttpClient c = client(cfg)) {
            String base = "http://127.0.0.1:" + port(server);
            List<CompletableFuture<ClientResponse>> all = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                all.add(c.get(base + "/slow"));
            }
            Thread.sleep(500);
            assertEquals(2, pendingCount(), "only two requests may be in flight on the single connection");
            int rounds = 0;
            while (all.stream().anyMatch(f -> !f.isDone()) && rounds++ < 100) {
                completePending("ok");
                Thread.sleep(50);
                assertTrue(pendingCount() <= 2);
            }
            for (var f : all) {
                assertEquals("ok", f.get(5, TimeUnit.SECONDS).bodyAsString());
            }
            assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened());
        }
    }

    @Test
    void readsAnEventStreamAndCancelsIt() throws Exception {
        try (Server server = cleartextServer(true); HttpClient c = client(h2c())) {
            StringBuilder got = new StringBuilder();
            CountDownLatch three = new CountDownLatch(1);
            StreamHandle h = c.stream(ClientRequest.get("http://127.0.0.1:" + port(server) + "/events"), new BodyListener() {
                @Override
                public void onData(ByteBuffer d, BodyControl ctl) {
                    synchronized (got) {
                        got.append(StandardCharsets.UTF_8.decode(d));
                        if (got.toString().split("\n\n").length >= 3) {
                            three.countDown();
                        }
                    }
                }
            });
            assertTrue(sseOpen.await(5, TimeUnit.SECONDS));
            for (int i = 1; i <= 3; i++) {
                emitter.get().send("event " + i);
            }
            assertTrue(three.await(5, TimeUnit.SECONDS));
            assertEquals("data: event 1\n\ndata: event 2\n\ndata: event 3\n\n", got.toString());
            h.cancel();
            assertTrue(sseClosed.await(5, TimeUnit.SECONDS), "cancelling resets the stream, which the server application sees");
            ExecutionException e = assertThrows(ExecutionException.class, () -> h.completion().get(5, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, e.getCause());
        }
    }

    @Test
    void closingTheClientFailsRunningRequests() throws Exception {
        try (Server server = cleartextServer(true)) {
            HttpClient c = client(h2c());
            CompletableFuture<ClientResponse> f = c.get("http://127.0.0.1:" + port(server) + "/slow");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (pendingCount() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            c.close();
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().contains("closed"), e.getCause().toString());
        }
    }
}
