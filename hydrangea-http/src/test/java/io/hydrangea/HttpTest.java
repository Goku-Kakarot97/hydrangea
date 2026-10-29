package io.hydrangea;

import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpHandler;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HttpTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private static final HttpHandler ECHO = HttpHandler.sync(req -> switch (req.path()) {
        case "/hello" -> HttpResponse.ok("hello " + (req.query() == null ? "world" : req.query()));
        case "/echo" -> HttpResponse.status(200).body(req.body()).header("X-Method", req.method());
        case "/boom" -> throw new IllegalStateException("handler bug");
        default -> HttpResponse.notFound();
    });

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    /** Sends raw bytes and returns everything the server writes until it closes the connection (or 3 s). */
    private static String raw(int port, String request) throws Exception {
        try (Socket s = new Socket("127.0.0.1", port)) {
            s.setSoTimeout(3000);
            s.getOutputStream().write(request.getBytes(StandardCharsets.ISO_8859_1));
            s.getOutputStream().flush();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = s.getInputStream();
            byte[] buf = new byte[4096];
            try {
                for (int n; (n = in.read(buf)) > 0; ) out.write(buf, 0, n);
            } catch (java.net.SocketTimeoutException ignored) {
                // keep what we have
            }
            return out.toString(StandardCharsets.ISO_8859_1);
        }
    }

    @Test
    void getPostAndKeepAliveWithJdkClient() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            String base = "http://127.0.0.1:" + port(server);

            var get = client.send(HttpRequest.newBuilder(URI.create(base + "/hello?there")).build(), BodyHandlers.ofString());
            assertEquals(200, get.statusCode());
            assertEquals("hello there", get.body());
            assertTrue(get.headers().firstValue("date").isPresent());

            var post = client.send(HttpRequest.newBuilder(URI.create(base + "/echo"))
                    .POST(HttpRequest.BodyPublishers.ofString("payload")).build(), BodyHandlers.ofString());
            assertEquals("payload", post.body());
            assertEquals("POST", post.headers().firstValue("x-method").orElseThrow());

            assertEquals(404, client.send(HttpRequest.newBuilder(URI.create(base + "/nope")).build(), BodyHandlers.ofString()).statusCode());
            assertEquals(1, group.metrics().snapshot().connectionsOpened(), "all three requests reused one connection");
        }
    }

    @Test
    void serverTimesRequestsOnceASinkIsBound() throws Exception {
        java.util.Map<String, java.util.function.DoubleSupplier> meters = new java.util.concurrent.ConcurrentHashMap<>();
        group.metrics().bindTo(new MetricsSink() {
            @Override
            public void counterFunction(String name, String unit, java.util.function.DoubleSupplier total, String... tags) {
                meters.put(name, total);
            }

            @Override
            public void gauge(String name, String unit, java.util.function.DoubleSupplier value, String... tags) {
            }

            @Override
            public Counter counter(String name, String unit, String... tags) {
                return NOOP.counter(name, unit, tags);
            }

            @Override
            public Timer timer(String name, String... tags) {
                return NOOP.timer(name, tags);
            }
        });
        HttpHandler slow = HttpHandler.sync(req -> {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return HttpResponse.ok("late");
        });
        try (Server server = HttpServer.bind(group, ANY, slow)) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            for (int i = 0; i < 5; i++) {
                client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/")).build(), BodyHandlers.ofString());
            }
        }
        assertEquals(5, meters.get("http.server.duration.count").getAsDouble());
        assertTrue(meters.get("http.server.duration.sum").getAsDouble() >= 5 * 0.02, "five 20 ms handlers");
        assertEquals(5, meters.get("http.requests").getAsDouble());
    }

    @Test
    void requestsBeingHandledAreCountedAndLeaveWhenAnswered() throws Exception {
        CaptureSink sink = new CaptureSink();
        group.metrics().bindTo(sink);
        CompletableFuture<HttpResponse> gate = new CompletableFuture<>();
        HttpRouter router = new HttpRouter();
        router.getAsync("/hold", req -> gate);
        try (Server server = HttpServer.bind(group, ANY, router)) {
            assertEquals(0, sink.get("http.server.active"));
            var pending = HttpClient.newHttpClient().sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/hold")).build(), BodyHandlers.ofString());
            CaptureSink.eventually(() -> sink.get("http.server.active") == 1, "the request to be counted");
            gate.complete(HttpResponse.ok("done"));
            assertEquals("done", pending.get(5, TimeUnit.SECONDS).body());
            CaptureSink.eventually(() -> sink.get("http.server.active") == 0, "the answered request to leave");

            CompletableFuture<HttpResponse> never = new CompletableFuture<>();
            HttpRouter hang = new HttpRouter();
            hang.getAsync("/hang", req -> never);
            try (Server other = HttpServer.bind(group, ANY, hang); Socket raw = new Socket("127.0.0.1", port(other))) {
                raw.getOutputStream().write("GET /hang HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                CaptureSink.eventually(() -> sink.get("http.server.active") == 1, "the hanging request to be counted");
                raw.close();   // the server does not read while the handler works, so it learns of this only when it next writes
                never.complete(HttpResponse.ok("nobody is listening"));
                CaptureSink.eventually(() -> sink.get("http.server.active") == 0, "the request to leave once answered to a closed connection");
            }
        }
    }

    @Test
    void chunkedUploadFromJdkClient() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            String big = "abcdefghij".repeat(5000);
            var res = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/echo"))
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new java.io.ByteArrayInputStream(big.getBytes(StandardCharsets.UTF_8))))
                    .build(), BodyHandlers.ofString()); // unknown length => Transfer-Encoding: chunked
            assertEquals(big, res.body());
        }
    }

    @Test
    void pipelinedRequestsAnswerInOrderEvenWhenAsync() throws Exception {
        var slow = new CompletableFuture<HttpResponse>();
        HttpHandler h = req -> req.path().equals("/slow") ? slow : CompletableFuture.completedFuture(HttpResponse.ok(req.path()));
        try (Server server = HttpServer.bind(group, ANY, h)) {
            var executor = Executors.newSingleThreadScheduledExecutor();
            executor.schedule(() -> slow.complete(HttpResponse.ok("slow-done")), 300, TimeUnit.MILLISECONDS);
            String out = raw(port(server), "GET /slow HTTP/1.1\r\nHost: x\r\n\r\nGET /fast HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            executor.shutdown();
            int a = out.indexOf("slow-done");
            int b = out.indexOf("/fast");
            assertTrue(a > 0 && b > a, "slow response must come first: " + out);
        }
    }

    @Test
    void http10ClosesAfterResponse() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            String out = raw(port(server), "GET /hello HTTP/1.0\r\n\r\n"); // returns only because the server closed
            assertTrue(out.startsWith("HTTP/1.1 200 OK"), out);
            assertTrue(out.contains("Connection: close"), out);
        }
    }

    @Test
    void headRequestHasLengthButNoBody() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            String out = raw(port(server), "HEAD /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            assertTrue(out.contains("Content-Length: 11"), out);
            assertTrue(out.endsWith("\r\n\r\n"), "no body expected: " + out);
        }
    }

    @Test
    void handlerExceptionBecomes500AndClosesConnection() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            String out = raw(port(server), "GET /boom HTTP/1.1\r\nHost: x\r\n\r\n");
            assertTrue(out.startsWith("HTTP/1.1 500"), out);
        }
    }

    @Test
    void expectContinueGetsInterimResponse() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            try (Socket s = new Socket("127.0.0.1", port(server))) {
                s.setSoTimeout(3000);
                s.getOutputStream().write(("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                byte[] buf = new byte[64];
                int n = s.getInputStream().read(buf);
                assertTrue(new String(buf, 0, n, StandardCharsets.ISO_8859_1).startsWith("HTTP/1.1 100 Continue"));
                s.getOutputStream().write("ok".getBytes());
                String rest = new String(s.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
                assertTrue(rest.startsWith("HTTP/1.1 200") && rest.endsWith("ok"), rest);
            }
        }
    }

    // ---- hostile input ----

    @Test
    void rejectsRequestSmugglingAndMalformedInput() throws Exception {
        try (Server server = HttpServer.bind(group, ANY, ECHO)) {
            int p = port(server);
            String[] bad = {
                    "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",  // CL + TE
                    "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nabcd",               // conflicting CL
                    "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n",
                    "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 1x\r\n\r\n",
                    "GET /hello HTTP/1.1\r\n\r\n",                                    // no Host
                    "GET /hello HTTP/1.1\r\nHost: x\r\nBad Header: v\r\n\r\n",        // space in name
                    "GET /hello HTTP/1.1\r\nHost: x\r\nName : v\r\n\r\n",             // space before colon
                    "GET /hello HTTP/1.1\r\nHost: x\r\n folded: v\r\n\r\n",           // obs-fold
                    "GET /hello HTTP/1.1\nHost: x\n\n",                               // bare LF
                    "GET /hello HTTP/1.1\r\nHost: x\r\nX: a\rb\r\n\r\n",              // bare CR
                    "GET  /hello HTTP/1.1\r\nHost: x\r\n\r\n",                        // double space
                    "BAD\r\n\r\n",
                    "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\n",   // bad chunk size
            };
            for (String req : bad) {
                String out = raw(p, req);
                assertTrue(out.startsWith("HTTP/1.1 400"), "expected 400 for " + req.replace("\r", "\\r").replace("\n", "\\n") + " but got: " + out);
            }
            assertTrue(raw(p, "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n").startsWith("HTTP/1.1 501"));
            assertTrue(raw(p, "GET / HTTP/2.0\r\nHost: x\r\n\r\n").startsWith("HTTP/1.1 505"));
        }
    }

    @Test
    void enforcesSizeLimits() throws Exception {
        HttpConfig cfg = new HttpConfig(512, 100);
        try (Server server = HttpServer.bind(group, ANY, Options.defaults(), null, cfg, ECHO)) {
            int p = port(server);
            String bigHeader = "GET /hello HTTP/1.1\r\nHost: x\r\nX: " + "a".repeat(1000) + "\r\n\r\n";
            assertTrue(raw(p, bigHeader).startsWith("HTTP/1.1 431"));
            // header never terminated: must also be cut off, not buffered forever
            assertTrue(raw(p, "GET /hello HTTP/1.1\r\nHost: x\r\nX: " + "a".repeat(2000)).startsWith("HTTP/1.1 431"));
            assertTrue(raw(p, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 101\r\n\r\n").startsWith("HTTP/1.1 413"));
            assertTrue(raw(p, "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n65\r\n").startsWith("HTTP/1.1 413"));
        }
    }

    @Test
    void responseHeaderInjectionIsRejectedByApi() {
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").header("X-A", "v\r\nSet-Cookie: evil=1"));
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").header("Bad Name", "v"));
    }

    @Test
    void idleKeepAliveConnectionIsClosed() throws Exception {
        Options opts = Options.defaults().withIdleTimeout(Duration.ofMillis(200));
        try (Server server = HttpServer.bind(group, ANY, opts, null, HttpConfig.defaults(), ECHO)) {
            try (Socket s = new Socket("127.0.0.1", port(server))) {
                s.setSoTimeout(3000);
                assertEquals(-1, s.getInputStream().read(), "server should close the idle connection");
            }
        }
    }

    // ---- HTTPS ----

    @Test
    void httpsWithJdkClient(@TempDir Path dir) throws Exception {
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

        CaptureSink sink = new CaptureSink();
        group.metrics().bindTo(sink);
        try (Server server = HttpServer.bind(group, ANY, Options.defaults(), serverCtx, HttpConfig.defaults(), ECHO)) {
            HttpClient client = HttpClient.newBuilder().sslContext(clientCtx).version(HttpClient.Version.HTTP_1_1).build();
            var res = client.send(HttpRequest.newBuilder(URI.create("https://localhost:" + port(server) + "/echo"))
                    .POST(HttpRequest.BodyPublishers.ofString("secret")).build(), BodyHandlers.ofString());
            assertEquals(200, res.statusCode());
            assertEquals("secret", res.body());
            String ok = sink.meters.keySet().stream().filter(k -> k.startsWith("tls.handshakes{result,ok,protocol,TLS")).findFirst().orElseThrow();
            assertEquals(1, sink.get(ok), ok);
            assertEquals(1, sink.get("tls.handshake.duration.count"));
            assertTrue(sink.get("tls.handshake.duration.sum") > 0);

            try (Socket plain = new Socket("127.0.0.1", port(server))) {   // speaks plain text to a TLS port
                plain.getOutputStream().write("GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                CaptureSink.eventually(() -> sink.meters.containsKey("tls.handshakes{result,failed}") && sink.get("tls.handshakes{result,failed}") == 1, "the refused handshake to be counted");
            }
        }
    }

    /**
     * A body big enough to be sent as its own buffer, read slowly so the socket buffer fills and the connection has to write it in
     * several partial steps (each capped at 256 KiB). Every byte must arrive, in order, for several responses on one connection,
     * and a HEAD for the same resource must carry the headers but no body.
     */
    @Test
    void aLargeBodyIsDeliveredIntactToASlowReader() throws Exception {
        byte[] body = new byte[3 * 1024 * 1024 + 123];
        new java.util.Random(11).nextBytes(body);
        Server server = HttpServer.bind(group, ANY, Options.defaults(), null, HttpConfig.defaults(),
                HttpHandler.sync(req -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(body)));
        try (Socket s = new Socket("127.0.0.1", port(server))) {
            s.setSoTimeout(10_000);
            s.setReceiveBufferSize(32 * 1024);
            var out = s.getOutputStream();
            var in = new java.io.DataInputStream(new java.io.BufferedInputStream(s.getInputStream(), 8192));
            for (int round = 0; round < 3; round++) {
                out.write("GET /big HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                String line = in.readLine();
                assertEquals("HTTP/1.1 200 OK", line);
                int length = -1;
                for (String h; !(h = in.readLine()).isEmpty(); ) {
                    if (h.toLowerCase().startsWith("content-length:")) {
                        length = Integer.parseInt(h.substring(15).trim());
                    }
                }
                assertEquals(body.length, length);
                byte[] got = new byte[length];
                for (int off = 0; off < length; ) {
                    int n = in.read(got, off, Math.min(64 * 1024, length - off));
                    assertTrue(n > 0);
                    off += n;
                    if (off % (512 * 1024) < 64 * 1024) {
                        Thread.sleep(2); // a reader slower than the sender
                    }
                }
                assertArrayEquals(body, got, "body corrupted in round " + round);
            }
            out.write("HEAD /big HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
            String rest = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
            assertTrue(rest.startsWith("HTTP/1.1 200 OK"), rest);
            assertTrue(rest.contains("Content-Length: " + body.length), rest);
            assertTrue(rest.endsWith("\r\n\r\n"), "a HEAD response must not carry a body");
        } finally {
            server.close();
        }
    }
}
