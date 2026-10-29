package io.hydrangea.sse;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SseClientTest {

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

    private HttpClient http(HttpClientConfig cfg) {
        HttpClient c = new HttpClient(clientGroup, cfg);
        cleanup.add(c);
        return c;
    }

    private HttpClient http() {
        return http(HttpClientConfig.defaults());
    }

    /** Records everything the subscription reports. */
    static class Rec implements SseClientListener {
        final List<SseMessage> messages = new CopyOnWriteArrayList<>();
        final List<String> log = Collections.synchronizedList(new ArrayList<>());
        final List<long[]> errorTimes = new CopyOnWriteArrayList<>();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        final List<Boolean> willReconnect = new CopyOnWriteArrayList<>();
        final CountDownLatch closed = new CountDownLatch(1);
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger overlaps = new AtomicInteger();

        private void in() {
            if (concurrent.incrementAndGet() > 1) overlaps.incrementAndGet();
        }

        @Override
        public void onOpen() {
            in();
            opens.incrementAndGet();
            log.add("open");
            concurrent.decrementAndGet();
        }

        @Override
        public void onMessage(SseMessage m) {
            in();
            messages.add(m);
            concurrent.decrementAndGet();
        }

        @Override
        public void onError(Throwable error, boolean reconnect) {
            in();
            errors.add(error);
            willReconnect.add(reconnect);
            errorTimes.add(new long[]{System.nanoTime()});
            log.add("error:" + reconnect);
            concurrent.decrementAndGet();
        }

        @Override
        public void onClose() {
            in();
            closes.incrementAndGet();
            log.add("close");
            closed.countDown();
            concurrent.decrementAndGet();
        }

        boolean awaitMessages(int n) throws InterruptedException {
            for (int i = 0; i < 200 && messages.size() < n; i++) Thread.sleep(25);
            return messages.size() >= n;
        }

        List<String> data() {
            return messages.stream().map(SseMessage::data).toList();
        }
    }

    // ---- against Hydrangea's own SSE server ----

    private final List<String> lastEventIdHeaders = new CopyOnWriteArrayList<>();
    private final AtomicInteger requests = new AtomicInteger();

    private Server sseServer(SseConfig config, Supplier<SseListener> listener) throws IOException {
        return sseServer(Options.defaults(), null, config, listener);
    }

    private Server sseServer(Options options, SSLContext tls, SseConfig config, Supplier<SseListener> listener) throws IOException {
        HttpRouter router = new HttpRouter().get("/events", r -> {
            requests.incrementAndGet();
            lastEventIdHeaders.add(String.valueOf(r.header("Last-Event-ID")));
            return SseResponse.accept(r.request(), config, listener);
        });
        Server s = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), options, tls, HttpConfig.defaults(), router);
        cleanup.add(s);
        return s;
    }

    private static String url(Server s, String scheme, String host) {
        return scheme + "://" + host + ":" + ((InetSocketAddress) s.localAddress()).getPort() + "/events";
    }

    private static String url(Server s) {
        return url(s, "http", "127.0.0.1");
    }

    @Test
    void receivesTypedEventsWithIdsAndMultiLineData() throws Exception {
        Server server = sseServer(SseConfig.defaults(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                e.send(SseEvent.data("first").id("1"));
                e.send(SseEvent.data("line one\nline two").event("note").id("2"));
                e.send(SseEvent.data("Việt Nam ✓ 🎉").id("3"));
                e.send(SseEvent.comment("a comment that must not surface"));
                e.send(SseEvent.data("last").event("done"));
            }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), url(server), rec);
        assertTrue(rec.awaitMessages(4));
        assertEquals(List.of(
                new SseMessage("message", "first", "1"),
                new SseMessage("note", "line one\nline two", "2"),
                new SseMessage("message", "Việt Nam ✓ 🎉", "3"),
                new SseMessage("done", "last", "3")), rec.messages);
        assertEquals("3", sub.lastEventId());
        assertEquals(1, rec.opens.get());
        sub.close();
        assertTrue(rec.closed.await(5, TimeUnit.SECONDS));
        assertEquals(0, rec.overlaps.get());
    }

    @Test
    void reconnectsResumesFromTheLastEventIdAndHonoursTheServersRetry() throws Exception {
        AtomicInteger connection = new AtomicInteger();
        Server server = sseServer(SseConfig.defaults().withRetry(Duration.ofMillis(80)).withHeartbeat(null), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                int n = connection.incrementAndGet();
                String from = e.lastEventId(); // a reconnecting client tells us where it was
                int next = from == null ? 1 : Integer.parseInt(from) + 1;
                e.send(SseEvent.data("event-" + next).id(String.valueOf(next)));
                e.send(SseEvent.data("event-" + (next + 1)).id(String.valueOf(next + 1)));
                if (n < 3) e.close(); // drop the stream twice; the third connection stays open
            }
        });
        Rec rec = new Rec();
        long t0 = System.nanoTime();
        SseSubscription sub = SseClient.subscribe(http(), url(server), SseClientConfig.defaults().withDefaultRetry(Duration.ofSeconds(30)), rec);
        assertTrue(rec.awaitMessages(6), "got " + rec.data());
        long elapsedMs = Duration.ofNanos(System.nanoTime() - t0).toMillis();
        assertEquals(List.of("event-1", "event-2", "event-3", "event-4", "event-5", "event-6"), rec.data(), "no gap, no duplicate");
        assertEquals(List.of("null", "2", "4"), lastEventIdHeaders, "each reconnect resumes after the last id it saw");
        assertEquals(3, rec.opens.get());
        assertTrue(elapsedMs < 10_000, "the server's retry:80 must override the 30 s default; took " + elapsedMs + " ms");
        assertEquals(List.of(true, true), rec.willReconnect.subList(0, 2));
        sub.close();
    }

    @Test
    void noContentEndsTheSubscriptionForGood() throws Exception {
        Server server = sseServer(SseConfig.defaults(), () -> new SseListener() { });
        // a route that answers 204
        HttpRouter router = new HttpRouter().get("/events", r -> {
            requests.incrementAndGet();
            return HttpResponse.status(204);
        });
        Server s204 = HttpServer.bind(serverGroup, ANY, router);
        cleanup.add(s204);
        Rec rec = new Rec();
        SseClient.subscribe(http(), url(s204), SseClientConfig.defaults().withDefaultRetry(Duration.ofMillis(20)), rec);
        assertTrue(rec.closed.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);
        assertEquals(1, requests.get(), "204 means: do not reconnect");
        assertEquals(1, rec.closes.get());
        assertEquals(0, rec.opens.get());
        assertTrue(rec.errors.isEmpty());
    }

    @Test
    void nonRetryableResponsesFailOnceAndStop() throws Exception {
        HttpRouter router = new HttpRouter()
                .get("/events", r -> {
                    requests.incrementAndGet();
                    return HttpResponse.status(404).text("nope");
                })
                .get("/html", r -> HttpResponse.ok("<html></html>").header("Content-Type", "text/html"));
        Server s = HttpServer.bind(serverGroup, ANY, router);
        cleanup.add(s);
        int port = ((InetSocketAddress) s.localAddress()).getPort();
        SseClientConfig fast = SseClientConfig.defaults().withDefaultRetry(Duration.ofMillis(20));

        Rec notFound = new Rec();
        SseClient.subscribe(http(), "http://127.0.0.1:" + port + "/events", fast, notFound);
        assertTrue(notFound.closed.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);
        assertEquals(1, requests.get(), "no retry after a 404");
        assertEquals(List.of(false), notFound.willReconnect);
        assertTrue(notFound.errors.get(0).getMessage().contains("404"), notFound.errors.get(0).getMessage());
        assertEquals(List.of("error:false", "close"), notFound.log);

        Rec wrongType = new Rec();
        SseClient.subscribe(http(), "http://127.0.0.1:" + port + "/html", fast, wrongType);
        assertTrue(wrongType.closed.await(5, TimeUnit.SECONDS));
        assertTrue(wrongType.errors.get(0).getMessage().contains("text/html"), wrongType.errors.get(0).getMessage());
        assertEquals(List.of(false), wrongType.willReconnect);
    }

    @Test
    void connectionFailuresBackOffExponentiallyAndRecoverWhenTheServerReturns() throws Exception {
        int port;
        try (ServerSocket tmp = new ServerSocket(0)) {
            port = tmp.getLocalPort();
        }
        Rec rec = new Rec();
        SseClientConfig cfg = SseClientConfig.defaults().withDefaultRetry(Duration.ofMillis(100)).withMaxRetry(Duration.ofMillis(400));
        SseSubscription sub = SseClient.subscribe(http(), "http://127.0.0.1:" + port + "/events", cfg, rec);
        for (int i = 0; i < 200 && rec.errors.size() < 5; i++) Thread.sleep(50);
        assertTrue(rec.errors.size() >= 5, "kept retrying: " + rec.errors.size());
        assertTrue(rec.willReconnect.stream().allMatch(b -> b));
        // gaps between consecutive failures: ~100, ~200, ~400, then capped at ~400
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < rec.errorTimes.size(); i++) gaps.add((rec.errorTimes.get(i)[0] - rec.errorTimes.get(i - 1)[0]) / 1_000_000);
        // Nominal delays are 100, 200, 400, then capped at 400 ms. A timer never fires early, so the nominal values are
        // hard lower bounds; scheduling jitter only ever adds, so there is no meaningful upper bound to assert tightly.
        assertTrue(gaps.get(0) >= 90, "first retry after about 100 ms: " + gaps);
        assertTrue(gaps.get(1) >= 180, "second delay doubled to about 200 ms: " + gaps);
        assertTrue(gaps.get(2) >= 360, "third delay doubled again to about 400 ms: " + gaps);
        assertTrue(gaps.get(3) >= 360 && gaps.get(3) < 1500, "capped at maxRetry (400 ms), not doubled to 800: " + gaps);

        // the server finally appears on that port
        Server late = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", port), Options.defaults(), null, HttpConfig.defaults(),
                new HttpRouter().get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        e.send("finally");
                    }
                })));
        cleanup.add(late);
        assertTrue(rec.awaitMessages(1));
        assertEquals("finally", rec.messages.get(0).data());
        sub.close();
    }

    @Test
    void closeStopsEverythingAndNotifiesOnce() throws Exception {
        CountDownLatch serverSawClose = new CountDownLatch(1);
        Server server = sseServer(SseConfig.defaults(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                e.send("hello");
            }

            @Override
            public void onClose(SseEmitter e) {
                serverSawClose.countDown();
            }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), url(server), SseClientConfig.defaults().withDefaultRetry(Duration.ofMillis(20)), rec);
        assertTrue(rec.awaitMessages(1));
        sub.close();
        sub.close(); // idempotent
        assertTrue(sub.isClosed());
        assertTrue(serverSawClose.await(5, TimeUnit.SECONDS), "the connection must be torn down");
        assertTrue(rec.closed.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);
        assertEquals(1, requests.get(), "no reconnect after close()");
        assertEquals(1, rec.closes.get());
        assertEquals("close", rec.log.get(rec.log.size() - 1));
    }

    @Test
    void closeWhileWaitingToReconnectCancelsTheTimer() throws Exception {
        Server server = sseServer(SseConfig.defaults().withHeartbeat(null), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                e.send("one");
                e.close();
            }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), url(server), SseClientConfig.defaults().withDefaultRetry(Duration.ofSeconds(5)), rec);
        assertTrue(rec.awaitMessages(1));
        for (int i = 0; i < 100 && rec.errors.isEmpty(); i++) Thread.sleep(20); // the disconnect was noticed; a 5 s reconnect is now pending
        sub.close();
        assertTrue(rec.closed.await(2, TimeUnit.SECONDS), "close() must not wait for the pending reconnect");
        Thread.sleep(300);
        assertEquals(1, requests.get());
    }

    @Test
    void aThrowingListenerDoesNotKillTheSubscription() throws Exception {
        Server server = sseServer(SseConfig.defaults(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                e.send("a");
                e.send("boom");
                e.send("c");
            }
        });
        List<String> got = new CopyOnWriteArrayList<>();
        Rec rec = new Rec() {
            @Override
            public void onMessage(SseMessage m) {
                got.add(m.data());
                if (m.data().equals("boom")) throw new IllegalStateException("listener bug");
            }
        };
        SseSubscription sub = SseClient.subscribe(http(), url(server), rec);
        for (int i = 0; i < 100 && got.size() < 3; i++) Thread.sleep(25);
        assertEquals(List.of("a", "boom", "c"), got, "the message after the throwing one still arrives");
        assertEquals("listener bug", rec.errors.get(0).getMessage());
        assertEquals(1, rec.opens.get(), "no reconnect was needed");
        sub.close();
    }

    @Test
    void aSilentStreamIsReconnectedAfterTheIdleTimeout() throws Exception {
        AtomicInteger n = new AtomicInteger();
        Server server = sseServer(SseConfig.defaults().withHeartbeat(null), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                if (n.incrementAndGet() == 2) e.send("alive again");
            }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(HttpClientConfig.defaults().withIdleTimeout(Duration.ofMillis(300))), url(server),
                SseClientConfig.defaults().withDefaultRetry(Duration.ofMillis(50)), rec);
        assertTrue(rec.awaitMessages(1));
        assertEquals("alive again", rec.messages.get(0).data());
        assertFalse(rec.errors.isEmpty(), "the dead first stream was reported");
        sub.close();
    }

    @Test
    void customHeadersAreSent() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        HttpRouter router = new HttpRouter().get("/events", r -> {
            seen.add(r.header("Authorization") + "|" + r.header("Accept") + "|" + r.header("Cache-Control"));
            return SseResponse.accept(r.request(), () -> new SseListener() {
                @Override
                public void onOpen(SseEmitter e) {
                    e.send("ok");
                }
            });
        });
        Server s = HttpServer.bind(serverGroup, ANY, router);
        cleanup.add(s);
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), url(s),
                SseClientConfig.defaults().withHeaders(java.util.Map.of("Authorization", "Bearer t0k3n")), rec);
        assertTrue(rec.awaitMessages(1));
        assertEquals(List.of("Bearer t0k3n|text/event-stream|no-cache"), seen);
        sub.close();
    }

    // ---- hostile or sloppy servers ----

    @Test
    void parsesAStreamDeliveredInTinyPiecesWithMixedLineEndings() throws Exception {
        String stream = "﻿retry: 5000\r\n: hello\r\nid: 10\r\nevent: tình\r\ndata: Việt\r\ndata: ✓ 🎉\r\n\r\ndata: second\rdata: line\r\r"
                + "event: ignored-no-data\n\ndata: third\n\n";
        byte[] body = stream.getBytes(StandardCharsets.UTF_8);
        ServerSocket ss = new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        cleanup.add(pool::shutdownNow);
        pool.execute(() -> {
            try (Socket s = ss.accept()) {
                InputStream in = s.getInputStream();
                int c, matched = 0;
                while ((c = in.read()) >= 0) { // request head
                    matched = (c == (matched % 2 == 0 ? '\r' : '\n')) ? matched + 1 : (c == '\r' ? 1 : 0);
                    if (matched == 4) break;
                }
                OutputStream out = s.getOutputStream();
                out.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").getBytes());
                for (byte b : body) { // one HTTP chunk per BYTE: splits UTF-8 characters, CRLF pairs, everything
                    out.write(("1\r\n").getBytes());
                    out.write(b);
                    out.write("\r\n".getBytes());
                    out.flush();
                }
                out.write("0\r\n\r\n".getBytes());
                out.flush();
                Thread.sleep(500);
            } catch (Exception ignored) { }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), "http://127.0.0.1:" + ss.getLocalPort() + "/", SseClientConfig.defaults().withReconnect(false), rec);
        assertTrue(rec.closed.await(15, TimeUnit.SECONDS));
        assertEquals(List.of(
                new SseMessage("tình", "Việt\n✓ 🎉", "10"),
                new SseMessage("message", "second\nline", "10"),
                new SseMessage("message", "third", "10")), rec.messages);
    }

    @Test
    void anEndlessLineIsCutOffInsteadOfBufferedForever() throws Exception {
        ServerSocket ss = new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        cleanup.add(pool::shutdownNow);
        pool.execute(() -> {
            try (Socket s = ss.accept()) {
                InputStream in = s.getInputStream();
                in.read(new byte[4096]);
                OutputStream out = s.getOutputStream();
                out.write("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: ".getBytes());
                byte[] junk = new byte[8192];
                java.util.Arrays.fill(junk, (byte) 'x');
                for (int i = 0; i < 100_000; i++) out.write(junk); // up to ~800 MB without a newline
            } catch (Exception ignored) { }
        });
        Rec rec = new Rec();
        SseClient.subscribe(http(), "http://127.0.0.1:" + ss.getLocalPort() + "/",
                SseClientConfig.defaults().withReconnect(false), rec);
        assertTrue(rec.closed.await(20, TimeUnit.SECONDS));
        assertTrue(rec.errors.get(0).getMessage().contains("exceeds") || String.valueOf(rec.errors.get(0).getCause()).contains("exceeds"),
                String.valueOf(rec.errors.get(0)));
    }

    // ---- interop and TLS ----

    @Test
    void worksAgainstTheJdkHttpServer() throws Exception {
        var jdk = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jdk.createContext("/sse", ex -> {
            ex.getResponseHeaders().add("Content-Type", "text/event-stream");
            ex.sendResponseHeaders(200, 0);
            try (OutputStream o = ex.getResponseBody()) {
                for (int i = 1; i <= 3; i++) {
                    o.write(("id: " + i + "\nevent: tick\ndata: " + i + "\n\n").getBytes());
                    o.flush();
                    Thread.sleep(30);
                }
                Thread.sleep(200);
            } catch (InterruptedException ignored) { }
        });
        jdk.start();
        cleanup.add(() -> jdk.stop(0));
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(), "http://127.0.0.1:" + jdk.getAddress().getPort() + "/sse",
                SseClientConfig.defaults().withReconnect(false), rec);
        assertTrue(rec.closed.await(10, TimeUnit.SECONDS));
        assertEquals(List.of("1", "2", "3"), rec.data());
        assertEquals("tick", rec.messages.get(0).event());
    }

    @Test
    void worksOverHttps(@TempDir Path dir) throws Exception {
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

        Server server = sseServer(Options.defaults(), serverCtx, SseConfig.defaults(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                e.send("secret one");
                e.send(SseEvent.data("x".repeat(200_000)).event("big"));
                e.send("secret two");
            }
        });
        Rec rec = new Rec();
        SseSubscription sub = SseClient.subscribe(http(HttpClientConfig.defaults().withSslContext(clientCtx)),
                url(server, "https", "localhost"), rec);
        assertTrue(rec.awaitMessages(3));
        assertEquals("secret one", rec.messages.get(0).data());
        assertEquals(200_000, rec.messages.get(1).data().length());
        assertEquals("secret two", rec.messages.get(2).data());
        sub.close();
    }
}
