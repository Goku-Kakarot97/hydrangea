package io.hydrangea;

import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.ResponseHead;
import io.hydrangea.http.StreamHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpClientStreamTest {

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

    /** Collects everything a stream reports and checks the threading contract. */
    static class Recorder implements BodyListener {
        final CompletableFuture<ResponseHead> head = new CompletableFuture<>();
        final CompletableFuture<Void> ended = new CompletableFuture<>();
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger completes = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger overlaps = new AtomicInteger();
        final java.util.Set<Thread> threads = java.util.concurrent.ConcurrentHashMap.newKeySet();
        volatile Throwable error;

        private void enter() {
            if (concurrent.incrementAndGet() > 1) overlaps.incrementAndGet();
            threads.add(Thread.currentThread());
        }

        private void leave() {
            concurrent.decrementAndGet();
        }

        @Override
        public void onHead(ResponseHead h, BodyControl c) {
            enter();
            events.add("head:" + h.status());
            head.complete(h);
            leave();
        }

        @Override
        public void onData(ByteBuffer data, BodyControl c) throws Exception {
            enter();
            try {
                byte[] b = new byte[data.remaining()];
                data.get(b);
                synchronized (body) {
                    body.write(b);
                }
                events.add("data");
                onChunk(b, c);
            } finally {
                leave();
            }
        }

        void onChunk(byte[] b, BodyControl c) throws Exception {
        }

        @Override
        public void onComplete() {
            enter();
            completes.incrementAndGet();
            events.add("complete");
            ended.complete(null);
            leave();
        }

        @Override
        public void onError(Throwable t) {
            enter();
            errors.incrementAndGet();
            error = t;
            events.add("error");
            head.completeExceptionally(t);
            ended.completeExceptionally(t);
            leave();
        }

        String bodyText() {
            synchronized (body) {
                return body.toString(StandardCharsets.UTF_8);
            }
        }

        int bodySize() {
            synchronized (body) {
                return body.size();
            }
        }

        Throwable awaitFailure() throws Exception {
            try {
                ended.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                return e.getCause();
            } catch (CancellationException e) {
                return e; // a future completed with CancellationException rethrows it as is
            }
            throw new AssertionError("expected the stream to fail");
        }
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

        String url(String path) {
            return "http://127.0.0.1:" + ss.getLocalPort() + path;
        }

        @Override
        public void close() throws IOException {
            ss.close();
            pool.shutdownNow();
        }
    }

    private static String readRequestHead(Socket s) throws IOException {
        InputStream in = s.getInputStream();
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        int c;
        while ((c = in.read()) >= 0) {
            b.write(c);
            byte[] a = b.toByteArray();
            int n = a.length;
            if (n >= 4 && a[n - 4] == '\r' && a[n - 3] == '\n' && a[n - 2] == '\r' && a[n - 1] == '\n') break;
        }
        return c < 0 ? null : b.toString(StandardCharsets.ISO_8859_1);
    }

    private static void write(Socket s, String data) throws IOException {
        OutputStream out = s.getOutputStream();
        out.write(data.getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    private static void writeChunk(Socket s, String data) throws IOException {
        write(s, Integer.toHexString(data.length()) + "\r\n" + data + "\r\n");
    }

    // ---- it really streams ----

    @Test
    void dataArrivesWhileTheServerIsStillSending() throws Exception {
        CountDownLatch clientGotFirst = new CountDownLatch(1);
        AtomicReference<String> serverSaw = new AtomicReference<>("never-got-first-chunk-ack");
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                writeChunk(s, "first");
                // a client that buffers the whole response can never get past this wait
                if (clientGotFirst.await(5, TimeUnit.SECONDS)) serverSaw.set("client-had-first-chunk-before-we-sent-more");
                writeChunk(s, "second");
                write(s, "0\r\n\r\n");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    if (new String(b).equals("first")) clientGotFirst.countDown();
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            rec.ended.get(10, TimeUnit.SECONDS);
            assertEquals("firstsecond", rec.bodyText());
            assertEquals("client-had-first-chunk-before-we-sent-more", serverSaw.get(), "the first chunk must be delivered before the response ends");
            assertEquals(List.of("head:200", "data", "data", "complete"), rec.events);
        }
    }

    @Test
    void headIsDeliveredBeforeAnyBodyExists() throws Exception {
        CountDownLatch headSeen = new CountDownLatch(1);
        AtomicReference<String> verdict = new AtomicReference<>("head not seen in time");
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Marker: yes\r\n\r\n"); // head only, body withheld
                if (headSeen.await(5, TimeUnit.SECONDS)) verdict.set("ok");
                write(s, "hello");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder() {
                @Override
                public void onHead(ResponseHead h, BodyControl c) {
                    super.onHead(h, c);
                    headSeen.countDown();
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            rec.ended.get(10, TimeUnit.SECONDS);
            assertEquals("ok", verdict.get());
            assertEquals("yes", rec.head.get().header("x-marker"));
            assertEquals("hello", rec.bodyText());
        }
    }

    @Test
    void sizeLimitsOfBufferedRequestsDoNotApplyToStreams() throws Exception {
        int size = 6 * 1024 * 1024;
        byte[] data = new byte[size];
        new java.util.Random(11).nextBytes(data);
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter().get("/big", r -> HttpResponse.status(200).body(data)));
        cleanup.add(server);
        HttpClient c = client(HttpClientConfig.defaults().withMaxResponseBytes(1024)); // a buffered send() would fail
        Recorder rec = new Recorder();
        c.stream(ClientRequest.get("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + "/big"), rec);
        rec.ended.get(20, TimeUnit.SECONDS);
        assertArrayEquals(data, rec.body.toByteArray());
        assertTrue(rec.events.stream().filter(e -> e.equals("data")).count() > 1, "delivered in several pieces, not as one blob");
    }

    @Test
    void closeDelimitedStreamEndsAtEof() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.0 200 OK\r\n\r\nline1\nline2\n");
                Thread.sleep(100);
                write(s, "line3\n");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            client().stream(ClientRequest.get(raw.url("/")), rec);
            rec.ended.get(10, TimeUnit.SECONDS);
            assertEquals("line1\nline2\nline3\n", rec.bodyText());
            assertEquals(1, rec.completes.get());
            assertEquals(0, rec.errors.get());
        }
    }

    @Test
    void noBodyResponsesCompleteImmediately() throws Exception {
        AtomicInteger n = new AtomicInteger();
        try (Raw raw = new Raw(s -> {
            try {
                while (readRequestHead(s) != null) {
                    if (n.getAndIncrement() == 0) write(s, "HTTP/1.1 204 No Content\r\n\r\n");
                    else write(s, "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n"); // HEAD
                }
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            Recorder a = new Recorder();
            c.stream(ClientRequest.get(raw.url("/")), a);
            a.ended.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("head:204", "complete"), a.events);
            Recorder b = new Recorder();
            c.stream(ClientRequest.head(raw.url("/")), b);
            b.ended.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("head:200", "complete"), b.events);
            assertEquals(1, raw.accepted.get(), "both streams ended cleanly, so the connection was reused");
        }
    }

    // ---- backpressure ----

    @Test
    void pauseStopsDeliveryAndTheServerSlowsDown() throws Exception {
        int total = 40 * 1024 * 1024;
        AtomicLong serverWrote = new AtomicLong();
        CountDownLatch serverDone = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: " + total + "\r\n\r\n");
                byte[] block = new byte[64 * 1024];
                OutputStream out = s.getOutputStream();
                for (long sent = 0; sent < total; sent += block.length) {
                    out.write(block); // blocks once the kernel buffers on both sides are full
                    serverWrote.addAndGet(block.length);
                }
                serverDone.countDown();
            } catch (Exception ignored) { }
        })) {
            AtomicReference<BodyControl> ctl = new AtomicReference<>();
            AtomicInteger pauses = new AtomicInteger();
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    ctl.set(c);
                    if (bodySize() >= 100_000 && pauses.getAndIncrement() == 0) c.pause(); // once; after resume() the stream runs free
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            for (int i = 0; i < 100 && ctl.get() == null; i++) Thread.sleep(20);
            Thread.sleep(800); // let any in-flight data settle
            int atPause = rec.bodySize();
            Thread.sleep(700);
            assertEquals(atPause, rec.bodySize(), "no data may be delivered while paused");
            long wroteWhilePaused = serverWrote.get();
            assertTrue(wroteWhilePaused < total / 2, "TCP backpressure must have throttled the server; it wrote " + wroteWhilePaused + " of " + total);
            assertFalse(rec.ended.isDone());

            ctl.get().resume();
            rec.ended.get(30, TimeUnit.SECONDS);
            assertEquals(total, rec.bodySize(), "every byte arrives after resume");
            assertEquals(0, rec.overlaps.get());
        }
    }

    @Test
    void pauseAndResumeManyTimesLosesNothingAndKeepsOrder() throws Exception {
        int total = 3 * 1024 * 1024;
        byte[] data = new byte[total];
        for (int i = 0; i < total; i++) data[i] = (byte) (i * 31 + (i >>> 9));
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter().get("/d", r -> HttpResponse.status(200).body(data)));
        cleanup.add(server);
        AtomicInteger chunks = new AtomicInteger();
        Recorder rec = new Recorder() {
            @Override
            void onChunk(byte[] b, BodyControl c) {
                if (chunks.incrementAndGet() % 3 == 0) {
                    c.pause();
                    clientGroup.next().schedule(c::resume, 1, TimeUnit.MILLISECONDS);
                }
            }
        };
        client().stream(ClientRequest.get("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + "/d"), rec);
        rec.ended.get(60, TimeUnit.SECONDS);
        assertArrayEquals(data, rec.body.toByteArray());
        assertEquals(0, rec.overlaps.get(), "callbacks never overlap");
    }

    @Test
    void pausingOnTheLastPieceStillEndsTheStreamAndLeavesTheConnectionUsable() throws Exception {
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter().get("/x", r -> HttpResponse.ok("tiny")));
        cleanup.add(server);
        String url = "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + "/x";
        HttpClient c = client();
        Recorder rec = new Recorder() {
            @Override
            void onChunk(byte[] b, BodyControl ctl) {
                ctl.pause(); // the body is already complete when this runs; there is nothing left to hold back
            }
        };
        c.stream(ClientRequest.get(url), rec);
        rec.ended.get(5, TimeUnit.SECONDS);
        assertEquals("tiny", rec.bodyText());
        // a connection that ended while "paused" must read again: the next request on it has to work
        assertEquals("tiny", c.send(ClientRequest.get(url)).get(5, TimeUnit.SECONDS).bodyAsString());
        assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened(), "and it was the same connection");
    }

    @Test
    void pauseBeforeTheEndHoldsBackTheRestAndNothingIsDeliveredMeanwhile() throws Exception {
        // two chunks arrive back-to-back in one TCP segment; pausing on the first must hold the second
        CountDownLatch paused = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n3\r\ntwo\r\n5\r\nthree\r\n0\r\n\r\n");
                paused.await(5, TimeUnit.SECONDS);
                Thread.sleep(1000);
            } catch (Exception ignored) { }
        })) {
            AtomicReference<BodyControl> ctl = new AtomicReference<>();
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    ctl.set(c);
                    if (new String(b).equals("one")) {
                        c.pause();
                        paused.countDown();
                    }
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            assertTrue(paused.await(5, TimeUnit.SECONDS));
            Thread.sleep(400);
            assertEquals("one", rec.bodyText(), "the bytes of the next chunks were already read, but must not be delivered while paused");
            assertFalse(rec.ended.isDone());
            ctl.get().resume();
            rec.ended.get(5, TimeUnit.SECONDS);
            assertEquals("onetwothree", rec.bodyText());
            assertEquals(List.of("head:200", "data", "data", "data", "complete"), rec.events);
        }
    }

    // ---- cancel, errors, timeouts ----

    @Test
    void cancelClosesTheConnectionAndReportsCancellation() throws Exception {
        CountDownLatch serverSawClose = new CountDownLatch(1);
        CountDownLatch firstSeen = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                writeChunk(s, "tick");
                while (true) { // endless stream until the client hangs up
                    Thread.sleep(50);
                    writeChunk(s, "tick");
                }
            } catch (IOException e) {
                serverSawClose.countDown();
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    firstSeen.countDown();
                }
            };
            StreamHandle h = client().stream(ClientRequest.get(raw.url("/")), rec);
            assertTrue(firstSeen.await(5, TimeUnit.SECONDS));
            h.cancel();
            assertInstanceOf(CancellationException.class, rec.awaitFailure());
            assertTrue(serverSawClose.await(5, TimeUnit.SECONDS), "cancel must close the connection");
            assertEquals(1, rec.errors.get());
            assertEquals(0, rec.completes.get());
            assertInstanceOf(CancellationException.class, failure(h.completion()));
        }
    }

    @Test
    void cancelFromInsideACallback() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                for (int i = 0; i < 1000; i++) {
                    writeChunk(s, "x");
                    Thread.sleep(2);
                }
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    if (bodySize() >= 3) c.cancel();
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            assertInstanceOf(CancellationException.class, rec.awaitFailure());
            Thread.sleep(200);
            assertTrue(rec.bodySize() <= 4, "nothing more is delivered after cancel; got " + rec.bodySize());
            assertEquals(1, rec.errors.get());
        }
    }

    @Test
    void serverHangingUpMidBodyIsAnError() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\npartial");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            client().stream(ClientRequest.get(raw.url("/")), rec);
            assertInstanceOf(IOException.class, rec.awaitFailure());
            assertEquals("partial", rec.bodyText(), "what arrived was delivered before the failure");
            assertEquals(0, rec.completes.get());
            assertEquals(1, rec.errors.get());
        }
    }

    @Test
    void malformedChunkingMidStreamIsAnError() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                writeChunk(s, "good");
                write(s, "ZZ\r\nbad\r\n");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            client().stream(ClientRequest.get(raw.url("/")), rec);
            assertInstanceOf(IOException.class, rec.awaitFailure());
            assertEquals("good", rec.bodyText());
        }
    }

    @Test
    void listenerExceptionAbortsTheStream() throws Exception {
        CountDownLatch serverSawClose = new CountDownLatch(1);
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                while (true) {
                    writeChunk(s, "data");
                    Thread.sleep(20);
                }
            } catch (IOException e) {
                serverSawClose.countDown();
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder() {
                @Override
                void onChunk(byte[] b, BodyControl c) {
                    throw new IllegalStateException("listener bug");
                }
            };
            client().stream(ClientRequest.get(raw.url("/")), rec);
            Throwable t = rec.awaitFailure();
            assertInstanceOf(IOException.class, t);
            assertEquals("listener bug", t.getCause().getMessage());
            assertTrue(serverSawClose.await(5, TimeUnit.SECONDS), "the connection must be closed");
            assertEquals(1, rec.errors.get());
        }
    }

    @Test
    void failuresBeforeAnyResponseAreReportedToTheListener() throws Exception {
        int dead;
        try (ServerSocket tmp = new ServerSocket(0)) {
            dead = tmp.getLocalPort();
        }
        Recorder rec = new Recorder();
        client().stream(ClientRequest.get("http://127.0.0.1:" + dead + "/"), rec);
        assertInstanceOf(IOException.class, rec.awaitFailure());
        assertEquals(List.of("error"), rec.events);
    }

    @Test
    void requestTimeoutCoversOnlyTheWaitForTheHeadNotTheStream() throws Exception {
        // 1. a server that never answers: the (short) request timeout fires
        try (Raw silent = new Raw(s -> {
            try {
                readRequestHead(s);
                Thread.sleep(5000);
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            client().stream(ClientRequest.get(silent.url("/")).timeout(Duration.ofMillis(300)), rec);
            assertInstanceOf(SocketTimeoutException.class, rec.awaitFailure());
        }
        // 2. a stream that keeps flowing for much longer than the request timeout is not cut
        try (Raw flowing = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                for (int i = 0; i < 12; i++) {
                    writeChunk(s, "beat");
                    Thread.sleep(100);
                }
                write(s, "0\r\n\r\n");
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            long t0 = System.nanoTime();
            client().stream(ClientRequest.get(flowing.url("/")).timeout(Duration.ofMillis(400)), rec);
            rec.ended.get(10, TimeUnit.SECONDS);
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() > 1000, "the stream outlived the 400 ms request timeout");
            assertEquals("beat".repeat(12), rec.bodyText());
        }
    }

    @Test
    void aStreamThatGoesSilentIsCutByTheIdleTimeout() throws Exception {
        try (Raw raw = new Raw(s -> {
            try {
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                writeChunk(s, "one");
                Thread.sleep(10_000); // then silence
            } catch (Exception ignored) { }
        })) {
            Recorder rec = new Recorder();
            client(HttpClientConfig.defaults().withIdleTimeout(Duration.ofMillis(400))).stream(ClientRequest.get(raw.url("/")), rec);
            assertInstanceOf(IOException.class, rec.awaitFailure());
            assertEquals("one", rec.bodyText());
        }
    }

    // ---- redirects, reuse, retry ----

    @Test
    void redirectsAreFollowedBeforeTheListenerSeesAnything() throws Exception {
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter()
                .get("/old", r -> HttpResponse.status(302).header("Location", "/new").text("moved"))
                .get("/new", r -> HttpResponse.ok("the real body"))
                .get("/loop", r -> HttpResponse.status(302).header("Location", "/loop")));
        cleanup.add(server);
        String base = "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort();
        HttpClient c = client();
        Recorder rec = new Recorder();
        c.stream(ClientRequest.get(base + "/old"), rec);
        rec.ended.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("head:200", "data", "complete"), rec.events, "no trace of the 302");
        assertEquals("the real body", rec.bodyText());
        assertEquals(base + "/new", rec.head.get().uri().toString());

        Recorder looping = new Recorder();
        c.stream(ClientRequest.get(base + "/loop"), looping);
        assertTrue(looping.awaitFailure().getMessage().contains("too many redirects"));

        Recorder noFollow = new Recorder();
        client(HttpClientConfig.defaults().withMaxRedirects(0)).stream(ClientRequest.get(base + "/old"), noFollow);
        noFollow.ended.get(10, TimeUnit.SECONDS);
        assertEquals(302, noFollow.head.get().status(), "following disabled: the 3xx is streamed like any response");
        assertEquals("moved", noFollow.bodyText());
    }

    @Test
    void connectionIsReusedAfterAStreamEnds() throws Exception {
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter().get("/n", r -> HttpResponse.ok("x".repeat(1000))));
        cleanup.add(server);
        String url = "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + "/n";
        HttpClient c = client();
        for (int i = 0; i < 4; i++) {
            Recorder rec = new Recorder();
            c.stream(ClientRequest.get(url), rec);
            rec.ended.get(5, TimeUnit.SECONDS);
        }
        assertEquals("x".repeat(1000), c.send(ClientRequest.get(url)).get(5, TimeUnit.SECONDS).bodyAsString(), "buffered and streamed requests share the pool");
        assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened());
    }

    @Test
    void staleConnectionIsRetriedForStreamsToo() throws Exception {
        AtomicInteger conn = new AtomicInteger();
        try (Raw raw = new Raw(s -> {
            try {
                int id = conn.incrementAndGet();
                readRequestHead(s);
                write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nr" + id);
                if (id == 1) {
                    readRequestHead(s); // second request on the reused connection: hang up without answering
                    return;
                }
                while (readRequestHead(s) != null) write(s, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nr" + id);
            } catch (Exception ignored) { }
        })) {
            HttpClient c = client();
            Recorder first = new Recorder();
            c.stream(ClientRequest.get(raw.url("/")), first);
            first.ended.get(5, TimeUnit.SECONDS);
            Recorder second = new Recorder();
            c.stream(ClientRequest.get(raw.url("/")), second);
            second.ended.get(5, TimeUnit.SECONDS);
            assertEquals("r1", first.bodyText());
            assertEquals("r2", second.bodyText(), "retried on a fresh connection before any byte of the response was seen");
            assertEquals(List.of("head:200", "data", "complete"), second.events);
        }
    }

    // ---- threading contract and interop ----

    @Test
    void callbacksAreSerialAndOnOneLoopThread() throws Exception {
        Server server = HttpServer.bind(serverGroup, ANY, new HttpRouter().get("/d", r -> HttpResponse.status(200).body(new byte[2_000_000])));
        cleanup.add(server);
        Recorder rec = new Recorder();
        client().stream(ClientRequest.get("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + "/d"), rec);
        rec.ended.get(20, TimeUnit.SECONDS);
        assertEquals(0, rec.overlaps.get());
        assertEquals(1, rec.threads.size(), "all callbacks of a stream run on its connection's loop thread: " + rec.threads);
        assertTrue(rec.threads.iterator().next().getName().startsWith("hydrangea-"));
    }

    @Test
    void worksAgainstTheJdkServerStreamingChunks() throws Exception {
        var jdk = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        CountDownLatch gotOne = new CountDownLatch(1);
        AtomicReference<Boolean> overlapped = new AtomicReference<>(false);
        jdk.createContext("/s", ex -> {
            ex.sendResponseHeaders(200, 0); // chunked
            try (OutputStream o = ex.getResponseBody()) {
                o.write("part-1;".getBytes());
                o.flush();
                overlapped.set(gotOne.await(5, TimeUnit.SECONDS)); // true only if the client delivered part 1 before we sent part 2
                o.write("part-2".getBytes());
            } catch (InterruptedException ignored) { }
        });
        jdk.start();
        cleanup.add(() -> jdk.stop(0));
        Recorder rec = new Recorder() {
            @Override
            void onChunk(byte[] b, BodyControl c) {
                gotOne.countDown();
            }
        };
        client().stream(ClientRequest.get("http://127.0.0.1:" + jdk.getAddress().getPort() + "/s"), rec);
        rec.ended.get(10, TimeUnit.SECONDS);
        assertEquals("part-1;part-2", rec.bodyText());
        assertTrue(overlapped.get());
    }

    private static Throwable failure(CompletableFuture<?> f) {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (CancellationException e) {
            return e;
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        }
        throw new AssertionError("expected failure");
    }
}
