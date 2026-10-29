package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.Http2Test.Response;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseEvent;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Server-Sent Events, WebSocket (RFC 8441) and streamed request bodies on HTTP/2 streams. */
class Http2StreamsTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private static final int DATA = 0, HEADERS = 1, RST_STREAM = 3, SETTINGS = 4, WINDOW_UPDATE = 8, END_STREAM = 1;

    private EventLoopGroup group;
    private final AtomicReference<SseEmitter> emitter = new AtomicReference<>();
    private final CountDownLatch sseOpen = new CountDownLatch(1);
    private final CountDownLatch sseClosed = new CountDownLatch(1);
    private final AtomicInteger sseWritableCallbacks = new AtomicInteger();
    private final List<String> wsEvents = new CopyOnWriteArrayList<>();
    private final CountDownLatch wsClosed = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private HttpRouter router() {
        return new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello"))
                .get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        emitter.set(e);
                        sseOpen.countDown();
                    }

                    @Override
                    public void onWritabilityChanged(SseEmitter e) {
                        sseWritableCallbacks.incrementAndGet();
                    }

                    @Override
                    public void onClose(SseEmitter e) {
                        sseClosed.countDown();
                    }
                }))
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), () -> new WebSocketListener() {
                    @Override
                    public void onText(WebSocket ws, String text) {
                        wsEvents.add("text:" + text);
                        ws.sendText("echo:" + text);
                    }

                    @Override
                    public void onClose(WebSocket ws, int code, String reason) {
                        wsEvents.add("close:" + code);
                        wsClosed.countDown();
                    }
                }));
    }

    private Server server(HttpRouter router) throws IOException {
        return HttpServer.bind(group, ANY, Options.defaults(), null, HttpConfig.defaults().withHttp2(Http2Config.defaults()), router);
    }

    // ---- Server-Sent Events ----

    @Test
    void anEventStreamRunsOnAnHttp2Stream() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/events");
            Frame head = p.nextInteresting();
            assertEquals(HEADERS, head.type());
            assertEquals(0, head.flags() & END_STREAM, "the stream stays open");
            List<String> hs = new ArrayList<>();
            p.dec.decode(head.payload(), (n, v) -> hs.add(n + ": " + v));
            assertTrue(hs.contains(":status: 200"), hs.toString());
            assertTrue(hs.stream().anyMatch(h -> h.startsWith("content-type: text/event-stream")), hs.toString());
            assertTrue(hs.stream().noneMatch(h -> h.startsWith("content-length") || h.startsWith("transfer-encoding")), hs.toString());

            assertTrue(sseOpen.await(5, TimeUnit.SECONDS));
            emitter.get().send(SseEvent.data("one").id("1"));
            emitter.get().send(SseEvent.data("two").event("greeting"));
            ByteArrayOutputStream got = new ByteArrayOutputStream();
            while (!got.toString(StandardCharsets.UTF_8).contains("two\n\n")) {
                Frame f = p.nextInteresting();
                assertEquals(DATA, f.type());
                got.write(f.payload());
            }
            assertEquals("id: 1\ndata: one\n\nevent: greeting\ndata: two\n\n", got.toString(StandardCharsets.UTF_8));

            emitter.get().close();                         // the application ends the stream
            Frame end;
            do {
                end = p.nextInteresting();
            } while ((end.flags() & END_STREAM) == 0);
            assertEquals(DATA, end.type());
            assertTrue(sseClosed.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aClientResetEndsTheEventStreamForTheApplication() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/events");
            assertTrue(sseOpen.await(5, TimeUnit.SECONDS));
            p.rst(1, 8);
            assertTrue(sseClosed.await(5, TimeUnit.SECONDS), "the listener must learn the client went away");
            assertFalse(emitter.get().isOpen());
        }
    }

    @Test
    void manyEventStreamsShareOneConnection() throws Exception {
        List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
        HttpRouter r = new HttpRouter().get("/events", x -> SseResponse.accept(x.request(), () -> new SseListener() {
            @Override
            public void onOpen(SseEmitter e) {
                emitters.add(e);
            }
        }));
        try (Server server = server(r); Peer p = Peer.connect(port(server)).handshake()) {
            for (int i = 0; i < 20; i++) {
                p.get(1 + 2 * i, "/events");
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (emitters.size() < 20 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(20, emitters.size());
            emitters.forEach(e -> e.send("tick"));
            int ticks = 0;
            while (ticks < 20) {
                Frame f = p.nextInteresting();
                if (f.type() == DATA) {
                    assertEquals("data: tick\n\n", new String(f.payload()));
                    ticks++;
                }
            }
        }
    }

    @Test
    void aStalledClientBacksUpTheEventStreamAndWritabilityComesBack() throws Exception {
        // the client announces a zero window: nothing can be sent, so the stream must report "not writable" instead of buffering for ever
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake(4, 0)) {
            p.get(1, "/events");
            assertTrue(sseOpen.await(5, TimeUnit.SECONDS));
            SseEmitter e = emitter.get();
            assertTrue(e.isWritable());
            String payload = "x".repeat(1000);
            for (int i = 0; i < 100 && e.isWritable(); i++) {
                e.send(payload).get(5, TimeUnit.SECONDS);
            }
            assertFalse(e.isWritable(), "more than the high-water mark is queued");
            p.windowUpdate(1, 1 << 20);
            p.windowUpdate(0, 1 << 20);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!e.isWritable() && System.nanoTime() < deadline) {
                p.pollOrNull(20);
            }
            assertTrue(e.isWritable(), "drained below the low-water mark");
            while (sseWritableCallbacks.get() == 0 && System.nanoTime() < deadline + TimeUnit.SECONDS.toNanos(5)) {
                Thread.sleep(5);   // the flag flips just before the listener is told
            }
            assertTrue(sseWritableCallbacks.get() >= 1);
        }
    }

    // ---- WebSocket over HTTP/2 (RFC 8441) ----

    private static byte[] clientFrame(int opcode, byte[] payload) {
        byte[] mask = {0x11, 0x22, 0x33, 0x44};
        ByteBuffer b = ByteBuffer.allocate(6 + payload.length);
        b.put((byte) (0x80 | opcode)).put((byte) (0x80 | payload.length)).put(mask);
        for (int i = 0; i < payload.length; i++) {
            b.put((byte) (payload[i] ^ mask[i & 3]));
        }
        return b.array();
    }

    private static void extendedConnect(Peer p, int sid, String path) throws IOException {
        p.headers(sid, false, true, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "https", ":path", path,
                ":authority", "localhost", "sec-websocket-version", "13");
    }

    @Test
    void theServerAdvertisesExtendedConnect() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            assertEquals(1L, p.serverSettings.get(8), "SETTINGS_ENABLE_CONNECT_PROTOCOL");
        }
    }

    @Test
    void aWebSocketRunsOnAnHttp2Stream() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            extendedConnect(p, 1, "/ws");
            Frame head = p.nextInteresting();
            assertEquals(HEADERS, head.type());
            assertEquals(0, head.flags() & END_STREAM);
            List<String> hs = new ArrayList<>();
            p.dec.decode(head.payload(), (n, v) -> hs.add(n + ": " + v));
            assertTrue(hs.contains(":status: 200"), "RFC 8441: a 200, not a 101: " + hs);
            assertTrue(hs.stream().noneMatch(h -> h.startsWith("sec-websocket-accept") || h.startsWith("upgrade")), hs.toString());

            p.data(1, false, clientFrame(1, "hello".getBytes()));     // a masked text frame
            Frame echo = p.nextInteresting();
            assertEquals(DATA, echo.type());
            byte[] f = echo.payload();
            assertEquals(0x81, f[0] & 0xff);                             // FIN + text
            assertEquals(10, f[1] & 0xff);                               // unmasked, "echo:hello"
            assertEquals("echo:hello", new String(f, 2, 10));

            p.data(1, false, clientFrame(8, new byte[]{0x03, (byte) 0xe8}));   // close, 1000
            boolean ended = false;
            while (!ended) {
                Frame x = p.nextInteresting();
                if (x.type() == DATA && x.payload().length > 0) {
                    assertEquals(0x88, x.payload()[0] & 0xff, "the server answers with a close frame");
                }
                ended = (x.flags() & END_STREAM) != 0 || x.type() == RST_STREAM;
            }
            assertTrue(wsClosed.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("text:hello", "close:1000"), wsEvents);
        }
    }

    @Test
    void severalWebSocketsShareOneConnection() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            for (int i = 0; i < 5; i++) {
                extendedConnect(p, 1 + 2 * i, "/ws");
            }
            for (int i = 0; i < 5; i++) {
                assertEquals(HEADERS, p.nextInteresting().type());
            }
            for (int i = 0; i < 5; i++) {
                p.data(1 + 2 * i, false, clientFrame(1, ("m" + i).getBytes()));
            }
            java.util.Set<String> echoes = new java.util.TreeSet<>();
            while (echoes.size() < 5) {
                Frame f = p.nextInteresting();
                echoes.add(new String(f.payload(), 2, f.payload().length - 2));
            }
            assertEquals(java.util.Set.of("echo:m0", "echo:m1", "echo:m2", "echo:m3", "echo:m4"), echoes);
        }
    }

    @Test
    void anAbruptWebSocketEndReachesTheApplication() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            extendedConnect(p, 1, "/ws");
            p.nextInteresting();
            p.rst(1, 8);
            assertTrue(wsClosed.await(5, TimeUnit.SECONDS));
            assertEquals("close:1006", wsEvents.get(wsEvents.size() - 1), "closed without a close handshake");
        }
    }

    @Test
    void notEveryRequestMayOpenATunnel() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            // CONNECT without :protocol: not offered
            p.headers(1, false, true, ":method", "CONNECT", ":authority", "example.com:443");
            assertEquals(1, p.response(1).reset, "PROTOCOL_ERROR");
            // :protocol on a method that is not CONNECT
            p.headers(3, true, true, ":method", "GET", ":protocol", "websocket", ":scheme", "https", ":path", "/ws", ":authority", "x");
            assertEquals(1, p.response(3).reset);
            // an extended CONNECT to a route that is not a WebSocket endpoint must not be answered 200
            extendedConnect(p, 5, "/hello");
            assertEquals(400, p.response(5).status);
            // a plain GET to the WebSocket route is not a handshake
            p.get(7, "/ws");
            assertEquals(400, p.response(7).status);
            // an unsupported protocol
            p.headers(9, false, true, ":method", "CONNECT", ":protocol", "chat", ":scheme", "https", ":path", "/ws", ":authority", "x");
            assertEquals(400, p.response(9).status);
        }
    }

    @Test
    void gracefulShutdownWindsDownEventStreamsAndWebSockets() throws Exception {
        Server server = server(router());
        try (Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/events");
            extendedConnect(p, 3, "/ws");
            assertTrue(sseOpen.await(5, TimeUnit.SECONDS));
            p.nextInteresting();
            p.nextInteresting();
            CompletableFuture<Void> done = server.shutdownGracefully(Duration.ofSeconds(10));
            done.get(8, TimeUnit.SECONDS);   // far sooner than the 10 s deadline: the long-lived streams were asked to finish
            assertTrue(sseClosed.await(2, TimeUnit.SECONDS));
            assertTrue(wsClosed.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void aUpgradeResponseIsNotImplementedOverHttp2() throws Exception {
        HttpRouter r = new HttpRouter().get("/up", x -> HttpResponse.status(101).header("Upgrade", "x").upgrade(() -> new io.hydrangea.pipeline.Stage() { }));
        try (Server server = server(r); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/up");
            assertEquals(501, p.response(1).status);
        }
    }

    // ---- streamed request bodies ----

    private HttpRouter uploads(AtomicLong total, AtomicReference<BodyControl> control, boolean pauseOnFirst, AtomicInteger errors, CountDownLatch resumedDone) {
        return new HttpRouter().postStream("/sink", (r, body) -> {
            CompletableFuture<HttpResponse> done = new CompletableFuture<>();
            if (r.request().headers().get("x-limit") != null && body.contentLength() > Long.parseLong(r.request().headers().get("x-limit"))) {
                return CompletableFuture.completedFuture(HttpResponse.status(413).text("too big"));
            }
            body.listen(new BodyListener() {
                boolean first = true;

                @Override
                public void onData(ByteBuffer data, BodyControl c) {
                    total.addAndGet(data.remaining());
                    control.set(c);
                    if (pauseOnFirst && first) {
                        first = false;
                        c.pause();
                    }
                }

                @Override
                public void onComplete() {
                    done.complete(HttpResponse.ok(Long.toString(total.get())));
                    resumedDone.countDown();
                }

                @Override
                public void onError(Throwable error) {
                    errors.incrementAndGet();
                    done.completeExceptionally(error);
                }
            });
            return done;
        });
    }

    @Test
    void aLargeUploadIsStreamedNotBuffered() throws Exception {
        AtomicLong total = new AtomicLong();
        // far larger than maxBodyBytes (1 MiB by default): only a streaming route can accept it
        try (Server server = server(uploads(total, new AtomicReference<>(), false, new AtomicInteger(), new CountDownLatch(1)));
             Peer p = Peer.connect(port(server)).handshake()) {
            byte[] chunk = new byte[16384];
            new Random(2).nextBytes(chunk);
            int target = 20 * 1024 * 1024;
            p.post(1, "/sink", false);
            int sent = 0;
            long window = 1 << 20;
            while (sent < target) {
                while (window < chunk.length) {
                    Frame f = p.next();
                    if (f.type() == WINDOW_UPDATE) {
                        window += ByteBuffer.wrap(f.payload()).getInt();
                    }
                }
                p.data(1, false, chunk);
                sent += chunk.length;
                window -= chunk.length;
            }
            p.data(1, true, new byte[0]);
            Response r = p.response(1);
            assertEquals(200, r.status);
            assertEquals(Long.toString(sent), r.text());
            assertEquals(sent, total.get());
        }
    }

    @Test
    void pausingAStreamedUploadStopsTheClientAndLosesNothing() throws Exception {
        AtomicLong total = new AtomicLong();
        AtomicReference<BodyControl> control = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        Http2Config small = Http2Config.defaults().withWindows(65535, 1 << 20);
        try (Server server = HttpServer.bind(group, ANY, Options.defaults(), null, HttpConfig.defaults().withHttp2(small),
                uploads(total, control, true, new AtomicInteger(), finished)); Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/sink", false);
            byte[] chunk = new byte[16384];
            p.data(1, false, chunk);
            // the listener pauses inside the first onData; everything after it must be held back, and no stream credit given
            Thread.sleep(200);
            long first = total.get();
            assertEquals(16384, first);
            p.data(1, false, chunk);
            p.data(1, false, chunk);
            p.data(1, false, new byte[16383]);          // 65535 bytes in all: the whole window
            Thread.sleep(300);
            assertEquals(first, total.get(), "no onData while paused");
            Frame f;
            while ((f = p.pollOrNull(200)) != null) {
                assertFalse(f.type() == WINDOW_UPDATE && f.sid() == 1, "the stream must not be credited while the application is paused");
            }
            control.get().resume();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (total.get() < 65535 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(65535, total.get(), "the held data arrives after resume");
            boolean credited = false;
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!credited && System.nanoTime() < deadline) {
                f = p.pollOrNull(100);
                credited = f != null && f.type() == WINDOW_UPDATE && f.sid() == 1;
            }
            assertTrue(credited, "credit comes back once the application resumed");
            p.data(1, true, new byte[0]);
            assertEquals("65535", p.response(1).text());
        }
    }

    @Test
    void anEarlyAnswerToAStreamedUploadResetsTheStream() throws Exception {
        AtomicInteger errors = new AtomicInteger();
        try (Server server = server(uploads(new AtomicLong(), new AtomicReference<>(), false, errors, new CountDownLatch(1)));
             Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/sink", false, "content-length", "5000000", "x-limit", "1000");
            Response r = p.response(1);
            assertEquals(413, r.status);
            // the answer ends the stream although the body was never sent: the client is told to stop (RST_STREAM NO_ERROR)
            Frame f = p.pollOrNull(1000);
            assertNotNull(f);
            assertEquals(RST_STREAM, f.type());
            assertEquals(0, f.errorCode());
        }
    }

    @Test
    void aClientThatResetsAStreamedUploadIsReportedToTheListener() throws Exception {
        AtomicInteger errors = new AtomicInteger();
        try (Server server = server(uploads(new AtomicLong(), new AtomicReference<>(), false, errors, new CountDownLatch(1)));
             Peer p = Peer.connect(port(server)).handshake()) {
            p.post(1, "/sink", false);
            p.data(1, false, new byte[1000]);
            p.rst(1, 8);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (errors.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, errors.get());
        }
    }

    @Test
    void aSettingsFrameIsNotConfusedWithStreamData() throws Exception {
        try (Server server = server(router()); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/hello");
            assertEquals("hello", p.response(1).text());
        }
    }
}
