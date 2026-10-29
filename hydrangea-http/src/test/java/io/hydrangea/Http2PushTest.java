package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.http.Headers;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Server push: PUSH_PROMISE on the page's stream, the promised resource answered on an even stream. */
class Http2PushTest {

    private static final int DATA = 0, HEADERS = 1, RST_STREAM = 3, PUSH_PROMISE = 5, GOAWAY = 7, END_STREAM = 1;

    private EventLoopGroup group;
    private final AtomicInteger styleRequests = new AtomicInteger();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private Server server(SSLContext tls) throws Exception {
        HttpRouter router = new HttpRouter()
                .get("/page", r -> HttpResponse.ok("<html>").push("/style.css").push("/app.js"))
                .get("/one", r -> HttpResponse.ok("one").push("/style.css"))
                .get("/broken", r -> HttpResponse.status(404).text("no such page").push("/style.css"))
                .get("/many", r -> {
                    HttpResponse resp = HttpResponse.ok("many");
                    for (int i = 0; i < 40; i++) {
                        resp.push("/item/" + i);
                    }
                    return resp;
                })
                .get("/with-cookie", r -> HttpResponse.ok("c").push("/whoami"))
                .get("/explicit", r -> {
                    Headers h = new Headers();
                    h.add("X-Chosen", "by the route");
                    return HttpResponse.ok("e").push("/headers", h);
                })
                .get("/style.css", r -> {
                    styleRequests.incrementAndGet();
                    return HttpResponse.status(200).header("Content-Type", "text/css").body("body{}".getBytes());
                })
                .get("/app.js", r -> HttpResponse.status(200).body("js()".getBytes()))
                .get("/item/{n}", r -> HttpResponse.ok("item " + r.pathVariable("n")))
                .get("/whoami", r -> HttpResponse.ok("cookie=" + r.request().headers().get("cookie")))
                .get("/headers", r -> HttpResponse.ok(r.request().headers().toString()))
                .get("/big", r -> HttpResponse.ok("big").push("/blob"))
                .get("/blob", r -> HttpResponse.status(200).body(new byte[50_000]));
        return HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), tls,
                new HttpConfig(16 * 1024, 1024 * 1024).withHttp2(Http2Config.defaults()), router);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    /** Everything the server sends for a request until all the streams it mentions have ended; header blocks are decoded in arrival order. */
    private static final class Seen {
        final List<String> order = new ArrayList<>();                     // "PUSH_PROMISE 1->2 :path=/style.css", "HEADERS 2 :status=200", ...
        final Map<Integer, StringBuilder> bodies = new java.util.TreeMap<>();
        final List<Integer> promised = new ArrayList<>();
        final Map<Integer, String> promisedPath = new java.util.TreeMap<>();
        final List<Frame> frames = new ArrayList<>();
    }

    private Seen collect(Peer p, int expectedStreams, long quietMillis) throws Exception {
        Seen seen = new Seen();
        java.util.Set<Integer> ended = new java.util.HashSet<>();
        Frame f;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && (f = p.pollOrNull(quietMillis)) != null) {
            seen.frames.add(f);
            switch (f.type()) {
                case PUSH_PROMISE -> {
                    ByteBuffer b = ByteBuffer.wrap(f.payload());
                    int promisedId = b.getInt() & 0x7fffffff;
                    List<String> hs = new ArrayList<>();
                    p.dec.decode(Arrays.copyOfRange(f.payload(), 4, f.payload().length), (n, v) -> hs.add(n + "=" + v));
                    seen.promised.add(promisedId);
                    seen.promisedPath.put(promisedId, hs.stream().filter(h -> h.startsWith(":path=")).findFirst().orElse("?").substring(6));
                    seen.order.add("PUSH_PROMISE " + f.sid() + "->" + promisedId + " " + hs);
                }
                case HEADERS -> {
                    List<String> hs = new ArrayList<>();
                    p.dec.decode(f.payload(), (n, v) -> hs.add(n + "=" + v));
                    seen.order.add("HEADERS " + f.sid() + " " + hs);
                    if ((f.flags() & END_STREAM) != 0) ended.add(f.sid());
                }
                case DATA -> {
                    seen.bodies.computeIfAbsent(f.sid(), k -> new StringBuilder()).append(new String(f.payload()));
                    seen.order.add("DATA " + f.sid() + " " + f.payload().length);
                    if ((f.flags() & END_STREAM) != 0) ended.add(f.sid());
                }
                case RST_STREAM -> seen.order.add("RST_STREAM " + f.sid() + " code " + f.errorCode());
                default -> { }
            }
            if (ended.size() >= expectedStreams) {
                Frame tail;
                while ((tail = p.pollOrNull(150)) != null) {   // a trailing RST_STREAM(NO_ERROR) or WINDOW_UPDATE
                    seen.frames.add(tail);
                }
                break;
            }
        }
        return seen;
    }

    @Test
    void promisesAreSentBeforeThePageAndTheResourcesFollowOnEvenStreams() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/page");
            Seen seen = collect(p, 3, 500);
            assertEquals(List.of(2, 4), seen.promised);
            assertEquals("/style.css", seen.promisedPath.get(2));
            assertEquals("/app.js", seen.promisedPath.get(4));
            int firstPromise = indexOf(seen.order, "PUSH_PROMISE");
            int pageHeaders = indexOf(seen.order, "HEADERS 1 ");
            assertTrue(firstPromise >= 0 && firstPromise < pageHeaders, "promises come before the response that refers to the resources: " + seen.order);
            assertTrue(seen.order.stream().anyMatch(l -> l.startsWith("PUSH_PROMISE 1->2") && l.contains(":method=GET") && l.contains(":scheme=http")
                    && l.contains(":authority=localhost")), seen.order.toString());
            assertEquals("<html>", seen.bodies.get(1).toString());
            assertEquals("body{}", seen.bodies.get(2).toString());
            assertEquals("js()", seen.bodies.get(4).toString());
            assertEquals(1, styleRequests.get());
        }
    }

    private static int indexOf(List<String> l, String prefix) {
        for (int i = 0; i < l.size(); i++) {
            if (l.get(i).startsWith(prefix)) return i;
        }
        return -1;
    }

    @Test
    void nothingIsPushedToAClientThatDisabledPush() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake(2, 0)) {
            p.get(1, "/page");
            Seen seen = collect(p, 1, 400);
            assertTrue(seen.promised.isEmpty(), seen.order.toString());
            assertEquals("<html>", seen.bodies.get(1).toString());
            assertEquals(0, styleRequests.get(), "no synthetic request either");
        }
    }

    @Test
    void theClientsStreamLimitBoundsThePushes() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake(3, 1)) {   // SETTINGS_MAX_CONCURRENT_STREAMS = 1
            p.get(1, "/page");
            Seen seen = collect(p, 2, 500);
            assertEquals(List.of(2), seen.promised, "two were offered, one fits: " + seen.order);
            p.get(3, "/page");  // the first push has finished: its slot is free again, but /style.css was promised before
            Seen again = collect(p, 2, 500);
            assertEquals(List.of(4), again.promised, "the path that was not promised yet gets its turn: " + again.order);
        }
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake(3, 0)) {   // MAX_CONCURRENT_STREAMS = 0
            p.get(1, "/page");
            assertTrue(collect(p, 1, 400).promised.isEmpty());
        }
    }

    @Test
    void aPathIsPromisedOncePerConnection() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/one");
            assertEquals(List.of(2), collect(p, 2, 400).promised);
            p.get(3, "/one");
            Seen second = collect(p, 1, 400);
            assertTrue(second.promised.isEmpty(), "the client already has /style.css: " + second.order);
            assertEquals(1, styleRequests.get());
        }
    }

    @Test
    void failedPagesPushNothingAndThereIsAnUpperBound() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/broken");
            Seen broken = collect(p, 1, 400);
            assertTrue(broken.promised.isEmpty(), "a 404 does not promise anything");
            p.get(3, "/many");
            Seen many = collect(p, 17, 600);
            assertEquals(16, many.promised.size(), "at most 16 pushes per response");
        }
    }

    @Test
    void thePushedRequestRunsThroughTheHandlerWithTheOriginalCookie() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.headers(1, true, true, ":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/with-cookie", "cookie", "a=1", "cookie", "b=2");
            Seen seen = collect(p, 2, 500);
            assertEquals("cookie=a=1; b=2", seen.bodies.get(2).toString());
        }
    }

    @Test
    void theRouteCanChooseTheRequestHeadersOfAPush() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.headers(1, true, true, ":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/explicit", "cookie", "secret=1", "x-other", "no");
            Seen seen = collect(p, 2, 500);
            String headers = seen.bodies.get(2).toString().toLowerCase();
            assertTrue(headers.contains("x-chosen"), headers);
            assertFalse(headers.contains("secret"), "only what the route chose is sent: " + headers);
            assertFalse(headers.contains("x-other"), headers);
        }
    }

    @Test
    void aClientCanCancelAPushedStream() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake(4, 0)) {   // window 0: nothing can be delivered yet
            p.get(1, "/big");
            Frame f;
            int promisedId = 0;
            while ((f = p.pollOrNull(500)) != null) {
                if (f.type() == PUSH_PROMISE) {
                    promisedId = ByteBuffer.wrap(f.payload()).getInt();
                }
            }
            assertEquals(2, promisedId);
            p.rst(2, 8);                        // CANCEL: the client already has it
            p.windowUpdate(1, 1 << 20);
            p.windowUpdate(0, 1 << 20);
            Seen after = collect(p, 1, 500);
            assertTrue(after.bodies.get(2) == null || after.bodies.get(2).length() == 0, "the cancelled stream gets no body");
            p.settings(4, 65535);               // new streams may be answered again
            p.get(3, "/one");                   // the connection is healthy
            Seen healthy = collect(p, 2, 500);
            assertNotNull(healthy.bodies.get(3), healthy.order.toString());
            assertEquals("one", healthy.bodies.get(3).toString());
        }
    }

    @Test
    void pushedResponsesObeyTheClientsFlowControl() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake(4, 1000)) {   // 1000-byte stream windows
            p.get(1, "/big");
            Frame f;
            long blob = 0;
            while ((f = p.pollOrNull(500)) != null) {
                if (f.type() == DATA && f.sid() == 2) {
                    blob += f.payload().length;
                }
            }
            assertEquals(1000, blob, "a pushed stream has its own window, announced by the client");
            p.windowUpdate(2, 100_000);
            p.windowUpdate(0, 100_000);
            long more = 0;
            while ((f = p.pollOrNull(500)) != null) {
                if (f.type() == DATA && f.sid() == 2) {
                    more += f.payload().length;
                }
            }
            assertEquals(49_000, more);
        }
    }

    @Test
    void framesTheClientMayNotSendOnPushedStreams() throws Exception {
        try (Server server = server(null); Peer p = Peer.connect(port(server)).handshake()) {
            p.get(1, "/one");
            collect(p, 2, 400);
            p.data(2, false, new byte[1]);                       // DATA on a promised stream: the client has nothing to say there
            Frame f;
            do {
                f = p.nextInteresting();
            } while (f.type() != RST_STREAM && f.type() != GOAWAY);
            assertEquals(5, f.errorCode(), "STREAM_CLOSED");
            p.rst(100, 8);                                       // a stream number the server never promised
            do {
                f = p.nextInteresting();
            } while (f.type() != GOAWAY);
            assertEquals(1, f.errorCode(), "PROTOCOL_ERROR");
        }
    }

    @Test
    void theJdkClientReceivesPushedResources(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server server = server(tls[0])) {
            HttpClient client = HttpClient.newBuilder().sslContext(tls[1]).version(HttpClient.Version.HTTP_2).build();
            Map<String, String> pushed = new ConcurrentHashMap<>();
            HttpRequest req = HttpRequest.newBuilder(URI.create("https://localhost:" + port(server) + "/with-cookie")).header("Cookie", "sid=7").build();
            java.net.http.HttpResponse<String> main = client.sendAsync(req, java.net.http.HttpResponse.BodyHandlers.ofString(), (initiating, promise, acceptor) ->
                    acceptor.apply(java.net.http.HttpResponse.BodyHandlers.ofString()).thenAccept(r -> pushed.put(promise.uri().getPath(), r.statusCode() + ":" + r.body())))
                    .get(10, TimeUnit.SECONDS);
            assertEquals("c", main.body());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (pushed.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("200:cookie=sid=7", pushed.get("/whoami"));
        }
    }

    @Test
    void pushIsIgnoredOverHttp11() throws Exception {
        try (Server server = server(null)) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            var r = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(server) + "/page")).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals("<html>", r.body());
            assertEquals(0, styleRequests.get());
        }
    }

    @Test
    void aBadPushPathIsRejectedWhenTheResponseIsBuilt() {
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").push("no-slash"));
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").push("/has space"));
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").push("/line\nbreak"));
        assertThrows(IllegalArgumentException.class, () -> HttpResponse.ok("x").push(""));
    }
}
