package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.HealthService;
import io.hydrangea.grpc.MethodDescriptor;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import io.hydrangea.grpc.StreamObserver;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.BIDI;
import static io.hydrangea.examples.GrpcEchoExample.CLIENT_STREAM;
import static io.hydrangea.examples.GrpcEchoExample.SERVER_STREAM;
import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** The gRPC server, looked at frame by frame as a raw HTTP/2 peer sees it, and through Hydrangea's own client. */
class GrpcServerTest {

    private static final int DATA = 0, HEADERS = 1, RST_STREAM = 3, END_STREAM = 1;

    private EventLoopGroup group;
    private Server server;
    private HealthService health;

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        health = new HealthService();
        server = GrpcEchoExample.builder().addService(health).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private int port() {
        return ((InetSocketAddress) server.localAddress()).getPort();
    }

    // ---- raw call helpers ----

    private static byte[] framed(Msg m) throws Exception {
        byte[] body = GrpcEchoExample.MSG.encode(m);
        ByteBuffer b = ByteBuffer.allocate(5 + body.length);
        b.put((byte) 0).putInt(body.length).put(body);
        return b.array();
    }

    private static void startCall(Peer p, int sid, String path, boolean endStream, String... extra) throws Exception {
        String[] base = {":method", "POST", ":scheme", "http", ":authority", "localhost", ":path", path, "content-type", "application/grpc", "te", "trailers"};
        String[] all = java.util.Arrays.copyOf(base, base.length + extra.length);
        System.arraycopy(extra, 0, all, base.length, extra.length);
        p.headers(sid, endStream, true, all);
    }

    /** What the server sent for one stream: header blocks (each a list of "name: value"), the DATA bytes, and how the stream ended. */
    private static final class Seen {
        final List<List<String>> blocks = new ArrayList<>();
        final List<Integer> blockFlags = new ArrayList<>();
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        final List<String> order = new ArrayList<>();
        boolean ended;
        int reset = -1;

        List<String> head() {
            return blocks.get(0);
        }

        List<String> trailers() {
            return blocks.get(blocks.size() - 1);
        }

        String value(List<String> block, String name) {
            return block.stream().filter(h -> h.startsWith(name + ": ")).map(h -> h.substring(name.length() + 2)).findFirst().orElse(null);
        }

        List<Msg> messages() throws Exception {
            byte[] d = data.toByteArray();
            List<Msg> out = new ArrayList<>();
            ByteBuffer b = ByteBuffer.wrap(d);
            while (b.remaining() >= 5) {
                b.get();
                byte[] payload = new byte[b.getInt()];
                b.get(payload);
                out.add(GrpcEchoExample.MSG.decode(payload));
            }
            assertEquals(0, b.remaining(), "no partial message");
            return out;
        }
    }

    private static Seen collect(Peer p, int sid) {
        Seen seen = new Seen();
        for (;;) {
            Frame f = p.nextInteresting();
            if (f.sid() != sid && f.type() != -1) {
                continue;
            }
            switch (f.type()) {
                case HEADERS -> {
                    List<String> hs = new ArrayList<>();
                    try {
                        p.dec.decode(f.payload(), (n, v) -> hs.add(n + ": " + v));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    seen.blocks.add(hs);
                    seen.blockFlags.add(f.flags());
                    seen.order.add("HEADERS");
                    if ((f.flags() & END_STREAM) != 0) {
                        seen.ended = true;
                        return seen;
                    }
                }
                case DATA -> {
                    seen.data.write(f.payload(), 0, f.payload().length);
                    seen.order.add("DATA");
                    if ((f.flags() & END_STREAM) != 0) {
                        seen.ended = true;
                        return seen;
                    }
                }
                case RST_STREAM -> {
                    seen.reset = f.errorCode();
                    seen.order.add("RST_STREAM");
                    return seen;
                }
                default -> fail("unexpected frame " + f.type());
            }
        }
    }

    /** Small messages are merged into bigger DATA frames, but a round of frames never exceeds the window the peer has granted. */
    @Test
    void mergedDataFramesRespectTheStreamWindow() throws Exception {
        respectsTheWindow(100, 200);   // small messages: copied together into frames
    }

    @Test
    void prefixedBigMessagesRespectTheStreamWindow() throws Exception {
        respectsTheWindow(10, 6000);   // big messages: the 5-byte prefix and the body are gathered into a frame without a copy
    }

    private void respectsTheWindow(int count, int blobSize) throws Exception {
        final int grant = 3000;
        try (Peer p = Peer.connect(port()).handshake(4, grant)) {   // SETTINGS_INITIAL_WINDOW_SIZE
            startCall(p, 1, SERVER_STREAM.path(), false);
            p.data(1, true, framed(new Msg("m", count, new byte[blobSize])));
            int total = 0;
            boolean ended = false;
            for (int round = 0; !ended && round < 60; round++) {
                int inRound = 0;
                for (Frame f = p.pollOrNull(300); f != null; f = p.pollOrNull(300)) {
                    if (f.sid() != 1) {
                        continue;
                    }
                    if (f.type() == DATA) {
                        inRound += f.payload().length;
                    }
                    if ((f.flags() & END_STREAM) != 0) {
                        ended = true;
                        break;
                    }
                }
                assertTrue(inRound <= grant, "round " + round + " sent " + inRound + " bytes into a window of " + grant);
                total += inRound;
                p.windowUpdate(1, grant);
            }
            assertTrue(ended, "the stream finished");
            int expected = 0;
            for (int i = 0; i < count; i++) {
                expected += 5 + GrpcEchoExample.MSG.encode(new Msg("m-" + i, i, new byte[blobSize])).length;
            }
            assertEquals(expected, total, "all " + count + " messages arrived, whole");
        }
    }

    // ---- the shape of a successful call ----

    @Test
    void aUnaryCallIsHeadersThenMessageThenTrailers() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Unary", false);
            p.data(1, false, framed(new Msg("hi", 3)));
            p.data(1, true, new byte[0]);
            Seen s = collect(p, 1);
            assertEquals(List.of("HEADERS", "DATA", "HEADERS"), s.order);
            assertEquals("200", s.value(s.head(), ":status"));
            assertEquals("application/grpc", s.value(s.head(), "content-type"));
            assertEquals(0, s.blockFlags.get(0) & END_STREAM, "the head does not end the stream");
            assertEquals("echo:hi", s.messages().get(0).text());
            assertEquals("0", s.value(s.trailers(), "grpc-status"));
            assertNotEquals(0, s.blockFlags.get(1) & END_STREAM, "the trailers end the stream");
            assertNull(s.value(s.trailers(), ":status"));
        }
    }

    @Test
    void statusMessagesArePercentEncodedInTheTrailers() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Unary", false);
            p.data(1, true, framed(new Msg("FAIL:5:café 100%")));
            Seen s = collect(p, 1);
            assertEquals("5", s.value(s.trailers(), "grpc-status"));
            assertEquals("caf%C3%A9 100%25", s.value(s.trailers(), "grpc-message"));
        }
    }

    // ---- calls that fail before they start: Trailers-Only ----

    @Test
    void unknownMethodsAndBadCallsAreAnsweredInASingleHeaderBlock() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Nope", false);
            Seen s = collect(p, 1);
            assertEquals(List.of("HEADERS"), s.order, "Trailers-Only: one HEADERS frame that ends the stream");
            assertEquals("12", s.value(s.head(), "grpc-status"));
            assertEquals("200", s.value(s.head(), ":status"));
            assertTrue(s.value(s.head(), "grpc-message").contains("Method%20not%20found") || s.value(s.head(), "grpc-message").contains("Method not found"), s.head().toString());

            startCall(p, 3, "/hydrangea.test.Echo/Unary", true, "grpc-encoding", "snappy");
            Seen enc = collect(p, 3);
            assertEquals("12", enc.value(enc.head(), "grpc-status"));
            assertEquals("identity,gzip", enc.value(enc.head(), "grpc-accept-encoding"));

            startCall(p, 5, "/hydrangea.test.Echo/Unary", true, "grpc-timeout", "soon");
            Seen t = collect(p, 5);
            assertEquals("13", t.value(t.head(), "grpc-status"));
        }
    }

    @Test
    void thingsThatAreNotGrpcGetPlainHttpErrors() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            p.headers(1, true, true, ":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/hydrangea.test.Echo/Unary", "content-type", "application/grpc");
            Seen get = collect(p, 1);
            assertEquals("405", get.value(get.head(), ":status"));
            p.headers(3, false, true, ":method", "POST", ":scheme", "http", ":authority", "x", ":path", "/hydrangea.test.Echo/Unary", "content-type", "application/json");
            p.data(3, true, new byte[0]);
            Seen json = collect(p, 3);
            assertEquals("415", json.value(json.head(), ":status"));
            p.headers(5, true, true, ":method", "GET", ":scheme", "http", ":authority", "x", ":path", "/elsewhere");
            Seen missing = collect(p, 5);
            assertEquals("404", missing.value(missing.head(), ":status"));
        }
    }

    @Test
    void anHttp11RequestToAGrpcMethodIsRefused() throws Exception {
        try (Socket s = new Socket("127.0.0.1", port())) {
            s.setSoTimeout(5000);
            s.getOutputStream().write(("POST /hydrangea.test.Echo/Unary HTTP/1.1\r\nHost: x\r\nContent-Type: application/grpc\r\nContent-Length: 0\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
            String reply = new String(s.getInputStream().readNBytes(40), StandardCharsets.ISO_8859_1);
            assertTrue(reply.startsWith("HTTP/1.1 505"), reply);
        }
    }

    // ---- the request side ----

    @Test
    void protocolViolationsEndTheCallWithTheRightStatus() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Unary", false);
            p.data(1, false, new byte[]{2, 0, 0, 0, 0});
            Seen flag = collect(p, 1);
            assertEquals("13", flag.value(flag.trailers(), "grpc-status"), "INTERNAL for a bad compression flag");

            startCall(p, 3, "/hydrangea.test.Echo/Unary", false);
            p.data(3, false, new byte[]{0, (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff});
            Seen big = collect(p, 3);
            assertEquals("8", big.value(big.trailers(), "grpc-status"), "RESOURCE_EXHAUSTED for a message past the limit, from the prefix alone");

            startCall(p, 5, "/hydrangea.test.Echo/Unary", false);
            p.data(5, false, framed(new Msg("one")));
            p.data(5, false, framed(new Msg("two")));
            Seen two = collect(p, 5);
            assertEquals("13", two.value(two.trailers(), "grpc-status"), "a unary method takes one request");

            startCall(p, 7, "/hydrangea.test.Echo/Unary", false);
            p.data(7, true, new byte[0]);
            Seen none = collect(p, 7);
            assertEquals("13", none.value(none.trailers(), "grpc-status"), "half-closed without a request");

            startCall(p, 9, "/hydrangea.test.Echo/Unary", false);
            p.data(9, true, new byte[]{0, 0, 0, 0, 9, 1});
            Seen cut = collect(p, 9);
            assertEquals("13", cut.value(cut.trailers(), "grpc-status"), "the stream ended inside a message");

            startCall(p, 11, "/hydrangea.test.Echo/Unary", false);
            p.data(11, true, new byte[]{0, 0, 0, 0, 2, (byte) 0xff, (byte) 0xff});   // not a protobuf message
            Seen junk = collect(p, 11);
            assertEquals("13", junk.value(junk.trailers(), "grpc-status"), "INTERNAL for bytes the marshaller rejects");
        }
    }

    // ---- deadlines and cancellation ----

    @Test
    void theServerEnforcesTheDeadlineItWasGiven() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Unary", false, "grpc-timeout", "150m");
            p.data(1, true, framed(new Msg("SLOW:5000")));
            long t0 = System.nanoTime();
            Seen s = collect(p, 1);
            long took = (System.nanoTime() - t0) / 1_000_000;
            assertEquals("4", s.value(s.trailers(), "grpc-status"), s.trailers().toString());
            assertTrue(took < 2000, "answered at the deadline, not when the handler finished: " + took + " ms");
        }
    }

    @Test
    void aResetStreamCancelsTheHandler() throws Exception {
        int before = GrpcEchoExample.CANCELLED.get();
        try (Peer p = Peer.connect(port()).handshake()) {
            startCall(p, 1, "/hydrangea.test.Echo/Unary", false);
            p.data(1, true, framed(new Msg("SLOW:5000")));
            Thread.sleep(200);
            p.rst(1, 8);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (GrpcEchoExample.CANCELLED.get() == before && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(before + 1, GrpcEchoExample.CANCELLED.get());
        }
    }

    // ---- the application API ----

    @Test
    void healthCheckAndWatchWorkThroughTheClient() throws Exception {
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port()).build()) {
            assertEquals(HealthService.ServingStatus.SERVING, ch.unary(HealthService.CHECK, "", CallOptions.DEFAULT).get(5, TimeUnit.SECONDS));
            StatusException e = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> ch.unary(HealthService.CHECK, "no.such.Service", CallOptions.DEFAULT).get(5, TimeUnit.SECONDS)).getCause() instanceof StatusException se ? se : null;
            assertEquals(Status.Code.NOT_FOUND, e.status().code());

            health.setStatus("hydrangea.test.Echo", HealthService.ServingStatus.SERVING);
            List<HealthService.ServingStatus> seen = new CopyOnWriteArrayList<>();
            CountDownLatch two = new CountDownLatch(2);
            var call = ch.serverStreaming(HealthService.WATCH, "hydrangea.test.Echo", CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(HealthService.ServingStatus s) { seen.add(s); two.countDown(); }
                public void onError(Throwable t) { }
                public void onCompleted() { }
            });
            Thread.sleep(200);
            health.setStatus("hydrangea.test.Echo", HealthService.ServingStatus.NOT_SERVING);
            assertTrue(two.await(5, TimeUnit.SECONDS), seen.toString());
            assertEquals(List.of(HealthService.ServingStatus.SERVING, HealthService.ServingStatus.NOT_SERVING), seen, "the current state first, then the change");
            call.cancel("done");
        }
    }

    @Test
    void ordinaryHttpAndGrpcShareAPort() throws Exception {
        HttpRouter router = new HttpRouter().get("/hello", r -> HttpResponse.ok("plain http"));
        try (Server both = GrpcEchoExample.builder().fallback(router).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) both.localAddress()).getPort()).build();
             Peer p = Peer.connect(((InetSocketAddress) both.localAddress()).getPort()).handshake()) {
            assertEquals("echo:x", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(5, TimeUnit.SECONDS).text());
            p.get(1, "/hello");
            Seen s = collect(p, 1);
            assertEquals("200", s.value(s.head(), ":status"));
            assertEquals("plain http", new String(s.data.toByteArray()));
        }
    }

    @Test
    void handlersCanRunOnAnExecutorWithoutLosingTheOrderOfMessages() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
             Server s2 = GrpcEchoExample.builder().executor(executor).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) s2.localAddress()).getPort()).build()) {
            List<String> echoes = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            var call = ch.bidiStreaming(BIDI, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { echoes.add(m.text()); }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            for (int i = 0; i < 2000; i++) {
                call.send(new Msg("m" + i));
            }
            call.halfClose();
            done.get(15, TimeUnit.SECONDS);
            assertEquals(2000, echoes.size());
            for (int i = 0; i < 2000; i++) {
                assertEquals("echo:m" + i, echoes.get(i));
            }
        }
    }

    @Test
    void overTlsWithAlpn(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server secure = GrpcEchoExample.builder().build().bind(group, new InetSocketAddress("127.0.0.1", 0), tls[0]);
             GrpcChannel ch = GrpcChannel.forTarget("https://localhost:" + ((InetSocketAddress) secure.localAddress()).getPort()).sslContext(tls[1]).build()) {
            assertEquals("echo:secret", ch.unary(UNARY, new Msg("secret"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            List<Integer> got = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            ch.serverStreaming(SERVER_STREAM, new Msg("r", 50), CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { got.add(m.n()); }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            done.get(10, TimeUnit.SECONDS);
            assertEquals(50, got.size());
        }
    }

    @Test
    void registeringAMethodWithTheWrongShapeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> GrpcServer.builder().unary(SERVER_STREAM, (req, ctx) -> null));
        assertThrows(IllegalArgumentException.class, () -> GrpcServer.builder().unary(UNARY, (req, ctx) -> null).unary(UNARY, (req, ctx) -> null));
        assertThrows(IllegalArgumentException.class, () -> MethodDescriptor.of("NoSlashHere", "", MethodDescriptor.MethodType.UNARY, GrpcEchoExample.MSG, GrpcEchoExample.MSG));
    }
}
