package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** The response head is held back until the handler has had its say: custom initial headers, and one header block for calls that fail at once. */
class GrpcResponseHeadersTest {

    private EventLoopGroup group;
    private Server server;
    private final CountDownLatch release = new CountDownLatch(1);

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        server = GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            ctx.responseHeaders().put("x-request-id", "r-42").putBinary("x-blob-bin", new byte[]{1, 2, 3});
            ctx.trailers().put("x-done", "yes");
            switch (req.text()) {
                case "FAIL" -> {
                    return CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.NOT_FOUND).withDescription("nothing here")));
                }
                case "WAIT" -> {
                    CompletableFuture<Msg> f = new CompletableFuture<>();
                    new Thread(() -> {
                        try { release.await(); } catch (InterruptedException ignored) { }
                        f.complete(new Msg("late"));
                    }).start();
                    return f;
                }
                default -> {
                    return CompletableFuture.completedFuture(new Msg("ok"));
                }
            }
        }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        group.close();
    }

    private int port() {
        return ((InetSocketAddress) server.localAddress()).getPort();
    }

    private static byte[] framed(Msg m) throws Exception {
        byte[] body = io.hydrangea.examples.GrpcEchoExample.MSG.encode(m);
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(body.length).put(body).array();
    }

    private static List<String> decode(Peer p, Frame f) throws Exception {
        List<String> hs = new ArrayList<>();
        p.dec.decode(f.payload(), (n, v) -> hs.add(n + ": " + v));
        return hs;
    }

    private static void call(Peer p, String text) throws Exception {
        p.headers(1, false, true, ":method", "POST", ":scheme", "http", ":authority", "x", ":path", "/hydrangea.test.Echo/Unary", "content-type", "application/grpc");
        p.data(1, true, framed(new Msg(text)));
    }

    @Test
    void nothingIsSentUntilTheHandlerAnswers() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            call(p, "WAIT");
            Frame early = p.pollOrNull(400);
            assertTrue(early == null || early.sid() != 1, "no HEADERS before the handler has anything to say");
            release.countDown();
            Frame head = p.nextInteresting();
            assertEquals(1, head.type());
            assertEquals(0, head.flags() & 1, "the head does not end the stream");
            List<String> hs = decode(p, head);
            assertTrue(hs.contains("x-request-id: r-42"), hs.toString());
            assertTrue(hs.contains(":status: 200"));
            assertTrue(hs.contains("content-type: application/grpc"));
        }
    }

    @Test
    void customHeadersComeBeforeTheFirstMessageAndBinaryOnesAreEncoded() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            call(p, "hi");
            Frame head = p.nextInteresting();
            List<String> hs = decode(p, head);
            assertTrue(hs.contains("x-request-id: r-42"), hs.toString());
            assertTrue(hs.contains("x-blob-bin: AQID"), "binary metadata is base64: " + hs);
            Frame data = p.nextInteresting();
            assertEquals(0, data.type());
            Frame trailers = p.nextInteresting();
            assertEquals(1, trailers.type());
            List<String> ts = decode(p, trailers);
            assertTrue(ts.contains("grpc-status: 0"));
            assertTrue(ts.contains("x-done: yes"));
            assertFalse(ts.contains("x-request-id: r-42"), "initial headers are not repeated in the trailers");
        }
    }

    @Test
    void aCallThatFailsAtOnceIsOneHeaderBlock() throws Exception {
        try (Peer p = Peer.connect(port()).handshake()) {
            call(p, "FAIL");
            Frame only = p.nextInteresting();
            assertEquals(1, only.type());
            assertNotEquals(0, only.flags() & 1, "HEADERS with END_STREAM: Trailers-Only");
            List<String> hs = decode(p, only);
            assertTrue(hs.contains(":status: 200"));
            assertTrue(hs.contains("grpc-status: 5"));
            assertTrue(hs.contains("grpc-message: nothing here"));
            assertTrue(hs.contains("x-request-id: r-42"), "the handler's headers ride along");
            assertNull(p.pollOrNull(300) == null ? null : "another frame followed", "nothing after the single block");
        }
    }

    @Test
    void theClientSeesTheInitialHeadersAndTrailersSeparately() throws Exception {
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port()).build()) {
            Metadata[] seen = new Metadata[2];
            CompletableFuture<Void> done = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.newCall(UNARY, CallOptions.DEFAULT, new ClientCall.Listener<>() {
                public void onHeaders(Metadata h) { seen[0] = h; }
                public void onClose(Status s, Metadata t) { seen[1] = t; done.complete(null); }
            });
            call.send(new Msg("hi"));
            call.halfClose();
            done.get(10, TimeUnit.SECONDS);
            assertEquals("r-42", seen[0].get("x-request-id"));
            assertArrayEquals(new byte[]{1, 2, 3}, seen[0].getBinary("x-blob-bin"));
            assertEquals("yes", seen[1].get("x-done"));
        }
    }

    @Test
    void aFailedCallStillDeliversItsHeadersToTheClient() throws Exception {
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port()).build()) {
            CompletableFuture<Metadata> trailers = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.newCall(UNARY, CallOptions.DEFAULT, new ClientCall.Listener<>() {
                public void onClose(Status s, Metadata t) { trailers.complete(t); }
            });
            call.send(new Msg("FAIL"));
            call.halfClose();
            Metadata t = trailers.get(10, TimeUnit.SECONDS);
            assertEquals("r-42", t.get("x-request-id"), "in a Trailers-Only response the client gets headers and trailers as one set");
        }
    }
}
