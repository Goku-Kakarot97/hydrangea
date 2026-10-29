package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** The gRPC client against a scripted HTTP/2 peer: what it sends, and how it reads servers that misbehave or are not gRPC at all. */
class GrpcClientTest {

    private static byte[] framed(Msg m) throws Exception {
        byte[] body = GrpcEchoExample.MSG.encode(m);
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(body.length).put(body).array();
    }

    /** Runs one unary call against a scripted peer: {@code script} gets the request headers and plays the server's side. */
    private interface Script {
        void play(Peer p, List<String> requestHeaders) throws Exception;
    }

    private static StatusException callAgainst(Script script, CallOptions options) throws Exception {
        try {
            callOk(script, options);
            return null;
        } catch (ExecutionException e) {
            return (StatusException) e.getCause();
        }
    }

    private static Msg callOk(Script script, CallOptions options) throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).build()) {
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("hi"), options);
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                List<String> hs = new ArrayList<>();
                for (;;) {
                    Frame f = p.nextInteresting();
                    if (f.type() == 1) {
                        p.dec.decode(f.payload(), (n, v) -> hs.add(n + ": " + v));
                        break;
                    }
                }
                script.play(p, hs);
                return call.get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static String header(List<String> hs, String name) {
        return hs.stream().filter(h -> h.startsWith(name + ": ")).map(h -> h.substring(name.length() + 2)).findFirst().orElse(null);
    }

    @Test
    void theRequestLooksLikeGrpc() throws Exception {
        List<String>[] seen = new List[1];
        callOk((p, hs) -> {
            seen[0] = hs;
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.data(1, false, framed(new Msg("ok")));
            p.headers(1, true, true, "grpc-status", "0");
        }, CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(5)));
        assertEquals("POST", header(seen[0], ":method"));
        assertEquals("/hydrangea.test.Echo/Unary", header(seen[0], ":path"));
        assertEquals("application/grpc", header(seen[0], "content-type"));
        assertEquals("trailers", header(seen[0], "te"));
        assertEquals("gzip", header(seen[0], "grpc-accept-encoding"));
        assertNotNull(header(seen[0], "user-agent"));
        String timeout = header(seen[0], "grpc-timeout");
        assertNotNull(timeout);
        assertTrue(timeout.matches("\\d{1,8}[HMSmun]"), timeout);
    }

    @Test
    void statusInTheTrailersIsTheOutcome() throws Exception {
        StatusException e = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.headers(1, true, true, "grpc-status", "7", "grpc-message", "no%20way%20caf%C3%A9");
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.PERMISSION_DENIED, e.status().code());
        assertEquals("no way café", e.status().description());
    }

    @Test
    void trailersOnlyResponsesAreUnderstood() throws Exception {
        StatusException e = callAgainst((p, hs) ->
                p.headers(1, true, true, ":status", "200", "content-type", "application/grpc", "grpc-status", "12", "grpc-message", "nope"), CallOptions.DEFAULT);
        assertEquals(Status.Code.UNIMPLEMENTED, e.status().code());
        assertEquals("nope", e.status().description());
    }

    @Test
    void httpErrorsFromNonGrpcServersAreMapped() throws Exception {
        int[][] table = {{400, 13}, {401, 16}, {403, 7}, {404, 12}, {429, 14}, {502, 14}, {503, 14}, {504, 14}, {500, 2}, {418, 2}};
        for (int[] row : table) {
            StatusException e = callAgainst((p, hs) -> {
                p.headers(1, false, true, ":status", String.valueOf(row[0]), "content-type", "text/html");
                p.data(1, true, "<html>".getBytes());
            }, CallOptions.DEFAULT);
            assertEquals(row[1], e.status().code().value(), "HTTP " + row[0]);
        }
    }

    @Test
    void aWrongContentTypeIsUnknown() throws Exception {
        StatusException e = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/json");
            p.data(1, true, "{}".getBytes());
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.UNKNOWN, e.status().code());
    }

    @Test
    void endingWithoutAStatusIsInternal() throws Exception {
        StatusException e = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.data(1, true, framed(new Msg("ok")));
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.INTERNAL, e.status().code());
    }

    @Test
    void okWithNoMessageOrTwoIsInternalForAUnaryCall() throws Exception {
        StatusException none = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.headers(1, true, true, "grpc-status", "0");
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.INTERNAL, none.status().code());
        StatusException two = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.data(1, false, framed(new Msg("a")));
            p.data(1, false, framed(new Msg("b")));
            p.headers(1, true, true, "grpc-status", "0");
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.INTERNAL, two.status().code());
    }

    @Test
    void aTruncatedMessageFailsTheCallEvenWithStatusOk() throws Exception {
        StatusException e = callAgainst((p, hs) -> {
            p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
            p.data(1, false, new byte[]{0, 0, 0, 0, 9, 1});
            p.headers(1, true, true, "grpc-status", "0");
        }, CallOptions.DEFAULT);
        assertEquals(Status.Code.INTERNAL, e.status().code());
    }

    @Test
    void anOversizedMessageIsRefusedAndTheCallReset() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).maxInboundMessageBytes(1000).build()) {
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT);
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                for (Frame f = p.nextInteresting(); f.type() != 1; f = p.nextInteresting()) { }
                p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
                p.data(1, false, new byte[]{0, 0, 0, 0x10, 0});
                ExecutionException ex = assertThrows(ExecutionException.class, () -> call.get(10, TimeUnit.SECONDS));
                assertEquals(Status.Code.RESOURCE_EXHAUSTED, ((StatusException) ex.getCause()).status().code());
                Frame f;
                do {
                    f = p.nextInteresting();
                } while (f.type() != 3);
                assertEquals(8, f.errorCode(), "the server is told to stop (CANCEL)");
            }
        }
    }

    @Test
    void aLostConnectionIsUnavailable() throws Exception {
        StatusException e = callAgainst((p, hs) -> p.close(), CallOptions.DEFAULT);
        assertEquals(Status.Code.UNAVAILABLE, e.status().code());
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:1").build()) {
            ExecutionException ex = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.UNAVAILABLE, ((StatusException) ex.getCause()).status().code());
        }
    }

    @Test
    void theClientEnforcesItsOwnDeadlineAndResetsTheStream() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).build()) {
            long t0 = System.nanoTime();
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(200)));
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();   // never answers
                ExecutionException ex = assertThrows(ExecutionException.class, () -> call.get(10, TimeUnit.SECONDS));
                assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusException) ex.getCause()).status().code());
                assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000);
                Frame f;
                do {
                    f = p.nextInteresting();
                } while (f.type() != 3);
                assertEquals(8, f.errorCode());
            }
        }
    }

    @Test
    void cancellingTheFutureCancelsTheCall() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).build()) {
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT);
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                for (Frame f = p.nextInteresting(); f.type() != 1; f = p.nextInteresting()) { }
                call.cancel(true);
                Frame f;
                do {
                    f = p.nextInteresting();
                } while (f.type() != 3);
                assertEquals(8, f.errorCode());
            }
        }
    }

    @Test
    void aTruncatedMessageInAStreamIsNotHiddenByStatusOk() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).build()) {
            CompletableFuture<Void> done = new CompletableFuture<>();
            ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("x", 1), CallOptions.DEFAULT, new io.hydrangea.grpc.StreamObserver<Msg>() {
                public void onNext(Msg m) { }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                for (Frame f = p.nextInteresting(); f.type() != 1; f = p.nextInteresting()) { }
                p.headers(1, false, true, ":status", "200", "content-type", "application/grpc");
                p.data(1, false, framed(new Msg("whole")));
                p.data(1, false, new byte[]{0, 0, 0, 0, 9, 1});
                p.headers(1, true, true, "grpc-status", "0");
                ExecutionException ex = assertThrows(ExecutionException.class, () -> done.get(10, TimeUnit.SECONDS));
                assertEquals(Status.Code.INTERNAL, ((StatusException) ex.getCause()).status().code());
            }
        }
    }

    @Test
    void sendingAfterHalfCloseFails() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).build()) {
            var call = ch.clientStreaming(GrpcEchoExample.CLIENT_STREAM, CallOptions.DEFAULT, new io.hydrangea.grpc.StreamObserver<Msg>() {
                public void onNext(Msg m) { }
                public void onError(Throwable t) { }
                public void onCompleted() { }
            });
            call.halfClose();
            assertThrows(ExecutionException.class, () -> call.send(new Msg("late")).get(5, TimeUnit.SECONDS));
            call.cancel("done");
        }
    }

    @Test
    void badTargetsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("localhost:50051").build());
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("ftp://x").build());
        assertThrows(IllegalArgumentException.class, () -> CallOptions.DEFAULT.withCompression("snappy"));
    }
}
