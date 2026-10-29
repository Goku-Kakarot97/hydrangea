package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A call the server provably did not process (GOAWAY below its stream, REFUSED_STREAM) is sent again by the channel by itself, even with no
 * retry policy, because nothing has happened to it; anything else that goes wrong is not repeated. Scripted server speaking raw frames.
 */
class GrpcTransparentRetryTest {

    private static final int HEADERS = 1, SETTINGS = 4, GOAWAY = 7;
    private static final int ACK = 1;

    private ServerSocket listener;
    private EventLoopGroup group;
    private GrpcChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        group = new EventLoopGroup(2);
        channel = GrpcChannel.forTarget("http://127.0.0.1:" + listener.getLocalPort()).eventLoopGroup(group).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.close();
        group.close();
        listener.close();
    }

    private Peer accept(int timeoutMs) throws Exception {
        listener.setSoTimeout(timeoutMs);
        Peer p = new Peer(listener.accept(), false, true);
        p.settings();
        for (;;) {
            Frame f = p.next();
            if (f.type() == SETTINGS && (f.flags() & ACK) == 0) {
                p.send(SETTINGS, ACK, 0, new byte[0]);
                break;
            }
        }
        return p;
    }

    private int awaitRequest(Peer p) throws Exception {
        Frame f;
        do {
            f = p.nextInteresting();   // the DATA of an earlier request may still arrive
        } while (f.type() == 0);
        assertEquals(HEADERS, f.type(), "expected a request");
        return f.sid();
    }

    private void answer(Peer p, int sid, Msg m) throws Exception {
        byte[] body = GrpcEchoExample.MSG.encode(m);
        ByteBuffer framed = ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(body.length).put(body);
        p.headers(sid, false, true, ":status", "200", "content-type", "application/grpc");
        p.data(sid, false, framed.array());
        p.headers(sid, true, true, "grpc-status", "0");
    }

    private CompletableFuture<Msg> call() {
        return channel.unary(GrpcEchoExample.UNARY, new Msg("hi"), CallOptions.DEFAULT.withDeadline(java.time.Duration.ofSeconds(20)));
    }

    @Test
    void goawayBeforeTheServerLookedAtTheCallSendsItAgainOnAnotherConnection() throws Exception {
        CompletableFuture<Msg> f = call();
        Peer first = accept(10_000);
        awaitRequest(first);
        first.send(GOAWAY, 0, 0, ByteBuffer.allocate(8).putInt(0).putInt(0).array());   // last processed stream: none
        Peer second = accept(10_000);
        answer(second, awaitRequest(second), new Msg("echo:hi"));
        assertEquals("echo:hi", f.get(10, TimeUnit.SECONDS).text());
    }

    @Test
    void aRefusedStreamIsSentAgain() throws Exception {
        CompletableFuture<Msg> f = call();
        Peer first = accept(10_000);
        first.rst(awaitRequest(first), 7);   // REFUSED_STREAM: the connection stays, the call goes again on a new stream
        answer(first, awaitRequest(first), new Msg("echo:hi"));
        assertEquals("echo:hi", f.get(10, TimeUnit.SECONDS).text());
    }

    @Test
    void theReplayIsLimitedSoARefusingServerStillGetsAnAnswer() throws Exception {
        CompletableFuture<Msg> f = call();
        Peer p = accept(10_000);
        for (int i = 0; i < 3; i++) {   // the first try and two replays; a fourth request would be left unanswered and the get below would time out
            p.rst(awaitRequest(p), 7);
        }
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertEquals(Status.Code.UNAVAILABLE, ((StatusException) e.getCause()).status().code());
    }

    @Test
    void aCallTheServerMayHaveProcessedIsNotSentAgain() throws Exception {
        CompletableFuture<Msg> f = call();
        Peer first = accept(10_000);
        first.rst(awaitRequest(first), 2);   // INTERNAL_ERROR: the server may have run the method
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertEquals(Status.Code.UNAVAILABLE, ((StatusException) e.getCause()).status().code());   // fails at once: no second request is waited for
    }
}
