package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A call given up before its HTTP/2 stream has started must not take the shared connection down with it. This covers the case where the
 * exchange has not even reached a connection yet; it does not reproduce the narrower race fixed in {@code Exchange.abortConnection} (connection
 * chosen, stream not started). That one only shows when a thread is descheduled inside a window of a few microseconds; it was found by
 * running the gRPC and HTTP/2 tests repeatedly with the machine's cores busy (GrpcServiceConfigTest failed in about half of such runs, none of 12 after the fix).
 */
class GrpcCancelBeforeStartTest {

    private final java.util.List<java.net.SocketAddress> peers = new java.util.concurrent.CopyOnWriteArrayList<>();

    @Test
    void aCallThatFailsBeforeItsStreamStartsLeavesTheConnectionAlone() throws Exception {
        EventLoopGroup serverGroup = new EventLoopGroup(1);
        EventLoopGroup clientGroup = new EventLoopGroup(1);
        try (Server server = GrpcServer.builder().unary(GrpcEchoExample.UNARY, (req, ctx) -> {
                 peers.add(ctx.remoteAddress());
                 return CompletableFuture.completedFuture(new Msg("echo:" + req.text()));
             }).build().bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort())
                     .eventLoopGroup(clientGroup).maxConnections(1).build()) {
            assertEquals("echo:warm", ch.unary(GrpcEchoExample.UNARY, new Msg("warm"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());

            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch blocked = new CountDownLatch(1);
            clientGroup.next().execute(() -> {   // the only loop: nothing the client queues runs until we say so
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                }
            });
            assertTrue(blocked.await(5, TimeUnit.SECONDS));

            // too big for the limit: refused by the client after the call was handed to the loop but before its stream started
            CompletableFuture<Msg> tooBig = ch.unary(GrpcEchoExample.UNARY, new Msg("x", 0, new byte[500]), CallOptions.DEFAULT.withMaxRequestBytes(100));
            ExecutionException e = assertThrows(ExecutionException.class, () -> tooBig.get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, ((StatusException) e.getCause()).status().code());
            release.countDown();

            // the connection is still good: the next call neither fails nor needs a new one
            assertEquals("echo:after", ch.unary(GrpcEchoExample.UNARY, new Msg("after"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            assertEquals(2, peers.size());
            assertEquals(peers.get(0), peers.get(1), "both calls arrived on the same connection: giving up one call did not close it");
        } finally {
            clientGroup.close();
            serverGroup.close();
        }
    }
}
