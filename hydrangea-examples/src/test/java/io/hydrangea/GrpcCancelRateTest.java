package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.GrpcServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** A gRPC client that cancels a lot is normal; the default flood limit of a gRPC server must not treat it as a rapid-reset attack. */
class GrpcCancelRateTest {

    @Test
    void aClientCancellingHundredsOfCallsPerSecondKeepsItsConnection() throws Exception {
        EventLoopGroup group = new EventLoopGroup(2);
        GrpcServer grpc = GrpcEchoExample.builder().build();
        Server server = grpc.bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort())
                .eventLoopGroup(group).maxConnections(1).build()) {
            for (int round = 0; round < 14; round++) {   // 700 resets in about a second, well over the 200 per second of a plain HTTP/2 server
                java.util.List<CompletableFuture<Msg>> batch = new java.util.ArrayList<>();
                for (int i = 0; i < 50; i++) {
                    batch.add(ch.unary(GrpcEchoExample.UNARY, new Msg("SLOW:3000"), CallOptions.DEFAULT));
                }
                Thread.sleep(20);   // let the streams open, so that cancelling resets them
                batch.forEach(f -> f.cancel(true));
            }
            Msg m = ch.unary(GrpcEchoExample.UNARY, new Msg("after"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            assertEquals("echo:after", m.text());
            assertEquals(2, group.metrics().snapshot().connectionsOpened(), "one client and one server side, no reconnect");
        } finally {
            server.shutdownGracefully(java.time.Duration.ofSeconds(2)).join();
            group.close();
        }
    }
}
