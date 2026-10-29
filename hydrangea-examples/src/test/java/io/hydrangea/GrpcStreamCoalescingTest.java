package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Small writes on one HTTP/2 stream are sent as fewer, bigger DATA frames; every message must still arrive whole, in order, once. */
class GrpcStreamCoalescingTest {

    private EventLoopGroup group;
    private Server server;
    private GrpcChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        server = GrpcEchoExample.builder().build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        channel = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).build();
    }

    @AfterEach
    void tearDown() {
        channel.close();
        group.close();
    }

    private List<Msg> stream(int count, int blobSize) throws Exception {
        byte[] blob = new byte[blobSize];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) (i * 31 + 7);
        }
        List<Msg> got = new ArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        channel.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("m", count, blob), CallOptions.DEFAULT, new StreamObserver<>() {
            public void onNext(Msg m) { got.add(m); }
            public void onError(Throwable t) { done.completeExceptionally(t); }
            public void onCompleted() { done.complete(null); }
        });
        done.get(30, TimeUnit.SECONDS);
        assertEquals(count, got.size());
        for (int i = 0; i < count; i++) {
            assertEquals(i, got.get(i).n(), "order");
            assertEquals("m-" + i, got.get(i).text());
            assertTrue(Arrays.equals(blob, got.get(i).blob()), "payload of message " + i);
        }
        return got;
    }

    @Test
    void thousandsOfSmallMessagesArriveWholeAndInOrder() throws Exception {
        stream(5000, 100);
    }

    @Test
    void messagesAroundTheMergeThresholdArriveWholeAndInOrder() throws Exception {
        stream(1500, 4000);   // just under: merged
        stream(1500, 4100);   // just over: sent alone
    }

    @Test
    void messagesLargerThanAFrameStillArrive() throws Exception {
        stream(300, 20000);
    }
}
