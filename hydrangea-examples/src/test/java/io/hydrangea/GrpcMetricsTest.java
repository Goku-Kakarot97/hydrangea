package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** The gRPC interceptors count calls by status code, messages in each direction, calls under way, and time them: on the server and on the channel. */
class GrpcMetricsTest {

    private static final String METHOD = UNARY.fullName();

    @Test
    void serverAndClientCountCallsByCodeMessagesAndTime() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(2)) {
            CaptureSink sink = new CaptureSink();
            group.metrics().bindTo(sink);
            Server server = GrpcServer.builder()
                    .intercept(GrpcMetrics.server(group.metrics()))
                    .unary(UNARY, (req, ctx) -> req.text().equals("FAIL")
                            ? CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.NOT_FOUND).withDescription("nothing")))
                            : CompletableFuture.completedFuture(new Msg("re:" + req.text())))
                    .build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
            try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort())
                    .intercept(GrpcMetrics.client(group.metrics())).build()) {
                for (int i = 0; i < 3; i++) {
                    assertEquals("re:" + i, ch.unary(UNARY, new Msg("" + i), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
                }
                ExecutionException failed = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("FAIL"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
                assertEquals(Status.Code.NOT_FOUND, ((StatusException) failed.getCause()).status().code());

                for (String side : new String[] {"server", "client"}) {
                    String m = "{method," + METHOD;
                    CaptureSink.eventually(() -> sink.get("grpc." + side + ".active" + m + "}") == 0, side + " calls to end");
                    assertEquals(3, sink.get("grpc." + side + ".calls" + m + ",code,OK}"), side);
                    assertEquals(1, sink.get("grpc." + side + ".calls" + m + ",code,NOT_FOUND}"), side);
                    assertEquals(4, sink.get("grpc." + side + ".duration.count" + m + "}"), side);
                    assertTrue(sink.get("grpc." + side + ".duration.sum" + m + "}") > 0, side);
                }
                assertEquals(4, sink.get("grpc.server.messages{method," + METHOD + ",direction,received}"));
                assertEquals(3, sink.get("grpc.server.messages{method," + METHOD + ",direction,sent}"), "the failed call answered with a status only");
                assertEquals(4, sink.get("grpc.client.messages{method," + METHOD + ",direction,sent}"));
                assertEquals(3, sink.get("grpc.client.messages{method," + METHOD + ",direction,received}"));
            }
        }
    }
}
