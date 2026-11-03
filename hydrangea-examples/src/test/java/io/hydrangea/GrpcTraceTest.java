package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.Metadata;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import io.hydrangea.log.LogContext;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A gRPC call is a CLIENT span on the channel and a SERVER span on the server, linked through traceparent metadata. */
class GrpcTraceTest {

    private final MemoryTraceSink spans = new MemoryTraceSink();
    private final List<String> seenOnServer = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
    }

    @Test
    void clientAndServerSpansFormOneTrace() throws Exception {
        Tracing.bindTo(spans);
        try (EventLoopGroup group = new EventLoopGroup(2)) {
            Server server = GrpcServer.builder()
                    .unary(UNARY, (req, ctx) -> {
                        seenOnServer.add(String.valueOf(ctx.requestMetadata().get("traceparent")) + "|" + LogContext.get(Tracing.TRACE_ID)
                                + "|" + (Tracing.current() == ctx.span()) + "|" + ctx.span().context().spanId());
                        return req.text().equals("FAIL")
                                ? CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.NOT_FOUND).withDescription("nothing")))
                                : CompletableFuture.completedFuture(new Msg("re:" + req.text()));
                    })
                    .build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
            try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).build()) {
                Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
                try (Span.Scope ignored = outer.makeCurrent()) {
                    assertEquals("re:hi", ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
                }
                outer.end();

                List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> s.context().traceId().equals(outer.context().traceId())).toList();
                assertEquals(3, mine.size(), mine.toString());
                MemoryTraceSink.Recorded client = mine.stream().filter(s -> s.kind() == Span.Kind.CLIENT).findFirst().orElseThrow();
                MemoryTraceSink.Recorded srv = mine.stream().filter(s -> s.kind() == Span.Kind.SERVER).findFirst().orElseThrow();
                assertEquals(UNARY.fullName(), client.name());
                assertEquals(outer.context().spanId(), client.parentSpanId());
                assertEquals(client.context().spanId(), srv.parentSpanId());
                assertEquals(UNARY.fullName(), srv.name());
                assertEquals("grpc", srv.attributes().get("rpc.system"));
                assertEquals(UNARY.serviceName(), srv.attributes().get("rpc.service"));
                assertEquals(UNARY.methodName(), srv.attributes().get("rpc.method"));
                assertEquals(0L, srv.attributes().get("rpc.grpc.status_code"));
                assertEquals(0L, client.attributes().get("rpc.grpc.status_code"));
                assertTrue(client.ended() && srv.ended());
                assertFalse(client.failed() || srv.failed());

                String[] seen = seenOnServer.get(0).split("\\|");
                assertEquals(client.context(), SpanContext.parse(seen[0]), "the metadata carried the client span: " + seen[0]);
                assertEquals(outer.context().traceId(), seen[1], "the handler's log context has the trace id");
                assertEquals("true", seen[2], "the server span is current while the handler runs");
                assertEquals(srv.context().spanId(), seen[3]);

                ExecutionException failed = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("FAIL"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
                assertEquals(Status.Code.NOT_FOUND, ((StatusException) failed.getCause()).status().code());
                List<MemoryTraceSink.Recorded> failedSpans = spans.spans().stream().filter(s -> s.failed()).toList();
                assertEquals(2, failedSpans.size(), "client and server spans of the failed call: " + failedSpans);
                for (MemoryTraceSink.Recorded s : failedSpans) {
                    assertEquals(5L, s.attributes().get("rpc.grpc.status_code"));
                    assertTrue(s.errorDescription().startsWith("NOT_FOUND"), s.errorDescription());
                }

                Metadata given = new Metadata().put("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
                ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMetadata(given)).get(10, TimeUnit.SECONDS);
                assertTrue(seenOnServer.get(2).startsWith("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01|0af7651916cd43dd8448eb211c80319c|"), "a traceparent set by the caller is kept: " + seenOnServer.get(2));
            }
        }
    }

    @Test
    void withoutASinkIdsStillFlow() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Server server = GrpcServer.builder()
                    .unary(UNARY, (req, ctx) -> {
                        assertNotNull(ctx.span());
                        assertSame(ctx.span(), Tracing.current());
                        seenOnServer.add(ctx.requestMetadata().get("traceparent"));
                        return CompletableFuture.completedFuture(new Msg(ctx.span().context().traceId()));
                    })
                    .build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
            try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).build()) {
                String traceId = ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text();
                SpanContext onTheWire = SpanContext.parse(seenOnServer.get(0));
                assertEquals(traceId, onTheWire.traceId());
                assertFalse(onTheWire.sampled());
                assertEquals(0, spans.spans().size());
            }
        }
    }
}
