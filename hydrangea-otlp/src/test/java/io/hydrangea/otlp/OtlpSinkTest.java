package io.hydrangea.otlp;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.proto.ProtoWriter;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The sink against a collector of our own: a gRPC {@code TraceService} and an HTTP {@code /v1/traces} that keep what they receive. */
class OtlpSinkTest {

    private final List<byte[]> received = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
        Tracing.sampleRatio(1.0);
    }

    @Test
    void spansReachAGrpcCollectorWithIdsParentsAttributesStatusAndResource() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Server collector = GrpcServer.builder()
                    .unary(Exporter.EXPORT, (req, ctx) -> {
                        received.add(req);
                        return CompletableFuture.completedFuture(new byte[0]);
                    })
                    .build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
            try (OtlpSink sink = OtlpSink.builder()
                    .endpoint("http://127.0.0.1:" + ((InetSocketAddress) collector.localAddress()).getPort())
                    .serviceName("orders").resource("deployment.environment", "test").eventLoopGroup(group)
                    .scheduleDelay(Duration.ofMillis(50)).shutdownHook(false).build()) {
                Tracing.bindTo(sink);

                SpanContext remote = SpanContext.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
                Span server = Tracing.start("GET", Span.Kind.SERVER, remote);
                assertTrue(server.isRecording());
                assertEquals("0af7651916cd43dd8448eb211c80319c", server.context().traceId());
                try (Span.Scope ignored = server.makeCurrent()) {
                    try (Span client = Tracing.start("HTTP GET", Span.Kind.CLIENT)) {
                        client.attribute("http.response.status_code", 503).attribute("retried", true).error("HTTP 503");
                    }
                    try (Span failed = Tracing.start("pg.query", Span.Kind.CLIENT)) {
                        failed.attribute("db.query.text", "select 1").error(new IllegalStateException("boom"));
                    }
                }
                server.name("GET /users/{id}").attribute("http.route", "/users/{id}");
                server.end();
                server.attribute("late", "ignored");

                sink.forceFlush().get(5, TimeUnit.SECONDS);
                assertEquals(3, sink.exported());
                assertEquals(0, sink.dropped());

                OtlpDecoder.Decoded d = OtlpDecoder.decode(received.get(0));
                assertEquals("orders", d.resource().get("service.name"));
                assertEquals("test", d.resource().get("deployment.environment"));
                assertEquals("hydrangea-otlp", d.resource().get("telemetry.sdk.name"));
                assertEquals(OtlpEncoder.SCOPE_NAME, d.scopeName());
                assertEquals(3, d.spans().size());

                OtlpDecoder.DecodedSpan client = d.spans().get(0);
                OtlpDecoder.DecodedSpan failed = d.spans().get(1);
                OtlpDecoder.DecodedSpan srv = d.spans().get(2);

                assertEquals("GET /users/{id}", srv.name());
                assertEquals(2, srv.kind(), "SERVER");
                assertEquals("0af7651916cd43dd8448eb211c80319c", srv.traceId());
                assertEquals("b7ad6b7169203331", srv.parentSpanId(), "the remote parent from traceparent");
                assertEquals(server.context().spanId(), srv.spanId());
                assertEquals("/users/{id}", srv.attributes().get("http.route"));
                assertNull(srv.attributes().get("late"), "nothing recorded after end");
                assertEquals(0, srv.statusCode(), "UNSET");
                assertEquals(1, srv.flags(), "sampled");
                assertTrue(srv.start() > 1_600_000_000_000_000_000L && srv.end() >= srv.start(), "epoch nanos: " + srv.start() + ".." + srv.end());

                assertEquals("HTTP GET", client.name());
                assertEquals(3, client.kind(), "CLIENT");
                assertEquals(srv.spanId(), client.parentSpanId());
                assertEquals(srv.traceId(), client.traceId());
                assertEquals(503L, client.attributes().get("http.response.status_code"));
                assertEquals(true, client.attributes().get("retried"));
                assertEquals(2, client.statusCode(), "ERROR");
                assertEquals("HTTP 503", client.statusMessage());
                assertTrue(client.events().isEmpty());

                assertEquals("pg.query", failed.name());
                assertEquals(2, failed.statusCode());
                assertEquals("java.lang.IllegalStateException: boom", failed.statusMessage());
                assertEquals(1, failed.events().size());
                Map<String, Object> exception = failed.events().get(0);
                assertEquals("exception", exception.get("name"));
                assertEquals("java.lang.IllegalStateException", exception.get("exception.type"));
                assertEquals("boom", exception.get("exception.message"));
                assertTrue(((String) exception.get("exception.stacktrace")).contains("OtlpSinkTest"), "a stack trace");
                assertEquals(failed.end(), exception.get("time"));
            }
            collector.close();
        }
    }

    @Test
    void spansReachAnHttpCollector() throws Exception {
        List<String> contentTypes = new CopyOnWriteArrayList<>();
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Server collector = HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), new HttpRouter()
                    .post("/v1/traces", r -> {
                        received.add(r.body());
                        contentTypes.add(r.header("Content-Type"));
                        return HttpResponse.ok("").header("Content-Type", "application/x-protobuf");
                    }));
            int port = ((InetSocketAddress) collector.localAddress()).getPort();
            try (OtlpSink sink = OtlpSink.fromEnvironment(Map.of(
                    "OTEL_SERVICE_NAME", "http-service",
                    "OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf",
                    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:" + port,
                    "OTEL_EXPORTER_OTLP_HEADERS", "x-api-key=secret, x-tenant=t1",
                    "OTEL_BSP_SCHEDULE_DELAY", "50")).eventLoopGroup(group).shutdownHook(false).build()) {
                Tracing.bindTo(sink);
                Tracing.start("job", Span.Kind.INTERNAL).end();
                sink.forceFlush().get(5, TimeUnit.SECONDS);
                assertEquals(1, sink.exported());
                OtlpDecoder.Decoded d = OtlpDecoder.decode(received.get(0));
                assertEquals("http-service", d.resource().get("service.name"));
                assertEquals("job", d.spans().get(0).name());
                assertEquals(1, d.spans().get(0).kind(), "INTERNAL");
                assertEquals("application/x-protobuf", contentTypes.get(0));
            }
            collector.close();
        }
    }

    @Test
    void batchesAreBoundedAndTheQueueDropsWhenFull() throws Exception {
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        AtomicInteger total = new AtomicInteger();
        Exporter exporter = request -> {
            int n = OtlpDecoder.decode(request).spans().size();
            sizes.add(n);
            total.addAndGet(n);
            return CompletableFuture.completedFuture(null);
        };
        try (OtlpSink sink = OtlpSink.builder().exporter(exporter).maxBatch(10).maxQueue(25).scheduleDelay(Duration.ofSeconds(60)).shutdownHook(false).build()) {
            Tracing.bindTo(sink);
            for (int i = 0; i < 1000; i++) {
                Tracing.start("s" + i, Span.Kind.INTERNAL).end();
            }
            sink.forceFlush().get(5, TimeUnit.SECONDS);
            assertTrue(sink.dropped() > 0, "a queue of 25 cannot hold 1000 spans");
            assertEquals(1000, sink.exported() + sink.dropped());
            assertEquals(total.get(), sink.exported());
            assertTrue(sizes.stream().allMatch(n -> n <= 10), sizes.toString());
        }
    }

    @Test
    void aFailingCollectorLosesTheBatchButNotTheService() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Exporter exporter = request -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("UNAVAILABLE"));
        };
        try (OtlpSink sink = OtlpSink.builder().exporter(exporter).scheduleDelay(Duration.ofMillis(20)).shutdownHook(false).build()) {
            Tracing.bindTo(sink);
            Tracing.start("a", Span.Kind.INTERNAL).end();
            sink.forceFlush().get(5, TimeUnit.SECONDS);
            Tracing.start("b", Span.Kind.INTERNAL).end();
            sink.forceFlush().get(5, TimeUnit.SECONDS);
            assertEquals(2, calls.get());
            assertEquals(2, sink.failedBatches());
            assertEquals(0, sink.exported());
        }
    }

    @Test
    void closeFlushesWhatIsQueued() throws Exception {
        AtomicInteger total = new AtomicInteger();
        Exporter exporter = request -> {
            total.addAndGet(OtlpDecoder.decode(request).spans().size());
            return CompletableFuture.completedFuture(null);
        };
        OtlpSink sink = OtlpSink.builder().exporter(exporter).scheduleDelay(Duration.ofSeconds(60)).shutdownHook(false).build();
        Tracing.bindTo(sink);
        for (int i = 0; i < 7; i++) {
            Tracing.start("s", Span.Kind.INTERNAL).end();
        }
        sink.close();
        assertEquals(7, total.get());
        Tracing.start("after", Span.Kind.INTERNAL).end();
        assertEquals(1, sink.dropped(), "after close spans are dropped, never queued");
        sink.close();   // idempotent
    }

    @Test
    void environmentIsReadTheOpenTelemetryWay() {
        OtlpSink.Builder b = OtlpSink.fromEnvironment(Map.of(
                "OTEL_SERVICE_NAME", "svc",
                "OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod,service.version=2.1, k8s.pod.name = web-1",
                "OTEL_TRACES_SAMPLER", "parentbased_traceidratio",
                "OTEL_TRACES_SAMPLER_ARG", "0.25",
                "OTEL_TRACES_EXPORTER", "none"));
        Map<String, String> r = b.resourceAttributes();
        assertEquals("svc", r.get("service.name"));
        assertEquals("prod", r.get("deployment.environment"));
        assertEquals("2.1", r.get("service.version"));
        assertEquals("web-1", r.get("k8s.pod.name"));
        try (OtlpSink sink = b.shutdownHook(false).build()) {
            assertEquals(0.25, Tracing.sampleRatio());
            assertEquals("svc", sink.resourceAttributes().get("service.name"));
        }

        assertEquals("unknown_service", OtlpSink.fromEnvironment(Map.of()).resourceAttributes().get("service.name"));
        assertEquals(Map.of("a", "1", "b", "x=y"), OtlpSink.Builder.pairs("a=1,b=x=y,broken"));
        assertThrows(IllegalArgumentException.class, () -> OtlpSink.fromEnvironment(Map.of("OTEL_TRACES_SAMPLER", "magic")));
        assertThrows(IllegalArgumentException.class, () -> OtlpSink.fromEnvironment(Map.of("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")));
    }

    @Test
    void partialSuccessIsRead() {
        byte[] none = new byte[0];
        assertNull(Exporter.partialSuccess(none));
        byte[] partial = new ProtoWriter().message(1, new ProtoWriter().varint(1, 3).string(2, "too old").toByteArray()).toByteArray();
        assertEquals("3 span(s) rejected: too old", Exporter.partialSuccess(partial));
    }

    @Test
    void unsampledAndNoopStayPropagating() {
        try (OtlpSink sink = OtlpSink.builder().exporter(Exporter.none()).shutdownHook(false).build()) {
            Tracing.bindTo(sink);
            Tracing.sampleRatio(0);
            Span s = Tracing.start("x", Span.Kind.INTERNAL);
            assertNotNull(s.context());
            assertTrue(!s.isRecording());
            s.end();
            assertEquals(0, sink.exported() + sink.dropped());
        }
    }
}
