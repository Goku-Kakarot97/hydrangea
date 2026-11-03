package io.hydrangea;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A client call inside a server span becomes its child, and the service called joins the same trace through traceparent. */
class ClientTraceTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Server server;
    private HttpClient client;
    private MemoryTraceSink spans;
    private final List<String> received = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        serverGroup = new EventLoopGroup(1);
        clientGroup = new EventLoopGroup(1);
        HttpRouter router = new HttpRouter()
                .get("/echo", r -> {
                    received.add(String.valueOf(r.header("traceparent")));
                    return HttpResponse.ok("ok");
                })
                .get("/down", r -> HttpResponse.status(502).text("bad gateway"));
        server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null, new HttpConfig(16 * 1024, 1024 * 1024), router);
        client = new HttpClient(clientGroup, HttpClientConfig.defaults());
    }

    @AfterEach
    void tearDown() {
        client.close();
        clientGroup.close();
        serverGroup.close();
        Tracing.bindTo(TraceSink.NOOP);
    }

    private String url(String path) {
        return "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + path;
    }

    @Test
    void theCallIsAChildSpanAndTheServerJoinsTheTrace() throws Exception {
        Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
        ClientResponse resp;
        try (Span.Scope ignored = outer.makeCurrent()) {
            resp = client.send(ClientRequest.get(url("/echo"))).get(5, TimeUnit.SECONDS);
        }
        outer.end();
        assertEquals(200, resp.status());
        assertEquals(outer.context().traceId(), resp.header("X-Trace-Id"), "the server answered inside our trace");

        List<MemoryTraceSink.Recorded> all = spans.spans().stream().filter(s -> s.context().traceId().equals(outer.context().traceId())).toList();
        assertEquals(3, all.size(), "outer, client, server: " + all);
        MemoryTraceSink.Recorded clientSpan = all.stream().filter(s -> s.kind() == Span.Kind.CLIENT).findFirst().orElseThrow();
        MemoryTraceSink.Recorded serverSpan = all.stream().filter(s -> s.kind() == Span.Kind.SERVER).findFirst().orElseThrow();
        assertEquals("HTTP GET", clientSpan.name());
        assertEquals(outer.context().spanId(), clientSpan.parentSpanId());
        assertEquals(outer.context().traceId(), clientSpan.context().traceId());
        assertEquals("127.0.0.1", clientSpan.attributes().get("server.address"));
        assertEquals("/echo", clientSpan.attributes().get("url.path"));
        assertEquals(200L, clientSpan.attributes().get("http.response.status_code"));
        assertTrue(clientSpan.ended());
        assertFalse(clientSpan.failed());

        assertEquals(1, received.size());
        SpanContext onTheWire = SpanContext.parse(received.get(0));
        assertEquals(clientSpan.context(), onTheWire, "the header carried the client span: " + received.get(0));
        assertEquals("GET /echo", serverSpan.name());
        assertEquals(clientSpan.context().spanId(), serverSpan.parentSpanId());
        assertEquals(outer.context().traceId(), serverSpan.context().traceId());
    }

    @Test
    void withoutACurrentSpanTheCallStartsATraceAndStillPropagates() throws Exception {
        client.send(ClientRequest.get(url("/echo"))).get(5, TimeUnit.SECONDS);
        assertNull(Tracing.current());
        SpanContext onTheWire = SpanContext.parse(received.get(0));
        MemoryTraceSink.Recorded clientSpan = spans.spans().stream().filter(x -> x.kind() == Span.Kind.CLIENT).findFirst().orElseThrow();
        assertEquals(clientSpan.context(), onTheWire);
        assertNull(clientSpan.parentSpanId());
    }

    @Test
    void aTraceparentSetByTheCallerIsKept() throws Exception {
        client.send(ClientRequest.get(url("/echo")).header("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")).get(5, TimeUnit.SECONDS);
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", received.get(0));
        assertTrue(spans.spans().stream().noneMatch(s -> s.kind() == Span.Kind.CLIENT), "the caller manages that span: none of ours " + spans.spans());
        assertEquals("b7ad6b7169203331", spans.spans().stream().filter(s -> s.kind() == Span.Kind.SERVER).findFirst().orElseThrow().parentSpanId());
    }

    @Test
    void serverErrorsAndFailuresMarkTheSpan() throws Exception {
        assertEquals(502, client.send(ClientRequest.get(url("/down"))).get(5, TimeUnit.SECONDS).status());
        MemoryTraceSink.Recorded bad = spans.spans().stream().filter(x -> x.kind() == Span.Kind.CLIENT && "/down".equals(x.attributes().get("url.path"))).findFirst().orElseThrow();
        assertTrue(bad.failed());
        assertEquals("HTTP 502", bad.errorDescription());
        assertEquals(502L, bad.attributes().get("http.response.status_code"));

        int closedPort;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            closedPort = s.getLocalPort();
        }
        assertThrows(ExecutionException.class, () -> client.send(ClientRequest.get("http://127.0.0.1:" + closedPort + "/x")).get(5, TimeUnit.SECONDS));
        List<MemoryTraceSink.Recorded> all = spans.spans();
        MemoryTraceSink.Recorded refused = all.stream().filter(s -> s.kind() == Span.Kind.CLIENT).reduce((a, b) -> b).orElseThrow();
        assertTrue(refused.failed());
        assertTrue(refused.exception() instanceof java.io.IOException, String.valueOf(refused.exception()));
        assertTrue(refused.ended());
    }
}
