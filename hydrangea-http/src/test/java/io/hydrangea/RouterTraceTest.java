package io.hydrangea;

import io.hydrangea.http.Headers;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.log.Appender;
import io.hydrangea.log.Level;
import io.hydrangea.log.Log;
import io.hydrangea.log.LogContext;
import io.hydrangea.log.LogEvent;
import io.hydrangea.log.Logger;
import io.hydrangea.log.MemoryAppender;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every request through the router is a server span: ids in the route's log lines, in the response header, in the route request. */
class RouterTraceTest {

    private static final Logger log = Logger.get("trace.route");
    private MemoryAppender mem;
    private MemoryTraceSink spans;

    @BeforeEach
    void setUp() {
        mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).apply();
        spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
    }

    @AfterEach
    void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
        Log.reset();
    }

    private static HttpRequest req(String target, String... headers) {
        Headers h = new Headers().add("Host", "x");
        for (int i = 0; i < headers.length; i += 2) {
            h.add(headers[i], headers[i + 1]);
        }
        return new HttpRequest("GET", target, "HTTP/1.1", h, new byte[0]);
    }

    private static HttpResponse call(HttpRouter router, HttpRequest request) throws Exception {
        return router.handle(request).toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static HttpRouter router() {
        return new HttpRouter()
                .get("/hello/{name}", r -> {
                    log.info("handling");
                    return HttpResponse.ok("id=" + r.traceId() + " ctx=" + LogContext.get(HttpRouter.TRACE_KEY) + " current=" + (Tracing.current() == r.span()));
                })
                .getAsync("/async", r -> {
                    log.info("sync part");
                    return CompletableFuture.supplyAsync(() -> {
                        try (Span.Scope ignored = r.span().makeCurrent(); LogContext.Scope ctx = LogContext.with(HttpRouter.TRACE_KEY, r.traceId())) {
                            log.info("async part");
                        }
                        return HttpResponse.ok("async");
                    });
                })
                .get("/mine", r -> HttpResponse.ok("x").header("X-Trace-Id", "chosen-by-route"))
                .get("/fail", r -> HttpResponse.status(503).text("down"))
                .get("/boom", r -> {
                    throw new IllegalStateException("kaboom");
                });
    }

    @Test
    void aFreshTraceIsStartedLoggedAndReturned() throws Exception {
        HttpResponse r = call(router(), req("/hello/ann"));
        String id = r.headers().get("X-Trace-Id");
        assertTrue(id.matches("[0-9a-f]{32}"), id);
        assertEquals("id=" + id + " ctx=" + id + " current=true", new String(r.body()));
        LogEvent e = mem.events().get(0);
        assertEquals("handling", e.message());
        assertEquals(id, e.context("trace_id"));
        assertTrue(e.context("span_id").matches("[0-9a-f]{16}"), e.context("span_id"));
        assertNull(e.context("request_id"));
        assertTrue(mem.lines().get(0).contains(" trace_id=" + id), mem.lines().get(0));
        assertNull(LogContext.get(HttpRouter.TRACE_KEY), "the scope is closed after the route");
        assertNull(Tracing.current());

        MemoryTraceSink.Recorded span = spans.spans().get(0);
        assertEquals("GET /hello/{name}", span.name());
        assertEquals(Span.Kind.SERVER, span.kind());
        assertNull(span.parentSpanId());
        assertEquals(id, span.context().traceId());
        assertEquals(e.context("span_id"), span.context().spanId());
        assertEquals("GET", span.attributes().get("http.request.method"));
        assertEquals("/hello/ann", span.attributes().get("url.path"));
        assertEquals("/hello/{name}", span.attributes().get("http.route"));
        assertEquals(200L, span.attributes().get("http.response.status_code"));
        assertTrue(span.ended());
        assertFalse(span.failed());

        assertNotEquals(id, call(router(), req("/hello/bob")).headers().get("X-Trace-Id"));
    }

    @Test
    void aTraceparentMakesTheRequestAChildOfTheCallersSpan() throws Exception {
        HttpResponse r = call(router(), req("/hello/ann", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        assertEquals("0af7651916cd43dd8448eb211c80319c", r.headers().get("X-Trace-Id"));
        MemoryTraceSink.Recorded span = spans.spans().get(0);
        assertEquals("0af7651916cd43dd8448eb211c80319c", span.context().traceId());
        assertEquals("b7ad6b7169203331", span.parentSpanId());
        assertEquals("0af7651916cd43dd8448eb211c80319c", mem.events().get(0).context("trace_id"));

        call(router(), req("/hello/ann", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"));
        assertEquals(1, spans.spans().size(), "an unsampled parent: ids flow, nothing is recorded");
        assertEquals("0af7651916cd43dd8448eb211c80319c", mem.events().get(1).context("trace_id"));

        String id = call(router(), req("/hello/ann", "traceparent", "garbage")).headers().get("X-Trace-Id");
        assertTrue(id.matches("[0-9a-f]{32}"), "an unusable header starts a new trace: " + id);
    }

    @Test
    void aRequestIdIsLoggedNotUsedAsTheTraceId() throws Exception {
        HttpResponse r = call(router(), req("/hello/ann", "X-Request-Id", "req-42.a_b"));
        assertTrue(r.headers().get("X-Trace-Id").matches("[0-9a-f]{32}"));
        assertEquals("req-42.a_b", mem.events().get(0).context("request_id"));
        for (String bad : List.of("", "has space", "a\nb", "x".repeat(65), "${jndi:ldap://x}", "é")) {
            mem.clear();
            call(router(), req("/hello/ann", "X-Request-Id", bad));
            assertNull(mem.events().get(0).context("request_id"), "dropped: " + bad);
        }
    }

    @Test
    void asyncRoutesReopenTheSpanInTheirCallback() throws Exception {
        HttpResponse r = call(router(), req("/async", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        assertEquals("0af7651916cd43dd8448eb211c80319c", r.headers().get("X-Trace-Id"));
        assertEquals(List.of("sync part", "async part"), mem.events().stream().map(LogEvent::message).toList());
        for (LogEvent e : mem.events()) {
            assertEquals("0af7651916cd43dd8448eb211c80319c", e.context("trace_id"), e.message());
        }
        assertTrue(spans.spans().get(0).ended(), "the span ends when the async response is ready");
    }

    @Test
    void failuresAreMarkedAndSpansAlwaysEnd() throws Exception {
        assertEquals("chosen-by-route", call(router(), req("/mine")).headers().get("X-Trace-Id"));
        assertEquals(404, call(router(), req("/missing")).status());
        assertEquals(503, call(router(), req("/fail")).status());
        assertThrows(IllegalStateException.class, () -> call(router(), req("/boom")), "propagates: the server answers 500");
        assertNull(LogContext.get(HttpRouter.TRACE_KEY), "the scope is closed even when the route throws");
        assertNull(Tracing.current());
        List<MemoryTraceSink.Recorded> all = spans.spans();
        assertEquals(4, all.size());
        assertEquals("GET /mine", all.get(0).name());
        assertFalse(all.get(0).failed());
        assertEquals("GET", all.get(1).name(), "no route matched: the method alone");
        assertEquals(404L, all.get(1).attributes().get("http.response.status_code"));
        assertFalse(all.get(1).failed());
        assertTrue(all.get(2).failed());
        assertEquals("HTTP 503", all.get(2).errorDescription());
        assertTrue(all.get(3).failed());
        assertEquals("kaboom", all.get(3).exception().getMessage());
        for (MemoryTraceSink.Recorded s : all) {
            assertTrue(s.ended(), s.toString());
        }
    }

    @Test
    void tracingCanBeTurnedOff() throws Exception {
        HttpResponse r = call(router().tracing(false), req("/hello/ann", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        assertNull(r.headers().get("X-Trace-Id"));
        assertEquals("id=null ctx=null current=true", new String(r.body()));
        assertEquals(0, mem.events().get(0).contextCount());
        assertEquals(0, spans.spans().size());
    }
}
