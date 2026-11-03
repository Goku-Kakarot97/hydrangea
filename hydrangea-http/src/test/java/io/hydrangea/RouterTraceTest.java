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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every request through the router gets a trace id: in the route's log lines, in the response header, in the route request. */
class RouterTraceTest {

    private static final Logger log = Logger.get("trace.route");
    private MemoryAppender mem;

    @BeforeEach
    void setUp() {
        mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).console().apply();
    }

    @AfterEach
    void tearDown() {
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
                .get("/hello", r -> {
                    log.info("handling");
                    return HttpResponse.ok("id=" + r.traceId() + " ctx=" + LogContext.get(HttpRouter.TRACE_KEY));
                })
                .getAsync("/async", r -> {
                    log.info("sync part");
                    String id = r.traceId();
                    return CompletableFuture.supplyAsync(() -> {
                        try (LogContext.Scope ignored = LogContext.with(HttpRouter.TRACE_KEY, id)) {
                            log.info("async part");
                        }
                        return HttpResponse.ok("async");
                    });
                })
                .get("/mine", r -> HttpResponse.ok("x").header("X-Trace-Id", "chosen-by-route"))
                .get("/boom", r -> {
                    throw new IllegalStateException("kaboom");
                });
    }

    @Test
    void aFreshIdIsGeneratedLoggedAndReturned() throws Exception {
        HttpResponse r = call(router(), req("/hello"));
        String id = r.headers().get("X-Trace-Id");
        assertEquals(32, id.length(), id);
        assertTrue(id.matches("[0-9a-f]{32}"), id);
        assertEquals("id=" + id + " ctx=" + id, new String(r.body()));
        LogEvent e = mem.events().get(0);
        assertEquals("handling", e.message());
        assertEquals(id, e.context("trace_id"));
        assertTrue(mem.lines().get(0).contains(" trace_id=" + id), mem.lines().get(0));
        assertNull(LogContext.get(HttpRouter.TRACE_KEY), "the scope is closed after the route");

        String other = call(router(), req("/hello")).headers().get("X-Trace-Id");
        assertNotEquals(id, other);
    }

    @Test
    void clientIdsAreHonouredWhenUsable() throws Exception {
        assertEquals("0af7651916cd43dd8448eb211c80319c",
                call(router(), req("/hello", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")).headers().get("X-Trace-Id"));
        assertEquals("req-42.a_b", call(router(), req("/hello", "X-Request-Id", "req-42.a_b")).headers().get("X-Trace-Id"));
        assertEquals("abc", call(router(), req("/hello", "X-Trace-Id", "abc", "X-Request-Id", "ignored")).headers().get("X-Trace-Id"));
        assertEquals("ok", call(router(), req("/hello", "traceparent", "garbage", "X-Request-Id", "ok")).headers().get("X-Trace-Id"));
        for (String bad : List.of("", "has space", "a\nb", "x".repeat(65), "${jndi:ldap://x}", "é")) {
            String id = call(router(), req("/hello", "X-Request-Id", bad)).headers().get("X-Trace-Id");
            assertTrue(id.matches("[0-9a-f]{32}"), "replaced: " + bad + " -> " + id);
        }
        String zeros = call(router(), req("/hello", "traceparent", "00-00000000000000000000000000000000-b7ad6b7169203331-01")).headers().get("X-Trace-Id");
        assertTrue(zeros.matches("[0-9a-f]{32}"), zeros);
    }

    @Test
    void asyncRoutesCarryTheIdIntoTheirCallbackThroughTheRequest() throws Exception {
        HttpResponse r = call(router(), req("/async", "X-Request-Id", "async-1"));
        assertEquals("async-1", r.headers().get("X-Trace-Id"));
        assertEquals(List.of("sync part", "async part"), mem.events().stream().map(LogEvent::message).toList());
        for (LogEvent e : mem.events()) {
            assertEquals("async-1", e.context("trace_id"), e.message());
        }
    }

    @Test
    void errorsNotFoundAndRouteChosenHeadersKeepTheirIds() throws Exception {
        assertEquals("chosen-by-route", call(router(), req("/mine", "X-Request-Id", "r1")).headers().get("X-Trace-Id"));
        assertEquals("r2", call(router(), req("/missing", "X-Request-Id", "r2")).headers().get("X-Trace-Id"));
        assertEquals(404, call(router(), req("/missing")).status());
        assertThrows(IllegalStateException.class, () -> call(router(), req("/boom", "X-Request-Id", "r3")), "propagates: the server answers 500");
        assertNull(LogContext.get(HttpRouter.TRACE_KEY), "the scope is closed even when the route throws");
    }

    @Test
    void tracingCanBeTurnedOff() throws Exception {
        HttpResponse r = call(router().tracing(false), req("/hello", "X-Request-Id", "r1"));
        assertNull(r.headers().get("X-Trace-Id"));
        assertEquals("id=null ctx=null", new String(r.body()));
        assertEquals(0, mem.events().get(0).contextCount());
    }
}
