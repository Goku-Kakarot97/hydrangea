package io.hydrangea.elasticsearch;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every request is one CLIENT span (whatever nodes it tries), child of the caller's span; the HTTP client underneath adds none. */
class EsTraceTest {

    private static EventLoopGroup group;
    private static EsClient es;

    @BeforeAll
    static void setUp() throws Exception {
        Es.require();
        group = new EventLoopGroup(1);
        es = EsClient.connect(group, Es.config()).get(10, TimeUnit.SECONDS);
    }

    @AfterAll
    static void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
        if (es != null) {
            es.close();
        }
        if (group != null) {
            group.close();
        }
    }

    @AfterEach
    void unbind() {
        Tracing.bindTo(TraceSink.NOOP);
    }

    @Test
    void requestsAreChildSpansWithoutAnExtraHttpSpan() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        String missing = "no-such-index-" + Long.toHexString(System.nanoTime());
        Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
        try (Span.Scope ignored = outer.makeCurrent()) {
            es.request("GET", "/").get(10, TimeUnit.SECONDS);
            ExecutionException e = assertThrows(ExecutionException.class, () -> es.request("POST", "/" + missing + "/_search", null, Map.of()).get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof EsException, String.valueOf(e.getCause()));
        }
        outer.end();
        List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> outer.context().traceId().equals(s.context().traceId()) && s.kind() == Span.Kind.CLIENT).toList();
        assertEquals(2, mine.size(), "one span per request, none for the HTTP transport: " + mine);
        MemoryTraceSink.Recorded info = mine.get(0);
        assertEquals("GET /", info.name());
        assertEquals(outer.context().spanId(), info.parentSpanId());
        assertEquals("elasticsearch", info.attributes().get("db.system.name"));
        assertEquals("GET", info.attributes().get("http.request.method"));
        assertEquals(200L, info.attributes().get("http.response.status_code"));
        assertFalse(info.failed());
        assertEquals("POST /" + missing + "/_search", mine.get(1).name());
        assertTrue(mine.get(1).failed());
        assertTrue(mine.get(1).exception() instanceof EsException);
        for (MemoryTraceSink.Recorded s : mine) {
            assertTrue(s.ended(), s.toString());
        }
    }
}
