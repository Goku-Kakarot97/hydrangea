package io.hydrangea.trace;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TracingTest {

    @AfterEach
    void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
        Tracing.sampleRatio(1.0);
    }

    @Test
    void traceparentRoundTrip() {
        SpanContext c = SpanContext.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assertEquals("0af7651916cd43dd8448eb211c80319c", c.traceId());
        assertEquals("b7ad6b7169203331", c.spanId());
        assertTrue(c.sampled());
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", c.traceparent());
        assertFalse(SpanContext.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00").sampled());
        assertEquals("0af7651916cd43dd8448eb211c80319c", SpanContext.parse("00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01").traceId(), "upper case lowered");
        assertEquals("b7ad6b7169203331", SpanContext.parse("01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra").spanId(), "a future version with more fields");
        for (String bad : List.of("", "garbage", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
                "00-00000000000000000000000000000000-b7ad6b7169203331-01", "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
                "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "00-0af7651916cd43dd8448eb211c80319z-b7ad6b7169203331-01",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0g", "00\n0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")) {
            assertNull(SpanContext.parse(bad), bad);
        }
        assertNull(SpanContext.parse(null));
        assertThrows(IllegalArgumentException.class, () -> new SpanContext("abc", "b7ad6b7169203331", true));
        assertThrows(IllegalArgumentException.class, () -> new SpanContext("0af7651916cd43dd8448eb211c80319c", "0000000000000000", true));
    }

    @Test
    void generatedIdsAreWellFormedAndDistinct() {
        String t = SpanContext.newTraceId();
        String s = SpanContext.newSpanId();
        assertTrue(t.matches("[0-9a-f]{32}"), t);
        assertTrue(s.matches("[0-9a-f]{16}"), s);
        assertNotEquals(SpanContext.newTraceId(), t);
        assertNotEquals(SpanContext.newSpanId(), s);
    }

    @Test
    void withoutASinkSpansOnlyPropagate() {
        assertFalse(Tracing.enabled());
        Span root = Tracing.start("root", Span.Kind.SERVER, null);
        assertFalse(root.isRecording());
        assertFalse(root.context().sampled());
        Span child = Tracing.start("child", Span.Kind.CLIENT, root.context());
        assertEquals(root.context().traceId(), child.context().traceId());
        assertNotEquals(root.context().spanId(), child.context().spanId());
        child.attribute("k", "v").error("x").name("y").end();      // all no-ops
        root.end();
        SpanContext remote = SpanContext.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        Span joined = Tracing.start("joined", Span.Kind.SERVER, remote);
        assertEquals("0af7651916cd43dd8448eb211c80319c", joined.context().traceId());
        assertTrue(joined.context().sampled(), "the parent's decision is carried even though nothing records here");
    }

    @Test
    void aBoundSinkRecordsParentChildAndErrors() {
        MemoryTraceSink sink = new MemoryTraceSink();
        Tracing.bindTo(sink);
        assertTrue(Tracing.enabled());
        try (Span server = Tracing.start("GET /x", Span.Kind.SERVER, null); Span.Scope scope = server.makeCurrent()) {
            assertSame(server, Tracing.current());
            assertTrue(server.isRecording());
            try (Span client = Tracing.start("HTTP GET", Span.Kind.CLIENT)) {
                client.attribute("http.response.status_code", 503).error("HTTP 503");
                assertEquals(server.context().traceId(), client.context().traceId());
            }
            server.attribute("http.route", "/x").name("GET /x!");
        }
        assertNull(Tracing.current());
        List<MemoryTraceSink.Recorded> spans = sink.spans();
        assertEquals(2, spans.size());
        MemoryTraceSink.Recorded server = spans.get(0);
        MemoryTraceSink.Recorded client = spans.get(1);
        assertEquals("GET /x!", server.name());
        assertNull(server.parentSpanId());
        assertEquals(server.context().spanId(), client.parentSpanId());
        assertEquals(Span.Kind.CLIENT, client.kind());
        assertTrue(client.failed());
        assertEquals("HTTP 503", client.errorDescription());
        assertEquals(503L, client.attributes().get("http.response.status_code"));
        assertFalse(server.failed());
        assertTrue(server.ended() && client.ended());
        assertTrue(server.durationNanos() >= client.durationNanos());
    }

    @Test
    void currentSpanNestsAndRestores() {
        MemoryTraceSink sink = new MemoryTraceSink();
        Tracing.bindTo(sink);
        Span a = Tracing.start("a", Span.Kind.INTERNAL, null);
        Span b = Tracing.start("b", Span.Kind.INTERNAL, null);
        try (Span.Scope sa = a.makeCurrent()) {
            try (Span.Scope sb = b.makeCurrent()) {
                assertSame(b, Tracing.current());
            }
            assertSame(a, Tracing.current());
            assertEquals(a.context(), Tracing.currentContext());
        }
        assertNull(Tracing.currentContext());
    }

    @Test
    void sampleRatioAppliesToNewTracesOnly() {
        MemoryTraceSink sink = new MemoryTraceSink();
        Tracing.bindTo(sink);
        Tracing.sampleRatio(0.0);
        Span root = Tracing.start("root", Span.Kind.SERVER, null);
        assertFalse(root.isRecording());
        assertFalse(root.context().sampled());
        SpanContext sampledParent = SpanContext.parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assertTrue(Tracing.start("joined", Span.Kind.SERVER, sampledParent).isRecording(), "a sampled parent wins over the ratio");
        Tracing.sampleRatio(1.0);
        int recorded = 0;
        for (int i = 0; i < 100; i++) {
            if (Tracing.start("r", Span.Kind.SERVER, null).isRecording()) {
                recorded++;
            }
        }
        assertEquals(100, recorded);
        assertThrows(IllegalArgumentException.class, () -> Tracing.sampleRatio(1.5));
    }
}
