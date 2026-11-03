package io.hydrangea.postgres;

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
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every query is a CLIENT span with the database attributes, child of the caller's span, failed when the server answers with an error. */
class PgTraceTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void setUp() {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(1);
    }

    @AfterAll
    static void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    @AfterEach
    void unbind() {
        Tracing.bindTo(TraceSink.NOOP);
    }

    @Test
    void queriesAreChildSpansWithDatabaseAttributes() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (PgClient pg = Pg.get(PgClient.connect(group, Pg.config()))) {
            Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
            try (Span.Scope ignored = outer.makeCurrent()) {
                assertEquals(42, Pg.get(pg.query("SELECT $1::int AS n", 42)).rows().get(0).getInt("n"));
                Pg.get(pg.execute("SELECT 1; SELECT 2"));
                ExecutionException e = assertThrows(ExecutionException.class, () -> pg.query("SELECT * FROM no_such_table_" + System.nanoTime()).get());
                assertTrue(e.getCause() instanceof PgException, String.valueOf(e.getCause()));
            }
            outer.end();
            List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CLIENT).toList();
            assertEquals(3, mine.size(), mine.toString());
            MemoryTraceSink.Recorded q = mine.get(0);
            assertEquals("pg.query", q.name());
            assertEquals(outer.context().spanId(), q.parentSpanId());
            assertEquals(outer.context().traceId(), q.context().traceId());
            assertEquals("postgresql", q.attributes().get("db.system.name"));
            assertEquals(Pg.config().database(), q.attributes().get("db.namespace"));
            assertEquals(Pg.HOST, q.attributes().get("server.address"));
            assertEquals((long) Pg.PORT, q.attributes().get("server.port"));
            assertEquals("SELECT $1::int AS n", q.attributes().get("db.query.text"));
            assertTrue(q.ended());
            assertFalse(q.failed());
            assertEquals("pg.execute", mine.get(1).name());
            assertTrue(mine.get(2).failed());
            assertTrue(mine.get(2).exception() instanceof PgException);
            assertTrue(mine.get(2).ended());
        }
    }

    @Test
    void withoutASinkNothingIsRecordedAndNothingBreaks() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        try (PgClient pg = Pg.get(PgClient.connect(group, Pg.config()))) {
            assertNull(Tracing.current());
            assertEquals(1, Pg.get(pg.query("SELECT 1 AS n")).rows().get(0).getInt("n"));
            assertEquals(0, spans.spans().size());
        }
    }
}
