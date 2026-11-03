package io.hydrangea.mysql;

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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every statement is a CLIENT span with the database attributes, child of the caller's span, failed when the server answers with an error. */
class MyTraceTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void setUp() {
        My.require(My.PORT);
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
    void statementsAreChildSpansWithDatabaseAttributes() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (MyClient my = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS)) {
            Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
            try (Span.Scope ignored = outer.makeCurrent()) {
                assertEquals(42L, my.query("SELECT ? AS n", 42).get(10, TimeUnit.SECONDS).rows().get(0).getLong("n"));
                my.queryText("SELECT 1").get(10, TimeUnit.SECONDS);
                ExecutionException e = assertThrows(ExecutionException.class, () -> my.query("SELECT * FROM no_such_table_" + System.nanoTime()).get(10, TimeUnit.SECONDS));
                assertTrue(e.getCause() instanceof MyException, String.valueOf(e.getCause()));
            }
            outer.end();
            List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CLIENT).toList();
            assertEquals(3, mine.size(), mine.toString());
            MemoryTraceSink.Recorded q = mine.get(0);
            assertEquals("my.query", q.name());
            assertEquals(outer.context().spanId(), q.parentSpanId());
            assertEquals("mysql", q.attributes().get("db.system.name"));
            assertEquals(My.DATABASE, q.attributes().get("db.namespace"));
            assertEquals(My.HOST, q.attributes().get("server.address"));
            assertEquals((long) My.PORT, q.attributes().get("server.port"));
            assertEquals("SELECT ? AS n", q.attributes().get("db.query.text"));
            assertTrue(q.ended());
            assertFalse(q.failed());
            assertTrue(mine.get(2).failed());
            assertTrue(mine.get(2).exception() instanceof MyException);
            for (MemoryTraceSink.Recorded s : mine) {
                assertTrue(s.ended(), s.toString());
            }
        }
    }
}
