package io.hydrangea.mongodb;

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

/** Every command is a CLIENT span named after the command and its collection, child of the caller's span. */
class MongoTraceTest {

    private static EventLoopGroup group;
    private static MongoClient mongo;

    @BeforeAll
    static void setUp() throws Exception {
        Mongo.require();
        group = new EventLoopGroup(1);
        mongo = MongoClient.connect(group, Mongo.config()).get(15, TimeUnit.SECONDS);
    }

    @AfterAll
    static void tearDown() {
        Tracing.bindTo(TraceSink.NOOP);
        if (mongo != null) {
            mongo.close();
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
    void commandsAreChildSpansNamedAfterCommandAndCollection() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        String coll = "trace_" + Long.toHexString(System.nanoTime());
        Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
        try (Span.Scope ignored = outer.makeCurrent()) {
            mongo.command(Mongo.DATABASE, Bson.doc("insert", coll, "documents", List.of(Bson.doc("a", 1)))).get(10, TimeUnit.SECONDS);
            mongo.command(Mongo.DATABASE, Bson.doc("find", coll, "filter", Bson.doc())).get(10, TimeUnit.SECONDS);
            ExecutionException e = assertThrows(ExecutionException.class, () -> mongo.command(Mongo.DATABASE, Bson.doc("noSuchCommandXyz", 1)).get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof MongoException, String.valueOf(e.getCause()));
            mongo.command(Mongo.DATABASE, Bson.doc("drop", coll)).get(10, TimeUnit.SECONDS);
        }
        outer.end();
        List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CLIENT && outer.context().traceId().equals(s.context().traceId())).toList();
        assertEquals(4, mine.size(), mine.toString());
        MemoryTraceSink.Recorded insert = mine.get(0);
        assertEquals("insert " + coll, insert.name());
        assertEquals(outer.context().spanId(), insert.parentSpanId());
        assertEquals("mongodb", insert.attributes().get("db.system.name"));
        assertEquals(Mongo.DATABASE, insert.attributes().get("db.namespace"));
        assertEquals("insert", insert.attributes().get("db.operation.name"));
        assertEquals(coll, insert.attributes().get("db.collection.name"));
        assertTrue(String.valueOf(insert.attributes().get("server.address")).contains(Mongo.HOST));
        assertEquals("find " + coll, mine.get(1).name());
        assertEquals("noSuchCommandXyz", mine.get(2).name());
        assertTrue(mine.get(2).failed());
        assertTrue(mine.get(2).exception() instanceof MongoException);
        assertFalse(mine.get(0).failed());
        for (MemoryTraceSink.Recorded s : mine) {
            assertTrue(s.ended(), s.toString());
        }
    }
}
