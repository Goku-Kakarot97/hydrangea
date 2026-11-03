package io.hydrangea.redis;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every command is a CLIENT span named after the command (never its arguments), child of the caller's span. */
class RedisTraceTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void setUp() {
        Redis.require(Redis.PORT);
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
    void commandsAreChildSpansNamedAfterTheCommand() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (RedisClient redis = RedisClient.connect(group, Redis.config()).get(10, TimeUnit.SECONDS)) {
            String key = Redis.key("trace");
            Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
            try (Span.Scope ignored = outer.makeCurrent()) {
                redis.set(key, "secret-value").get(10, TimeUnit.SECONDS);
                assertEquals("secret-value", redis.get(key).get(10, TimeUnit.SECONDS));
                ExecutionException e = assertThrows(ExecutionException.class, () -> redis.command("NOSUCHCOMMAND", key).get(10, TimeUnit.SECONDS));
                assertTrue(e.getCause() instanceof RedisException, String.valueOf(e.getCause()));
            }
            outer.end();
            List<MemoryTraceSink.Recorded> mine = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CLIENT && outer.context().traceId().equals(s.context().traceId())).toList();
            assertEquals(3, mine.size(), mine.toString());
            assertEquals("SET", mine.get(0).name());
            assertEquals("GET", mine.get(1).name());
            assertEquals("NOSUCHCOMMAND", mine.get(2).name());
            assertEquals(outer.context().spanId(), mine.get(0).parentSpanId());
            assertEquals("redis", mine.get(0).attributes().get("db.system.name"));
            assertEquals("SET", mine.get(0).attributes().get("db.operation.name"));
            assertEquals(Redis.HOST, mine.get(0).attributes().get("server.address"));
            assertEquals((long) Redis.PORT, mine.get(0).attributes().get("server.port"));
            assertNull(mine.get(0).attributes().get("db.query.text"), "no arguments recorded");
            assertFalse(mine.get(0).toString().contains("secret"));
            assertTrue(mine.get(2).failed());
            for (MemoryTraceSink.Recorded s : mine) {
                assertTrue(s.ended(), s.toString());
            }
            redis.del(key).get(10, TimeUnit.SECONDS);
        }
    }
}
