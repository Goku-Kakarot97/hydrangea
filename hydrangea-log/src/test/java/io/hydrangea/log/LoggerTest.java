package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggerTest {

    private MemoryAppender mem;

    @BeforeEach
    void setUp() {
        mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).apply();
    }

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    private LogEvent last() {
        List<LogEvent> events = mem.events();
        return events.get(events.size() - 1);
    }

    @Test
    void placeholdersAreFilledInOrder() {
        Logger log = Logger.get("t.placeholders");
        log.info("a={} b={} c={}", 1, "two", null);
        assertEquals("a=1 b=two c=null", last().message());
        log.info("one {}", 1);
        assertEquals("one 1", last().message());
        log.info("two {} {}", 1, 2);
        assertEquals("two 1 2", last().message());
        log.info("escaped \\{} and {}", 7);
        assertEquals("escaped {} and 7", last().message());
        log.info("missing {} {}", 1);
        assertEquals("missing 1 {}", last().message());
        log.info("extra {}", 1, 2, 3);
        assertEquals("extra 1", last().message());
        log.info("array {} {}", new int[] {1, 2}, new Object[] {"x", new String[] {"y"}});
        assertEquals("array [1, 2] [x, [y]]", last().message());
        log.info("none");
        assertEquals("none", last().message());
    }

    @Test
    void trailingThrowableBecomesTheCause() {
        Logger log = Logger.get("t.throwable");
        RuntimeException e = new RuntimeException("boom");
        log.error("failed", e);
        assertSame(e, last().cause());
        assertEquals("failed", last().message());

        log.error("failed {}", "x", e);                 // one placeholder, two args: the exception is the cause
        assertSame(e, last().cause());
        assertEquals("failed x", last().message());

        log.error("failed {}", (Object) e);              // one placeholder, one arg: rendered, not the cause (SLF4J rule)
        assertNull(last().cause());
        assertEquals("failed java.lang.RuntimeException: boom", last().message());

        log.error("failed {} {}", "a", "b", e);
        assertSame(e, last().cause());
        assertEquals("failed a b", last().message());

        log.warn("failed {}", "a", e);
        assertSame(e, last().cause());
        assertEquals(Level.WARN, last().level());
    }

    @Test
    void levelsFilterAndHierarchyApplies() {
        Log.configure().async(false).level(Level.WARN).level("t.levels.sub", Level.DEBUG).stage(mem).apply();
        Logger parent = Logger.get("t.levels");
        Logger child = Logger.get("t.levels.sub.Deep");
        Logger lookalike = Logger.get("t.levels.subway");
        assertEquals(Level.WARN, parent.level());
        assertEquals(Level.DEBUG, child.level());
        assertEquals(Level.WARN, lookalike.level());
        assertFalse(parent.isInfoEnabled());
        assertTrue(child.isDebugEnabled());
        assertFalse(child.isTraceEnabled());
        assertFalse(child.isEnabled(Level.OFF));

        parent.info("dropped");
        child.debug("kept");
        parent.error("kept too");
        assertEquals(List.of("kept", "kept too"), mem.events().stream().map(LogEvent::message).toList());

        Log.level("t.levels", Level.TRACE);              // live change reaches existing loggers
        assertTrue(parent.isTraceEnabled());
        assertEquals(Level.DEBUG, child.level());        // the more specific setting still wins
        Log.level("t.levels.sub", null);
        assertEquals(Level.TRACE, child.level());
        child.level(Level.OFF);
        child.error("never");
        assertEquals(2, mem.size());
    }

    @Test
    void disabledCallDoesNotTouchArguments() {
        Log.configure().async(false).level(Level.INFO).stage(mem).apply();
        Logger log = Logger.get("t.lazy");
        AtomicInteger calls = new AtomicInteger();
        Object arg = new Object() {
            @Override
            public String toString() {
                calls.incrementAndGet();
                return "x";
            }
        };
        log.debug("{}", arg);
        log.debug(() -> {
            calls.incrementAndGet();
            return "x";
        });
        log.at(Level.DEBUG).kv("k", arg).log("x {}", arg);
        assertEquals(0, calls.get());
        assertSame(LogEntry.NOOP, log.at(Level.TRACE));
        assertEquals(0, mem.size());
        log.info("{}", arg);
        assertEquals(1, calls.get());
    }

    @Test
    void fluentEntryCarriesFieldsAndCause() {
        Logger log = Logger.get("t.fluent");
        RuntimeException e = new RuntimeException("x");
        log.at(Level.WARN).kv("order", 42).kv("ms", 317L).cause(e).log("slow {}", "checkout");
        LogEvent ev = last();
        assertEquals(Level.WARN, ev.level());
        assertEquals("slow checkout", ev.message());
        assertEquals(2, ev.fieldCount());
        assertEquals("order", ev.fieldKey(0));
        assertEquals(42, ev.fieldValue(0));
        assertEquals(317L, ev.field("ms"));
        assertSame(e, ev.cause());
        assertTrue(mem.lines().get(0).contains("order=42 ms=317"));

        log.at(Level.INFO).log("plain {}", "a", e);      // trailing exception rule applies here too
        assertSame(e, last().cause());
        assertEquals("plain a", last().message());

        for (int i = 0; i < 10; i++) {                    // the field array grows
            LogEntry entry = log.at(Level.INFO);
            for (int j = 0; j <= i; j++) {
                entry.kv("k" + j, j);
            }
            entry.log("n");
            assertEquals(i + 1, last().fieldCount());
        }
    }

    @Test
    void contextTravelsWithTheEvent() {
        Logger log = Logger.get("t.context");
        LogContext.clear();
        log.info("none");
        assertEquals(0, last().contextCount());
        LogContext.put("request", "r1");
        try (LogContext.Scope ignored = LogContext.with("user", "u1")) {
            log.info("both");
            assertEquals("r1", last().context("request"));
            assertEquals("u1", last().context("user"));
            try (LogContext.Scope inner = LogContext.with("user", "u2")) {
                log.info("inner");
                assertEquals("u2", last().context("user"));
            }
            log.info("restored");
            assertEquals("u1", last().context("user"));
        }
        log.info("after");
        assertNull(last().context("user"));
        assertEquals("r1", last().context("request"));
        LogContext.put("request", null);
        log.info("removed");
        assertEquals(0, last().contextCount());
        assertTrue(mem.text().contains("both request=r1 user=u1"));
        LogContext.clear();
    }

    @Test
    void sameLoggerObjectEveryTime() {
        assertSame(Logger.get("t.same"), Logger.get("t.same"));
        assertSame(Logger.get(LoggerTest.class), Log.logger(LoggerTest.class.getName()));
    }

    @Test
    void eventCopiesKeepEverythingElse() {
        Logger log = Logger.get("t.copy");
        LogContext.put("c", "1");
        log.at(Level.INFO).kv("a", 1).log("m");
        LogContext.clear();
        LogEvent e = last();
        LogEvent redacted = e.withMessage("***").with("b", 2);
        assertEquals("***", redacted.message());
        assertEquals(2, redacted.fieldCount());
        assertEquals(1, redacted.field("a"));
        assertEquals(2, redacted.field("b"));
        assertEquals("1", redacted.context("c"));
        assertEquals(e.timestamp(), redacted.timestamp());
        assertEquals(1, e.fieldCount());
    }
}
