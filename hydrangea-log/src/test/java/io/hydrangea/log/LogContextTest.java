package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** The context is per thread: many threads with their own trace ids never see each other's, and a thread that never set one has none. */
class LogContextTest {

    @AfterEach
    void tearDown() {
        LogContext.clear();
        Log.reset();
    }

    @Test
    void eachThreadKeepsItsOwnTraceId() throws Exception {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(true).level(Level.TRACE).stage(mem).apply();
        int threads = 16;
        int perThread = 5_000;
        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        Logger log = Logger.get("ctx.threads");
        for (int t = 0; t < threads; t++) {
            String trace = "trace-" + t;
            Thread w = new Thread(() -> {
                try {
                    start.await();
                    LogContext.put("trace_id", trace);
                    for (int i = 0; i < perThread; i++) {
                        if (i % 100 == 0) {
                            LogContext.put("span", "s" + i);          // changes while the others are logging
                        }
                        log.info("{}", i);
                        Thread.onSpinWait();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }, trace);
            w.start();
        }
        done.await();
        Log.flush();
        List<LogEvent> events = mem.events();
        assertEquals(threads * perThread, events.size());
        for (LogEvent e : events) {
            assertEquals(e.thread(), e.context("trace_id"), "the trace id in the event is the one its own thread set");
            int i = Integer.parseInt(e.message());
            assertEquals("s" + (i / 100 * 100), e.context("span"), "the context is the one at the time of the call");
        }
        assertNull(LogContext.get("trace_id"), "the test thread never set one");
    }

    @Test
    void contextDoesNotLeakIntoOtherThreadsOrPoolsByItself() throws Exception {
        LogContext.put("trace_id", "parent");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            assertNull(pool.submit(() -> LogContext.get("trace_id")).get(), "a pool thread has no context unless it is given one");
            assertNull(CompletableFuture.supplyAsync(() -> LogContext.get("trace_id"), pool).get());
            Thread child = new Thread(() -> {
                if (LogContext.get("trace_id") != null) {
                    throw new AssertionError("leaked");
                }
            });
            child.start();
            child.join();
        } finally {
            pool.shutdownNow();
        }
        assertEquals("parent", LogContext.get("trace_id"));
    }

    @Test
    void wrapAndCaptureCarryTheContextAcrossThreadsAndRestoreWhatWasThere() throws Exception {
        LogContext.put("trace_id", "t-1");
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> LogContext.put("trace_id", "pool-own")).get();          // the pool thread has a context of its own
            assertEquals("t-1", pool.submit(LogContext.wrap(() -> LogContext.get("trace_id"))).get());
            assertEquals("pool-own", pool.submit(() -> LogContext.get("trace_id")).get(), "restored after the wrapped task");
            Executor traced = LogContext.wrap(pool);
            CompletableFuture<String> seen = new CompletableFuture<>();
            traced.execute(() -> seen.complete(LogContext.get("trace_id")));
            assertEquals("t-1", seen.get());

            LogContext.Snapshot snap = LogContext.capture();
            LogContext.put("trace_id", "t-2");
            assertEquals(java.util.Map.of("trace_id", "t-1"), snap.asMap());
            try (LogContext.Scope ignored = snap.attach()) {
                assertEquals("t-1", LogContext.get("trace_id"));
            }
            assertEquals("t-2", LogContext.get("trace_id"));
            LogContext.Snapshot empty = pool.submit(() -> {
                LogContext.clear();
                return LogContext.capture();
            }).get();
            try (LogContext.Scope ignored = empty.attach()) {
                assertNull(LogContext.get("trace_id"), "an empty snapshot attaches an empty context");
            }
            assertEquals("t-2", LogContext.get("trace_id"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void oneThreadServingManyRequestsMustScopeEachOne() {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).apply();
        Logger log = Logger.get("ctx.loop");
        // an event loop: one thread, requests interleaved; each handler invocation wraps itself in a scope
        for (String request : List.of("r1", "r2", "r3")) {
            try (LogContext.Scope ignored = LogContext.with("trace_id", request)) {
                log.info("start");
            }
        }
        try (LogContext.Scope ignored = LogContext.with("trace_id", "r2")) {
            log.info("continue");
        }
        log.info("between requests");
        List<LogEvent> events = mem.events();
        assertEquals(java.util.Arrays.asList("r1", "r2", "r3", "r2", null), events.stream().map(e -> e.context("trace_id")).toList());
    }
}
