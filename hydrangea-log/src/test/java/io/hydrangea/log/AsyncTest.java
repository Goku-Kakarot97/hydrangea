package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncTest {

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    @Test
    void everyEventArrivesOnceInPerThreadOrder() throws InterruptedException {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(true).buffer(1024).level(Level.TRACE).stage(mem).apply();
        int threads = 8;
        int perThread = 20_000;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Logger log = Logger.get("a.t" + t);
            Thread w = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    log.info("{}", i);
                }
            }, "w" + t);
            workers.add(w);
            w.start();
        }
        start.countDown();
        for (Thread w : workers) {
            w.join();
        }
        assertTrue(Log.flush());
        List<LogEvent> events = mem.events();
        assertEquals(threads * perThread, events.size());
        Map<String, Integer> next = new HashMap<>();
        for (LogEvent e : events) {
            int expected = next.getOrDefault(e.logger(), 0);
            assertEquals(expected, Integer.parseInt(e.message()), "order within " + e.logger());
            next.put(e.logger(), expected + 1);
        }
        assertEquals(0, Log.stats().dropped());
        assertTrue(Log.stats().async());
        assertEquals(1024, Log.stats().capacity());
    }

    @Test
    void flushMeansWritten() {
        MemoryAppender mem = Appender.memory();
        AtomicInteger flushes = new AtomicInteger();
        LogStage counting = new LogStage() {
            @Override
            public void handle(LogEvent event, LogChain next) {
                next.next(event);
            }

            @Override
            public void flush() {
                flushes.incrementAndGet();
            }
        };
        Log.configure().async(true).level(Level.TRACE).stage(counting).stage(mem).apply();
        Logger log = Logger.get("a.flush");
        for (int round = 0; round < 50; round++) {
            log.info("r{}", round);
            assertTrue(Log.flush());
            assertEquals(round + 1, mem.size());
        }
        assertTrue(flushes.get() >= 50);
        assertTrue(Log.flush());                        // nothing pending: returns at once
    }

    @Test
    void dropWhenFullCountsInsteadOfBlocking() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        MemoryAppender mem = Appender.memory();
        Log.configure().async(true).buffer(16).dropWhenFull(true).level(Level.TRACE)
                .stage((e, next) -> {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    next.next(e);
                })
                .stage(mem)
                .apply();
        Logger log = Logger.get("a.drop");
        for (int i = 0; i < 1000; i++) {
            log.info("{}", i);
        }
        LogStats stats = Log.stats();
        assertTrue(stats.dropped() > 0, "some were dropped: " + stats);
        assertTrue(stats.dropped() + stats.queued() + 1 >= 1000 - 16, "" + stats);
        release.countDown();
        assertTrue(Log.flush());
        assertTrue(mem.size() >= 16 && mem.size() <= 18, "" + mem.size());
    }

    @Test
    void blockWhenFullLosesNothing() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        MemoryAppender mem = Appender.memory();
        Log.configure().async(true).buffer(16).level(Level.TRACE)
                .stage((e, next) -> {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    next.next(e);
                })
                .stage(mem)
                .apply();
        Logger log = Logger.get("a.block");
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                log.info("{}", i);
            }
        });
        producer.start();
        Thread.sleep(100);
        assertTrue(producer.isAlive(), "the producer waits for room");
        release.countDown();
        producer.join(5_000);
        assertFalse(producer.isAlive());
        assertTrue(Log.flush());
        assertEquals(500, mem.size());
        assertEquals(0, Log.stats().dropped());
    }

    @Test
    void shutdownDrainsAndThenWritesInline() {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(true).level(Level.TRACE).stage(mem).apply();
        Logger log = Logger.get("a.shutdown");
        for (int i = 0; i < 1000; i++) {
            log.info("{}", i);
        }
        Log.shutdown();
        assertEquals(1000, mem.size());
        log.info("late");
        assertEquals(1001, mem.size());                 // no flush needed: inline after shutdown
        Log.shutdown();                                 // idempotent
        Log.configure().async(true).level(Level.TRACE).stage(mem).apply();   // a new configuration starts a new log thread
        log.info("later");
        assertTrue(Log.stats().async());
        assertTrue(Log.flush());
        assertEquals(1002, mem.size());
    }

    @Test
    void reconfigurationDrainsTheOldQueueIntoTheNewChain() {
        MemoryAppender first = Appender.memory();
        MemoryAppender second = Appender.memory();
        Log.configure().async(true).buffer(1 << 12).level(Level.TRACE).stage(first).apply();
        Logger log = Logger.get("a.reconf");
        for (int i = 0; i < 2000; i++) {
            log.info("{}", i);
        }
        Log.configure().async(true).buffer(1 << 10).level(Level.TRACE).stage(second).apply();
        assertTrue(Log.flush());
        assertEquals(2000, first.size() + second.size());
        for (int i = 0; i < 100; i++) {
            log.info("new {}", i);
        }
        assertTrue(Log.flush());
        assertEquals(2100, first.size() + second.size());
        assertTrue(second.size() >= 100);
    }

    @Test
    void ringBufferRoundsUpAndWrapsCorrectly() {
        RingBuffer q = new RingBuffer(5);
        assertEquals(8, q.capacity());
        for (int lap = 0; lap < 1000; lap++) {
            for (int i = 0; i < 8; i++) {
                assertTrue(q.offer(lap * 8 + i));
            }
            assertFalse(q.offer(-1));
            assertEquals(8, q.size());
            for (int i = 0; i < 8; i++) {
                assertEquals(lap * 8 + i, q.poll());
            }
            assertEquals(null, q.poll());
            assertTrue(q.isEmpty());
        }
    }
}
