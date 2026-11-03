package io.hydrangea.log;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Takes events from loggers to the stages. Async: a {@link RingBuffer} and one daemon thread that drains it in batches, running the
 * chain for each event and flushing the appenders when the queue runs empty (or every {@link #FLUSH_EVERY} events under sustained
 * load), so many events share one write to the OS. Sync: the calling thread runs the chain and flushes.
 * <p>
 * When the buffer is full the producer waits for room (nothing is lost; the application is slowed to the speed of the appenders)
 * unless {@code dropWhenFull} was chosen, in which case the event is counted and dropped. After {@link #shutdown} (the JVM shutdown
 * hook, or a reconfiguration) events are delivered on the calling thread, so what is logged during shutdown still gets out.
 * The log thread itself and anything logged from inside a stage are delivered inline too, never queued (no deadlock, no recursion).
 */
final class Dispatcher {

    private static final int SPINS = 64;
    private static final int YIELDS = 8;
    static final int FLUSH_EVERY = 512;

    final boolean async;
    final boolean dropWhenFull;
    private final RingBuffer queue;
    private final Thread thread;
    private final LongAdder syncPublished = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private volatile boolean waiting;
    private volatile boolean stopping;
    private volatile boolean stopped;
    private volatile long flushed;

    Dispatcher(boolean async, int capacity, boolean dropWhenFull) {
        this.async = async;
        this.dropWhenFull = dropWhenFull;
        if (async) {
            queue = new RingBuffer(capacity);
            thread = new Thread(this::run, "hydrangea-log");
            thread.setDaemon(true);
            thread.start();
        } else {
            queue = null;
            thread = null;
        }
    }

    void publish(LogEvent event) {
        if (queue == null || stopped || Thread.currentThread() == thread) {
            inline(event);
            return;
        }
        if (!queue.offer(event)) {
            if (dropWhenFull) {
                dropped.increment();
                return;
            }
            int spins = 0;
            while (!queue.offer(event)) {
                if (stopped) {
                    inline(event);
                    return;
                }
                wake();
                if (++spins < 32) {
                    Thread.onSpinWait();
                } else {
                    LockSupport.parkNanos(10_000L);
                }
            }
        }
        if (waiting) {
            wake();
        }
    }

    private void wake() {
        waiting = false;
        LockSupport.unpark(thread);
    }

    private void inline(LogEvent event) {
        syncPublished.increment();
        Routes routes = Log.routes();
        routes.deliver(event);
        routes.flushAll();
    }

    private void run() {
        int idle = 0;
        int sinceFlush = 0;
        boolean dirty = false;
        try {
            for (;;) {
                LogEvent e = (LogEvent) queue.poll();
                if (e != null) {
                    idle = 0;
                    Log.routes().deliver(e);
                    dirty = true;
                    if (++sinceFlush >= FLUSH_EVERY) {
                        flushNow();
                        sinceFlush = 0;
                        dirty = false;
                    }
                    continue;
                }
                if (dirty) {
                    flushNow();
                    sinceFlush = 0;
                    dirty = false;
                    continue;
                }
                if (stopping) {
                    return;
                }
                if (idle < SPINS) {
                    idle++;
                    Thread.onSpinWait();
                } else if (idle < SPINS + YIELDS) {
                    idle++;
                    Thread.yield();
                } else {
                    waiting = true;
                    if (queue.isEmpty() && !stopping) {
                        LockSupport.parkNanos(this, 1_000_000_000L);
                    }
                    waiting = false;
                }
            }
        } catch (Throwable t) {
            Log.failure("log thread", t);
        }
    }

    private void flushNow() {
        Log.routes().flushAll();
        flushed = queue.consumed();
    }

    /** Waits until everything queued before the call has gone through the chain and the appenders were flushed. */
    boolean flush(long timeoutNanos) {
        if (queue == null || Thread.currentThread() == thread) {
            Log.routes().flushAll();
            return true;
        }
        long target = queue.claimed();
        long deadline = System.nanoTime() + timeoutNanos;
        while (flushed < target) {
            if (stopped) {
                Log.routes().flushAll();
                return true;
            }
            wake();
            if (System.nanoTime() - deadline > 0) {
                return false;
            }
            LockSupport.parkNanos(20_000L);
        }
        return true;
    }

    /** Stops the thread after it drained the queue; later events are delivered inline. Safe to call more than once. */
    void shutdown() {
        if (queue == null) {
            Log.routes().flushAll();
            return;
        }
        stopping = true;
        wake();
        if (Thread.currentThread() != thread) {
            try {
                thread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        stopped = true;
        // A producer may have claimed a slot after the thread saw an empty queue: drain what is published, waiting briefly for a claim in flight.
        int spins = 0;
        while (queue.consumed() < queue.claimed() && spins < 1_000_000) {
            LogEvent e = (LogEvent) queue.poll();
            if (e == null) {
                spins++;
                Thread.onSpinWait();
                continue;
            }
            Log.routes().deliver(e);
        }
        Log.routes().flushAll();
    }

    boolean isStopped() {
        return stopped;
    }

    LogStats stats(long errors) {
        if (queue == null) {
            return new LogStats(syncPublished.sum(), 0, errors, 0, 0, false);
        }
        return new LogStats(queue.claimed() + syncPublished.sum(), dropped.sum(), errors, queue.size(), queue.capacity(), true);
    }
}
