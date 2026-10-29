package io.hydrangea;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single thread running a {@link Selector}, a task queue and a timer heap.
 * <p>
 * Everything that touches a channel registered with this loop happens on this thread, so channel state needs
 * no locks. Other threads interact only through {@link #execute(Runnable)} / {@link #schedule}.
 * <p>
 * Selector spin protection: some JDK/kernel combinations make {@code select()} return immediately with no
 * ready keys, burning 100% CPU. After {@value #SPIN_THRESHOLD} consecutive such returns the loop migrates all
 * channels to a fresh selector (same remedy as Netty/Jetty).
 */
public final class EventLoop implements Executor {

    private static final Logger LOG = System.getLogger(EventLoop.class.getName());
    /** Size of the shared read buffer; one per loop instead of one per connection. */
    private static final int READ_BUFFER_SIZE = 64 * 1024;
    static final int SPIN_THRESHOLD = 512;

    private volatile Selector selector;
    private final Thread thread;
    private final Metrics metrics;
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final PriorityQueue<Timer> timers = new PriorityQueue<>(); // loop thread only
    private int purgeAt = 1024; // loop thread only: queue size that triggers dropping cancelled timers
    private final AtomicBoolean wakeupPending = new AtomicBoolean();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
    private volatile boolean shutdown;
    private long timerSeq;
    private int spinCount;

    EventLoop(String name, Metrics metrics) {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.metrics = metrics;
        this.thread = new Thread(this::run, name);
    }

    void start() {
        thread.start();
    }

    public boolean inEventLoop() {
        return Thread.currentThread() == thread;
    }

    public Metrics metrics() {
        return metrics;
    }

    /** Runs {@code task} on the loop thread (FIFO). */
    @Override
    public void execute(Runnable task) {
        if (shutdown && !inEventLoop()) {
            throw new RejectedExecutionException("event loop is shut down");
        }
        tasks.add(task);
        if (!inEventLoop() && wakeupPending.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    /** Runs {@code task} after the delay. The returned handle may be cancelled from any thread. */
    public Scheduled schedule(Runnable task, long delay, TimeUnit unit) {
        Timer timer = new Timer(task, System.nanoTime() + Math.max(0, unit.toNanos(delay)));
        if (inEventLoop()) {
            addTimer(timer); // the common case (a connection re-arming its deadline): no queue node, no one-iteration delay
        } else {
            execute(() -> addTimer(timer));
        }
        return timer;
    }

    private void addTimer(Timer timer) {
        timer.seq = timerSeq++;
        timers.add(timer);
        if (timers.size() > purgeAt) {
            // Cancelled timers stay queued until their deadline (a 5-minute keep-alive timer outlives its connection by 5
            // minutes), so under connection churn the queue would grow without bound. Compact it whenever it has doubled.
            timers.removeIf(x -> x.cancelled);
            purgeAt = Math.max(1024, timers.size() * 2);
        }
    }

    /** Asks the loop to close all its channels and exit. The future completes once the thread is done. */
    public CompletableFuture<Void> shutdown() {
        shutdown = true;
        wakeupPending.set(true);
        selector.wakeup(); // unconditional: shutting down must never depend on the "already requested" flag
        return terminated;
    }

    public CompletableFuture<Void> terminationFuture() {
        return terminated;
    }

    // ---- package-private API for channels (loop thread only unless stated) ----

    ByteBuffer readBuffer() {
        return readBuffer;
    }

    SelectionKey register(SelectableChannel channel, int ops, SelectionHandle handle) throws IOException {
        return channel.register(selector, ops, handle);
    }

    /**
     * Closes {@code channel} (registered with this loop) and waits until the socket is really released.
     * <p>
     * Why this exists: closing a channel that is registered with a selector does not free its file descriptor; the
     * kernel keeps the socket (a listening port keeps accepting!) until the selector next processes the cancelled
     * key, which may be never if the loop is parked in {@code select()}. So the close is done on the loop thread and
     * followed by a {@code selectNow()}, and the caller waits for it.
     */
    void closeAndRelease(Channel channel) throws IOException {
        if (inEventLoop()) {
            closeNow(channel);
            return;
        }
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            execute(() -> {
                try {
                    closeNow(channel);
                    done.complete(null);
                } catch (Throwable t) {
                    done.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException loopIsDown) {
            channel.close(); // shutdown closes everything anyway
            return;
        }
        try {
            done.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            throw new IOException("event loop did not release the channel in time", e);
        }
    }

    private void closeNow(Channel channel) throws IOException {
        channel.close();
        selector.selectNow(); // processes the cancelled key; only now does the kernel drop the socket
    }

    /** Moves every channel to a new selector. Loop thread only. Visible for tests. */
    void rebuildSelector() throws IOException {
        Selector old = selector;
        Selector fresh = Selector.open();
        int moved = 0;
        for (SelectionKey k : new ArrayList<>(old.keys())) {
            if (!k.isValid()) {
                continue;
            }
            Object attachment = k.attachment();
            try {
                SelectionKey nk = k.channel().register(fresh, k.interestOps(), attachment);
                if (attachment instanceof SelectionHandle h) {
                    h.rebind(nk);
                }
                moved++;
            } catch (ClosedChannelException | CancelledKeyException e) {
                // channel closed concurrently with the rebuild; it is gone anyway
            }
            k.cancel();
        }
        selector = fresh;
        old.close();
        metrics.selectorRebuilds.increment();
        LOG.log(Level.WARNING, "{0}: selector rebuilt after spurious wakeups, {1} channels migrated",
                thread.getName(), moved);
    }

    // ---- the loop ----

    private void run() {
        try {
            while (true) {
                runTasks();
                long nextDeadline = runTimers();
                if (shutdown && shutdownStep()) {
                    return;
                }
                // Reset BEFORE the queue check inside select(): an execute() that adds a task after this point either sees the
                // flag false and wakes the selector, or its task is already visible to the check. (Resetting after select()
                // returned lost wakeups whenever something else, e.g. a selectNow() inside a task, had swallowed the
                // selector's wakeup while the flag stayed true: later execute() calls then skipped wakeup() forever.)
                wakeupPending.set(false);
                boolean premature = select(nextDeadline);
                spinCount = premature ? spinCount + 1 : 0;
                if (spinCount >= SPIN_THRESHOLD) {
                    spinCount = 0;
                    rebuildSelector();
                }
                dispatch();
            }
        } catch (Throwable t) {
            LOG.log(Level.ERROR, "event loop " + thread.getName() + " crashed", t);
            closeAll();
        } finally {
            try {
                selector.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
            terminated.complete(null);
        }
    }

    private static final int MAX_TASKS_PER_BATCH = 1024;

    private final ArrayList<SocketConnection> deferredFlushes = new ArrayList<>();
    private boolean runningTasks;

    private void runTasks() {
        Runnable task;
        runningTasks = true;
        int n = 0;
        while ((task = tasks.poll()) != null) {
            safeRun(task);
            if (++n == MAX_TASKS_PER_BATCH) {   // bounded, so that what the tasks wrote is not held back by a long queue
                flushDeferred();
                n = 0;
            }
        }
        runningTasks = false;
        flushDeferred();
    }

    /** True while the loop runs its queued tasks: writes made by them are sent together when the batch ends. */
    boolean batchingTasks() {
        return runningTasks;
    }

    /** Loop thread only: {@code c} has output that goes out at the end of the current task batch. */
    void flushAtEndOfBatch(SocketConnection c) {
        deferredFlushes.add(c);
    }

    private void flushDeferred() {
        for (int i = 0; i < deferredFlushes.size(); i++) {   // a flush may make a callback that writes on another connection
            deferredFlushes.get(i).flushDeferredWrites();
        }
        deferredFlushes.clear();
    }

    /** Fires due timers and returns the next deadline (nanoTime based) or {@code Long.MIN_VALUE} if none. */
    private long runTimers() {
        long now = System.nanoTime();
        Timer t;
        while ((t = timers.peek()) != null) {
            if (t.cancelled) {
                timers.poll();
            } else if (t.deadline - now <= 0) {
                timers.poll();
                Runnable task = t.task;
                if (task != null) {
                    safeRun(task);
                }
            } else {
                return t.deadline;
            }
        }
        return Long.MIN_VALUE;
    }

    /** @return true when everything is closed and the loop may exit */
    private boolean shutdownStep() {
        closeAll();
        try {
            selector.selectNow(); // flush cancelled keys
        } catch (IOException ignored) {
            // fall through; keys() check below decides
        }
        return selector.keys().isEmpty() && tasks.isEmpty();
    }

    private void closeAll() {
        for (SelectionKey key : new ArrayList<>(selector.keys())) {
            if (key.attachment() instanceof SelectionHandle h) {
                try {
                    h.terminate();
                } catch (Throwable t) {
                    LOG.log(Level.WARNING, "error terminating " + key.channel(), t);
                }
            }
            key.cancel();
        }
    }

    /** @return true if the select returned suspiciously early with nothing to do (possible JDK spin bug) */
    private boolean select(long nextDeadline) throws IOException {
        Selector sel = selector;
        if (!tasks.isEmpty() || shutdown) {
            sel.selectNow();
            return false;
        }
        long start = System.nanoTime();
        int ready;
        long expected;
        if (nextDeadline == Long.MIN_VALUE) {
            ready = sel.select();
            expected = Long.MAX_VALUE;
        } else {
            expected = nextDeadline - start;
            if (expected <= 0) {
                sel.selectNow();
                return false;
            }
            ready = sel.select(TimeUnit.NANOSECONDS.toMillis(expected) + 1); // round up; 0 would block forever
        }
        return ready == 0 && !wakeupPending.get() && tasks.isEmpty() && !shutdown
                && System.nanoTime() - start < 1_000_000L && expected > 2_000_000L;
    }

    private void dispatch() {
        var selected = selector.selectedKeys();
        if (selected.isEmpty()) {
            return;
        }
        for (SelectionKey key : selected) {
            long t0 = System.nanoTime();
            try {
                ((SelectionHandle) key.attachment()).ready(key.readyOps());
            } catch (CancelledKeyException ignored) {
                // closed while being handled
            } catch (Throwable t) {
                LOG.log(Level.WARNING, "unhandled error for " + key.channel(), t);
            }
            long took = System.nanoTime() - t0;
            if (took > metrics.maxBlockNanos.get()) { // read first: the accumulator is shared by every loop, so do not write to it per event
                metrics.maxBlockNanos.accumulate(took);
            }
        }
        selected.clear();
    }

    private void safeRun(Runnable r) {
        long t0 = System.nanoTime();
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Level.ERROR, "task failed", t);
        }
        metrics.tasksExecuted.increment();
        long took = System.nanoTime() - t0;
        if (took > metrics.maxBlockNanos.get()) {
            metrics.maxBlockNanos.accumulate(took);
        }
    }

    private static final class Timer implements Scheduled, Comparable<Timer> {
        volatile Runnable task; // dropped on cancel so a cancelled timer does not keep its owner (a connection) alive
        final long deadline;
        long seq;
        volatile boolean cancelled;

        Timer(Runnable task, long deadline) {
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public void cancel() {
            cancelled = true; // the entry itself is dropped lazily by the loop
            task = null;
        }

        @Override
        public int compareTo(Timer o) {
            long d = deadline - o.deadline;
            return d != 0 ? (d < 0 ? -1 : 1) : Long.compare(seq, o.seq);
        }
    }
}
