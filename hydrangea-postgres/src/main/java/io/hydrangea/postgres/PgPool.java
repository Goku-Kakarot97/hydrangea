package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Scheduled;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A fixed number of {@link PgClient} connections. A server runs one query at a time per connection, so several connections give it parallel work; each
 * connection still pipelines what it is given. {@link #query} sends to the connection with the fewest queries outstanding. A transaction takes one connection for
 * itself, as does a {@link #stream} until it ends.
 * <ul>
 *   <li><b>No connection free</b> (all taken by transactions, or broken and being replaced): the call waits, up to {@link PgPoolOptions#acquireTimeout()} and
 *       {@link PgPoolOptions#maxWaiters()} callers, instead of failing.</li>
 *   <li><b>A connection that breaks</b> is replaced by a new one (one attempt per second per connection, made even when nobody is calling). Queries that were on it
 *       fail: they may or may not have run.</li>
 *   <li><b>Idle connections</b> are pinged ({@link PgPoolOptions#pingInterval()}), so one that a firewall dropped without a word is found and replaced instead of
 *       failing the next query; and a connection older than {@link PgPoolOptions#maxLifetime()} is replaced when idle.</li>
 *   <li><b>Transactions</b>: {@link #transaction}, with isolation level and read-only through {@link TxOptions}, and {@link #transactionWithRetry} for serialization
 *       failures and deadlocks.</li>
 * </ul>
 */
public final class PgPool implements AutoCloseable {

    private static final long RECONNECT_GAP_NANOS = 1_000_000_000L;

    private final EventLoopGroup group;
    private final PgConfig config;
    private final PgPoolOptions options;
    private final PgListener listener;
    private final Slot[] slots;
    private final AtomicInteger rotor = new AtomicInteger();
    private final ConcurrentLinkedQueue<Waiter> waiters = new ConcurrentLinkedQueue<>();       // transactions waiting for a connection of their own
    private final ConcurrentLinkedQueue<Work<?>> work = new ConcurrentLinkedQueue<>();         // plain calls waiting for any usable connection
    private final AtomicInteger waiting = new AtomicInteger();
    private volatile Scheduled maintenance;
    private volatile boolean closed;
    // metrics: callers waiting for a connection, how long they waited, waits that ran out, connections replaced (broken, or at the end of their life), pings that failed
    private final io.hydrangea.Metrics.Count mWaiting, mAcquireTimeouts, mReplaced, mRetired, mPingsFailed;
    private final io.hydrangea.Latency mWait;

    private static final class Slot {
        volatile PgClient client;
        final AtomicBoolean reserved = new AtomicBoolean();     // a transaction owns it
        final AtomicBoolean connecting = new AtomicBoolean();
        final AtomicBoolean pinging = new AtomicBoolean();
        /** Plain calls between choosing this connection and having their query queued on it. A transaction waits for it to be 0 before it starts (see {@link #awaitQuiet}). */
        final AtomicInteger dispatching = new AtomicInteger();
        volatile long lastAttempt;
        volatile long born = System.nanoTime();
        volatile long lastPing = System.nanoTime();
        volatile long lifetimeNanos = Long.MAX_VALUE;
    }

    private static final class Waiter {
        final CompletableFuture<Slot> future = new CompletableFuture<>();
        volatile Scheduled timer;
        long start;
    }

    private static final class Work<T> {
        final Function<PgClient, CompletableFuture<T>> op;
        final CompletableFuture<T> result = new CompletableFuture<>();
        volatile Scheduled timer;
        long start;

        Work(Function<PgClient, CompletableFuture<T>> op) {
            this.op = op;
        }
    }

    private PgPool(EventLoopGroup group, PgConfig config, PgPoolOptions options, PgListener listener, int size) {
        this.group = group;
        this.config = config.withReconnect(null);       // the pool replaces a connection that is lost; two parties doing it would make two
        this.options = options;
        this.listener = listener;
        this.slots = new Slot[size];
        for (int i = 0; i < size; i++) {
            slots[i] = new Slot();
        }
        io.hydrangea.Metrics m = group.metrics();
        mWaiting = m.gauge("pg.pool.waiting", null);
        mWait = m.latency("pg.pool.acquire.duration");
        mAcquireTimeouts = m.counter("pg.pool.acquire.timeouts", null);
        mReplaced = m.counter("pg.pool.connections.replaced", null, "reason", "broken");
        mRetired = m.counter("pg.pool.connections.replaced", null, "reason", "lifetime");
        mPingsFailed = m.counter("pg.pool.pings.failed", null);
    }

    /** Opens {@code size} connections; completes when all are ready, or fails (and closes the others) if one cannot connect. */
    public static CompletableFuture<PgPool> create(EventLoopGroup group, PgConfig config, int size) {
        return create(group, config, size, PgPoolOptions.defaults(), null);
    }

    public static CompletableFuture<PgPool> create(EventLoopGroup group, PgConfig config, int size, PgListener listener) {
        return create(group, config, size, PgPoolOptions.defaults(), listener);
    }

    public static CompletableFuture<PgPool> create(EventLoopGroup group, PgConfig config, int size, PgPoolOptions options, PgListener listener) {
        if (size < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("size"));
        }
        PgPool pool = new PgPool(group, config, options, listener, size);
        List<CompletableFuture<PgClient>> all = new ArrayList<>();
        for (Slot s : pool.slots) {
            CompletableFuture<PgClient> f = PgClient.connect(group, pool.config, listener);
            all.add(f);
            f.thenAccept(c -> pool.adopt(s, c));
        }
        CompletableFuture<PgPool> result = new CompletableFuture<>();
        CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).whenComplete((v, err) -> {
            if (err == null) {
                pool.startMaintenance();
                result.complete(pool);
            } else {
                for (CompletableFuture<PgClient> f : all) {
                    if (f.isDone() && !f.isCompletedExceptionally()) {
                        f.join().close();
                    }
                }
                result.completeExceptionally(err instanceof CompletionException && err.getCause() != null ? err.getCause() : err);
            }
        });
        return result;
    }

    /** This pool behind the {@link io.hydrangea.sql.SqlPool} interface shared with the MySQL client ({@code ?} placeholders, common result and row types). */
    public io.hydrangea.sql.SqlPool asSql() {
        return new PgSql.Pool(this);
    }

    public int size() {
        return slots.length;
    }

    // ---- calls ----

    /** Runs a statement on the least busy connection; see {@link PgClient#query}. */
    public CompletableFuture<PgResult> query(String sql, Object... params) {
        return run(c -> c.query(sql, params));
    }

    /** As {@link #query(String, Object...)} with its own time limit; see {@link PgClient#query(java.time.Duration, String, Object...)}. */
    public CompletableFuture<PgResult> query(Duration timeout, String sql, Object... params) {
        return run(c -> c.query(timeout, sql, params));
    }

    /** Reads a query in pieces on the least busy connection, which belongs to the stream until it ends; see {@link PgClient#stream}. */
    public CompletableFuture<PgRowStream> stream(int fetchSize, String sql, Object... params) {
        return run(c -> c.stream(fetchSize, sql, params));
    }

    /** A batch on the least busy connection; see {@link PgClient#executeBatch(String, java.util.List)}. */
    public CompletableFuture<long[]> executeBatch(String sql, List<Object[]> parameterSets) {
        return run(c -> c.executeBatch(sql, parameterSets));
    }

    /** Runs simple-protocol statements on the least busy connection; see {@link PgClient#execute}. */
    public CompletableFuture<List<PgResult>> execute(String sql) {
        return run(c -> c.execute(sql));
    }

    /** {@code COPY ... FROM STDIN} on the least busy connection (which waits for the copy to end before it runs anything else); see {@link PgClient#copyIn(String, Iterable)}. */
    public CompletableFuture<Long> copyIn(String sql, Iterable<byte[]> chunks) {
        return run(c -> c.copyIn(sql, chunks));
    }

    public CompletableFuture<Long> copyIn(String sql, byte[] data) {
        return run(c -> c.copyIn(sql, data));
    }

    /** {@code COPY ... TO STDOUT} on the least busy connection; see {@link PgClient#copyOut(String, Consumer)}. */
    public CompletableFuture<Long> copyOut(String sql, Consumer<byte[]> sink) {
        return run(c -> c.copyOut(sql, sink));
    }

    public CompletableFuture<byte[]> copyOut(String sql) {
        return run(c -> c.copyOut(sql));
    }

    /**
     * Runs {@code body} in a transaction on a connection of its own: {@code BEGIN}, then the stage {@code body} returns, then {@code COMMIT}
     * if it succeeded or {@code ROLLBACK} if it failed. Use the {@link PgClient} given to {@code body} for every query of the transaction.
     * With every connection in use by other transactions the call waits for one.
     * <p>
     * The {@code BEGIN} is sent in the same batch as the first query of {@code body} (one round trip fewer); if it fails, the server skips that query.
     * A body that sends no query sends no {@code BEGIN} and no {@code COMMIT}. Queries after the first are pipelined as usual, so they are already
     * on their way when the first one's answer comes back: a {@code BEGIN} that fails on a live connection (it practically does not) would let them
     * run outside a transaction.
     */
    public <T> CompletableFuture<T> transaction(Function<PgClient, ? extends CompletionStage<T>> body) {
        return transaction(TxOptions.defaults(), body);
    }

    /** As {@link #transaction(Function)} with an isolation level, read-only, deferrable. */
    public <T> CompletableFuture<T> transaction(TxOptions tx, Function<PgClient, ? extends CompletionStage<T>> body) {
        CompletableFuture<T> out = new CompletableFuture<>();
        acquire().whenComplete((slot, err) -> {
            if (err != null) {
                out.completeExceptionally(err);
                return;
            }
            awaitQuiet(slot);
            PgClient c = slot.client;
            c.armBegin(tx.beginSql());     // the BEGIN goes out with the first query of the body, not before it
            CompletableFuture<T> bodyDone;
            try {
                bodyDone = body.apply(c).toCompletableFuture();
            } catch (Throwable t) {
                bodyDone = CompletableFuture.failedFuture(t);
            }
            bodyDone.whenComplete((value, failure) -> {
                if (failure == null) {
                    c.endTransaction(true).whenComplete((r, e) -> {
                        releaseClean(slot, c);
                        if (e == null) {
                            out.complete(value);
                        } else {
                            out.completeExceptionally(unwrap(e));
                        }
                    });
                } else {
                    c.endTransaction(false).whenComplete((r, e) -> {
                        releaseClean(slot, c);
                        out.completeExceptionally(unwrap(failure));
                    });
                }
            });
        });
        return out;
    }

    /**
     * As {@link #transaction(TxOptions, Function)}, run again (up to {@code maxAttempts} times in all, with a short growing pause) when it fails with a serialization failure
     * (SQLSTATE 40001), a deadlock (40P01) or a cancel meant for another query (see {@link PgClient#query(Duration, String, Object...)}): at the SERIALIZABLE and REPEATABLE READ levels these are expected and the cure is to run the transaction again. {@code body} may
     * therefore run more than once, and must do nothing outside the database that cannot be repeated.
     */
    public <T> CompletableFuture<T> transactionWithRetry(TxOptions tx, int maxAttempts, Function<PgClient, ? extends CompletionStage<T>> body) {
        if (maxAttempts < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("maxAttempts"));
        }
        CompletableFuture<T> out = new CompletableFuture<>();
        attempt(tx, maxAttempts, 1, body, out);
        return out;
    }

    private <T> void attempt(TxOptions tx, int maxAttempts, int n, Function<PgClient, ? extends CompletionStage<T>> body, CompletableFuture<T> out) {
        transaction(tx, body).whenComplete((v, e) -> {
            if (e == null) {
                out.complete(v);
                return;
            }
            Throwable cause = unwrap(e);
            String state = cause instanceof PgException pe ? pe.sqlState() : null;
            boolean strayCancel = "57014".equals(state) && cause instanceof PgException pe && pe.retriable();      // cancelled by the time limit of another query: it did not complete
            if (n < maxAttempts && ("40001".equals(state) || "40P01".equals(state) || strayCancel) && !closed) {
                long pauseMillis = Math.min(1000, 5L << Math.min(n, 8)) + ThreadLocalRandom.current().nextLong(5);
                try {
                    group.next().schedule(() -> attempt(tx, maxAttempts, n + 1, body, out), pauseMillis, TimeUnit.MILLISECONDS);
                } catch (RuntimeException shutDown) {
                    out.completeExceptionally(cause);
                }
            } else {
                out.completeExceptionally(cause);
            }
        });
    }

    /** Closes every connection; queries in flight fail. */
    public CompletableFuture<Void> disconnect() {
        closed = true;
        Scheduled m = maintenance;
        if (m != null) {
            m.cancel();
        }
        List<CompletableFuture<Void>> all = new ArrayList<>();
        for (Slot s : slots) {
            PgClient c = s.client;
            if (c != null) {
                all.add(c.disconnect());
            }
        }
        CompletableFuture<Void> done = CompletableFuture.allOf(all.toArray(new CompletableFuture[0]));
        PgException gone = new PgException("the pool is closed", false);
        for (Waiter w; (w = waiters.poll()) != null; ) {
            cancelTimer(w.timer);
            mWaiting.add(-1);
            w.future.completeExceptionally(gone);
        }
        for (Work<?> w; (w = work.poll()) != null; ) {
            cancelTimer(w.timer);
            waiting.decrementAndGet();
            mWaiting.add(-1);
            w.result.completeExceptionally(gone);
        }
        return done;
    }

    @Override
    public void close() {
        disconnect();
    }

    // ---- choosing a connection ----

    private <T> CompletableFuture<T> run(Function<PgClient, CompletableFuture<T>> op) {
        Slot slot = pickSlot();
        if (slot != null) {
            try {
                return op.apply(slot.client);
            } finally {
                slot.dispatching.decrementAndGet();
            }
        }
        if (closed) {
            return CompletableFuture.failedFuture(new PgException("the pool is closed", false));
        }
        if (waiting.get() >= options.maxWaiters()) {
            return CompletableFuture.failedFuture(new PgException("too many calls wait for a connection (maxWaiters " + options.maxWaiters() + ")", true));
        }
        Work<T> w = new Work<>(op);
        w.start = mWait.now();
        waiting.incrementAndGet();
        mWaiting.add(1);
        work.add(w);
        Duration limit = options.acquireTimeout();
        if (limit != null) {
            w.timer = group.next().schedule(() -> {
                if (w.result.completeExceptionally(new PgException("no connection became free within " + limit.toMillis() + " ms", true)) && work.remove(w)) {
                    waiting.decrementAndGet();
                    mWaiting.add(-1);
                    mAcquireTimeouts.increment();
                }
            }, limit.toNanos(), TimeUnit.NANOSECONDS);
        }
        serveWork();      // one may have been freed while we queued
        return w.result;
    }

    private void serveWork() {
        for (;;) {
            Work<?> w = work.peek();
            if (w == null) {
                return;
            }
            Slot slot = pickSlot();
            if (slot == null) {
                return;
            }
            if (!work.remove(w)) {
                slot.dispatching.decrementAndGet();
                continue;
            }
            waiting.decrementAndGet();
            mWaiting.add(-1);
            cancelTimer(w.timer);
            try {
                if (!w.result.isDone()) {
                    mWait.endAt(w.start, mWait.now());
                    dispatch(w, slot.client);
                }
            } finally {
                slot.dispatching.decrementAndGet();
            }
        }
    }

    private <T> void dispatch(Work<T> w, PgClient c) {
        try {
            w.op.apply(c).whenComplete((v, e) -> {
                if (e == null) {
                    w.result.complete(v);
                } else {
                    w.result.completeExceptionally(unwrap(e));
                }
            });
        } catch (Throwable t) {
            w.result.completeExceptionally(t);
        }
    }

    private static void cancelTimer(Scheduled t) {
        if (t != null) {
            t.cancel();
        }
    }

    /**
     * The least busy connection that no transaction owns, with its {@code dispatching} count raised: the caller must lower it once its query is queued on the connection. Without
     * this a transaction could take the connection between the choice and the queuing, and the plain query would land in the middle of it (and, as the first query after the
     * transaction's {@code BEGIN}, carry it, and drag the transaction down if it failed). The count is raised before the {@code reserved} flag is read and a transaction sets the flag
     * before it reads the count, so one of the two always sees the other.
     */
    private Slot pickSlot() {
        for (int attempt = 0; attempt < 4; attempt++) {
            int n = slots.length;
            int start = Math.floorMod(rotor.getAndIncrement(), n);
            Slot best = null;
            int bestLoad = Integer.MAX_VALUE;
            for (int k = 0; k < n; k++) {
                Slot s = slots[(start + k) % n];
                PgClient c = s.client;
                if (c == null || !c.isOpen()) {
                    replace(s);
                    continue;
                }
                if (s.reserved.get()) {
                    continue;
                }
                int load = c.inflight();
                if (load < bestLoad || (load == bestLoad && c.onOwnLoop())) {
                    best = s;
                    bestLoad = load;
                    if (load == 0 && c.onOwnLoop()) {     // idle and on this very thread: written without waking another loop
                        break;
                    }
                }
            }
            if (best == null) {
                return null;
            }
            best.dispatching.incrementAndGet();
            if (!best.reserved.get() && best.client != null && best.client.isOpen()) {
                return best;
            }
            best.dispatching.decrementAndGet();      // a transaction took it meanwhile: choose again
        }
        return null;
    }

    /** A transaction owns {@code s}; wait until the plain calls that chose it before that have queued their queries (a matter of microseconds), so none can slip in after the BEGIN. */
    private static void awaitQuiet(Slot s) {
        int spins = 0;
        while (s.dispatching.get() != 0) {
            if (++spins > 1000) {
                Thread.yield();
            } else {
                Thread.onSpinWait();
            }
        }
    }

    // ---- connections coming and going ----

    private void adopt(Slot s, PgClient c) {
        s.client = c;
        s.born = System.nanoTime();
        s.lastPing = s.born;
        Duration life = options.maxLifetime();
        s.lifetimeNanos = life == null ? Long.MAX_VALUE : (long) (life.toNanos() * (0.9 + 0.2 * ThreadLocalRandom.current().nextDouble()));
    }

    /** Starts replacing a broken connection, unless one is being made or the last attempt was less than a second ago. */
    private void replace(Slot s) {
        PgClient old = s.client;
        if (closed || (old != null && old.isOpen()) || System.nanoTime() - s.lastAttempt < RECONNECT_GAP_NANOS && s.lastAttempt != 0) {
            return;
        }
        if (!s.connecting.compareAndSet(false, true)) {
            return;
        }
        s.lastAttempt = System.nanoTime();
        PgClient.connect(group, config, listener).whenComplete((c, err) -> {
            if (err == null) {
                if (closed) {
                    c.close();
                } else {
                    adopt(s, c);
                    mReplaced.increment();
                    serveWaiters();
                    serveWork();
                }
            }
            s.connecting.set(false);
        });
    }

    /** Replaces a healthy connection that has lived long enough: the new one is made first, so there is never a gap. */
    private void retire(Slot s) {
        PgClient old = s.client;
        if (!s.connecting.compareAndSet(false, true)) {
            return;
        }
        PgClient.connect(group, config, listener).whenComplete((c, err) -> {
            if (err == null && !closed) {
                adopt(s, c);
                mRetired.increment();
                closeWhenIdle(old, 0);
            } else if (err == null) {
                c.close();
            } else {
                s.born = System.nanoTime() - s.lifetimeNanos + TimeUnit.SECONDS.toNanos(30);     // could not connect: keep the old one and try again in 30 s
            }
            s.connecting.set(false);
        });
    }

    private void closeWhenIdle(PgClient old, int attempts) {
        if (old.inflight() == 0 || attempts >= 100) {
            old.close();
            return;
        }
        try {
            group.next().schedule(() -> closeWhenIdle(old, attempts + 1), 100, TimeUnit.MILLISECONDS);
        } catch (RuntimeException shutDown) {
            old.close();
        }
    }

    private void startMaintenance() {
        long periodMillis = 1000;       // always: a connection found dead is replaced even when nobody is calling
        if (options.pingInterval() != null) {
            periodMillis = Math.max(50, Math.min(periodMillis, options.pingInterval().toMillis() / 2));
        }
        scheduleMaintenance(periodMillis);
    }

    private void scheduleMaintenance(long periodMillis) {
        if (closed) {
            return;
        }
        try {
            maintenance = group.next().schedule(() -> {
                try {
                    maintain();
                } finally {
                    scheduleMaintenance(periodMillis);
                }
            }, periodMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException shutDown) {
            // the loops are gone
        }
    }

    private void maintain() {
        long now = System.nanoTime();
        Duration pingEvery = options.pingInterval();
        for (Slot s : slots) {
            PgClient c = s.client;
            if (c == null || !c.isOpen()) {
                replace(s);
                continue;
            }
            if (s.reserved.get()) {
                continue;
            }
            if (now - s.born > s.lifetimeNanos && c.inflight() == 0) {
                retire(s);
                continue;
            }
            if (pingEvery != null && c.inflight() == 0 && now - s.lastPing >= pingEvery.toNanos() && s.pinging.compareAndSet(false, true)) {
                s.lastPing = now;
                c.query(options.pingTimeout(), "SELECT 1").whenComplete((r, e) -> {
                    s.pinging.set(false);
                    Throwable cause = e == null ? null : unwrap(e);
                    if (cause != null && !(cause instanceof PgException pe && pe.sqlState() != null)) {     // no answer, or the connection broke: it is dead whatever it looks like
                        mPingsFailed.increment();
                        c.close();
                    }
                });
            }
        }
    }

    // ---- connections for transactions ----

    private CompletableFuture<Slot> acquire() {
        if (closed) {
            return CompletableFuture.failedFuture(new PgException("the pool is closed", false));
        }
        Slot s = tryReserve();
        if (s != null) {
            return CompletableFuture.completedFuture(s);
        }
        if (waiting.get() >= options.maxWaiters()) {
            return CompletableFuture.failedFuture(new PgException("too many calls wait for a connection (maxWaiters " + options.maxWaiters() + ")", true));
        }
        Waiter w = new Waiter();
        w.start = mWait.now();
        mWaiting.add(1);
        waiters.add(w);
        Duration limit = options.acquireTimeout();
        if (limit != null) {
            w.timer = group.next().schedule(() -> {
                if (w.future.completeExceptionally(new PgException("no connection became free for the transaction within " + limit.toMillis() + " ms", true))) {
                    waiters.remove(w);
                    mWaiting.add(-1);
                    mAcquireTimeouts.increment();
                }
            }, limit.toNanos(), TimeUnit.NANOSECONDS);
        }
        s = tryReserve();        // one may have been released while we queued
        if (s != null) {
            deliver(s);
        }
        return w.future;
    }

    private Slot tryReserve() {
        for (Slot s : slots) {
            PgClient c = s.client;
            if (c == null || !c.isOpen()) {
                replace(s);
                continue;
            }
            if (s.reserved.compareAndSet(false, true)) {
                if (c.isOpen()) {
                    return s;
                }
                s.reserved.set(false);
            }
        }
        return null;
    }

    /** Hands a reserved slot to the longest waiting transaction, or gives it back if nobody waits any more. */
    private void deliver(Slot s) {
        Waiter w = waiters.poll();
        if (w == null) {
            s.reserved.set(false);
            serveWork();
        } else {
            cancelTimer(w.timer);
            if (w.future.complete(s)) {
                mWaiting.add(-1);
                mWait.endAt(w.start, mWait.now());
            } else {
                deliver(s);
            }
        }
    }

    /**
     * Gives a transaction's connection back, but only if it is outside any transaction. If the COMMIT or ROLLBACK did not take (it failed, or the answer never came) the server may
     * still hold the transaction open; handing such a connection to the next caller would run its queries inside it, so it is closed instead (the server rolls an abandoned
     * transaction back) and the slot gets a new connection.
     */
    private void releaseClean(Slot s, PgClient c) {
        if (c.isOpen() && c.transactionStatus() != 'I') {
            c.close();
        }
        release(s);
    }

    private void release(Slot s) {
        if (closed) {
            s.reserved.set(false);
            return;
        }
        PgClient c = s.client;
        if (c == null || !c.isOpen()) {
            s.reserved.set(false);      // broken: queries go to the others until it is replaced
            serveWaiters();
            serveWork();
            return;
        }
        deliver(s);
    }

    private void serveWaiters() {
        while (!waiters.isEmpty()) {
            Slot s = tryReserve();
            if (s == null) {
                return;
            }
            deliver(s);
        }
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
