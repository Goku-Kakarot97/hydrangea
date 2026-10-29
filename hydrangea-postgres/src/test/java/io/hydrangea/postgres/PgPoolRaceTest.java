package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A plain call and a transaction both want the same connection: the plain call must not land inside the transaction. Half of the plain calls here fail on purpose; had one been
 * queued behind a transaction's BEGIN it would have carried it and, failing, aborted the transaction of somebody else (SQLSTATE 25P02 for that transaction's statements).
 */
class PgPoolRaceTest {

    @Test
    void aFailingPlainCallNeverSpoilsATransactionOnTheSameConnection() throws Exception {
        Pg.notOnRemote();               // a heavy load, for a database of our own
        Pg.require(Pg.PORT);
        try (EventLoopGroup group = new EventLoopGroup(2); PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 4))) {
            AtomicBoolean stop = new AtomicBoolean();
            ConcurrentHashMap<String, AtomicLong> problems = new ConcurrentHashMap<>();
            AtomicLong plain = new AtomicLong(), transactions = new AtomicLong();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < 27; w++) {
                boolean isTx = w < 3;                 // a few transactions among many plain calls, so the connections are mostly free for the plain ones to choose
                threads.add(Thread.ofVirtual().start(() -> {
                    while (!stop.get()) {
                        try {
                            if (isTx) {
                                pool.<Integer>transaction(db -> db.query("SELECT 1").thenCompose(a -> db.query("SELECT 2")).thenApply(r -> r.first().getInt(0))).get(30, TimeUnit.SECONDS);
                                transactions.incrementAndGet();
                            } else {
                                try {
                                    pool.query("SELECT 1/0").get(30, TimeUnit.SECONDS);
                                    problems.computeIfAbsent("division by zero succeeded", k -> new AtomicLong()).incrementAndGet();
                                } catch (ExecutionException e) {
                                    if (e.getCause() instanceof PgException pe && "22012".equals(pe.sqlState())) {
                                        plain.incrementAndGet();
                                    } else {
                                        throw e;
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            Throwable c = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
                            problems.computeIfAbsent((isTx ? "transaction: " : "plain: ") + c, k -> new AtomicLong()).incrementAndGet();
                        }
                    }
                }));
            }
            Thread.sleep(4000);
            stop.set(true);
            for (Thread t : threads) {
                t.join(30_000);
            }
            assertTrue(problems.isEmpty(), "problems: " + problems);
            assertTrue(plain.get() > 3000 && transactions.get() > 300, "the load really ran: " + plain + " failing calls, " + transactions + " transactions");
            assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void timeoutsAndTheirCancelsNeverLeaveAConnectionInsideATransaction() throws Exception {
        Pg.notOnRemote();
        Pg.require(Pg.PORT);
        try (EventLoopGroup group = new EventLoopGroup(2); PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 4))) {
            AtomicBoolean stop = new AtomicBoolean();
            ConcurrentHashMap<String, AtomicLong> problems = new ConcurrentHashMap<>();
            AtomicLong done = new AtomicLong(), timeouts = new AtomicLong(), strays = new AtomicLong();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < 28; w++) {
                int kind = w < 4 ? 0 : w < 12 ? 1 : 2;           // transactions, queries that time out, plain queries
                threads.add(Thread.ofVirtual().start(() -> {
                    java.util.Random rnd = new java.util.Random(Thread.currentThread().threadId());
                    while (!stop.get()) {
                        try {
                            if (kind == 0) {
                                pool.<Integer>transactionWithRetry(TxOptions.defaults(), 5, db -> db.query("SELECT 1").thenCompose(a -> db.query("SELECT 2")).thenApply(r -> r.first().getInt(0))).get(30, TimeUnit.SECONDS);
                            } else if (kind == 1) {
                                pool.query(java.time.Duration.ofMillis(40), "SELECT pg_sleep($1::float8)", rnd.nextInt(3) * 0.04).get(30, TimeUnit.SECONDS);
                            } else {
                                pool.query("SELECT $1::int", rnd.nextInt(100)).get(30, TimeUnit.SECONDS);
                            }
                            done.incrementAndGet();
                        } catch (ExecutionException e) {
                            if (e.getCause() instanceof PgException pe && pe.isTimeout()) {
                                timeouts.incrementAndGet();
                            } else if (e.getCause() instanceof PgException pe && pe.retriable() && "57014".equals(pe.sqlState())) {
                                strays.incrementAndGet();         // a cancel meant for another query, reported as the documented retriable error
                            } else {
                                problems.computeIfAbsent(e.getCause().toString(), k -> new AtomicLong()).incrementAndGet();
                            }
                        } catch (Throwable t) {
                            problems.computeIfAbsent(t.toString(), k -> new AtomicLong()).incrementAndGet();
                        }
                    }
                }));
            }
            Thread.sleep(8000);
            stop.set(true);
            for (Thread t : threads) {
                t.join(30_000);
            }
            assertTrue(problems.isEmpty(), "problems (a connection left in a transaction shows as 25P02): " + problems);
            assertTrue(done.get() > 1000 && timeouts.get() > 50, "the load ran: " + done + " ok, " + timeouts + " timeouts, " + strays + " stray cancels");
            assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
        }
    }
}
