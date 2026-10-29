package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgPoolFeaturesTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private PgPool pool(int size, PgPoolOptions options) throws Exception {
        return Pg.get(PgPool.create(group, Pg.config(), size, options, null));
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(Pg.REMOTE ? 120 : 20, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    /** Holds the pool's only connection in a transaction until {@code release} completes. */
    private static CompletableFuture<Integer> hold(PgPool pool, CompletableFuture<Void> release) {
        return pool.transaction(db -> db.query("SELECT pg_backend_pid()").thenCompose(r -> release.thenApply(x -> r.first().getInt(0))));
    }

    // ---- waiting for a connection ----

    @Test
    void aCallWaitsForAConnectionThatATransactionHoldsInsteadOfFailing() throws Exception {
        try (PgPool pool = pool(1, PgPoolOptions.defaults())) {
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Integer> tx = hold(pool, release);
            Thread.sleep(200);
            CompletableFuture<PgResult> waiting = pool.query("SELECT 7");
            CompletableFuture<PgResult> alsoWaiting = pool.query("SELECT 8");
            Thread.sleep(300);
            assertFalse(waiting.isDone(), "the only connection is in a transaction");
            release.complete(null);
            Pg.get(tx);
            assertEquals(7, Pg.get(waiting).first().getInt(0));
            assertEquals(8, Pg.get(alsoWaiting).first().getInt(0));
        }
    }

    @Test
    void aCallThatWaitsTooLongFailsRetriably() throws Exception {
        try (PgPool pool = pool(1, PgPoolOptions.defaults().withAcquireTimeout(Duration.ofMillis(300)))) {
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Integer> tx = hold(pool, release);
            Thread.sleep(200);
            long t0 = System.nanoTime();
            PgException e = failure(pool.query("SELECT 1"));
            assertTrue(e.retriable(), e.toString());
            assertTrue(e.getMessage().contains("no connection became free"), e.getMessage());
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000);
            PgException txe = failure(pool.transaction(db -> db.query("SELECT 1")));
            assertTrue(txe.retriable() && txe.getMessage().contains("transaction"), txe.getMessage());
            release.complete(null);
            Pg.get(tx);
            assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0), "and the pool is fine afterwards");
        }
    }

    @Test
    void tooManyWaitersFailAtOnce() throws Exception {
        try (PgPool pool = pool(1, PgPoolOptions.defaults().withMaxWaiters(2))) {
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Integer> tx = hold(pool, release);
            Thread.sleep(200);
            CompletableFuture<PgResult> a = pool.query("SELECT 1");
            CompletableFuture<PgResult> b = pool.query("SELECT 2");
            PgException e = failure(pool.query("SELECT 3"));
            assertTrue(e.retriable() && e.getMessage().contains("maxWaiters"), e.getMessage());
            release.complete(null);
            Pg.get(tx);
            assertEquals(1, Pg.get(a).first().getInt(0));
            assertEquals(2, Pg.get(b).first().getInt(0));
        }
    }

    @Test
    void closingThePoolFailsWhoWaits() throws Exception {
        PgPool pool = pool(1, PgPoolOptions.defaults());
        CompletableFuture<Void> release = new CompletableFuture<>();
        hold(pool, release);
        Thread.sleep(200);
        CompletableFuture<PgResult> waiting = pool.query("SELECT 1");
        Pg.get(pool.disconnect());
        assertTrue(failure(waiting).getMessage().contains("closed"));
    }

    // ---- connection lifetime ----

    @Test
    void anOldConnectionIsReplacedWithoutAnyCallFailing() throws Exception {
        PgPoolOptions opts = PgPoolOptions.defaults().withMaxLifetime(Duration.ofMillis(700)).withPing(Duration.ofMillis(300), Duration.ofSeconds(5));
        try (PgPool pool = pool(2, opts)) {
            Set<Integer> pids = new HashSet<>();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            int calls = 0;
            while (System.nanoTime() < end) {
                pids.add(Pg.get(pool.query("SELECT pg_backend_pid()")).first().getInt(0));
                calls++;
                Thread.sleep(20);
            }
            assertTrue(pids.size() >= 4, "two connections, replaced at least once each: " + pids);
            assertTrue(calls > (Pg.REMOTE ? 20 : 100), "calls made: " + calls);        // a remote server answers more slowly, so fewer calls fit in the five seconds
        }
    }

    // ---- transactions ----

    @Test
    void aTransactionCanChooseItsIsolationAndReadOnly() throws Exception {
        try (PgPool pool = pool(2, PgPoolOptions.defaults())) {
            assertEquals("serializable", Pg.get(pool.transaction(TxOptions.of(TxOptions.Isolation.SERIALIZABLE), db -> db.query("SHOW transaction_isolation"))).first().getString(0));
            assertEquals("repeatable read", Pg.get(pool.transaction(TxOptions.of(TxOptions.Isolation.REPEATABLE_READ), db -> db.query("SHOW transaction_isolation"))).first().getString(0));
            assertEquals("read committed", Pg.get(pool.transaction(db -> db.query("SHOW transaction_isolation"))).first().getString(0));
            PgResult ro = Pg.get(pool.transaction(TxOptions.defaults().asReadOnly(), db -> db.query("SHOW transaction_read_only")));
            assertEquals("on", ro.first().getString(0));
            PgException e = failure(pool.transaction(TxOptions.defaults().asReadOnly(), db -> db.execute("CREATE TEMP TABLE nope (a int)")));
            assertEquals("25006", e.sqlState(), "the server refuses a write in a read-only transaction");
            PgResult def = Pg.get(pool.transaction(TxOptions.of(TxOptions.Isolation.SERIALIZABLE).asDeferrable(), db -> db.query("SHOW transaction_deferrable")));
            assertEquals("on", def.first().getString(0));
            assertEquals("off", Pg.get(pool.query("SHOW transaction_read_only")).first().getString(0), "and the next plain query is not in it");
            assertThrows(IllegalArgumentException.class, () -> new TxOptions(null, false, true));
        }
    }

    @Test
    void aTransactionIsRunAgainOnSerializationFailureAndDeadlock() throws Exception {
        try (PgPool pool = pool(2, PgPoolOptions.defaults())) {
            AtomicInteger attempts = new AtomicInteger();
            String r = Pg.get(pool.<String>transactionWithRetry(TxOptions.of(TxOptions.Isolation.SERIALIZABLE), 5, db -> {
                int n = attempts.incrementAndGet();
                String code = n == 1 ? "40001" : "40P01";
                if (n <= 2) {
                    return db.execute("DO $$ BEGIN RAISE EXCEPTION 'conflict' USING ERRCODE = '" + code + "'; END $$").thenApply(x -> "unreachable");
                }
                return db.query("SELECT 'ok on attempt ' || $1::int", n).thenApply(x -> x.first().getString(0));
            }));
            assertEquals("ok on attempt 3", r);
            assertEquals(3, attempts.get());

            AtomicInteger other = new AtomicInteger();
            PgException e = failure(pool.transactionWithRetry(TxOptions.defaults(), 5, db -> {
                other.incrementAndGet();
                return db.query("SELECT 1/0");
            }));
            assertEquals("22012", e.sqlState());
            assertEquals(1, other.get(), "other errors are not retried");

            AtomicInteger tries = new AtomicInteger();
            PgException last = failure(pool.transactionWithRetry(TxOptions.defaults(), 3, db -> {
                tries.incrementAndGet();
                return db.execute("DO $$ BEGIN RAISE EXCEPTION 'conflict' USING ERRCODE = '40001'; END $$");
            }));
            assertEquals("40001", last.sqlState());
            assertEquals(3, tries.get(), "gives up after maxAttempts and reports the last error");
        }
    }

    @Test
    void realWriteSkewUnderSerializableIsResolvedByTheRetry() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(4, PgPoolOptions.defaults())) {
            String t = Pg.table("ws");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id serial PRIMARY KEY, seen int)"));
            try {
                // each transaction counts the rows and then inserts one that records the count it saw: without serializability both see 0
                List<CompletableFuture<Long>> txs = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    txs.add(pool.<Long>transactionWithRetry(TxOptions.of(TxOptions.Isolation.SERIALIZABLE), 10, db ->
                            db.query("SELECT count(*), pg_sleep(0.3) FROM " + t)
                                    .thenCompose(r -> db.query("INSERT INTO " + t + " (seen) VALUES ($1)", r.first().getInt(0)))
                                    .thenApply(PgResult::updateCount)));
                }
                for (var f : txs) {
                    assertEquals(1L, Pg.get(f).longValue());
                }
                List<Integer> seen = new ArrayList<>();
                for (PgRow row : Pg.get(pool.query("SELECT seen FROM " + t + " ORDER BY seen")).rows()) {
                    seen.add(row.getInt(0));
                }
                assertEquals(List.of(0, 1), seen, "one saw nothing, the other saw the first: a serial order");
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void savepointsWorkInsideATransaction() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(2, PgPoolOptions.defaults())) {
            String t = Pg.table("sv");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int PRIMARY KEY)"));
            try {
                Long n = Pg.get(pool.<Long>transaction(db -> db.query("INSERT INTO " + t + " VALUES (1)")
                        .thenCompose(a -> db.execute("SAVEPOINT s1"))
                        .thenCompose(a -> db.query("INSERT INTO " + t + " VALUES (1)").handle((r, e) -> e))     // the duplicate fails: the transaction is aborted until the savepoint
                        .thenCompose(failed -> {
                            assertTrue(failed != null);
                            return db.execute("ROLLBACK TO SAVEPOINT s1");
                        })
                        .thenCompose(a -> db.query("INSERT INTO " + t + " VALUES (2)"))
                        .thenCompose(a -> db.query("SELECT count(*) FROM " + t))
                        .thenApply(r -> r.first().getLong(0))));
                assertEquals(2L, n.longValue());
                assertEquals(2, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0), "committed: the rows 1 and 2");
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }
}
