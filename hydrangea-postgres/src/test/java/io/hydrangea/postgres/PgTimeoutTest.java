package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Query time limits and the cancel request, against a real PostgreSQL. */
class PgTimeoutTest {

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

    private PgClient connect(PgConfig config) throws Exception {
        return Pg.get(PgClient.connect(group, config));
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(15, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    private static long millisSince(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    @Test
    void aSlowQueryTimesOutAndTheServerStopsRunningIt() throws Exception {
        try (PgClient c = connect(Pg.config())) {
            long t0 = System.nanoTime();
            PgException e = failure(c.query(Duration.ofMillis(300), "SELECT pg_sleep(30)"));
            assertTrue(e.isTimeout(), e.toString());
            assertFalse(e.retriable(), "it was sent: it may have run");
            assertTrue(millisSince(t0) < 3000, "the caller is not kept waiting: " + millisSince(t0));
            // the connection is free again quickly only if the server really stopped the sleep (it would otherwise be busy for 30 s)
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
            assertTrue(millisSince(t0) < 5000, "the cancel reached the backend: " + millisSince(t0));
            assertEquals('I', c.transactionStatus());
        }
    }

    @Test
    void theConfiguredLimitAppliesToQueryAndExecuteAndEachCallCanOverrideIt() throws Exception {
        try (PgClient c = connect(Pg.config().withQueryTimeout(Duration.ofMillis(300)))) {
            assertTrue(failure(c.query("SELECT pg_sleep(0.8)")).isTimeout());
            assertTrue(failure(c.execute("SELECT pg_sleep(0.8)")).isTimeout());
            assertEquals(1, c.query(Duration.ofSeconds(10), "SELECT pg_sleep(0.8), 1").get(15, TimeUnit.SECONDS).first().getInt(1), "its own, longer limit");
            assertEquals(1, c.query((Duration) null, "SELECT pg_sleep(0.8), 1").get(15, TimeUnit.SECONDS).first().getInt(1), "no limit");
            assertEquals(5, c.query("SELECT 5").get(10, TimeUnit.SECONDS).first().getInt(0), "quick queries are not affected");
        }
        try (PgClient c = connect(Pg.config())) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query(Duration.ZERO, "SELECT 1").get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    void queriesPipelinedBehindATimedOutOneAreNotHurtByTheCancel() throws Exception {
        try (PgClient c = connect(Pg.config())) {
            CompletableFuture<PgResult> slow = c.query(Duration.ofMillis(300), "SELECT pg_sleep(30)");
            List<CompletableFuture<PgResult>> behind = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                behind.add(c.query("SELECT $1::int", i));
            }
            assertTrue(failure(slow).isTimeout());
            for (int i = 0; i < 50; i++) {
                assertEquals(i, behind.get(i).get(15, TimeUnit.SECONDS).first().getInt(0), "query " + i);
            }
        }
    }

    @Test
    void aQueryWaitingBehindASlowOneTimesOutButTheSlowOneIsLeftAlone() throws Exception {
        try (PgClient c = connect(Pg.config())) {
            CompletableFuture<PgResult> slow = c.query("SELECT pg_sleep(1.2), 'slow done'");
            CompletableFuture<PgResult> waiting = c.query(Duration.ofMillis(200), "SELECT pg_sleep(0.01), 'late'");
            long t0 = System.nanoTime();
            assertTrue(failure(waiting).isTimeout());
            assertTrue(millisSince(t0) < 1000, "the waiting query gave up on time, not when the slow one ended: " + millisSince(t0));
            assertFalse(slow.isDone(), "the slow query was not cancelled for it");
            assertEquals("slow done", slow.get(15, TimeUnit.SECONDS).first().getString(1));
            // the cancel for the waiting query goes out when its turn comes; if it lands on the next query that one fails with an error that says so
            int ok = 0;
            for (int i = 0; i < 10; i++) {
                try {
                    c.query("SELECT 1").get(10, TimeUnit.SECONDS);
                    ok++;
                } catch (ExecutionException e) {
                    assertTrue(((PgException) e.getCause()).retriable(), "a stray cancel is reported as retriable: " + e.getCause());
                }
            }
            assertTrue(ok >= 9, "at most one query was hit: " + ok);
        }
    }

    @Test
    void aCancelOnlyHitsTheBackendOfItsOwnConnection() throws Exception {
        try (PgClient a = connect(Pg.config()); PgClient b = connect(Pg.config())) {
            CompletableFuture<PgResult> other = b.query("SELECT pg_sleep(1.5), 'b done'");
            Thread.sleep(100);
            assertTrue(failure(a.query(Duration.ofMillis(300), "SELECT pg_sleep(30)")).isTimeout());
            assertEquals("b done", other.get(15, TimeUnit.SECONDS).first().getString(1), "the other session's query was not cancelled");
        }
    }

    @Test
    void manyQuickQueriesWithALimitAreAllAnswered() throws Exception {
        try (PgClient c = connect(Pg.config().withQueryTimeout(Duration.ofSeconds(5)))) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                fs.add(c.query("SELECT $1::int", i));
            }
            for (int i = 0; i < fs.size(); i++) {
                assertEquals(i, fs.get(i).get(15, TimeUnit.SECONDS).first().getInt(0));
            }
            Thread.sleep(200);
            assertEquals(0, c.inflight());
        }
    }

    @Test
    void aTimeoutInsideATransactionRollsItBack() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config().withQueryTimeout(Duration.ofMillis(400)), 2))) {
            String t = Pg.table("to");
            Pg.get(pool.execute("CREATE TABLE " + t + " (n int)"));
            try {
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES (1)")
                        .thenCompose(x -> db.query("SELECT pg_sleep(30)"))).get(20, TimeUnit.SECONDS));
                assertTrue(((PgException) e.getCause()).isTimeout(), e.getCause().toString());
                assertEquals(0, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0), "the insert was rolled back with the transaction");
                for (int i = 0; i < 20; i++) {
                    assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
                }
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void aCopyIsNotSubjectToTheLimit() throws Exception {
        try (PgClient c = connect(Pg.config().withQueryTimeout(Duration.ofMillis(300)))) {
            String t = Pg.table("tc");
            Pg.get(c.execute(Duration.ofSeconds(10), "CREATE TEMP TABLE " + t + " (n int)"));
            PgCopyIn copy = Pg.get(c.copyIn("COPY " + t + " FROM STDIN"));
            Thread.sleep(700);
            Pg.get(copy.write("1\n".getBytes()));
            assertEquals(1L, Pg.get(copy.finish()).longValue());
        }
    }

    @Test
    void anExpiredLimitIsCountedAndAsksForOneCancel() throws Exception {
        try (EventLoopGroup g = new EventLoopGroup(1)) {
            java.util.Map<String, java.util.function.DoubleSupplier> meters = new java.util.HashMap<>();
            g.metrics().bindTo(new io.hydrangea.MetricsSink() {
                public void counterFunction(String name, String unit, java.util.function.DoubleSupplier total, String... tags) {
                    meters.put(tags.length == 0 ? name : name + "{" + String.join(",", tags) + "}", total);
                }

                public void gauge(String name, String unit, java.util.function.DoubleSupplier value, String... tags) {
                    meters.put(name, value);
                }

                public Counter counter(String name, String unit, String... tags) {
                    return NOOP.counter(name, unit, tags);
                }

                public Timer timer(String name, String... tags) {
                    return NOOP.timer(name, tags);
                }
            });
            try (PgClient c = Pg.get(PgClient.connect(g, Pg.config()))) {
                assertTrue(failure(c.query(Duration.ofMillis(200), "SELECT pg_sleep(30)")).isTimeout());
                c.query("SELECT 1").get(10, TimeUnit.SECONDS);
                assertEquals(1.0, meters.get("pg.queries{outcome,timeout}").getAsDouble());
                assertEquals(1.0, meters.get("pg.cancels").getAsDouble());
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (meters.get("pg.queries.pending").getAsDouble() != 0 && System.nanoTime() < end) {
                    Thread.sleep(10);
                }
                assertEquals(0.0, meters.get("pg.queries.pending").getAsDouble(), "the dropped answer left nothing pending");
                assertNotNull(meters.get("pg.statements.replanned"));
            }
        }
    }
}
