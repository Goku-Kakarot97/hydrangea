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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgBatchTest {

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

    private PgClient connect() throws Exception {
        return Pg.get(PgClient.connect(group, Pg.config()));
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(Pg.REMOTE ? 120 : 20, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    private static List<Object[]> rows(int n) {
        List<Object[]> sets = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            sets.add(new Object[]{i, "name " + i});
        }
        return sets;
    }

    @Test
    void insertsTenThousandRowsInOneRoundTripAndReportsEachCount() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("bt");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int PRIMARY KEY, name text)"));
            long[] counts = Pg.get(c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", rows(10_000)));
            assertEquals(10_000, counts.length);
            for (long n : counts) {
                assertEquals(1, n);
            }
            assertEquals(10_000, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0));
            assertEquals("name 9999", Pg.get(c.query("SELECT name FROM " + t + " WHERE id = $1", 9999)).first().getString(0));
        }
    }

    @Test
    void eachItemHasItsOwnCount() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("bu");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, v int); INSERT INTO " + t + " VALUES (1, 0), (2, 0), (2, 0), (4, 0)"));
            long[] counts = Pg.get(c.executeBatch("UPDATE " + t + " SET v = v + 1 WHERE id = $1", List.of(new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4})));
            assertArrayEquals(new long[]{1, 2, 0, 1}, counts);
            long[] deleted = Pg.get(c.executeBatch("DELETE FROM " + t + " WHERE id = $1", List.of(new Object[]{2}, new Object[]{2})));
            assertArrayEquals(new long[]{2, 0}, deleted);
        }
    }

    @Test
    void aFailingItemFailsTheWholeBatchAndNothingIsKept() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("bf");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int PRIMARY KEY, name text)"));
            Pg.get(c.query("INSERT INTO " + t + " VALUES (3, 'already')"));
            List<Object[]> sets = List.of(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"}, new Object[]{3, "dup"}, new Object[]{4, "e"});
            PgException e = failure(c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", sets));
            assertEquals("23505", e.sqlState());
            assertTrue(e.getMessage().startsWith("batch item 3:"), e.getMessage());
            assertEquals(1, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0), "items 0-2 were rolled back with the rest");
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertEquals('I', c.transactionStatus());
        }
    }

    @Test
    void emptyAndMalformedBatches() throws Exception {
        try (PgClient c = connect()) {
            assertEquals(0, Pg.get(c.executeBatch("SELECT 1", List.of())).length);
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.executeBatch("SELECT $1::int + $2::int",
                    List.of(new Object[]{1, 2}, new Object[]{3})).get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void itemsMayHaveNullsAndOtherTypesAfterTheFirst() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("bn");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (a int, b text)"));
            long[] counts = Pg.get(c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", List.of(new Object[]{null, "x"}, new Object[]{1, null}, new Object[]{2L, "z"})));
            assertArrayEquals(new long[]{1, 1, 1}, counts);
            assertEquals(1, Pg.get(c.query("SELECT count(*) FROM " + t + " WHERE a IS NULL")).first().getInt(0));
        }
    }

    @Test
    void worksWithoutAStatementCacheAndPipelinedAmongOtherQueries() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withStatementCacheSize(0)))) {
            String t = Pg.table("bp");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, name text)"));
            var before = c.query("SELECT 'before'");
            var batch = c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", rows(500));
            var after = c.query("SELECT count(*) FROM " + t);
            assertEquals("before", Pg.get(before).first().getString(0));
            assertEquals(500, Pg.get(batch).length);
            assertEquals(500, Pg.get(after).first().getInt(0), "queued behind the batch, it saw its rows");
        }
    }

    @Test
    void aBatchInATransactionRollsBackWithIt() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 2))) {
            String t = Pg.table("bx");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int, name text)"));
            try {
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", rows(100))
                        .thenCompose(x -> db.query("SELECT 1/0"))).get(20, TimeUnit.SECONDS));
                assertEquals("22012", ((PgException) e.getCause()).sqlState());
                assertEquals(0, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0));
                long[] counts = Pg.get(pool.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", rows(100)));
                assertEquals(100, counts.length);
                assertEquals(100, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0));
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void theTimeLimitCoversTheWholeBatchAndTheServerIsAskedToStop() throws Exception {
        try (PgClient c = connect()) {
            List<Object[]> sets = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                sets.add(new Object[]{0.5});
            }
            long t0 = System.nanoTime();
            PgException e = failure(c.executeBatch(Duration.ofMillis(400), "SELECT pg_sleep($1::float8)", sets));
            assertTrue(e.isTimeout());
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 5000, "not the 10 seconds the batch would take");
        }
    }
}
