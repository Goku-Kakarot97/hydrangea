package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgPoolTest {

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

    private PgPool pool(int size) throws Exception {
        return Pg.get(PgPool.create(group, Pg.config(), size));
    }

    @Test
    void spreadsQueriesOverTheConnections() throws Exception {
        try (PgPool pool = pool(4)) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                fs.add(pool.query("SELECT pg_backend_pid(), pg_sleep(0.005), $1::int", i));
            }
            Set<Integer> pids = new HashSet<>();
            for (int i = 0; i < fs.size(); i++) {
                PgRow r = Pg.get(fs.get(i)).first();
                assertEquals(i, r.getInt(2));
                pids.add(r.getInt(0));
            }
            assertEquals(4, pids.size(), "every connection took part: " + pids);
        }
    }

    @Test
    void aFailedPoolCreationClosesWhatItOpened() throws Exception {
        Pg.notOnRemote();
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> PgPool.create(group, Pg.config().withPassword("nope"), 3).get(10, TimeUnit.SECONDS));
        assertEquals("28P01", ((PgException) e.getCause()).sqlState());
        assertThrows(ExecutionException.class, () -> PgPool.create(group, PgConfig.of("127.0.0.1", 1, "u", "d"), 2).get(10, TimeUnit.SECONDS));
    }

    @Test
    void aTransactionCommitsOrRollsBack() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(3)) {
            String t = Pg.table("tx");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int PRIMARY KEY, v text)"));
            try {
                String r = Pg.get(pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES (1, 'a')")
                        .thenCompose(x -> db.query("INSERT INTO " + t + " VALUES (2, 'b')"))
                        .thenApply(x -> "done " + x.updateCount())));
                assertEquals("done 1", r);
                assertEquals(2, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0));

                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES (3, 'c')")
                        .thenCompose(x -> db.query("INSERT INTO " + t + " VALUES (1, 'dup')"))).get(10, TimeUnit.SECONDS));
                assertEquals("23505", ((PgException) e.getCause()).sqlState());
                assertEquals(2, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0), "row 3 was rolled back");

                ExecutionException thrown = assertThrows(ExecutionException.class, () -> pool.transaction(db -> {
                    throw new IllegalStateException("boom");
                }).get(10, TimeUnit.SECONDS));
                assertTrue(thrown.getCause() instanceof IllegalStateException);
                assertEquals(1, Pg.get(pool.query("SELECT 1")).size(), "the connection went back to the pool in a clean state");
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void queriesOfOneTransactionDoNotMixWithOthersAndTransactionsWaitForAConnection() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(1)) {
            String t = Pg.table("tw");
            Pg.get(pool.execute("CREATE TABLE " + t + " (n int)"));
            try {
                List<CompletableFuture<Integer>> txs = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    int n = i;
                    txs.add(pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES ($1)", n)
                            .thenCompose(x -> db.query("SELECT count(*) FROM " + t))
                            .thenApply(r -> r.first().getInt(0))));
                }
                // one connection: each transaction runs alone, so each sees exactly the rows of the ones before plus its own
                for (int i = 0; i < 20; i++) {
                    assertEquals(i + 1, Pg.get(txs.get(i)));
                }
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void plainQueriesAvoidAConnectionThatIsInATransaction() throws Exception {
        try (PgPool pool = pool(2)) {
            CompletableFuture<Integer> hold = new CompletableFuture<>();
            CompletableFuture<Integer> tx = pool.transaction(db -> db.query("SELECT pg_backend_pid()").thenCompose(r -> {
                int pid = r.first().getInt(0);
                return hold.thenApply(x -> pid);
            }));
            Thread.sleep(200);
            for (int i = 0; i < 20; i++) {
                PgRow r = Pg.get(pool.query("SELECT pg_backend_pid()")).first();
                assertTrue(!hold.isDone(), "still holding");
                assertEquals(1, r.size());
            }
            Set<Integer> pids = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                pids.add(Pg.get(pool.query("SELECT pg_backend_pid()")).first().getInt(0));
            }
            hold.complete(0);
            int txPid = Pg.get(tx);
            assertTrue(!pids.contains(txPid), "plain queries never ran on the transaction's connection");
        }
    }

    @Test
    void aBrokenConnectionIsReplaced() throws Exception {
        try (PgPool pool = pool(2); PgClient admin = Pg.get(PgClient.connect(group, Pg.config()))) {
            Set<Integer> before = new HashSet<>();
            for (int i = 0; i < 40; i++) {
                before.add(Pg.get(pool.query("SELECT pg_backend_pid(), pg_sleep(0.002)")).first().getInt(0));
            }
            assertEquals(2, before.size());
            int victim = before.iterator().next();
            Pg.get(admin.query("SELECT pg_terminate_backend($1)", victim));
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Set<Integer> after = new HashSet<>();
            while (System.nanoTime() < end && (after.size() < 2 || after.contains(victim))) {
                after.clear();
                for (int i = 0; i < 40; i++) {
                    try {
                        after.add(pool.query("SELECT pg_backend_pid(), pg_sleep(0.002)").get(5, TimeUnit.SECONDS).first().getInt(0));
                    } catch (ExecutionException e) {
                        // the query that was on the broken connection, or none available for a moment
                    }
                }
                Thread.sleep(100);
            }
            assertEquals(2, after.size(), "two live connections again: " + after);
            assertTrue(!after.contains(victim));
        }
    }

    @Test
    void aClosedPoolRefuses() throws Exception {
        PgPool pool = pool(2);
        Pg.get(pool.disconnect());
        ExecutionException e = assertThrows(ExecutionException.class, () -> pool.query("SELECT 1").get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof PgException);
        assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("SELECT 1")).get(5, TimeUnit.SECONDS));
    }

    @Test
    void theTransactionBeginsWithItsFirstQueryAndSeesItsOwnState() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(2)) {
            String t = Pg.table("tb");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int PRIMARY KEY)"));
            try {
                char[] statuses = new char[3];
                Pg.get(pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES (1)").thenCompose(x -> {
                    statuses[0] = db.transactionStatus();
                    return db.query("SELECT count(*) FROM " + t);
                }).thenApply(r -> {
                    statuses[1] = db.transactionStatus();
                    return r.first().getInt(0);
                })));
                assertEquals('T', statuses[0], "in a transaction after the first query");
                assertEquals('T', statuses[1]);
                assertEquals(1, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0));

                // a query that fails to parse is the first one: the BEGIN it carried is rolled back with it
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("SELEC 1")).get(10, TimeUnit.SECONDS));
                assertEquals("42601", ((PgException) e.getCause()).sqlState());
                for (int i = 0; i < 20; i++) {
                    assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
                }
                // no connection was left in an open or aborted transaction
                Set<Character> after = new HashSet<>();
                try (PgClient probe = Pg.get(PgClient.connect(group, Pg.config()))) {
                    after.add(probe.transactionStatus());
                }
                assertEquals(Set.of('I'), after);
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void aTransactionThatSendsNoQueryDoesNothing() throws Exception {
        try (PgPool pool = pool(1)) {
            assertEquals("nothing", Pg.get(pool.transaction(db -> CompletableFuture.completedFuture("nothing"))));
            assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void concurrentTransactionsAreEachAtomic() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = pool(4)) {
            String t = Pg.table("ta");
            Pg.get(pool.execute("CREATE TABLE " + t + " (a int, b int)"));
            try {
                List<CompletableFuture<Integer>> txs = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    int n = i;
                    txs.add(pool.transaction(db -> {
                        var one = db.query("INSERT INTO " + t + " VALUES ($1, 1)", n);
                        var two = db.query("INSERT INTO " + t + " VALUES ($1, 2)", n);
                        if (n % 5 == 0) {
                            return one.thenCombine(two, (x, y) -> 0).thenCompose(z -> db.query("SELECT 1/0")).thenApply(r -> 0);   // fails: both rows must go
                        }
                        return one.thenCombine(two, (x, y) -> n);
                    }));
                }
                int ok = 0;
                for (var f : txs) {
                    try {
                        f.get(30, TimeUnit.SECONDS);
                        ok++;
                    } catch (ExecutionException expected) {
                        assertEquals("22012", ((PgException) expected.getCause()).sqlState());
                    }
                }
                assertEquals(160, ok);
                assertEquals(320, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0), "two rows for each of the 160 that committed, none for the rest");
                assertEquals(0, Pg.get(pool.query("SELECT count(*) FROM (SELECT a FROM " + t + " GROUP BY a HAVING count(*) <> 2) x")).first().getInt(0));
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }
}
