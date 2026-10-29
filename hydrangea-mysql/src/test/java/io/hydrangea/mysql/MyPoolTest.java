package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MyPoolTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        My.require(My.PORT);
        group = new EventLoopGroup(4);
    }

    @AfterAll
    static void stop() {
        if (group != null) {
            group.close();
        }
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(15, TimeUnit.SECONDS);
    }

    static MyPool pool(int n) throws Exception {
        return await(MyPool.create(group, My.config(), n));
    }

    @Test
    void queriesSpreadOverTheConnections() throws Exception {
        try (MyPool p = pool(4)) {
            List<CompletableFuture<MyResult>> fs = new ArrayList<>();
            for (int i = 0; i < 400; i++) {
                fs.add(p.query("SELECT CONNECTION_ID() AS id, ? AS i", i));
            }
            java.util.Set<Long> ids = new java.util.HashSet<>();
            for (int i = 0; i < 400; i++) {
                MyRow r = await(fs.get(i)).first();
                assertEquals(i, r.getInt("i"));
                ids.add(r.getLong("id"));
            }
            assertEquals(4, ids.size(), "every connection did some work");
        }
    }

    @Test
    void aTransactionCommitsOnItsOwnConnectionAndRollsBackOnFailure() throws Exception {
        try (MyPool p = pool(3)) {
            await(p.query("DROP TABLE IF EXISTS t_tx"));
            await(p.query("CREATE TABLE t_tx (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"));
            await(p.query("INSERT INTO t_tx VALUES (1, 10), (2, 20)"));
            Long moved = await(p.transaction(db -> db.query("UPDATE t_tx SET v = v - 5 WHERE id = ?", 1)
                    .thenCompose(a -> db.query("UPDATE t_tx SET v = v + 5 WHERE id = ?", 2))
                    .thenCompose(b -> db.query("SELECT SUM(v) FROM t_tx")).thenApply(r -> r.first().getLong(0))));
            assertEquals(30L, moved);
            assertEquals(5, await(p.query("SELECT v FROM t_tx WHERE id = 1")).first().getInt(0));
            ExecutionException e = assertThrows(ExecutionException.class, () -> await(p.transaction(db -> db.query("UPDATE t_tx SET v = 0")
                    .thenCompose(a -> db.query("INSERT INTO t_tx VALUES (1, 99)")))));
            assertTrue(((MyException) e.getCause()).isDuplicateKey());
            assertEquals(5, await(p.query("SELECT v FROM t_tx WHERE id = 1")).first().getInt(0), "the failed transaction was rolled back");
            assertEquals(25, await(p.query("SELECT v FROM t_tx WHERE id = 2")).first().getInt(0));
            await(p.query("DROP TABLE t_tx"));
        }
    }

    @Test
    void plainCallsNeverLandInsideATransaction() throws Exception {
        try (MyPool p = pool(2)) {
            await(p.query("DROP TABLE IF EXISTS t_iso"));
            await(p.query("CREATE TABLE t_iso (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"));
            List<CompletableFuture<MyResult>> plain = new ArrayList<>();
            List<CompletableFuture<Long>> txs = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                int id = i;
                txs.add(p.transaction(db -> db.query("INSERT INTO t_iso VALUES (?, 1)", id).thenCompose(x -> db.query("SELECT 1")).thenApply(r -> 1L)));
                plain.add(p.query("SELECT COUNT(*) FROM t_iso"));
                plain.add(p.query("INSERT INTO t_iso VALUES (?, 2)", 1000 + i));
            }
            for (CompletableFuture<Long> t : txs) {
                await(t);
            }
            for (CompletableFuture<MyResult> f : plain) {
                await(f);
            }
            assertEquals(600, await(p.query("SELECT COUNT(*) FROM t_iso")).first().getInt(0));
            assertEquals(300, await(p.query("SELECT COUNT(*) FROM t_iso WHERE v = 2")).first().getInt(0));
            await(p.query("DROP TABLE t_iso"));
        }
    }

    @Test
    void moreTransactionsThanConnectionsWait() throws Exception {
        try (MyPool p = pool(2)) {
            List<CompletableFuture<Long>> fs = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                fs.add(p.transaction(db -> db.query("SELECT SLEEP(0.01)").thenCompose(x -> db.query("SELECT CONNECTION_ID()")).thenApply(r -> r.first().getLong(0))));
            }
            java.util.Set<Long> conns = new java.util.HashSet<>();
            for (CompletableFuture<Long> f : fs) {
                conns.add(await(f));
            }
            assertEquals(2, conns.size());
        }
    }

    @Test
    void readOnlyAndIsolation() throws Exception {
        try (MyPool p = pool(2)) {
            await(p.query("DROP TABLE IF EXISTS t_ro"));
            await(p.query("CREATE TABLE t_ro (id INT PRIMARY KEY) ENGINE=InnoDB"));
            ExecutionException e = assertThrows(ExecutionException.class,
                    () -> await(p.transaction(MyTxOptions.defaults().asReadOnly(), db -> db.query("INSERT INTO t_ro VALUES (1)"))));
            assertEquals(1792, ((MyException) e.getCause()).errorCode());     // ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
            String level = await(p.transaction(MyTxOptions.of(MyTxOptions.Isolation.SERIALIZABLE), db -> db.query("SELECT ISOLATION_LEVEL FROM performance_schema.events_transactions_current WHERE THREAD_ID = PS_CURRENT_THREAD_ID()"))).first().getString(0);
            assertEquals("SERIALIZABLE", level);
            String after = await(p.transaction(db -> db.query("SELECT ISOLATION_LEVEL FROM performance_schema.events_transactions_current WHERE THREAD_ID = PS_CURRENT_THREAD_ID()"))).first().getString(0);
            assertEquals("REPEATABLE READ", after, "the level was for one transaction only");
            await(p.query("DROP TABLE t_ro"));
        }
    }

    @Test
    void deadlocksAreRetried() throws Exception {
        try (MyPool p = pool(2)) {
            await(p.query("DROP TABLE IF EXISTS t_dl"));
            await(p.query("CREATE TABLE t_dl (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"));
            await(p.query("INSERT INTO t_dl VALUES (1, 0), (2, 0)"));
            int rounds = 30;
            List<CompletableFuture<Integer>> fs = new ArrayList<>();
            for (int i = 0; i < rounds; i++) {
                boolean up = i % 2 == 0;
                fs.add(p.transactionWithRetry(MyTxOptions.defaults(), 20, db -> db.query("UPDATE t_dl SET v = v + 1 WHERE id = ?", up ? 1 : 2)
                        .thenCompose(a -> db.query("SELECT SLEEP(0.005)"))
                        .thenCompose(a -> db.query("UPDATE t_dl SET v = v + 1 WHERE id = ?", up ? 2 : 1)).thenApply(r -> 1)));
            }
            for (CompletableFuture<Integer> f : fs) {
                await(f);
            }
            assertEquals(2 * rounds, await(p.query("SELECT SUM(v) FROM t_dl")).first().getInt(0), "every transaction took effect exactly once");
            await(p.query("DROP TABLE t_dl"));
        }
    }

    @Test
    void aBrokenConnectionIsReplaced() throws Exception {
        try (MyPool p = pool(2); MyClient admin = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS)) {
            long victim = await(p.query("SELECT CONNECTION_ID()")).first().getLong(0);
            await(admin.query("KILL " + victim));
            boolean healed = false;
            for (int i = 0; i < 100 && !healed; i++) {
                try {
                    await(p.query("SELECT 1"));
                    healed = true;
                } catch (ExecutionException e) {
                    Thread.sleep(50);
                }
            }
            assertTrue(healed);
            Thread.sleep(1500);
            java.util.Set<Long> ids = new java.util.HashSet<>();
            for (int i = 0; i < 100; i++) {
                ids.add(await(p.query("SELECT CONNECTION_ID()")).first().getLong(0));
            }
            assertEquals(2, ids.size(), "the pool is back to two connections");
            assertFalse(ids.contains(victim));
        }
    }

    @Test
    void closedPoolRefuses() throws Exception {
        MyPool p = pool(1);
        p.disconnect().get(5, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class, () -> p.query("SELECT 1").get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof MyException);
    }
}
