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

class MyBatchTest {

    static EventLoopGroup group;
    static MyClient c;

    @BeforeAll
    static void start() throws Exception {
        My.require(My.PORT);
        group = new EventLoopGroup(2);
        c = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS);
        await(c.query("DROP TABLE IF EXISTS t_batch"));
        await(c.query("CREATE TABLE t_batch (id INT PRIMARY KEY, v VARCHAR(50), b BLOB) ENGINE=InnoDB"));
    }

    @AfterAll
    static void stop() throws Exception {
        if (c != null) {
            await(c.query("DROP TABLE t_batch"));
            c.close();
        }
        if (group != null) {
            group.close();
        }
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    @Test
    void insertsThousandsOfRowsAndReportsEachCount() throws Exception {
        List<Object[]> sets = new ArrayList<>();
        byte[] blob = {1, 2, 3};
        for (int i = 0; i < 2000; i++) {       // every set is its own commit outside a transaction: about a millisecond each on this disk
            sets.add(new Object[]{i, "v" + i, blob});
        }
        long[] counts = c.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", sets).get(60, TimeUnit.SECONDS);
        assertEquals(2000, counts.length);
        for (long n : counts) {
            assertEquals(1, n);
        }
        blob[0] = 9;        // the batch copied it when it was made
        assertEquals(2000, await(c.query("SELECT COUNT(*) FROM t_batch WHERE id < 100000")).first().getInt(0));
        assertArrayEquals(new byte[]{1, 2, 3}, await(c.query("SELECT b FROM t_batch WHERE id = ?", 10)).first().getBytes(0));
        long[] updates = await(c.executeBatch("UPDATE t_batch SET v = ? WHERE id < ?", List.of(new Object[]{"x", 10}, new Object[]{"y", 0}, new Object[]{"z", 3})));
        assertArrayEquals(new long[]{10, 0, 3}, updates);
        assertEquals(0, c.pendingCalls());
    }

    @Test
    void aFailingSetDoesNotStopTheOthersAndTheErrorNamesIt() throws Exception {
        await(c.query("DELETE FROM t_batch WHERE id >= 100000"));
        List<Object[]> sets = List.of(new Object[]{100000, "a", null}, new Object[]{100000, "dup", null}, new Object[]{100001, "b", null});
        ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", sets)));
        MyException me = (MyException) e.getCause();
        assertTrue(me.isDuplicateKey());
        assertTrue(me.getMessage().startsWith("set 1:"), me.getMessage());
        assertEquals(2, await(c.query("SELECT COUNT(*) FROM t_batch WHERE id >= 100000")).first().getInt(0), "the sets after the failing one ran");
        assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
    }

    @Test
    void insideATransactionAFailureRollsEverythingBack() throws Exception {
        try (MyPool p = MyPool.create(group, My.config(), 1).get(10, TimeUnit.SECONDS)) {
            await(p.query("DELETE FROM t_batch WHERE id >= 200000"));
            List<Object[]> sets = List.of(new Object[]{200000, "a", null}, new Object[]{200001, "b", null}, new Object[]{200000, "dup", null});
            assertThrows(ExecutionException.class, () -> await(p.transaction(db -> db.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", sets))));
            assertEquals(0, await(p.query("SELECT COUNT(*) FROM t_batch WHERE id >= 200000")).first().getInt(0));
            long[] ok = await(p.transaction(db -> db.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", sets.subList(0, 2))));
            assertEquals(2, ok.length);
            assertEquals(2, await(p.query("SELECT COUNT(*) FROM t_batch WHERE id >= 200000")).first().getInt(0));
        }
    }

    @Test
    void wrongParameterCountAndEmptyBatch() throws Exception {
        assertEquals(0, await(c.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", List.of())).length);
        ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.executeBatch("INSERT INTO t_batch VALUES (?, ?, ?)", List.of(new Object[]{1, "a", null}, new Object[]{2}))));
        assertTrue(e.getCause().getMessage().contains("set 1"), e.getCause().getMessage());
        assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
    }
}
