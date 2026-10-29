package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MyResultLimitTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        My.require(My.PORT);
        group = new EventLoopGroup(1);
    }

    @AfterAll
    static void stop() {
        if (group != null) {
            group.close();
        }
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    static MyClient connect(MyConfig config) throws Exception {
        MyClient c = MyClient.connect(group, config).get(10, TimeUnit.SECONDS);
        await(c.query("SET SESSION cte_max_recursion_depth = 100000"));
        return c;
    }

    static final String ROWS = "WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5000) SELECT n, REPEAT('x', 100) AS pad FROM s";

    @Test
    void tooManyRowsFailsTheCallAndTheConnectionStaysUsable() throws Exception {
        try (MyClient c = connect(My.config().withResultLimits(1000, 64 << 20))) {
            CompletableFuture<MyResult> big = c.query(ROWS);
            CompletableFuture<MyResult> after = c.query("SELECT ? AS v", 7);
            ExecutionException e = assertThrows(ExecutionException.class, () -> await(big));
            assertTrue(e.getCause().getMessage().contains("1000 rows"), e.getCause().getMessage());
            assertEquals(7, await(after).first().getInt("v"), "the rows over the limit were read and dropped; the next call is fine");
            assertEquals(1000, await(c.query(ROWS.replace("n < 5000", "n < 1000"))).size(), "exactly the limit passes");
            assertEquals(0, c.pendingCalls());
        }
    }

    @Test
    void tooManyBytesFailsToo() throws Exception {
        try (MyClient c = connect(My.config().withResultLimits(1_000_000, 100_000))) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.queryPrepared(ROWS)));
            assertTrue(e.getCause().getMessage().contains("100000 bytes"), e.getCause().getMessage());
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void aStreamIsNotLimited() throws Exception {
        try (MyClient c = connect(My.config().withResultLimits(10, 1000))) {
            MyRowStream s = await(c.stream(500, ROWS));
            int n = 0;
            for (List<MyRow> piece = await(s.next()); !piece.isEmpty(); piece = await(s.next())) {
                n += piece.size();
            }
            assertEquals(5000, n);
        }
    }
}
