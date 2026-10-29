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

class MyStreamTest {

    static EventLoopGroup group;
    static MyClient c;

    @BeforeAll
    static void start() throws Exception {
        My.require(My.PORT);
        group = new EventLoopGroup(2);
        c = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS);
        await(c.query("DROP TABLE IF EXISTS t_stream"));
        await(c.query("CREATE TABLE t_stream (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=InnoDB"));
        List<Object[]> sets = new ArrayList<>();
        for (int i = 1; i <= 2500; i++) {
            sets.add(new Object[]{i, "row " + i});
        }
        await(c.query("SET autocommit = 0"));
        c.executeBatch("INSERT INTO t_stream VALUES (?, ?)", sets).get(60, TimeUnit.SECONDS);
        await(c.query("COMMIT"));
        await(c.query("SET autocommit = 1"));
    }

    @AfterAll
    static void stop() throws Exception {
        if (c != null) {
            await(c.query("DROP TABLE t_stream"));
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
    void readsEveryRowInPiecesOfFetchSize() throws Exception {
        MyRowStream s = await(c.stream(1000, "SELECT id, v FROM t_stream WHERE id > ? ORDER BY id", 500));
        assertEquals(List.of("id", "v"), s.columns().stream().map(MyColumn::name).toList());
        int total = 0, pieces = 0, expect = 501;
        for (List<MyRow> piece = await(s.next()); !piece.isEmpty(); piece = await(s.next())) {
            pieces++;
            assertTrue(piece.size() <= 1000);
            for (MyRow r : piece) {
                assertEquals(expect, r.getInt("id"));
                assertEquals("row " + expect, r.getString("v"));
                assertTrue(r.wasBinary());
                expect++;
                total++;
            }
        }
        assertEquals(2000, total);
        assertEquals(2, pieces);
        assertTrue(s.isDone());
        assertTrue(await(s.next()).isEmpty(), "after the end: empty again");
        assertEquals(0, c.pendingCalls());
    }

    @Test
    void callsMadeDuringAStreamWaitAndRunInOrderAfterIt() throws Exception {
        MyRowStream s = await(c.stream(100, "SELECT id FROM t_stream ORDER BY id"));
        CompletableFuture<MyResult> a = c.query("SELECT 'a'");
        CompletableFuture<MyResult> b = c.query("SELECT ? AS b", 2);
        List<MyRow> first = await(s.next());
        assertEquals(100, first.size());
        Thread.sleep(100);
        assertFalse(a.isDone(), "waits for the stream");
        await(s.close());
        assertEquals("a", await(a).first().getString(0));
        assertEquals(2, await(b).first().getInt("b"));
        assertEquals(0, c.pendingCalls());
    }

    @Test
    void aStatementWithoutACursorIsOnePiece() throws Exception {
        MyRowStream s = await(c.stream(10, "SELECT COUNT(*) AS n FROM t_stream"));
        List<MyRow> piece = await(s.next());
        assertEquals(1, piece.size());
        assertEquals(2500, piece.get(0).getInt("n"));
        assertTrue(await(s.next()).isEmpty());
        MyRowStream dml = await(c.stream(10, "UPDATE t_stream SET v = v WHERE id = ?", 1));
        assertTrue(await(dml.next()).isEmpty());
        assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
    }

    @Test
    void anErrorInTheStatementFailsTheStreamAndFreesTheConnection() throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.stream(10, "SELECT * FROM no_such_table_stream")));
        assertEquals(1146, ((MyException) e.getCause()).errorCode());
        assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
    }

    @Test
    void theStreamOfAPoolHoldsOneConnection() throws Exception {
        try (MyPool p = MyPool.create(group, My.config(), 2).get(10, TimeUnit.SECONDS)) {
            MyRowStream s = await(p.stream(700, "SELECT id FROM t_stream ORDER BY id"));
            int n = 0;
            for (List<MyRow> piece = await(s.next()); !piece.isEmpty(); piece = await(s.next())) {
                n += piece.size();
                assertEquals(1, await(p.query("SELECT 1")).first().getInt(0), "the other connection answers meanwhile");
            }
            assertEquals(2500, n);
        }
    }
}
