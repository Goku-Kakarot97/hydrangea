package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MyClientTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        My.require(My.PORT);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() {
        if (group != null) {
            group.close();
        }
    }

    static MyClient connect() throws Exception {
        return MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS);
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    static MyException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (MyException) e.getCause();
        }
        return fail("expected a failure");
    }

    @Test
    void textQueryAndPreparedQueryReturnTheSameValues() throws Exception {
        try (MyClient c = connect()) {
            MyRow t = await(c.query("SELECT 7 AS n, 'héllo' AS s, 2.50 AS d, NULL AS z, 1.5e0 AS f")).first();
            MyRow b = await(c.queryPrepared("SELECT 7 AS n, 'héllo' AS s, 2.50 AS d, NULL AS z, 1.5e0 AS f")).first();
            assertFalse(t.wasBinary());
            assertTrue(b.wasBinary());
            for (MyRow r : List.of(t, b)) {
                assertEquals(7, r.getInt("n"));
                assertEquals("héllo", r.getString("s"));
                assertEquals(new BigDecimal("2.50"), r.getBigDecimal("d"));
                assertTrue(r.isNull("z"));
                assertNull(r.getString("z"));
                assertEquals(1.5, r.getDouble("f"));
            }
            assertTrue(c.serverVersion().startsWith("8."));
            assertTrue(c.threadId() > 0);
        }
    }

    @Test
    void parametersOfEveryKind() throws Exception {
        try (MyClient c = connect()) {
            MyRow r = await(c.query("SELECT ? AS i, ? AS l, ? AS s, ? AS d, ? AS b, ? AS dt, ? AS dd, ? AS z, ? AS bytes, ? AS bd",
                    42, 9_000_000_000L, "x'y\"z", 1.25, true, LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000), LocalDate.of(2020, 1, 31), null,
                    new byte[]{1, 2, 3, (byte) 0xff}, new BigDecimal("12345.678"))).first();
            assertEquals(42, r.getInt("i"));
            assertEquals(9_000_000_000L, r.getLong("l"));
            assertEquals("x'y\"z", r.getString("s"));
            assertEquals(1.25, r.getDouble("d"));
            assertEquals(1, r.getInt("b"));
            assertEquals(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_456_000), r.getLocalDateTime(r.indexOf("dt")));
            assertEquals(LocalDate.of(2020, 1, 31), r.getLocalDate(r.indexOf("dd")));
            assertTrue(r.isNull("z"));
            assertArrayEquals(new byte[]{1, 2, 3, (byte) 0xff}, r.getBytes("bytes"));
            assertEquals(new BigDecimal("12345.678"), r.getBigDecimal("bd"));
        }
    }

    @Test
    void errorsAreSeenPerCallAndTheConnectionStaysUsable() throws Exception {
        try (MyClient c = connect()) {
            MyException e = failure(c.query("SELECT * FROM no_such_table_here"));
            assertEquals(1146, e.errorCode());
            assertEquals("42S02", e.sqlState());
            MyException p = failure(c.query("SELECT * FROM no_such_table_here WHERE a = ?", 1));
            assertEquals(1146, p.errorCode());
            MyException n = failure(c.queryPrepared("SELECT ?, ?", 1));
            assertTrue(n.getMessage().contains("2 parameters"), n.getMessage());
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
            assertEquals(5, await(c.query("SELECT ? + 1", 4)).first().getInt(0));
        }
    }

    @Test
    void aBurstIsAnsweredInOrderOnOneConnection() throws Exception {
        try (MyClient c = connect()) {
            List<CompletableFuture<MyResult>> fs = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                fs.add(i % 3 == 0 ? c.query("SELECT " + i) : c.query("SELECT ? + 0", i));
            }
            for (int i = 0; i < 2000; i++) {
                assertEquals(i, await(fs.get(i)).first().getInt(0), "answer " + i);
            }
            assertEquals(0, c.pendingCalls());
        }
    }

    @Test
    void severalThreadsShareOneConnection() throws Exception {
        try (MyClient c = connect()) {
            List<Thread> threads = new ArrayList<>();
            List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int t = 0; t < 8; t++) {
                int id = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (int i = 0; i < 300; i++) {
                            long v = await(c.query("SELECT ? * 1000 + ?", id, i)).first().getLong(0);
                            if (v != id * 1000L + i) {
                                errors.add(new AssertionError("got " + v + " for " + id + "," + i));
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
            assertTrue(errors.isEmpty(), errors.toString());
        }
    }

    @Test
    void dmlReportsAffectedRowsAndGeneratedKey() throws Exception {
        try (MyClient c = connect()) {
            await(c.query("DROP TABLE IF EXISTS t_dml"));
            await(c.query("CREATE TABLE t_dml (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20))"));
            MyResult a = await(c.query("INSERT INTO t_dml (v) VALUES ('a'), ('b')"));
            assertEquals(2, a.affectedRows());
            assertEquals(1, a.lastInsertId());
            MyResult b = await(c.query("INSERT INTO t_dml (v) VALUES (?)", "c"));
            assertEquals(1, b.affectedRows());
            assertEquals(3, b.lastInsertId());
            assertEquals(2, await(c.query("UPDATE t_dml SET v = ? WHERE id < ?", "z", 3)).affectedRows());
            MyException dup = failure(c.query("INSERT INTO t_dml (id, v) VALUES (1, 'dup')"));
            assertTrue(dup.isDuplicateKey());
            assertEquals(3, await(c.query("SELECT COUNT(*) FROM t_dml")).first().getInt(0));
            await(c.query("DROP TABLE t_dml"));
        }
    }

    @Test
    void manyColumnTypesInBothProtocols() throws Exception {
        try (MyClient c = connect()) {
            await(c.query("DROP TABLE IF EXISTS t_types"));
            await(c.query("CREATE TABLE t_types (a TINYINT, b SMALLINT UNSIGNED, c INT, d BIGINT UNSIGNED, e FLOAT, f DOUBLE, g DECIMAL(10,3), h DATE, i DATETIME(6), "
                    + "j TIME(3), k YEAR, l VARCHAR(10), m BLOB, n JSON, o BIT(8), p CHAR(3), q ENUM('x','y'), r TIMESTAMP NULL)"));
            await(c.query("INSERT INTO t_types VALUES (-5, 65535, -2000000000, 18446744073709551615, 1.5, 2.25, 123.456, '2024-03-01', '2024-03-01 10:20:30.123456', "
                    + "'-12:34:56.789', 2024, 'text', X'00FF10', '{\"a\": 1}', b'10101010', 'ab', 'y', NULL)"));
            await(c.query("INSERT INTO t_types (a) VALUES (NULL)"));
            for (boolean binary : new boolean[]{false, true}) {
                CompletableFuture<MyResult> f = binary ? c.queryPrepared("SELECT * FROM t_types ORDER BY a DESC") : c.query("SELECT * FROM t_types ORDER BY a DESC");
                List<MyRow> rows = await(f).rows();
                MyRow r = rows.get(0);        // a = -5 (NULL is the lowest value: it sorts last in DESC)
                String how = binary ? "binary" : "text";
                assertEquals(binary, r.wasBinary());
                assertEquals(-5, r.getInt(0), how);
                assertEquals(65535, r.getInt(1), how);
                assertEquals(-2000000000, r.getInt(2), how);
                assertEquals(new java.math.BigInteger("18446744073709551615"), r.getObject(3), how);
                assertEquals(1.5, r.getDouble(4), how);
                assertEquals(2.25, r.getDouble(5), how);
                assertEquals(new BigDecimal("123.456"), r.getBigDecimal(6), how);
                assertEquals(LocalDate.of(2024, 3, 1), r.getLocalDate(7), how);
                assertEquals(LocalDateTime.of(2024, 3, 1, 10, 20, 30, 123_456_000), r.getLocalDateTime(8), how);
                assertEquals(java.time.Duration.ofHours(12).plusMinutes(34).plusSeconds(56).plusMillis(789).negated(), r.getObject(9), how);
                assertEquals(2024, r.getInt(10), how);
                assertEquals("text", r.getString(11), how);
                assertArrayEquals(new byte[]{0, (byte) 0xFF, 0x10}, r.getBytes(12), how);
                assertEquals("{\"a\": 1}", r.getString(13), how);
                assertArrayEquals(new byte[]{(byte) 0xAA}, r.getBytes(14), how);
                assertEquals("ab", r.getString(15), how);
                assertEquals("y", r.getString(16), how);
                assertTrue(r.isNull(17), how);
                MyRow nulls = rows.get(1);
                for (int i = 0; i < nulls.size(); i++) {
                    assertTrue(nulls.isNull(i), how + " column " + i);
                }
                assertEquals(0, nulls.getInt(0));
                assertEquals(0.0, nulls.getDouble(4));
                assertFalse(nulls.getBoolean(0));
            }
            await(c.query("DROP TABLE t_types"));
        }
    }

    @Test
    void aStatementWithSeveralResultsAnswersAll() throws Exception {
        try (MyClient c = connect()) {
            await(c.query("DROP PROCEDURE IF EXISTS p_two"));
            await(c.query("CREATE PROCEDURE p_two(IN x INT) BEGIN SELECT x AS a; SELECT x * 2 AS b, 'k' AS c; END"));
            for (boolean prepared : new boolean[]{false, true}) {
                MyResult r = await(prepared ? c.queryPrepared("CALL p_two(?)", 21) : c.query("CALL p_two(21)"));
                assertEquals(21, r.first().getInt(0));
                MyResult second = r.next();
                assertNotNull(second);
                assertEquals(42, second.first().getInt("b"));
                MyResult ok = second.next();
                assertNotNull(ok);          // the OK of the CALL itself
                assertNull(ok.next());
            }
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
            await(c.query("DROP PROCEDURE p_two"));
        }
    }

    @Test
    void aBigRowCrossesThe16MbPacketBoundary() throws Exception {
        try (MyClient c = connect()) {
            byte[] big = new byte[17 * 1024 * 1024];
            java.util.Arrays.fill(big, (byte) 'x');
            big[big.length - 1] = 'y';
            String text = new String(big, java.nio.charset.StandardCharsets.ISO_8859_1);
            MyRow r = await(c.query("SELECT ? AS big, LENGTH(?) AS n", text, text)).first();
            assertEquals(big.length, r.getInt("n"));
            assertEquals(text, r.getString("big"));
            MyRow t = await(c.query("SELECT REPEAT('z', 17000000) AS big")).first();
            assertEquals(17_000_000, t.getString("big").length());
        }
    }

    @Test
    void theCacheEvictsAndKeepsWorking() throws Exception {
        try (MyClient c = MyClient.connect(group, My.config().withStatementCacheSize(3)).get(10, TimeUnit.SECONDS)) {
            for (int round = 0; round < 3; round++) {
                for (int i = 0; i < 10; i++) {
                    assertEquals(i + 1, await(c.query("SELECT ? + " + i, 1)).first().getInt(0));
                }
            }
            MyResult st = await(c.query("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances WHERE OWNER_THREAD_ID = PS_CURRENT_THREAD_ID()"));
            assertTrue(st.first().getInt(0) <= 3, "the server keeps at most 3 statements of this connection: " + st.first());
        }
    }

    @Test
    void withoutACacheEveryCallClosesItsStatement() throws Exception {
        try (MyClient c = MyClient.connect(group, My.config().withStatementCacheSize(0)).get(10, TimeUnit.SECONDS)) {
            for (int i = 0; i < 20; i++) {
                assertEquals(i, await(c.query("SELECT ? + 0", i)).first().getInt(0));
            }
            MyResult st = await(c.query("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances WHERE OWNER_THREAD_ID = PS_CURRENT_THREAD_ID()"));
            assertEquals(0, st.first().getInt(0));
        }
    }

    @Test
    void closeFailsWhatWaitsAndRefusesNewCalls() throws Exception {
        MyClient c = connect();
        CompletableFuture<MyResult> slow = c.query("SELECT SLEEP(5)");
        c.close();
        c.disconnect().get(5, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class, () -> slow.get(5, TimeUnit.SECONDS));
        MyException e = failure(c.query("SELECT 1"));
        assertFalse(c.isOpen());
        assertTrue(e.getMessage().contains("closed"), e.getMessage());
    }

    @Test
    void nativePasswordAndWrongPassword() throws Exception {
        My.require(My.NATIVE_PORT);
        try (MyClient c = MyClient.connect(group, My.config(My.NATIVE_PORT)).get(10, TimeUnit.SECONDS)) {
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
        }
        for (int port : new int[]{My.PORT, My.NATIVE_PORT}) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> MyClient.connect(group, My.config(port).withPassword("wrong")).get(10, TimeUnit.SECONDS));
            MyException me = (MyException) e.getCause();
            assertEquals(1045, me.errorCode(), me.getMessage());
        }
    }

    @Test
    void connectingToNothingFails() {
        ExecutionException e = assertThrows(ExecutionException.class, () -> MyClient.connect(group, MyConfig.of("127.0.0.1", 1, "u", "d")).get(10, TimeUnit.SECONDS));
        assertTrue(((MyException) e.getCause()).retriable());
    }
}
