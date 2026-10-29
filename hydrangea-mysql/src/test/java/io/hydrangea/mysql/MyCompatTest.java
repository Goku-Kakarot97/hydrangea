package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The same scenario against the servers that speak MySQL's protocol but are not MySQL 8.4: MariaDB ({@code start-mariadb.sh}, 23310) and MySQL behind ProxySQL
 * ({@code start-proxysql.sh}, 23311). A server that is not there is skipped.
 */
class MyCompatTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() {
        group.close();
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(15, TimeUnit.SECONDS);
    }

    static final int MARIADB = 23310, PROXYSQL = 23311;

    static MyPool pool(int port, int size) throws Exception {
        My.require(port);
        MyConfig config = My.config(port);
        if (port == PROXYSQL) {
            config = config.withMaxInFlight(1);     // ProxySQL reads one command at a time: see proxySqlDropsAPipeliningClient
        }
        return MyPool.create(group, config, size).get(15, TimeUnit.SECONDS);
    }

    /** ProxySQL 2.7 closes a client that sends a second command before the first is answered ("Unexpected packet from client"): {@code withMaxInFlight(1)} is the cure. */
    @org.junit.jupiter.api.Test
    void proxySqlDropsAPipeliningClient() throws Exception {
        My.require(PROXYSQL);
        try (MyClient c = MyClient.connect(group, My.config(PROXYSQL)).get(15, TimeUnit.SECONDS)) {
            List<CompletableFuture<MyResult>> fs = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                fs.add(c.query("SELECT " + i));
            }
            boolean lost = false;
            for (CompletableFuture<MyResult> f : fs) {
                try {
                    await(f);
                } catch (ExecutionException e) {
                    lost = true;
                }
            }
            assertTrue(lost, "ProxySQL dropped the pipelining connection");
        }
        try (MyClient c = MyClient.connect(group, My.config(PROXYSQL).withMaxInFlight(1)).get(15, TimeUnit.SECONDS)) {
            List<CompletableFuture<MyResult>> fs = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                fs.add(c.query("SELECT " + i));
            }
            for (int i = 0; i < 20; i++) {
                assertEquals(i, await(fs.get(i)).first().getInt(0));
            }
        }
    }

    @ParameterizedTest(name = "port {0}")
    @ValueSource(ints = {MARIADB, PROXYSQL})
    void queriesTypesTransactionsBatchesAndStreams(int port) throws Exception {
        try (MyPool p = pool(port, 4)) {
            String version = await(p.query("SELECT VERSION()")).first().getString(0);
            await(p.query("DROP TABLE IF EXISTS t_compat"));
            await(p.query("CREATE TABLE t_compat (id INT PRIMARY KEY, name VARCHAR(40), amount DECIMAL(10,2), d DATE, b BLOB) ENGINE=InnoDB"));
            // text and prepared, every parameter kind
            assertEquals(1, await(p.query("INSERT INTO t_compat VALUES (?, ?, ?, ?, ?)", 1, "one", new BigDecimal("1.50"), LocalDate.of(2024, 5, 6), new byte[]{1, 2})).affectedRows());
            MyRow r = await(p.query("SELECT id, name, amount, d, b FROM t_compat WHERE id = ?", 1)).first();
            assertTrue(r.wasBinary());
            assertEquals("one", r.getString("name"));
            assertEquals(new BigDecimal("1.50"), r.getBigDecimal("amount"));
            assertEquals(LocalDate.of(2024, 5, 6), r.getLocalDate(3));
            assertArrayEquals(new byte[]{1, 2}, r.getBytes("b"));
            MyRow t = await(p.query("SELECT id, name, amount, d FROM t_compat")).first();
            assertFalse(t.wasBinary());
            assertEquals(new BigDecimal("1.50"), t.getBigDecimal(2));
            // errors are per call
            assertEquals(1146, ((MyException) assertThrows(ExecutionException.class, () -> await(p.query("SELECT * FROM t_nope"))).getCause()).errorCode());
            assertTrue(((MyException) assertThrows(ExecutionException.class, () -> await(p.query("INSERT INTO t_compat (id) VALUES (?)", 1))).getCause()).isDuplicateKey());
            // a burst on shared connections
            List<CompletableFuture<MyResult>> fs = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                fs.add(p.query("SELECT ? + 0", i));
            }
            for (int i = 0; i < 500; i++) {
                assertEquals(i, await(fs.get(i)).first().getInt(0));
            }
            // a batch, a transaction that rolls back, a stream
            List<Object[]> sets = new ArrayList<>();
            for (int i = 2; i < 300; i++) {
                sets.add(new Object[]{i, "n" + i, BigDecimal.TEN, null, null});
            }
            long[] counts = await(p.transaction(db -> db.executeBatch("INSERT INTO t_compat VALUES (?, ?, ?, ?, ?)", sets)));
            assertEquals(298, counts.length);
            assertThrows(ExecutionException.class, () -> await(p.transaction(db -> db.query("UPDATE t_compat SET amount = 0")
                    .thenCompose(x -> db.query("INSERT INTO t_compat (id) VALUES (?)", 1)))));
            assertEquals(new BigDecimal("1.50"), await(p.query("SELECT amount FROM t_compat WHERE id = 1")).first().getBigDecimal(0), "rolled back on " + version);
            MyRowStream s = await(p.stream(100, "SELECT id FROM t_compat ORDER BY id"));
            int n = 0;
            for (List<MyRow> piece = await(s.next()); !piece.isEmpty(); piece = await(s.next())) {
                n += piece.size();
            }
            assertEquals(299, n);
            // a time limit
            MyException to = (MyException) assertThrows(ExecutionException.class, () -> await(p.query(Duration.ofMillis(300), "SELECT SLEEP(5)"))).getCause();
            assertTrue(to.isTimeout());
            assertEquals(1, await(p.query("SELECT 1")).first().getInt(0));
            await(p.query("DROP TABLE t_compat"));
            System.out.println("compat ok on " + version + " (port " + port + ")");
        }
    }
}
