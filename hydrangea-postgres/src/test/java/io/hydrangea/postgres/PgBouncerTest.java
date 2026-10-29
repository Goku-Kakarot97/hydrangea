package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Through PgBouncer in transaction pooling mode ({@code start-pgbouncer.sh}: ports 26432 with prepared-statement support and 4 server connections, 26433 without and with one);
 * skipped when it is not running. In this mode a server connection serves one transaction of one client at a time, so nothing of a session survives.
 */
class PgBouncerTest {

    static final int PORT = Integer.getInteger("pgbouncer.port", 26432);
    static final int OLD_PORT = Integer.getInteger("pgbouncer.old.port", 26433);

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        Pg.notOnRemote();
        require(PORT);
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(Pg.HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        org.junit.jupiter.api.Assumptions.assumeTrue(up, "no PgBouncer on " + Pg.HOST + ":" + port + " (start one with hydrangea-postgres/src/test/resources/postgres/start-pgbouncer.sh)");
    }

    private PgConfig config(int port) {
        return PgConfig.of(Pg.HOST, port, "hydra", "hydra").withPassword("hydra");
    }

    private PgClient connect(int port) throws Exception {
        return Pg.get(PgClient.connect(group, config(port)));
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(20, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    @Test
    void queriesParametersTypesAndPipelinesGoThrough() throws Exception {
        try (PgClient c = connect(PORT)) {
            for (int i = 0; i < 6; i++) {          // the statement is cached after the first run and binary after the second
                PgRow r = Pg.get(c.query("SELECT $1::int4, $2::bytea, $3::int[], $4::numeric, $5::text", i, new byte[]{1, 2}, List.of(1, 2, 3), new java.math.BigDecimal("1.50"), "héllo")).first();
                assertEquals(i, r.getInt(0));
                assertArrayEquals(new byte[]{1, 2}, r.getBytes(1));
                assertEquals(List.of(1, 2, 3), r.getList(2));
                assertEquals(new java.math.BigDecimal("1.50"), r.getBigDecimal(3));
                assertEquals("héllo", r.getString(4));
            }
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                fs.add(c.query("SELECT $1::int", i));
            }
            for (int i = 0; i < fs.size(); i++) {
                assertEquals(i, Pg.get(fs.get(i)).first().getInt(0));
            }
        }
    }

    @Test
    void manyClientsShareFewServerConnections() throws Exception {
        try (PgPool pool = Pg.get(PgPool.create(group, config(PORT), 16)); PgClient admin = Pg.get(PgClient.connect(group, config(PORT).withApplicationName("admin")))) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                fs.add(pool.query("SELECT $1::int, pg_sleep(0.001)", i));
            }
            for (int i = 0; i < fs.size(); i++) {
                assertEquals(i, Pg.get(fs.get(i)).first().getInt(0));
            }
            // 16 client connections, at most 4 server connections behind them
            PgClient console = Pg.get(PgClient.connect(group, PgConfig.of(Pg.HOST, PORT, "hydra", "pgbouncer").withPassword("hydra")));
            List<PgResult> servers = Pg.get(console.execute("SHOW SERVERS"));
            assertTrue(servers.get(0).size() <= 4, "server connections: " + servers.get(0).size());
            List<PgResult> clients = Pg.get(console.execute("SHOW CLIENTS"));
            assertTrue(clients.get(0).size() >= 17, "client connections: " + clients.get(0).size());
            console.close();
        }
    }

    @Test
    void transactionsAndTheirRollbackThroughTheBouncer() throws Exception {
        try (PgPool pool = Pg.get(PgPool.create(group, config(PORT), 8))) {
            String t = Pg.table("pb");
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int PRIMARY KEY, v text)"));
            try {
                for (int i = 0; i < 40; i++) {
                    int n = i;
                    Long count = Pg.get(pool.<Long>transaction(db -> db.query("INSERT INTO " + t + " VALUES ($1, 'x')", n)
                            .thenCompose(a -> db.query("SELECT count(*) FROM " + t + " WHERE id = $1", n)).thenApply(r -> r.first().getLong(0))));
                    assertEquals(1L, count.longValue());
                }
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("INSERT INTO " + t + " VALUES (1000, 'y')")
                        .thenCompose(a -> db.query("SELECT 1/0"))).get(20, TimeUnit.SECONDS));
                assertEquals("22012", ((PgException) e.getCause()).sqlState());
                assertEquals(0, Pg.get(pool.query("SELECT count(*) FROM " + t + " WHERE id = 1000")).first().getInt(0), "rolled back");
                assertEquals("serializable", Pg.get(pool.transaction(TxOptions.of(TxOptions.Isolation.SERIALIZABLE), db -> db.query("SHOW transaction_isolation"))).first().getString(0));
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void copyStreamBatchAndArraysGoThrough() throws Exception {
        try (PgClient c = connect(PORT)) {
            String t = Pg.table("pc");
            Pg.get(c.execute("CREATE TABLE " + t + " (id int, name text)"));
            try {
                assertEquals(3L, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", "1\ta\n2\tb\n3\tc\n".getBytes())).longValue());
                assertEquals("1\ta\n2\tb\n3\tc\n", new String(Pg.get(c.copyOut("COPY " + t + " TO STDOUT"))));
                long[] counts = Pg.get(c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2)", List.of(new Object[]{10, "x"}, new Object[]{11, "y"})));
                assertArrayEquals(new long[]{1, 1}, counts);
                for (int round = 0; round < 3; round++) {                // the stream spans several Executes with no Sync between: the server connection must stay with it
                    assertEquals(20_000L, Pg.get(Pg.get(c.stream(500, "SELECT g FROM generate_series(1, 20000) g")).forEach(r -> { })).longValue());
                }
                assertEquals(2, Pg.get(c.query("SELECT * FROM " + t + " WHERE id = ANY($1)", List.of(10, 11))).size());
            } finally {
                Pg.get(c.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void aTimeoutIsCancelledThroughTheBouncer() throws Exception {
        try (PgClient c = connect(PORT)) {
            long t0 = System.nanoTime();
            assertTrue(failure(c.query(Duration.ofMillis(300), "SELECT pg_sleep(30)")).isTimeout());
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 5000, "the cancel went through PgBouncer to the right server connection");
        }
    }

    @Test
    void anotherClientsDeallocateAllCannotBreakTheStatementCacheWhenTheBouncerKnowsPreparedStatements() throws Exception {
        try (PgClient a = connect(PORT); PgClient b = connect(PORT)) {
            for (int i = 0; i < 4; i++) {
                assertEquals(i, Pg.get(a.query("SELECT $1::int + 0", i)).first().getInt(0));
            }
            for (int i = 0; i < 20; i++) {
                Pg.get(b.execute("DEALLOCATE ALL"));
                assertEquals(i, Pg.get(a.query("SELECT $1::int + 0", i)).first().getInt(0));
            }
        }
    }

    @Test
    void aBouncerWithoutPreparedStatementsBreaksTheCacheAndNoCacheIsTheCure() throws Exception {
        require(OLD_PORT);
        try (PgClient a = connect(OLD_PORT); PgClient b = connect(OLD_PORT)) {
            for (int i = 0; i < 3; i++) {
                assertEquals(i, Pg.get(a.query("SELECT $1::int + 1", i)).first().getInt(0) - 1);
            }
            Pg.get(b.execute("DEALLOCATE ALL"));      // the one server connection forgets what a prepared
            PgException e = failure(a.query("SELECT $1::int + 1", 5));
            assertEquals("26000", e.sqlState(), e.toString());
        }
        try (PgClient a = Pg.get(PgClient.connect(group, config(OLD_PORT).withStatementCacheSize(0))); PgClient b = connect(OLD_PORT)) {
            for (int i = 0; i < 20; i++) {
                assertEquals(i + 1, Pg.get(a.query("SELECT $1::int + 1", i)).first().getInt(0));
                Pg.get(b.execute("DEALLOCATE ALL"));
            }
        }
    }
}
