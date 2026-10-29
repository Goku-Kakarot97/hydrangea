package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.sql.SqlClient;
import io.hydrangea.sql.SqlException;
import io.hydrangea.sql.SqlIsolation;
import io.hydrangea.sql.SqlPool;
import io.hydrangea.sql.SqlResult;
import io.hydrangea.sql.SqlRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The shared {@link SqlPool} interface on PostgreSQL. {@code MySqlTest} in hydrangea-mysql runs the same scenario on MySQL. */
class PgSqlTest {

    static EventLoopGroup group;
    static SqlPool db;

    @BeforeAll
    static void start() throws Exception {
        Pg.require(Pg.PORT);
        Pg.notOnRemote();
        group = new EventLoopGroup(2);
        db = PgPool.create(group, Pg.config(), 2).get(10, TimeUnit.SECONDS).asSql();
        await(db.execute("DROP TABLE IF EXISTS sql_accounts"));
        await(db.execute("CREATE TABLE sql_accounts (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL, balance DECIMAL(12,2) NOT NULL, opened DATE, note VARCHAR(100))"));
        await(db.execute("INSERT INTO sql_accounts VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)", 1, "alice", new BigDecimal("100.50"), LocalDate.of(2024, 1, 2), null, 2, "bob", new BigDecimal("20.00"), LocalDate.of(2024, 3, 4), "vip"));
    }

    @AfterAll
    static void stop() throws Exception {
        if (db != null) {
            await(db.execute("DROP TABLE sql_accounts"));
            db.close();
        }
        if (group != null) {
            group.close();
        }
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    static SqlException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (SqlException) e.getCause();
        }
        return fail("expected a failure");
    }

    @Test
    void queryWithQuestionMarksAndTheRowGetters() throws Exception {
        SqlResult r = await(db.execute("SELECT id, name, balance, opened, note FROM sql_accounts WHERE id = ? OR name = ? ORDER BY id", 1, "bob"));
        assertEquals(2, r.size());
        assertEquals(java.util.List.of("id", "name", "balance", "opened", "note"), r.columnNames());
        SqlRow a = r.first();
        assertEquals(1, a.getInt("id"));
        assertEquals("alice", a.getString(1));
        assertEquals(new BigDecimal("100.50"), a.getBigDecimal("balance"));
        assertEquals(LocalDate.of(2024, 1, 2), a.getLocalDate("opened"));
        assertEquals(LocalDateTime.of(2024, 1, 2, 0, 0), a.getLocalDateTime("opened"));
        assertTrue(a.isNull("note"));
        assertNull(a.getString("note"));
        assertEquals(0, a.getInt("note"));
        assertFalse(a.getBoolean("note"));
        assertThrows(IllegalArgumentException.class, () -> a.getString("nope"));
        assertEquals(-1, a.indexOf("nope"));
        SqlRow b = r.rows().get(1);
        assertEquals(20.0, b.getDouble("balance"));
        assertEquals("vip", b.getString("note"));
        assertEquals(0, r.affectedRows());
    }

    @Test
    void affectedRowsAndDuplicateKey() throws Exception {
        assertEquals(1, await(db.execute("UPDATE sql_accounts SET note = ? WHERE id = ?", "x", 1)).affectedRows());
        assertEquals(0, await(db.execute("UPDATE sql_accounts SET note = ? WHERE id = ?", "x", 99)).affectedRows());
        SqlException dup = failure(db.execute("INSERT INTO sql_accounts (id, name, balance) VALUES (?, ?, ?)", 1, "again", BigDecimal.ONE));
        assertTrue(dup.isDuplicateKey());
        assertNotNull(dup.sqlState());
        assertFalse(dup.isTimeout());
        await(db.execute("UPDATE sql_accounts SET note = NULL WHERE id = ?", 1));
    }

    @Test
    void transactionCommitsAndRollsBack() throws Exception {
        BigDecimal total = await(db.transaction(tx -> tx.execute("UPDATE sql_accounts SET balance = balance - ? WHERE id = ?", new BigDecimal("10.50"), 1)
                .thenCompose(r -> tx.execute("UPDATE sql_accounts SET balance = balance + ? WHERE id = ?", new BigDecimal("10.50"), 2))
                .thenCompose(r -> tx.execute("SELECT SUM(balance) FROM sql_accounts"))
                .thenApply(r -> r.first().getBigDecimal(0))));
        assertEquals(0, new BigDecimal("120.50").compareTo(total));
        assertEquals(0, new BigDecimal("90.00").compareTo(await(db.execute("SELECT balance FROM sql_accounts WHERE id = ?", 1)).first().getBigDecimal(0)));
        SqlException e = failure(db.transaction(tx -> tx.execute("UPDATE sql_accounts SET balance = 0")
                .thenCompose(r -> tx.execute("INSERT INTO sql_accounts (id, name, balance) VALUES (?, ?, ?)", 2, "dup", BigDecimal.ONE))));
        assertTrue(e.isDuplicateKey());
        assertEquals(0, new BigDecimal("90.00").compareTo(await(db.execute("SELECT balance FROM sql_accounts WHERE id = ?", 1)).first().getBigDecimal(0)), "rolled back");
        String name = await(db.transaction(SqlIsolation.SERIALIZABLE, true, tx -> tx.execute("SELECT name FROM sql_accounts WHERE id = ?", 2))).first().getString(0);
        assertEquals("bob", name);
        assertThrows(ExecutionException.class, () -> await(db.transaction(SqlIsolation.READ_COMMITTED, true, tx -> tx.execute("UPDATE sql_accounts SET note = 'w'"))), "read only");
    }

    @Test
    void retryAndTimeout() throws Exception {
        int[] attempts = {0};
        String v = await(db.transactionWithRetry(3, tx -> {
            attempts[0]++;
            return tx.execute("SELECT name FROM sql_accounts WHERE id = ?", 1).thenApply(r -> r.first().getString(0));
        }));
        assertEquals("alice", v);
        assertEquals(1, attempts[0]);
        SqlException t = failure(db.execute(Duration.ofMillis(200), "SELECT pg_sleep(5)"));
        assertTrue(t.isTimeout());
        assertEquals(1, await(db.execute("SELECT 1")).first().getInt(0));
    }

    @Test
    void aLiteralQuestionMarkIsWrittenTwice() throws Exception {
        SqlRow r = await(db.execute("SELECT '{\"k\":1}'::jsonb ?? ? AS has, ? AS v", "k", 7)).first();
        assertTrue(r.getBoolean("has"));
        assertEquals(7, r.getInt("v"));
    }

    @Test
    void theClientOfATransactionIsTheSameInterface() throws Exception {
        boolean open = await(db.transaction(tx -> {
            assertInstanceOf(SqlClient.class, tx);
            return tx.execute("SELECT 1").thenApply(r -> tx.isOpen());
        }));
        assertTrue(open);
        assertEquals(2, db.size());
    }
}
