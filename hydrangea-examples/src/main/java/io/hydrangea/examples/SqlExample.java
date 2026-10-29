package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.mysql.MyConfig;
import io.hydrangea.mysql.MyPool;
import io.hydrangea.postgres.PgConfig;
import io.hydrangea.postgres.PgPool;
import io.hydrangea.sql.SqlException;
import io.hydrangea.sql.SqlPool;
import io.hydrangea.sql.SqlRow;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The same repository code on PostgreSQL and on MySQL, through the {@link SqlPool} interface the two clients share: {@code ?} placeholders, the common
 * result and row types, transactions, the error classification. The repository ({@link Accounts}) knows nothing of which database it runs on; only the
 * {@code main} that builds the pool does, and the SQL stays within what both databases accept.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.SqlExample postgres [host] [port] [user] [password] [database]
 * java -cp target/classes io.hydrangea.examples.SqlExample mysql    [host] [port] [user] [password] [database]
 * </pre>
 * Defaults: localhost, 5432 / 3306, user postgres / root, password secret, database postgres / mysql. It creates and drops a table {@code example_sql_accounts}.
 */
public final class SqlExample {

    /** Application code written once. */
    static final class Accounts {
        private final SqlPool db;

        Accounts(SqlPool db) {
            this.db = db;
        }

        CompletableFuture<Void> open(int id, String name, BigDecimal balance) {
            return db.execute("INSERT INTO example_sql_accounts (id, name, balance) VALUES (?, ?, ?)", id, name, balance).thenApply(r -> null);
        }

        CompletableFuture<BigDecimal> balance(int id) {
            return db.execute("SELECT balance FROM example_sql_accounts WHERE id = ?", id).thenApply(r -> r.isEmpty() ? null : r.first().getBigDecimal(0));
        }

        /** Moves money in one transaction; fails (and rolls back) when the source would go negative. */
        CompletableFuture<Void> transfer(int from, int to, BigDecimal amount) {
            return db.transactionWithRetry(3, tx -> tx.execute("UPDATE example_sql_accounts SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, from, amount)
                    .thenCompose(r -> {
                        if (r.affectedRows() != 1) {
                            throw new IllegalStateException("account " + from + " cannot pay " + amount);
                        }
                        return tx.execute("UPDATE example_sql_accounts SET balance = balance + ? WHERE id = ?", amount, to);
                    })
                    .thenApply(r -> null));
        }

        CompletableFuture<List<String>> richest(int n) {
            return db.execute("SELECT name, balance FROM example_sql_accounts ORDER BY balance DESC, id LIMIT " + n)
                    .thenApply(r -> r.rows().stream().map(row -> row.getString("name") + "=" + row.getBigDecimal("balance").toPlainString()).toList());
        }
    }

    public static void main(String[] args) throws Exception {
        String kind = args.length > 0 ? args[0] : "postgres";
        boolean pg = kind.equals("postgres");
        String host = args.length > 1 ? args[1] : "localhost";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : (pg ? 5432 : 3306);
        String user = args.length > 3 ? args[3] : (pg ? "postgres" : "root");
        String password = args.length > 4 ? args[4] : "secret";
        String database = args.length > 5 ? args[5] : (pg ? "postgres" : "mysql");

        try (EventLoopGroup group = new EventLoopGroup(2)) {
            SqlPool db = pg
                    ? PgPool.create(group, PgConfig.of(host, port, user, database).withPassword(password), 2).get(10, TimeUnit.SECONDS).asSql()
                    : MyPool.create(group, MyConfig.of(host, port, user, database).withPassword(password)
                            .withAllowPublicKeyRetrieval(true), 2).get(10, TimeUnit.SECONDS).asSql();      // a local container without TLS: the first login of a user needs the server's key
            try (db) {
                db.execute("DROP TABLE IF EXISTS example_sql_accounts").get(10, TimeUnit.SECONDS);
                db.execute("CREATE TABLE example_sql_accounts (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL, balance DECIMAL(12,2) NOT NULL)").get(10, TimeUnit.SECONDS);

                Accounts accounts = new Accounts(db);
                accounts.open(1, "alice", new BigDecimal("100.00")).get(10, TimeUnit.SECONDS);
                accounts.open(2, "bob", new BigDecimal("20.00")).get(10, TimeUnit.SECONDS);
                try {
                    accounts.open(2, "bob again", BigDecimal.ONE).get(10, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException e) {
                    SqlException se = (SqlException) e.getCause();
                    System.out.println("duplicate key: " + se.isDuplicateKey() + " (SQLSTATE " + se.sqlState() + ")");
                }
                accounts.transfer(1, 2, new BigDecimal("30.00")).get(10, TimeUnit.SECONDS);
                System.out.println("after the transfer: alice " + accounts.balance(1).get(10, TimeUnit.SECONDS) + ", bob " + accounts.balance(2).get(10, TimeUnit.SECONDS));
                try {
                    accounts.transfer(2, 1, new BigDecimal("1000.00")).get(10, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException e) {
                    System.out.println("refused and rolled back: " + e.getCause().getMessage() + "; bob still " + accounts.balance(2).get(10, TimeUnit.SECONDS));
                }
                System.out.println("richest: " + accounts.richest(2).get(10, TimeUnit.SECONDS));
                SqlRow row = db.execute("SELECT COUNT(*) AS n FROM example_sql_accounts").get(10, TimeUnit.SECONDS).first();
                System.out.println("accounts: " + row.getLong("n") + " on " + kind);
                db.execute("DROP TABLE example_sql_accounts").get(10, TimeUnit.SECONDS);
            }
        }
    }
}
