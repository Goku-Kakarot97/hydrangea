package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.postgres.PgClient;
import io.hydrangea.postgres.PgColumn;
import io.hydrangea.postgres.PgConfig;
import io.hydrangea.postgres.PgCopyIn;
import io.hydrangea.postgres.PgException;
import io.hydrangea.postgres.PgPool;
import io.hydrangea.postgres.PgResult;
import io.hydrangea.postgres.PgRowStream;
import io.hydrangea.postgres.PgRow;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * PostgreSQL: queries with parameters, functions and procedures, reading the rows, ten thousand queries in flight on one connection, errors, reading in pieces, a batch and an array, a time limit, COPY, LISTEN/NOTIFY, and a pool with a transaction.
 * It creates a temporary table (gone when the connection closes) and one real table, {@code example_accounts}, which it drops at the end: use a database
 * where that does no harm.
 * <pre>
 * docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=secret postgres:16
 * java -cp target/classes io.hydrangea.examples.PgExample [host] [port] [user] [password] [database]
 * </pre>
 * Defaults: localhost 5432 postgres secret postgres. The connection is not encrypted; for TLS add {@code .withTls(sslContext)} to the {@link PgConfig}.
 */
public final class PgExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5432;
        String user = args.length > 2 ? args[2] : "postgres";
        String password = args.length > 3 ? args[3] : "secret";
        String database = args.length > 4 ? args[4] : "postgres";
        PgConfig config = PgConfig.of(host, port, user, database).withPassword(password);

        try (EventLoopGroup group = new EventLoopGroup(2)) {
            PgClient db = PgClient.connect(group, config).get(10, TimeUnit.SECONDS);
            System.out.println("connected to PostgreSQL " + db.serverParameter("server_version") + ", backend " + db.backendPid());

            // a query with parameters; the row is read by column name, converted by the column's type
            PgRow row = db.query("SELECT $1::int + $2::int AS sum, $3::text || '!' AS shout, now() AS at", 40, 2, "héllo").get().first();
            System.out.println("query: sum=" + row.getInt("sum") + " shout=" + row.getString("shout") + " at=" + row.get("at"));

            // queries issued together travel together: 10 000 of them without waiting for each answer
            List<CompletableFuture<PgResult>> pending = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                pending.add(db.query("SELECT $1::int", i));
            }
            System.out.println("last of 10000 pipelined: " + pending.get(pending.size() - 1).get().first().getInt(0));

            db.execute("CREATE TEMP TABLE people (id int PRIMARY KEY, name text)").get();
            System.out.println("insert: " + db.query("INSERT INTO people VALUES ($1, $2), ($3, $4) RETURNING id", 1, "Ada", 2, "Grace").get().tag());
            System.out.println("update count: " + db.query("UPDATE people SET name = upper(name) WHERE id = $1", 1).get().updateCount());

            // a function and a procedure, created in the session's own pg_temp schema: a function is called from SELECT, a procedure with CALL (its OUT / INOUT values come back as one row)
            db.execute("CREATE FUNCTION pg_temp.add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b'; "
                    + "CREATE PROCEDURE pg_temp.double_it(INOUT x int) LANGUAGE plpgsql AS $$ BEGIN x := x * 2; END $$").get();
            System.out.println("function: " + db.query("SELECT pg_temp.add($1, $2)", 2, 3).get().first().getInt(0));
            System.out.println("procedure: " + db.query("CALL pg_temp.double_it($1::int)", 21).get().first().getInt("x"));

            // reading rows: a SELECT with a parameter, then a loop over the rows; columns by name or by position (from 0), each converted by its type
            PgResult found = db.query("SELECT id, name, length(name) AS letters, NULL::text AS nickname FROM people WHERE id >= $1 ORDER BY id", 1).get();
            System.out.println("select: " + found.size() + " rows, columns " + found.columns().stream().map(PgColumn::name).toList());
            for (PgRow r : found.rows()) {
                int id = r.getInt("id");                       // an int column as int
                String name = r.getString("name");
                Object letters = r.get("letters");             // get(...) picks the Java type from the column's type: Integer here
                String nickname = r.isNull("nickname") ? "(none)" : r.getString("nickname");   // SQL NULL is null: ask isNull before a primitive getter
                System.out.println("  id=" + id + " name=" + name + " letters=" + letters + " nickname=" + nickname);
            }
            PgRow one = db.query("SELECT name FROM people WHERE id = $1", 2).get().first();      // first() is null when there is no row
            System.out.println("one row: " + (one == null ? "not found" : one.getString(0)));
            System.out.println("no row: " + (db.query("SELECT name FROM people WHERE id = $1", 99).get().first() == null ? "not found" : "found"));

            // an error carries the server's SQLSTATE; the connection and the queries queued behind it are not affected
            try {
                db.query("INSERT INTO people VALUES ($1, $2)", 1, "again").get();
            } catch (Exception e) {
                if (e.getCause() instanceof PgException pe) {
                    System.out.println("server error " + pe.sqlState() + ": " + pe.getMessage());
                }
            }

            // a million rows would not fit in a PgResult (there is a limit); read them in pieces of 1000 with a cursor, memory stays small
            PgRowStream rows = db.stream(1000, "SELECT g, 'row ' || g FROM generate_series(1, $1::int) g", 250_000).get();
            long[] sum = {0};
            long seen = rows.forEach(r -> sum[0] += r.getInt(0)).get();
            System.out.println("stream: " + seen + " rows, sum " + sum[0]);

            // a batch: one statement for many sets of parameters, one round trip, all or nothing; and an array parameter
            long[] counts = db.executeBatch("INSERT INTO people VALUES ($1, $2)", java.util.List.of(new Object[]{10, "Linus"}, new Object[]{11, "Barbara"}, new Object[]{12, "Ken"})).get();
            System.out.println("batch: " + java.util.Arrays.toString(counts));
            System.out.println("array parameter: " + db.query("SELECT name FROM people WHERE id = ANY($1) ORDER BY id", java.util.List.of(10, 12)).get().rows().stream().map(r -> r.getString(0)).toList());
            db.query("DELETE FROM people WHERE id >= $1", 10).get();

            // a time limit: the caller gets a timeout at once and the server is asked to cancel the query (here: a 30 s sleep stopped after 0.3 s)
            try {
                db.query(java.time.Duration.ofMillis(300), "SELECT pg_sleep(30)").get();
            } catch (Exception e) {
                if (e.getCause() instanceof PgException pe && pe.isTimeout()) {
                    System.out.println("time limit: " + pe.getMessage());
                }
            }
            System.out.println("and the connection is free again: " + db.query("SELECT 1").get().first().getInt(0));

            // COPY: rows in, rows out
            long copied = db.copyIn("COPY people FROM STDIN", "3\tAlan\n4\tEdsger\n".getBytes(StandardCharsets.UTF_8)).get();
            System.out.println("copy in: " + copied + " rows");
            System.out.print("copy out:\n" + new String(db.copyOut("COPY (SELECT id, name FROM people ORDER BY id) TO STDOUT").get(), StandardCharsets.UTF_8));
            PgCopyIn stream = db.copyIn("COPY people FROM STDIN").get();     // the same by hand, for data that comes in pieces from elsewhere
            stream.write("5\tBarbara\n".getBytes(StandardCharsets.UTF_8)).get();
            System.out.println("copy in by hand: " + stream.finish().get() + " row");

            // LISTEN / NOTIFY needs a connection of its own that stays idle; its listener gets the notifications
            CompletableFuture<String> heard = new CompletableFuture<>();
            PgClient listener = PgClient.connect(group, config, new io.hydrangea.postgres.PgListener() {
                @Override
                public void onNotification(int backendPid, String channel, String payload) {
                    heard.complete(channel + ": " + payload);
                }
            }).get(10, TimeUnit.SECONDS);
            listener.execute("LISTEN example_news").get();
            db.query("SELECT pg_notify('example_news', $1)", "hello listeners").get();
            System.out.println("notification: " + heard.get(5, TimeUnit.SECONDS));
            listener.close();
            db.close();

            // a pool of connections, each pipelining; a transaction takes one connection for itself
            try (PgPool pool = PgPool.create(group, config, 4).get(10, TimeUnit.SECONDS)) {
                pool.execute("DROP TABLE IF EXISTS example_accounts; CREATE TABLE example_accounts (id int PRIMARY KEY, balance int)").get();
                try {
                    pool.query("INSERT INTO example_accounts VALUES (1, 100), (2, 0)").get();
                    long changed = pool.<Long>transaction(tx -> tx.query("UPDATE example_accounts SET balance = balance - 30 WHERE id = 1")
                            .thenCompose(a -> tx.query("UPDATE example_accounts SET balance = balance + 30 WHERE id = 2"))
                            .thenApply(PgResult::updateCount)).get();
                    System.out.println("transfer committed, last update touched " + changed + " row");
                    try {
                        pool.transaction(tx -> tx.query("UPDATE example_accounts SET balance = balance - 1000 WHERE id = 1")
                                .thenCompose(a -> tx.query("SELECT 1 / 0"))).get();
                    } catch (Exception e) {
                        System.out.println("second transfer rolled back: " + e.getCause().getMessage());
                    }
                    PgResult balances = pool.query("SELECT id, balance FROM example_accounts ORDER BY id").get();
                    for (PgRow r : balances.rows()) {
                        System.out.println("account " + r.getInt("id") + ": " + r.getInt("balance"));
                    }
                } finally {
                    pool.execute("DROP TABLE example_accounts").get();
                }
            }
        }
    }
}
