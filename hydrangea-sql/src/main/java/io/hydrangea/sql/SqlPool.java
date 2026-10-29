package io.hydrangea.sql;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A pool of connections to one database, the same for PostgreSQL ({@code PgPool.asSql()}) and MySQL ({@code MyPool.asSql()}). A statement goes to the
 * least busy connection; a transaction takes a connection for itself and the calls of its body must go through the {@link SqlClient} it is given.
 * <pre>{@code
 * SqlPool db = PgPool.create(group, config, 8).join().asSql();      // or MyPool.create(...).join().asSql()
 * CompletableFuture<String> name = db.execute("SELECT name FROM users WHERE id = ?", 42).thenApply(r -> r.first().getString("name"));
 * CompletableFuture<Void> moved = db.transaction(tx ->
 *         tx.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, from)
 *           .thenCompose(r -> tx.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, to))
 *           .thenApply(r -> null));
 * }</pre>
 * What differs between the databases and is not hidden here: the SQL itself ({@code RETURNING}, {@code ON CONFLICT} against {@code ON DUPLICATE KEY},
 * the generated key of an INSERT: {@code RETURNING id} against {@code MyResult.lastInsertId()}), the default isolation level (READ COMMITTED against
 * REPEATABLE READ), DDL inside a transaction (transactional in PostgreSQL, an implicit commit in MySQL), and the type mappings of {@link SqlRow#getObject}.
 */
public interface SqlPool extends AutoCloseable {

    CompletableFuture<SqlResult> execute(String sql, Object... params);

    CompletableFuture<SqlResult> execute(Duration timeout, String sql, Object... params);

    /**
     * Runs {@code body} in a transaction on a connection of its own: begin, then the stage {@code body} returns, then COMMIT if it succeeded or ROLLBACK
     * if it failed. The transaction's statements go through the {@link SqlClient} given to {@code body}, which is only valid until the body's stage completes.
     */
    <T> CompletableFuture<T> transaction(Function<SqlClient, ? extends CompletionStage<T>> body);

    /** As {@link #transaction(Function)} with an isolation level and read-only. */
    <T> CompletableFuture<T> transaction(SqlIsolation isolation, boolean readOnly, Function<SqlClient, ? extends CompletionStage<T>> body);

    /**
     * As {@link #transaction(Function)}, run again (up to {@code maxAttempts} times in all, with a short growing pause) when it fails with a
     * {@link SqlException#isRetriableServerError() serialization failure or deadlock}. {@code body} may therefore run more than once.
     */
    <T> CompletableFuture<T> transactionWithRetry(int maxAttempts, Function<SqlClient, ? extends CompletionStage<T>> body);

    <T> CompletableFuture<T> transactionWithRetry(SqlIsolation isolation, boolean readOnly, int maxAttempts, Function<SqlClient, ? extends CompletionStage<T>> body);

    /** The number of connections. */
    int size();

    /** Closes every connection; statements in flight fail. */
    CompletableFuture<Void> disconnect();

    @Override
    default void close() {
        disconnect();
    }
}
