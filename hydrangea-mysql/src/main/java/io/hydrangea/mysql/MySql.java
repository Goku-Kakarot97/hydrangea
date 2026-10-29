package io.hydrangea.mysql;

import io.hydrangea.sql.SqlClient;
import io.hydrangea.sql.SqlIsolation;
import io.hydrangea.sql.SqlPool;
import io.hydrangea.sql.SqlResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/** The {@link SqlClient} and {@link SqlPool} views of {@link MyClient} and {@link MyPool}: the placeholders are already {@code ?}, everything is passed through. */
final class MySql {

    private MySql() {
    }

    static final class Client implements SqlClient {
        private final MyClient client;

        Client(MyClient client) {
            this.client = client;
        }

        @Override
        public CompletableFuture<SqlResult> execute(String sql, Object... params) {
            return client.query(sql, params).thenApply(r -> r);
        }

        @Override
        public CompletableFuture<SqlResult> execute(Duration timeout, String sql, Object... params) {
            return client.query(timeout, sql, params).thenApply(r -> r);
        }

        @Override
        public boolean isOpen() {
            return client.isOpen();
        }

        @Override
        public CompletableFuture<Void> disconnect() {
            return client.disconnect();
        }

        @Override
        public String toString() {
            return client.toString();
        }
    }

    static final class Pool implements SqlPool {
        private final MyPool pool;

        Pool(MyPool pool) {
            this.pool = pool;
        }

        @Override
        public CompletableFuture<SqlResult> execute(String sql, Object... params) {
            return pool.query(sql, params).thenApply(r -> r);
        }

        @Override
        public CompletableFuture<SqlResult> execute(Duration timeout, String sql, Object... params) {
            return pool.query(timeout, sql, params).thenApply(r -> r);
        }

        @Override
        public <T> CompletableFuture<T> transaction(Function<SqlClient, ? extends CompletionStage<T>> body) {
            return pool.transaction(c -> body.apply(c.asSql()));
        }

        @Override
        public <T> CompletableFuture<T> transaction(SqlIsolation isolation, boolean readOnly, Function<SqlClient, ? extends CompletionStage<T>> body) {
            return pool.transaction(options(isolation, readOnly), c -> body.apply(c.asSql()));
        }

        @Override
        public <T> CompletableFuture<T> transactionWithRetry(int maxAttempts, Function<SqlClient, ? extends CompletionStage<T>> body) {
            return pool.transactionWithRetry(MyTxOptions.defaults(), maxAttempts, c -> body.apply(c.asSql()));
        }

        @Override
        public <T> CompletableFuture<T> transactionWithRetry(SqlIsolation isolation, boolean readOnly, int maxAttempts, Function<SqlClient, ? extends CompletionStage<T>> body) {
            return pool.transactionWithRetry(options(isolation, readOnly), maxAttempts, c -> body.apply(c.asSql()));
        }

        private static MyTxOptions options(SqlIsolation isolation, boolean readOnly) {
            MyTxOptions tx = isolation == null ? MyTxOptions.defaults() : MyTxOptions.of(MyTxOptions.Isolation.valueOf(isolation.name()));
            return readOnly ? tx.asReadOnly() : tx;
        }

        @Override
        public int size() {
            return pool.size();
        }

        @Override
        public CompletableFuture<Void> disconnect() {
            return pool.disconnect();
        }
    }
}
