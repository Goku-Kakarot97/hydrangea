package io.hydrangea.sql;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * One connection, or the connection a transaction runs on. Placeholders are {@code ?} for both databases (the PostgreSQL adapter turns them into
 * {@code $1, $2...}; a literal {@code ?} for PostgreSQL's JSON operators is written {@code ??}, as with pgjdbc). Parameters: null, String, numbers,
 * Boolean, byte[], UUID, BigDecimal, an Enum (its name), {@code java.time} LocalDate, LocalTime, LocalDateTime, Instant, OffsetDateTime; each
 * client's own {@code query} documents what more it takes. {@code SELECT ... RETURNING}, arrays, COPY, LISTEN and the other features of one database
 * are reached through that database's own client ({@code PgClient}, {@code MyClient}), which the adapters give back.
 */
public interface SqlClient extends AutoCloseable {

    /** Runs one statement with parameters; the future completes on the client's event loop thread, so what is chained to it must not block. */
    CompletableFuture<SqlResult> execute(String sql, Object... params);

    /** As {@link #execute(String, Object...)} with a time limit, counted from the call; see the concrete client for what is done with the running statement. */
    CompletableFuture<SqlResult> execute(Duration timeout, String sql, Object... params);

    /** True once logged in and until closed or lost. */
    boolean isOpen();

    /** Closes the connection; calls still waiting fail. */
    CompletableFuture<Void> disconnect();

    @Override
    default void close() {
        disconnect();
    }
}
