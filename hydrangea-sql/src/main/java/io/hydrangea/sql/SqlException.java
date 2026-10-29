package io.hydrangea.sql;

/**
 * What the PostgreSQL and MySQL clients throw ({@code PgException}, {@code MyException} extend it): an error the server sent, or a failure of the
 * connection or of the client. The classifications here are the ones application code acts on; for the database's own detail (SQLSTATE, error number,
 * detail and hint fields) catch the concrete class.
 */
public abstract class SqlException extends RuntimeException {

    protected SqlException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The five-character SQLSTATE the server sent, or null for a failure that did not come from the server (PostgreSQL always sends one; MySQL for most errors). */
    public abstract String sqlState();

    /** True when the client knows the statement was not executed (not sent, or the server said it rolled it back), so it can be sent again as it is. */
    public abstract boolean retriable();

    /** True when the call gave up waiting; the statement may still have run. */
    public abstract boolean isTimeout();

    /** A unique or primary key violation (SQLSTATE 23505, MySQL error 1062). */
    public abstract boolean isDuplicateKey();

    /** A failure the database says to answer by running the transaction again: a serialization failure or a deadlock (PostgreSQL 40001, 40P01; MySQL 1213, 1205). */
    public abstract boolean isRetriableServerError();
}
