package io.hydrangea.postgres;

import java.util.Map;

/**
 * A failed query or connection. An error the server reported carries its SQLSTATE ({@link #sqlState()}, for example {@code 23505} for a unique
 * violation) and the other fields of the {@code ErrorResponse}. {@link #retriable()} is true when the query certainly did not run
 * (it never left the client); a query that was sent when the connection broke may or may not have been executed.
 */
public final class PgException extends io.hydrangea.sql.SqlException {

    private final Map<Character, String> fields;
    private final boolean retriable;
    private final boolean timeout;

    PgException(String message, boolean retriable, Throwable cause) {
        super(message, cause);
        this.fields = Map.of();
        this.retriable = retriable;
        this.timeout = false;
    }

    private PgException(String message, boolean retriable, boolean timeout, Map<Character, String> fields) {
        super(message, null);
        this.fields = fields;
        this.retriable = retriable;
        this.timeout = timeout;
    }

    /** A query that did not finish in time; it may have run (see {@link PgClient#query(java.time.Duration, String, Object...)}). */
    static PgException timeout(String message) {
        return new PgException(message, false, true, Map.of());
    }

    /** The error of one item of a batch: the same SQLSTATE and fields, with the position of the item in the message. */
    static PgException batchFailure(PgException e, int index) {
        return new PgException("batch item " + index + ": " + e.getMessage(), e.retriable(), false, e.fields());
    }

    /** A server error that this client knows did not take effect, so the query can be sent again. */
    static PgException retriableServerError(Map<Character, String> fields, String message) {
        return new PgException(message, true, false, fields);
    }

    PgException(String message, boolean retriable) {
        this(message, retriable, null);
    }

    PgException(Map<Character, String> fields) {
        super(fields.getOrDefault('M', "error") + (fields.containsKey('C') ? " (SQLSTATE " + fields.get('C') + ")" : ""), null);
        this.fields = fields;
        this.retriable = false;
        this.timeout = false;
    }

    /** SQLSTATE 23505: a unique or primary key violation. */
    @Override
    public boolean isDuplicateKey() {
        return "23505".equals(sqlState());
    }

    /** SQLSTATE 40001 (serialization failure) or 40P01 (deadlock): run the transaction again. */
    @Override
    public boolean isRetriableServerError() {
        String st = sqlState();
        return "40001".equals(st) || "40P01".equals(st);
    }

    /** The five-character SQLSTATE, or null for an error that did not come from the server. */
    public String sqlState() {
        return fields.get('C');
    }

    public String severity() {
        return fields.get('S');
    }

    public String detail() {
        return fields.get('D');
    }

    public String hint() {
        return fields.get('H');
    }

    /** Every field of the server's error message, by its one-letter code (S severity, C code, M message, D detail, H hint, P position...). */
    public Map<Character, String> fields() {
        return fields;
    }

    /** True if the query was given up on because it did not finish within its time limit ({@link PgConfig#queryTimeout()}). */
    public boolean isTimeout() {
        return timeout;
    }

    public boolean retriable() {
        return retriable;
    }
}
