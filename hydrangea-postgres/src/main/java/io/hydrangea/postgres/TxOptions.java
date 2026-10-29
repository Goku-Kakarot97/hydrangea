package io.hydrangea.postgres;

/**
 * How a transaction of {@link PgPool#transaction(TxOptions, java.util.function.Function)} begins: {@code BEGIN ISOLATION LEVEL ... READ ONLY ... DEFERRABLE}.
 * Savepoints need no support: send {@code SAVEPOINT x}, {@code ROLLBACK TO x} and {@code RELEASE x} as ordinary statements on the transaction's connection.
 *
 * @param isolation  null = the server's default (READ COMMITTED)
 * @param readOnly   {@code READ ONLY}: the server refuses writes
 * @param deferrable {@code DEFERRABLE}: only meaningful for SERIALIZABLE READ ONLY, where it waits for a moment in which it cannot be aborted by a serialization failure
 */
public record TxOptions(Isolation isolation, boolean readOnly, boolean deferrable) {

    public enum Isolation {
        READ_UNCOMMITTED("READ UNCOMMITTED"), READ_COMMITTED("READ COMMITTED"), REPEATABLE_READ("REPEATABLE READ"), SERIALIZABLE("SERIALIZABLE");

        final String sql;

        Isolation(String sql) {
            this.sql = sql;
        }
    }

    public TxOptions {
        if (deferrable && !readOnly) {
            throw new IllegalArgumentException("DEFERRABLE only goes with READ ONLY");
        }
    }

    public static TxOptions defaults() {
        return new TxOptions(null, false, false);
    }

    public static TxOptions of(Isolation isolation) {
        return new TxOptions(isolation, false, false);
    }

    public TxOptions asReadOnly() {
        return new TxOptions(isolation, true, deferrable);
    }

    public TxOptions asDeferrable() {
        return new TxOptions(isolation, true, true);
    }

    String beginSql() {
        StringBuilder sb = new StringBuilder("BEGIN");
        if (isolation != null) {
            sb.append(" ISOLATION LEVEL ").append(isolation.sql);
        }
        if (readOnly) {
            sb.append(" READ ONLY");
        }
        if (deferrable) {
            sb.append(" DEFERRABLE");
        }
        return sb.toString();
    }
}
