package io.hydrangea.mysql;

import java.util.ArrayList;
import java.util.List;

/**
 * How a transaction of {@link MyPool#transaction(MyTxOptions, java.util.function.Function)} begins: {@code START TRANSACTION [READ ONLY]}, after a
 * {@code SET TRANSACTION ISOLATION LEVEL ...} when an isolation level is given (MySQL sets the level apart from the transaction; it holds for the next one only).
 *
 * @param isolation null = the server's default (REPEATABLE READ)
 * @param readOnly  {@code READ ONLY}: the server refuses writes
 */
public record MyTxOptions(Isolation isolation, boolean readOnly) {

    public enum Isolation {
        READ_UNCOMMITTED("READ UNCOMMITTED"), READ_COMMITTED("READ COMMITTED"), REPEATABLE_READ("REPEATABLE READ"), SERIALIZABLE("SERIALIZABLE");

        final String sql;

        Isolation(String sql) {
            this.sql = sql;
        }
    }

    public static MyTxOptions defaults() {
        return new MyTxOptions(null, false);
    }

    public static MyTxOptions of(Isolation isolation) {
        return new MyTxOptions(isolation, false);
    }

    public MyTxOptions asReadOnly() {
        return new MyTxOptions(isolation, true);
    }

    List<String> beginSql() {
        List<String> l = new ArrayList<>(2);
        if (isolation != null) {
            l.add("SET TRANSACTION ISOLATION LEVEL " + isolation.sql);
        }
        l.add(readOnly ? "START TRANSACTION READ ONLY" : "START TRANSACTION");
        return l;
    }
}
