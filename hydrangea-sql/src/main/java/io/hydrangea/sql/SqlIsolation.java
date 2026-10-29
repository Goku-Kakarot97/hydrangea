package io.hydrangea.sql;

/** The isolation level of a transaction of {@link SqlPool#transaction(SqlIsolation, boolean, java.util.function.Function)}. */
public enum SqlIsolation {
    READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
}
