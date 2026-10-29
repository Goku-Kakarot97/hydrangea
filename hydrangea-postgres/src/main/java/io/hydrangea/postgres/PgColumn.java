package io.hydrangea.postgres;

/** One column of a result: its name and the OID of its type (see {@code pg_type}). */
public record PgColumn(String name, int typeOid) {
}
