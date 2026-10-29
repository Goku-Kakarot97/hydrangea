package io.hydrangea.postgres;

import java.util.List;

/**
 * The outcome of one statement: its rows (none for an {@code INSERT} without {@code RETURNING}) and its command tag
 * ({@code "SELECT 3"}, {@code "INSERT 0 1"}, {@code "UPDATE 7"}...).
 */
public final class PgResult implements io.hydrangea.sql.SqlResult {

    static final PgColumn[] NO_COLUMNS = new PgColumn[0];

    private final PgColumn[] columns;
    private final List<PgRow> rows;
    private final String tag;

    PgResult(PgColumn[] columns, List<PgRow> rows, String tag) {
        this.columns = columns;
        this.rows = rows;
        this.tag = tag;
    }

    public List<PgColumn> columns() {
        return List.of(columns);
    }

    @Override
    public List<String> columnNames() {
        List<String> names = new java.util.ArrayList<>(columns().size());
        for (PgColumn c : columns()) {
            names.add(c.name());
        }
        return names;
    }

    /** As {@link #updateCount()}, but 0 for a SELECT (whose command tag counts the rows returned). */
    @Override
    public long affectedRows() {
        String t = tag();
        return t != null && t.startsWith("SELECT") ? 0 : updateCount();
    }

    public List<PgRow> rows() {
        return rows;
    }

    public int size() {
        return rows.size();
    }

    /** The first row, or null if there is none. */
    public PgRow first() {
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** The command tag as the server sent it; empty for an empty query string. */
    public String tag() {
        return tag;
    }

    /** Rows affected by an {@code INSERT}, {@code UPDATE}, {@code DELETE}, {@code MERGE}, {@code SELECT}, {@code MOVE}, {@code FETCH} or {@code COPY}; -1 for other commands. */
    public long updateCount() {
        return countOf(tag);
    }

    static long countOf(String tag) {
        int sp = tag.lastIndexOf(' ');
        if (sp < 0) {
            return -1;
        }
        String cmd = tag.substring(0, tag.indexOf(' '));
        switch (cmd) {
            case "INSERT", "UPDATE", "DELETE", "MERGE", "SELECT", "MOVE", "FETCH", "COPY" -> {
                try {
                    return Long.parseLong(tag.substring(sp + 1));
                } catch (NumberFormatException e) {
                    return -1;
                }
            }
            default -> {
                return -1;
            }
        }
    }

    @Override
    public String toString() {
        return tag + " (" + rows.size() + " rows)";
    }
}
