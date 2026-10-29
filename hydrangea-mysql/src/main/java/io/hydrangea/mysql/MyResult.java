package io.hydrangea.mysql;

import java.util.List;

/**
 * What one statement answered: the rows and columns of a result set, or for a statement that changes data the number of rows it affected
 * and the generated key. A {@code CALL} (or a statement of several results) answers one result after another: {@link #next()}.
 */
public final class MyResult implements io.hydrangea.sql.SqlResult {

    private final List<MyColumn> columns;
    private final List<MyRow> rows;
    private final long affectedRows;
    private final long lastInsertId;
    private final int warnings;
    private final int status;
    private MyResult next;

    MyResult(List<MyColumn> columns, List<MyRow> rows, long affectedRows, long lastInsertId, int warnings, int status) {
        this.columns = columns;
        this.rows = rows;
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.warnings = warnings;
        this.status = status;
    }

    public List<MyColumn> columns() {
        return columns;
    }

    @Override
    public List<String> columnNames() {
        List<String> names = new java.util.ArrayList<>(columns.size());
        for (MyColumn c : columns) {
            names.add(c.name());
        }
        return names;
    }

    public List<MyRow> rows() {
        return rows;
    }

    /** The first row; throws if there is none. */
    public MyRow first() {
        if (rows.isEmpty()) {
            throw new IllegalStateException("the result has no rows");
        }
        return rows.get(0);
    }

    /** Rows changed by INSERT, UPDATE or DELETE (the rows that changed, not those matched: UPDATE to the same value counts 0). */
    public long affectedRows() {
        return affectedRows;
    }

    /** The AUTO_INCREMENT value an INSERT generated (0 if none). */
    public long lastInsertId() {
        return lastInsertId;
    }

    public int warnings() {
        return warnings;
    }

    /** The server status flags of the answer (SERVER_STATUS_IN_TRANS = 1, SERVER_STATUS_AUTOCOMMIT = 2, SERVER_MORE_RESULTS_EXISTS = 8...). */
    public int status() {
        return status;
    }

    /** The next result of a multi-result answer, or null. */
    public MyResult next() {
        return next;
    }

    void setNext(MyResult r) {
        next = r;
    }

    public boolean hasRows() {
        return !columns.isEmpty();
    }
}
