package io.hydrangea.sql;

import java.util.List;

/** What one statement answered: rows for a query, the number of rows changed for an INSERT, UPDATE or DELETE. */
public interface SqlResult {

    /** The column names, in order (empty for a statement that returns no rows). */
    List<String> columnNames();

    List<? extends SqlRow> rows();

    /** The first row; throws {@link IllegalStateException} if there is none. */
    SqlRow first();

    /** Rows changed by an INSERT, UPDATE or DELETE; 0 for a query. (MySQL counts the rows that changed, not those matched.) */
    long affectedRows();

    default boolean isEmpty() {
        return rows().isEmpty();
    }

    default int size() {
        return rows().size();
    }
}
