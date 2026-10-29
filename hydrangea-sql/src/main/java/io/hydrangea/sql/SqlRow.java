package io.hydrangea.sql;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * One row of a {@link SqlResult}, the same for both databases. Indexes start at 0; names are matched without regard to case. A NULL column gives
 * {@code null} from the getters that return objects and 0 or false from the ones that return primitives (as JDBC does): ask {@link #isNull} first
 * when the column may be NULL. {@link #getObject} gives the database's own mapping (the concrete row class documents it); the typed getters convert
 * what can be converted and throw {@link ClassCastException} or {@link NumberFormatException} otherwise.
 */
public interface SqlRow {

    int size();

    /** The index of a column by name, or -1. */
    int indexOf(String name);

    boolean isNull(int i);

    Object getObject(int i);

    String getString(int i);

    int getInt(int i);

    long getLong(int i);

    double getDouble(int i);

    boolean getBoolean(int i);

    BigDecimal getBigDecimal(int i);

    byte[] getBytes(int i);

    default LocalDate getLocalDate(int i) {
        Object o = getObject(i);
        if (o == null) {
            return null;
        }
        if (o instanceof LocalDate d) {
            return d;
        }
        if (o instanceof LocalDateTime t) {
            return t.toLocalDate();
        }
        if (o instanceof String s) {
            return LocalDate.parse(s);
        }
        throw new ClassCastException("column " + i + " is a " + o.getClass().getSimpleName() + ", not a date: use getObject");
    }

    default LocalDateTime getLocalDateTime(int i) {
        Object o = getObject(i);
        if (o == null) {
            return null;
        }
        if (o instanceof LocalDateTime t) {
            return t;
        }
        if (o instanceof LocalDate d) {
            return d.atStartOfDay();
        }
        if (o instanceof String s) {
            return LocalDateTime.parse(s.replace(' ', 'T'));
        }
        throw new ClassCastException("column " + i + " is a " + o.getClass().getSimpleName() + ", not a date and time without a zone: use getObject");
    }

    private int index(String name) {
        int i = indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("no column named " + name);
        }
        return i;
    }

    default boolean isNull(String name) {
        return isNull(index(name));
    }

    default Object getObject(String name) {
        return getObject(index(name));
    }

    default String getString(String name) {
        return getString(index(name));
    }

    default int getInt(String name) {
        return getInt(index(name));
    }

    default long getLong(String name) {
        return getLong(index(name));
    }

    default double getDouble(String name) {
        return getDouble(index(name));
    }

    default boolean getBoolean(String name) {
        return getBoolean(index(name));
    }

    default BigDecimal getBigDecimal(String name) {
        return getBigDecimal(index(name));
    }

    default byte[] getBytes(String name) {
        return getBytes(index(name));
    }

    default LocalDate getLocalDate(String name) {
        return getLocalDate(index(name));
    }

    default LocalDateTime getLocalDateTime(String name) {
        return getLocalDateTime(index(name));
    }
}
