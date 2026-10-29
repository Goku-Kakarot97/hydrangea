package io.hydrangea.mysql;

/** One column of a result: its name, MySQL type code ({@code enum_field_types}), flags and character set. */
public record MyColumn(String name, String table, int type, int flags, int charset, int decimals) {

    static final int UNSIGNED_FLAG = 32;
    static final int BINARY_CHARSET = 63;

    public boolean unsigned() {
        return (flags & UNSIGNED_FLAG) != 0;
    }

    /** True for BLOB and string types that carry bytes (character set 63), not text. */
    public boolean binary() {
        return charset == BINARY_CHARSET;
    }
}
