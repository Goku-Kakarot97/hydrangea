package io.hydrangea.sql;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns the {@code ?} placeholders of {@link SqlClient} into PostgreSQL's {@code $1, $2...}. A {@code ?} inside a string literal ({@code '...'}, with
 * {@code ''} and E-strings' backslash escapes), a quoted identifier ({@code "..."}), a dollar-quoted string ({@code $tag$...$tag$}), a line comment
 * ({@code -- ...}) or a block comment ({@code /* ... *&#47;}, nested) is left alone; {@code ??} becomes one literal {@code ?} (the JSON operators: {@code ??|}).
 * Statements already written with {@code $n} and no {@code ?} pass through unchanged. The last 4096 distinct statements are remembered.
 */
public final class Placeholders {

    private static final int CACHE_LIMIT = 4096;
    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    private Placeholders() {
    }

    public static String toDollar(String sql) {
        if (sql.indexOf('?') < 0) {
            return sql;
        }
        String done = CACHE.get(sql);
        if (done != null) {
            return done;
        }
        done = convert(sql);
        if (CACHE.size() >= CACHE_LIMIT) {
            CACHE.clear();
        }
        CACHE.put(sql, done);
        return done;
    }

    static String convert(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 8);
        int n = sql.length();
        int param = 0;
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '?') {
                if (i + 1 < n && sql.charAt(i + 1) == '?') {
                    out.append('?');
                    i += 2;
                } else {
                    out.append('$').append(++param);
                    i++;
                }
            } else if (c == '\'' || c == '"') {
                boolean escapes = c == '\'' && i > 0 && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e') && (i == 1 || !Character.isLetterOrDigit(sql.charAt(i - 2)));
                int j = i + 1;
                while (j < n) {
                    char d = sql.charAt(j);
                    if (escapes && d == '\\' && j + 1 < n) {
                        j += 2;
                        continue;
                    }
                    if (d == c) {
                        if (j + 1 < n && sql.charAt(j + 1) == c) {        // '' or "" inside
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                int end = Math.min(n, j + 1);
                out.append(sql, i, end);
                i = end;
            } else if (c == '$' && (i + 1 < n) && (sql.charAt(i + 1) == '$' || Character.isLetter(sql.charAt(i + 1)) || sql.charAt(i + 1) == '_')) {
                int j = i + 1;
                while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
                    j++;
                }
                if (j < n && sql.charAt(j) == '$') {                          // a dollar-quoted string: find its closing tag
                    String tag = sql.substring(i, j + 1);
                    int close = sql.indexOf(tag, j + 1);
                    int end = close < 0 ? n : close + tag.length();
                    out.append(sql, i, end);
                    i = end;
                } else {
                    out.append(sql, i, j);
                    i = j;
                }
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                end = end < 0 ? n : end;
                out.append(sql, i, end);
                i = end;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int depth = 1;
                int j = i + 2;
                while (j < n && depth > 0) {
                    if (sql.startsWith("/*", j)) {
                        depth++;
                        j += 2;
                    } else if (sql.startsWith("*/", j)) {
                        depth--;
                        j += 2;
                    } else {
                        j++;
                    }
                }
                out.append(sql, i, j);
                i = j;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
