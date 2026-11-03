package io.hydrangea.log;

import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * One JSON object per line, for log collectors:
 * <pre>
 * {"ts":"2026-11-03T03:15:42.123Z","level":"INFO","logger":"io.hydrangea.Server","thread":"main","msg":"listening","port":8080}
 * </pre>
 * {@code ts}, {@code level}, {@code logger}, {@code thread}, {@code msg} first, then the context entries and the fields as top-level
 * keys (a field named like a fixed key is written after it; most collectors keep the last), then {@code error} with {@code type},
 * {@code message} and {@code stack} when there is an exception. Numbers and booleans are written as JSON numbers and booleans,
 * everything else as a string. Strings are escaped per RFC 8259 (plus U+2028/2029); nothing can break the line.
 */
public final class JsonFormat implements LogFormat {

    static final JsonFormat DEFAULT = new JsonFormat(ZoneOffset.UTC);

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final Timestamps timestamps;

    public JsonFormat() {
        this(ZoneOffset.UTC);
    }

    public JsonFormat(ZoneId zone) {
        this.timestamps = new Timestamps(zone, true);
    }

    @Override
    public void format(LogEvent e, StringBuilder sb) {
        sb.append("{\"ts\":\"");
        timestamps.append(sb, e.timestamp());
        sb.append("\",\"level\":\"").append(e.level().name()).append("\",\"logger\":");
        string(sb, e.logger());
        sb.append(",\"thread\":\"");
        if (e.thread().isEmpty()) {
            sb.append("thread-").append(e.threadId());
        } else {
            escape(sb, e.thread());
        }
        sb.append("\",\"msg\":");
        string(sb, e.message());
        for (int i = 0, n = e.contextCount(); i < n; i++) {
            sb.append(',');
            string(sb, e.contextKey(i));
            sb.append(':');
            string(sb, e.contextValue(i));
        }
        for (int i = 0, n = e.fieldCount(); i < n; i++) {
            sb.append(',');
            string(sb, e.fieldKey(i));
            sb.append(':');
            value(sb, e.fieldValue(i));
        }
        Throwable t = e.cause();
        if (t != null) {
            sb.append(",\"error\":{\"type\":");
            string(sb, t.getClass().getName());
            String message;
            try {
                message = t.getLocalizedMessage();
            } catch (RuntimeException x) {
                message = null;
            }
            if (message != null) {
                sb.append(",\"message\":");
                string(sb, message);
            }
            sb.append(",\"stack\":");
            StringBuilder stack = new StringBuilder(512);
            Throwables.append(stack, t);
            string(sb, stack);
            sb.append('}');
        }
        sb.append("}\n");
    }

    static void value(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Boolean || v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            sb.append(v);
        } else if (v instanceof Double d) {
            if (d.isNaN() || d.isInfinite()) {
                string(sb, d.toString());
            } else {
                sb.append(d.doubleValue());
            }
        } else if (v instanceof Float f) {
            if (f.isNaN() || f.isInfinite()) {
                string(sb, f.toString());
            } else {
                sb.append(f.floatValue());
            }
        } else if (v instanceof CharSequence s) {
            string(sb, s);
        } else {
            StringBuilder tmp = new StringBuilder();
            Messages.append(tmp, v);
            string(sb, tmp);
        }
    }

    static void string(StringBuilder sb, CharSequence s) {
        sb.append('"');
        escape(sb, s);
        sb.append('"');
    }

    static void escape(StringBuilder sb, CharSequence s) {
        int len = s.length();
        int i = 0;
        while (i < len && plain(s.charAt(i))) {
            i++;
        }
        sb.append(s, 0, i);
        for (; i < len; i++) {
            char c = s.charAt(i);
            if (plain(c)) {
                sb.append(c);
                continue;
            }
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> sb.append("\\u").append(HEX[(c >> 12) & 0xf]).append(HEX[(c >> 8) & 0xf]).append(HEX[(c >> 4) & 0xf]).append(HEX[c & 0xf]);
            }
        }
    }

    private static boolean plain(char c) {
        return c >= 0x20 && c != '"' && c != '\\' && c != 0x7f && c != ' ' && c != ' ';
    }
}
