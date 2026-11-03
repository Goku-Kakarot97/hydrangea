package io.hydrangea.log;

import java.time.ZoneId;

/**
 * Text lines for people:
 * <pre>
 * 2026-11-03 10:15:42.123 INFO  [main] io.hydrangea.http.HttpServerStage - listening port=8080
 * 2026-11-03 10:15:42.124 ERROR [loop-1] io.hydrangea.Server - accept failed
 * java.io.IOException: too many open files
 *     at ...
 * </pre>
 * Context entries and fields follow the message as {@code key=value}; a value with a space, quote or {@code =} is quoted. Messages,
 * values and exception messages are sanitized so one event is one line (plus its stack trace).
 */
public final class LineFormat implements LogFormat {

    static final LineFormat DEFAULT = new LineFormat(ZoneId.systemDefault(), false);

    private final Timestamps timestamps;
    private final boolean shortLogger;

    public LineFormat() {
        this(ZoneId.systemDefault(), false);
    }

    /** @param shortLogger only the last segment of the logger name ({@code HttpServerStage} for {@code io.hydrangea.http.HttpServerStage}) */
    public LineFormat(ZoneId zone, boolean shortLogger) {
        this.timestamps = new Timestamps(zone, false);
        this.shortLogger = shortLogger;
    }

    @Override
    public void format(LogEvent e, StringBuilder sb) {
        timestamps.append(sb, e.timestamp());
        sb.append(' ').append(e.level().padded()).append(" [");
        thread(sb, e);
        sb.append("] ");
        String logger = e.logger();
        if (shortLogger) {
            sb.append(logger, logger.lastIndexOf('.') + 1, logger.length());
        } else {
            sb.append(logger);
        }
        sb.append(" - ");
        Sanitizer.append(sb, e.message());
        for (int i = 0, n = e.contextCount(); i < n; i++) {
            pair(sb, e.contextKey(i), e.contextValue(i));
        }
        for (int i = 0, n = e.fieldCount(); i < n; i++) {
            pair(sb, e.fieldKey(i), e.fieldValue(i));
        }
        if (e.cause() != null) {
            sb.append('\n');
            Throwables.append(sb, e.cause());
        }
        sb.append('\n');
    }

    static void thread(StringBuilder sb, LogEvent e) {
        if (e.thread().isEmpty()) {
            sb.append("thread-").append(e.threadId());
        } else {
            Sanitizer.append(sb, e.thread());
        }
    }

    private static void pair(StringBuilder sb, String key, Object value) {
        sb.append(' ');
        Sanitizer.append(sb, key);
        sb.append('=');
        int start = sb.length();
        if (value instanceof CharSequence s) {
            Sanitizer.append(sb, s);
        } else {
            StringBuilder tmp = new StringBuilder();
            Messages.append(tmp, value);
            Sanitizer.append(sb, tmp);
        }
        boolean quote = sb.length() == start;
        for (int i = start, n = sb.length(); i < n && !quote; i++) {
            char c = sb.charAt(i);
            quote = c == ' ' || c == '"' || c == '=';
        }
        if (quote) {
            for (int i = sb.length() - 1; i >= start; i--) {
                if (sb.charAt(i) == '"' || sb.charAt(i) == '\\') {
                    sb.insert(i, '\\');
                }
            }
            sb.insert(start, '"').append('"');
        }
    }
}
