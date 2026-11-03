package io.hydrangea.log;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One log call, frozen: when, how severe, from which logger and thread, the rendered message, the exception, the structured fields
 * given with {@link LogEntry#kv} and the thread's {@link LogContext} at the time. Immutable, so it can travel through the async
 * queue and the {@link LogChain} without copies; stages that want to change it make a copy with {@link #withMessage} or {@link #with}.
 */
public final class LogEvent {

    private static final Object[] NO_FIELDS = new Object[0];
    private static final String[] NO_CONTEXT = new String[0];

    private final long timestamp;
    private final Level level;
    private final String logger;
    private final String thread;
    private final long threadId;
    private final String message;
    private final Throwable cause;
    private final Object[] fields;
    private final int fieldCount;
    private final String[] context;

    /**
     * @param timestamp  epoch milliseconds
     * @param fields     alternating key, value; may be {@code null}; only the first {@code fieldCount} entries count
     * @param context    alternating key, value from {@link LogContext}; may be {@code null}
     */
    public LogEvent(long timestamp, Level level, String logger, String thread, long threadId, String message, Throwable cause,
                    Object[] fields, int fieldCount, String[] context) {
        this.timestamp = timestamp;
        this.level = Objects.requireNonNull(level, "level");
        this.logger = Objects.requireNonNull(logger, "logger");
        this.thread = thread == null ? "" : thread;
        this.threadId = threadId;
        this.message = message == null ? "" : message;
        this.cause = cause;
        this.fields = fields == null ? NO_FIELDS : fields;
        this.fieldCount = fields == null ? 0 : fieldCount;
        this.context = context == null ? NO_CONTEXT : context;
    }

    /** Epoch milliseconds. */
    public long timestamp() {
        return timestamp;
    }

    public Level level() {
        return level;
    }

    public String logger() {
        return logger;
    }

    /** The name of the thread that logged; empty for an unnamed virtual thread (see {@link #threadId}). */
    public String thread() {
        return thread;
    }

    public long threadId() {
        return threadId;
    }

    /** The message with its {@code {}} placeholders filled in. */
    public String message() {
        return message;
    }

    /** The exception, or {@code null}. */
    public Throwable cause() {
        return cause;
    }

    /** How many key/value fields the call attached. */
    public int fieldCount() {
        return fieldCount;
    }

    public String fieldKey(int i) {
        Objects.checkIndex(i, fieldCount);
        return (String) fields[2 * i];
    }

    public Object fieldValue(int i) {
        Objects.checkIndex(i, fieldCount);
        return fields[2 * i + 1];
    }

    /** The first field with that key, or {@code null}. */
    public Object field(String key) {
        for (int i = 0; i < fieldCount; i++) {
            if (key.equals(fields[2 * i])) {
                return fields[2 * i + 1];
            }
        }
        return null;
    }

    /** The fields as a map (allocates; the indexed accessors do not). */
    public Map<String, Object> fields() {
        if (fieldCount == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            m.put((String) fields[2 * i], fields[2 * i + 1]);
        }
        return m;
    }

    /** How many {@link LogContext} entries the logging thread had. */
    public int contextCount() {
        return context.length / 2;
    }

    public String contextKey(int i) {
        Objects.checkIndex(i, contextCount());
        return context[2 * i];
    }

    public String contextValue(int i) {
        Objects.checkIndex(i, contextCount());
        return context[2 * i + 1];
    }

    /** The context entry with that key, or {@code null}. */
    public String context(String key) {
        for (int i = 0; i < context.length; i += 2) {
            if (key.equals(context[i])) {
                return context[i + 1];
            }
        }
        return null;
    }

    /** The context as a map (allocates). */
    public Map<String, String> context() {
        if (context.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < context.length; i += 2) {
            m.put(context[i], context[i + 1]);
        }
        return m;
    }

    /** A copy with another message (for redaction stages). */
    public LogEvent withMessage(String newMessage) {
        return new LogEvent(timestamp, level, logger, thread, threadId, newMessage, cause, fields, fieldCount, context);
    }

    /** A copy with one more field (for enrichment stages). */
    public LogEvent with(String key, Object value) {
        Object[] f = new Object[2 * fieldCount + 2];
        System.arraycopy(fields, 0, f, 0, 2 * fieldCount);
        f[2 * fieldCount] = Objects.requireNonNull(key, "key");
        f[2 * fieldCount + 1] = value;
        return new LogEvent(timestamp, level, logger, thread, threadId, message, cause, f, fieldCount + 1, context);
    }

    /** A copy without the exception. */
    public LogEvent withoutCause() {
        return new LogEvent(timestamp, level, logger, thread, threadId, message, null, fields, fieldCount, context);
    }

    @Override
    public String toString() {
        return level + " " + logger + " - " + message;
    }
}
