package io.hydrangea.log;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A log call built step by step: {@code log.at(Level.INFO).kv("order", id).kv("ms", took).cause(e).log("slow {}", what)}. Fields
 * travel with the event (text output: {@code order=42 ms=317}; JSON: top-level keys). When the level is off, {@link Logger#at}
 * returns a shared no-op entry and nothing here allocates.
 */
public final class LogEntry {

    static final LogEntry NOOP = new LogEntry(null, null);

    private final Logger logger;
    private final Level level;
    private Object[] fields;
    private int size;
    private Throwable cause;

    LogEntry(Logger logger, Level level) {
        this.logger = logger;
        this.level = level;
    }

    /** {@code false} for the no-op entry. */
    public boolean enabled() {
        return logger != null;
    }

    public LogEntry kv(String key, Object value) {
        if (logger == null) {
            return this;
        }
        Objects.requireNonNull(key, "key");
        if (fields == null) {
            fields = new Object[8];
        } else if (size == fields.length) {
            fields = Arrays.copyOf(fields, size * 2);
        }
        fields[size++] = key;
        fields[size++] = value;
        return this;
    }

    public LogEntry cause(Throwable cause) {
        if (logger != null) {
            this.cause = cause;
        }
        return this;
    }

    public void log(String message) {
        if (logger != null) {
            logger.emit(level, message, cause, fields, size / 2);
        }
    }

    /** {@code {}} placeholders as in {@link Logger}; a trailing exception without a placeholder becomes the cause unless one was set. */
    public void log(String message, Object... args) {
        if (logger == null) {
            return;
        }
        int n = args == null ? 0 : args.length;
        Throwable t = cause;
        if (t == null && n > 0 && args[n - 1] instanceof Throwable x && !Messages.hasPlaceholders(message, n)) {
            t = x;
            n--;
        }
        logger.emit(level, n == 0 ? message : Messages.render(message, args, n), t, fields, size / 2);
    }

    public void log(Supplier<String> message) {
        if (logger != null) {
            logger.emit(level, message.get(), cause, fields, size / 2);
        }
    }
}
