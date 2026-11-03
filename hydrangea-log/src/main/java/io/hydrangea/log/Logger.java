package io.hydrangea.log;

import java.util.function.Supplier;

/**
 * What code logs through. One per name, usually the class name; get it once, keep it in a static field.
 *
 * <pre>{@code
 * private static final Logger log = Logger.get(OrderService.class);
 *
 * log.info("order {} placed by {}", orderId, user);           // {} placeholders, no String.format
 * log.error("payment failed", exception);                      // stack trace follows
 * log.at(Level.WARN).kv("order", orderId).kv("ms", took).log("slow");   // structured fields
 * }</pre>
 *
 * A call below the logger's level costs one volatile read and a compare: no varargs array, no rendering, no object. Above it, the
 * message is rendered on the calling thread (so arguments are read now, not later on the log thread) and the event is handed to the
 * dispatcher, which in async mode is one lock-free enqueue.
 * <p>
 * Arguments: {@code {}} placeholders are filled in order; {@code \{}} is a literal; an exception given as the last argument without a
 * placeholder for it is the event's cause and gets its stack trace printed (the SLF4J rule). Arrays print element by element.
 * <p>
 * Levels: the effective level comes from the most specific {@link Log#level(String, Level) configured prefix} of the name, else the
 * root level. Only {@link Level#OFF} is never enabled.
 */
public final class Logger {

    private static final int TRACE = 0;
    private static final int DEBUG = 1;
    private static final int INFO = 2;
    private static final int WARN = 3;
    private static final int ERROR = 4;

    private final String name;
    private volatile int threshold;

    Logger(String name, int threshold) {
        this.name = name;
        this.threshold = threshold;
    }

    public static Logger get(Class<?> type) {
        return Log.logger(type.getName());
    }

    public static Logger get(String name) {
        return Log.logger(name);
    }

    public String name() {
        return name;
    }

    /** The effective level. */
    public Level level() {
        return Level.VALUES[threshold];
    }

    /** Sets this logger's level (and, as a prefix, its descendants'); {@code null} removes the setting. Same as {@link Log#level(String, Level)}. */
    public void level(Level level) {
        Log.level(name, level);
    }

    void threshold(int threshold) {
        this.threshold = threshold;
    }

    public boolean isEnabled(Level level) {
        return level != Level.OFF && level.ordinal() >= threshold;
    }

    public boolean isTraceEnabled() {
        return TRACE >= threshold;
    }

    public boolean isDebugEnabled() {
        return DEBUG >= threshold;
    }

    public boolean isInfoEnabled() {
        return INFO >= threshold;
    }

    public boolean isWarnEnabled() {
        return WARN >= threshold;
    }

    public boolean isErrorEnabled() {
        return ERROR >= threshold;
    }

    /** A fluent event: add fields and a cause, then {@link LogEntry#log}. Costs nothing when the level is off. */
    public LogEntry at(Level level) {
        return isEnabled(level) ? new LogEntry(this, level) : LogEntry.NOOP;
    }

    // ---- trace

    public void trace(String message) {
        if (TRACE >= threshold) {
            emit(Level.TRACE, message, null, null, 0);
        }
    }

    public void trace(String message, Object arg) {
        if (TRACE >= threshold) {
            one(Level.TRACE, message, arg);
        }
    }

    public void trace(String message, Object arg1, Object arg2) {
        if (TRACE >= threshold) {
            two(Level.TRACE, message, arg1, arg2);
        }
    }

    public void trace(String message, Object... args) {
        if (TRACE >= threshold) {
            many(Level.TRACE, message, args);
        }
    }

    public void trace(String message, Throwable cause) {
        if (TRACE >= threshold) {
            emit(Level.TRACE, message, cause, null, 0);
        }
    }

    public void trace(Supplier<String> message) {
        if (TRACE >= threshold) {
            emit(Level.TRACE, message.get(), null, null, 0);
        }
    }

    // ---- debug

    public void debug(String message) {
        if (DEBUG >= threshold) {
            emit(Level.DEBUG, message, null, null, 0);
        }
    }

    public void debug(String message, Object arg) {
        if (DEBUG >= threshold) {
            one(Level.DEBUG, message, arg);
        }
    }

    public void debug(String message, Object arg1, Object arg2) {
        if (DEBUG >= threshold) {
            two(Level.DEBUG, message, arg1, arg2);
        }
    }

    public void debug(String message, Object... args) {
        if (DEBUG >= threshold) {
            many(Level.DEBUG, message, args);
        }
    }

    public void debug(String message, Throwable cause) {
        if (DEBUG >= threshold) {
            emit(Level.DEBUG, message, cause, null, 0);
        }
    }

    public void debug(Supplier<String> message) {
        if (DEBUG >= threshold) {
            emit(Level.DEBUG, message.get(), null, null, 0);
        }
    }

    // ---- info

    public void info(String message) {
        if (INFO >= threshold) {
            emit(Level.INFO, message, null, null, 0);
        }
    }

    public void info(String message, Object arg) {
        if (INFO >= threshold) {
            one(Level.INFO, message, arg);
        }
    }

    public void info(String message, Object arg1, Object arg2) {
        if (INFO >= threshold) {
            two(Level.INFO, message, arg1, arg2);
        }
    }

    public void info(String message, Object... args) {
        if (INFO >= threshold) {
            many(Level.INFO, message, args);
        }
    }

    public void info(String message, Throwable cause) {
        if (INFO >= threshold) {
            emit(Level.INFO, message, cause, null, 0);
        }
    }

    public void info(Supplier<String> message) {
        if (INFO >= threshold) {
            emit(Level.INFO, message.get(), null, null, 0);
        }
    }

    // ---- warn

    public void warn(String message) {
        if (WARN >= threshold) {
            emit(Level.WARN, message, null, null, 0);
        }
    }

    public void warn(String message, Object arg) {
        if (WARN >= threshold) {
            one(Level.WARN, message, arg);
        }
    }

    public void warn(String message, Object arg1, Object arg2) {
        if (WARN >= threshold) {
            two(Level.WARN, message, arg1, arg2);
        }
    }

    public void warn(String message, Object... args) {
        if (WARN >= threshold) {
            many(Level.WARN, message, args);
        }
    }

    public void warn(String message, Throwable cause) {
        if (WARN >= threshold) {
            emit(Level.WARN, message, cause, null, 0);
        }
    }

    public void warn(Supplier<String> message) {
        if (WARN >= threshold) {
            emit(Level.WARN, message.get(), null, null, 0);
        }
    }

    // ---- error

    public void error(String message) {
        if (ERROR >= threshold) {
            emit(Level.ERROR, message, null, null, 0);
        }
    }

    public void error(String message, Object arg) {
        if (ERROR >= threshold) {
            one(Level.ERROR, message, arg);
        }
    }

    public void error(String message, Object arg1, Object arg2) {
        if (ERROR >= threshold) {
            two(Level.ERROR, message, arg1, arg2);
        }
    }

    public void error(String message, Object... args) {
        if (ERROR >= threshold) {
            many(Level.ERROR, message, args);
        }
    }

    public void error(String message, Throwable cause) {
        if (ERROR >= threshold) {
            emit(Level.ERROR, message, cause, null, 0);
        }
    }

    public void error(Supplier<String> message) {
        if (ERROR >= threshold) {
            emit(Level.ERROR, message.get(), null, null, 0);
        }
    }

    // ---- any level

    public void log(Level level, String message, Object... args) {
        if (isEnabled(level)) {
            many(level, message, args);
        }
    }

    public void log(Level level, String message, Throwable cause) {
        if (isEnabled(level)) {
            emit(level, message, cause, null, 0);
        }
    }

    private void one(Level level, String message, Object arg) {
        if (arg instanceof Throwable t && !Messages.hasPlaceholders(message, 1)) {
            emit(level, message, t, null, 0);
        } else {
            emit(level, Messages.render(message, arg), null, null, 0);
        }
    }

    private void two(Level level, String message, Object arg1, Object arg2) {
        if (arg2 instanceof Throwable t && !Messages.hasPlaceholders(message, 2)) {
            emit(level, Messages.render(message, arg1), t, null, 0);
        } else {
            emit(level, Messages.render(message, arg1, arg2), null, null, 0);
        }
    }

    private void many(Level level, String message, Object[] args) {
        int n = args == null ? 0 : args.length;
        Throwable cause = null;
        if (n > 0 && args[n - 1] instanceof Throwable t && !Messages.hasPlaceholders(message, n)) {
            cause = t;
            n--;
        }
        emit(level, n == 0 ? message : Messages.render(message, args, n), cause, null, 0);
    }

    void emit(Level level, String message, Throwable cause, Object[] fields, int fieldCount) {
        Thread thread = Thread.currentThread();
        Log.publish(new LogEvent(System.currentTimeMillis(), level, name, thread.getName(), thread.threadId(), message, cause,
                fields, fieldCount, LogContext.snapshot()));
    }

    @Override
    public String toString() {
        return "Logger[" + name + " " + level() + "]";
    }
}
