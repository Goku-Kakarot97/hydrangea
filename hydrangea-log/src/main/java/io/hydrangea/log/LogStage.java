package io.hydrangea.log;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * One step of a {@link LogChain}: a filter, an enricher, a redactor, an appender, an alerting hook. It gets the event and the rest of
 * the chain; it may call {@code next.next(event)} with the same or a modified event, or not call it and so stop the event there.
 * <p>
 * In asynchronous mode every stage runs on the single log thread, one event at a time, so a stage needs no locking of its own; in
 * synchronous mode it runs on whichever thread logged, so it must be thread safe ({@link Appender} locks internally). A stage must not
 * block for long: it holds up every event behind it.
 */
@FunctionalInterface
public interface LogStage {

    void handle(LogEvent event, LogChain next);

    /** Called when a batch of events is done (async) or after every event (sync); appenders push buffered output out. */
    default void flush() {
    }

    /** Called when the configuration that used this stage is replaced, for stages the configuration created itself. */
    default void close() {
    }

    /** Passes on the events the predicate accepts. */
    static LogStage filter(Predicate<? super LogEvent> keep) {
        return (event, next) -> {
            if (keep.test(event)) {
                next.next(event);
            }
        };
    }

    /** Passes on {@code level} and above. */
    static LogStage minLevel(Level level) {
        return filter(e -> e.level().atLeast(level));
    }

    /** Passes on events from loggers whose name is {@code prefix} or starts with {@code prefix + "."}. */
    static LogStage loggerPrefix(String prefix) {
        return filter(e -> e.logger().equals(prefix) || (e.logger().startsWith(prefix) && e.logger().charAt(prefix.length()) == '.'));
    }

    /** Passes on what the function returns (a copy with a redacted message, an added field ...), or stops the event when it returns {@code null}. */
    static LogStage map(UnaryOperator<LogEvent> f) {
        return (event, next) -> {
            LogEvent out = f.apply(event);
            if (out != null) {
                next.next(out);
            }
        };
    }

    /** Runs the consumer and passes the event on unchanged. */
    static LogStage each(Consumer<? super LogEvent> consumer) {
        return (event, next) -> {
            consumer.accept(event);
            next.next(event);
        };
    }
}
