package io.hydrangea.log;

/**
 * Turns an event into text, appended to a builder the appender reuses. Implementations must be thread safe (they are shared between
 * appenders and, in synchronous mode, called from any thread) and must end the text with {@code \n}.
 */
@FunctionalInterface
public interface LogFormat {

    void format(LogEvent event, StringBuilder out);

    /** {@code 2026-11-03 10:15:42.123 INFO  [thread] logger - message key=value} and the stack trace, local time. */
    static LogFormat line() {
        return LineFormat.DEFAULT;
    }

    /** One JSON object per line, UTC timestamps. */
    static LogFormat json() {
        return JsonFormat.DEFAULT;
    }
}
