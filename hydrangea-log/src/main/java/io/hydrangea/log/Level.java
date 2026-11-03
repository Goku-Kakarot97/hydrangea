package io.hydrangea.log;

import java.util.Locale;

/**
 * Severity of a log event, lowest first. A logger set to a level lets through that level and everything above it; {@link #OFF}
 * lets nothing through.
 */
public enum Level {
    TRACE, DEBUG, INFO, WARN, ERROR, OFF;

    static final Level[] VALUES = values();

    private final String padded;

    Level() {
        padded = (name() + "     ").substring(0, 5);
    }

    /** The name padded to five characters ({@code "INFO "}), so text output lines up. */
    public String padded() {
        return padded;
    }

    /** {@code true} when this level is {@code other} or more severe. */
    public boolean atLeast(Level other) {
        return ordinal() >= other.ordinal();
    }

    /** Case-insensitive; also accepts the names other libraries use ({@code WARNING}, {@code FINE}, {@code SEVERE}, {@code ALL}, {@code FATAL} ...). */
    public static Level parse(String s) {
        return switch (s.trim().toUpperCase(Locale.ROOT)) {
            case "TRACE", "ALL", "FINEST", "FINER" -> TRACE;
            case "DEBUG", "FINE", "CONFIG" -> DEBUG;
            case "INFO" -> INFO;
            case "WARN", "WARNING" -> WARN;
            case "ERROR", "SEVERE", "FATAL" -> ERROR;
            case "OFF", "NONE" -> OFF;
            default -> throw new IllegalArgumentException("unknown log level: " + s);
        };
    }
}
