package io.hydrangea.log;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Renders an exception the way {@link Throwable#printStackTrace} does (cause chain, suppressed, "... n more"), but into a
 * {@link StringBuilder}, with exception messages sanitized, cycles cut and the chain bounded.
 */
final class Throwables {

    private static final int MAX_DEPTH = 64;

    private Throwables() {
    }

    /** Appends the trace; starts with the exception's own line, with no leading line break, every further line with {@code \n}. */
    static void append(StringBuilder sb, Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        seen.add(t);
        header(sb, t);
        StackTraceElement[] trace = trace(t);
        for (StackTraceElement el : trace) {
            sb.append("\n\tat ").append(el);
        }
        for (Throwable s : t.getSuppressed()) {
            enclosed(sb, s, trace, "Suppressed: ", "\t", seen, 1);
        }
        Throwable cause = t.getCause();
        if (cause != null) {
            enclosed(sb, cause, trace, "Caused by: ", "", seen, 1);
        }
    }

    private static void enclosed(StringBuilder sb, Throwable t, StackTraceElement[] enclosing, String caption, String prefix,
                                 Set<Throwable> seen, int depth) {
        sb.append('\n').append(prefix).append(caption);
        if (!seen.add(t)) {
            sb.append("[CIRCULAR REFERENCE: ");
            header(sb, t);
            sb.append(']');
            return;
        }
        if (depth > MAX_DEPTH) {
            sb.append("... (chain truncated after ").append(MAX_DEPTH).append(" causes)");
            return;
        }
        header(sb, t);
        StackTraceElement[] trace = trace(t);
        int m = trace.length - 1;
        int n = enclosing.length - 1;
        while (m >= 0 && n >= 0 && trace[m].equals(enclosing[n])) {
            m--;
            n--;
        }
        int common = trace.length - 1 - m;
        for (int i = 0; i <= m; i++) {
            sb.append('\n').append(prefix).append("\tat ").append(trace[i]);
        }
        if (common > 0) {
            sb.append('\n').append(prefix).append("\t... ").append(common).append(" more");
        }
        for (Throwable s : t.getSuppressed()) {
            enclosed(sb, s, trace, "Suppressed: ", prefix + "\t", seen, depth + 1);
        }
        Throwable cause = t.getCause();
        if (cause != null) {
            enclosed(sb, cause, trace, "Caused by: ", prefix, seen, depth + 1);
        }
    }

    private static StackTraceElement[] trace(Throwable t) {
        try {
            StackTraceElement[] trace = t.getStackTrace();
            return trace == null ? new StackTraceElement[0] : trace;
        } catch (RuntimeException e) {
            return new StackTraceElement[0];
        }
    }

    /** {@code the.Class: message}, message sanitized; a {@code getMessage} that throws does not escape. */
    static void header(StringBuilder sb, Throwable t) {
        sb.append(t.getClass().getName());
        String message;
        try {
            message = t.getLocalizedMessage();
        } catch (RuntimeException e) {
            message = "[getMessage failed: " + e.getClass().getName() + "]";
        }
        if (message != null) {
            sb.append(": ");
            Sanitizer.append(sb, message);
        }
    }
}
