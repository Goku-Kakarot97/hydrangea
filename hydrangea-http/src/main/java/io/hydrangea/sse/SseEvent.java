package io.hydrangea.sse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * One Server-Sent Event. Immutable; build with {@code SseEvent.data("...").event("price").id("42")}.
 * <ul>
 *   <li>{@code data}: may contain newlines (any of \n, \r\n, \r); each line goes out as its own {@code data:} field, so
 *       payload text can never be mistaken for another field.</li>
 *   <li>{@code event}: the type; clients listen with {@code addEventListener(type, ...)}; default type is "message".</li>
 *   <li>{@code id}: becomes the client's {@code lastEventId} and is sent back as {@code Last-Event-ID} on reconnect.</li>
 *   <li>{@code retry}: tells the client how long to wait before reconnecting.</li>
 *   <li>{@code comment}: ignored by clients; used for heartbeats.</li>
 * </ul>
 * {@code event} and {@code id} must not contain line breaks (they would let a value smuggle in extra fields) and
 * {@code id} must not contain NUL; violations throw {@link IllegalArgumentException}.
 */
public record SseEvent(String data, String event, String id, Duration retry, String comment) {

    public SseEvent {
        if (event != null && hasLineBreak(event)) {
            throw new IllegalArgumentException("event name must not contain line breaks");
        }
        if (id != null && (hasLineBreak(id) || id.indexOf('\0') >= 0)) {
            throw new IllegalArgumentException("event id must not contain line breaks or NUL");
        }
        if (retry != null && (retry.isNegative() || retry.toMillis() > Integer.MAX_VALUE)) {
            throw new IllegalArgumentException("retry out of range");
        }
    }

    public static SseEvent data(String data) {
        return new SseEvent(data, null, null, null, null);
    }

    public static SseEvent comment(String comment) {
        return new SseEvent(null, null, null, null, comment);
    }

    public SseEvent event(String event) {
        return new SseEvent(data, event, id, retry, comment);
    }

    public SseEvent id(String id) {
        return new SseEvent(data, event, id, retry, comment);
    }

    public SseEvent retry(Duration retry) {
        return new SseEvent(data, event, id, retry, comment);
    }

    /** Adds a comment line to this event (use {@link #comment(String)} for a comment-only heartbeat). */
    public SseEvent withComment(String comment) {
        return new SseEvent(data, event, id, retry, comment);
    }

    /** The wire format: field lines followed by a blank line. */
    byte[] encode() {
        StringBuilder sb = new StringBuilder(64 + (data == null ? 0 : data.length()));
        if (comment != null) {
            for (String line : lines(comment)) {
                sb.append(':').append(line.isEmpty() ? "" : " ").append(line).append('\n');
            }
        }
        if (retry != null) {
            sb.append("retry: ").append(retry.toMillis()).append('\n');
        }
        if (event != null) {
            sb.append("event: ").append(event).append('\n');
        }
        if (id != null) {
            sb.append("id: ").append(id).append('\n');
        }
        if (data != null) {
            for (String line : lines(data)) {
                sb.append("data: ").append(line).append('\n');
            }
        }
        sb.append('\n');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean hasLineBreak(String s) {
        return s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
    }

    /** Splits on \r\n, \r and \n, keeping empty lines (the trailing empty piece of "a\n" is a real empty data line). */
    private static String[] lines(String s) {
        return s.split("\r\n|\r|\n", -1);
    }
}
