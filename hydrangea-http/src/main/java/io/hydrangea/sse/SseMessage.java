package io.hydrangea.sse;

/**
 * An event received by {@link SseClient}.
 *
 * @param event the event type, "message" when the server did not name one
 * @param data  the data lines joined with "\n" (no trailing newline)
 * @param id    the last event id in effect when this event was dispatched; null if the server never sent one
 */
public record SseMessage(String event, String data, String id) {
}
