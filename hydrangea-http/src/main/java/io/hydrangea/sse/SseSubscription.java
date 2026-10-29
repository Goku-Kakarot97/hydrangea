package io.hydrangea.sse;

/** A running {@link SseClient} subscription. */
public interface SseSubscription {

    /** Stops for good: cancels the current connection and any pending reconnect. Idempotent; thread-safe. */
    void close();

    boolean isClosed();

    /** The id of the last event received, which is what the next reconnect will send as {@code Last-Event-ID}. */
    String lastEventId();
}
