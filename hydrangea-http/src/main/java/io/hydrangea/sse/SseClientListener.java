package io.hydrangea.sse;

/**
 * Receives what an {@link SseClient} subscription sees, always on an event loop thread and never concurrently.
 * Do not block.
 */
public interface SseClientListener {

    /** A connection was established (called again after every reconnect). */
    default void onOpen() throws Exception {
    }

    default void onMessage(SseMessage message) throws Exception {
    }

    /**
     * A connection ended or could not be made, or {@link #onMessage} threw (in which case the stream keeps running).
     *
     * @param willReconnect true if the client will try again; false if this is final (then {@link #onClose} follows)
     */
    default void onError(Throwable error, boolean willReconnect) {
    }

    /** The subscription is over for good: closed by the caller, a non-retryable response, or reconnecting disabled. Once. */
    default void onClose() {
    }
}
