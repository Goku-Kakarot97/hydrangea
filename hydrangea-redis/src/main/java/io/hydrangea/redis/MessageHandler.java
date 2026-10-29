package io.hydrangea.redis;

/** Receives the messages of a pub/sub subscription. Runs on the client's event-loop thread, so it must not block. */
@FunctionalInterface
public interface MessageHandler {

    /**
     * @param channel the channel the message was published to
     * @param pattern the pattern that matched, for a {@code psubscribe}; null for a plain subscription
     * @param payload the message; the array may be shared with other handlers of the same message: do not change it
     */
    void onMessage(String channel, String pattern, byte[] payload);
}
