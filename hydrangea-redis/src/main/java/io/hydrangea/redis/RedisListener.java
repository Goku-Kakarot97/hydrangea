package io.hydrangea.redis;

import java.time.Duration;
import java.util.List;

/** Events of the connection itself. Called on the client's event-loop thread: do not block. */
public interface RedisListener {

    /** The connection is up and logged in; {@code reconnected} is false for the first time. Subscriptions are already being restored. */
    default void connected(boolean reconnected) {
    }

    /** An established connection was lost; the commands waiting for an answer have failed (retriable). */
    default void connectionLost(Throwable cause) {
    }

    default void reconnecting(int attempt, Duration delay) {
    }

    /** The client gave up and closed (no reconnecting configured, or too many attempts, or a failure that trying again cannot fix, such as a refused password). */
    default void gaveUp(Throwable cause) {
    }

    /** A RESP3 push the client does not handle itself (for example a client-side-caching invalidation). */
    default void push(List<Object> items) {
    }
}
