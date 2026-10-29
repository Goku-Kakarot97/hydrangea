package io.hydrangea.mysql;

import java.time.Duration;

/** What a {@link MyClient} tells about its connection; every method has an empty default. Called on the client's event loop thread: do not block. */
public interface MyListener {

    /** The connection ended: {@code cause} null when the client was closed, otherwise why it was lost. */
    default void onClose(Throwable cause) {
    }

    /** With {@link MyConfig#withReconnect}: the client will try to connect again after {@code delay} ({@code attempt} counts from 1 since the loss). */
    default void onReconnecting(int attempt, Duration delay) {
    }

    /** With {@link MyConfig#withReconnect}: a new session is up. The prepared statements of the old one are gone (the cache starts again); session state ({@code SET}, temporary tables) is lost. */
    default void onReconnected() {
    }

    /** With {@link MyConfig#withReconnect}: the attempts ran out ({@link io.hydrangea.Backoff#maxAttempts()}); the client is closed. */
    default void onGaveUp(Throwable cause) {
    }
}
