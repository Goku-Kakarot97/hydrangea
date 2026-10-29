package io.hydrangea.postgres;

import java.util.Map;

/** What a connection reports besides query results. Called on the connection's event loop thread: must not block. */
public interface PgListener {

    /** A {@code NoticeResponse} (a warning or message from {@code RAISE NOTICE}); the fields are as in {@link PgException#fields()}. */
    default void onNotice(Map<Character, String> fields) {
    }

    /** A {@code NOTIFY} on a channel this connection {@code LISTEN}s to. */
    default void onNotification(int backendPid, String channel, String payload) {
    }

    /** The connection ended (after it was online); {@code cause} is null for a close the client asked for. */
    default void onClose(Throwable cause) {
    }

    /** With {@link PgConfig#reconnect()}: attempt number {@code attempt} (1 = the first) to get the connection back will be made after {@code delay}. */
    default void onReconnecting(int attempt, java.time.Duration delay) {
    }

    /** With {@link PgConfig#reconnect()}: the connection is back (a new session: see {@link PgConfig#reconnect()} for what that loses). */
    default void onReconnected() {
    }

    /** With {@link PgConfig#reconnect()}: the {@link io.hydrangea.Backoff} ran out of attempts; the client is closed for good. */
    default void onGaveUp(Throwable cause) {
    }
}
