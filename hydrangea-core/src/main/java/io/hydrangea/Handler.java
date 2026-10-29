package io.hydrangea;

import java.nio.ByteBuffer;

/**
 * Receives the events of one {@link Connection}. All methods are invoked on the connection's event loop thread,
 * never concurrently. A throwable escaping any callback closes the connection and is reported to {@link #onClose}.
 * <p>
 * One handler instance is created per connection, so it may keep per-connection state in plain fields.
 */
public interface Handler {

    /** The connection is registered and ready for I/O. */
    default void onActive(Connection connection) throws Exception {
    }

    /**
     * Bytes were read. {@code data} is only valid during this call (it is a view of a shared buffer);
     * copy whatever you need to keep, or extend {@link io.hydrangea.codec.FramingHandler}.
     */
    default void onRead(Connection connection, ByteBuffer data) throws Exception {
    }

    /** {@link Connection#isWritable()} changed; use it to pause/resume producing data. */
    default void onWritabilityChanged(Connection connection) throws Exception {
    }

    /**
     * The server is shutting down gracefully ({@link Server#shutdownGracefully}): finish what is in progress and close the
     * connection yourself, soon. Connections that are still open when the grace period ends are closed by force. The
     * default does nothing, which means "wait for the deadline". Called on the connection's loop thread.
     */
    default void onDrain(Connection connection) throws Exception {
    }

    /**
     * The connection is closed. Called exactly once, only if {@link #onActive} was reached.
     *
     * @param cause why it was closed, or {@code null} for a normal close / orderly EOF
     */
    default void onClose(Connection connection, Throwable cause) {
    }
}
