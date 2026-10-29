package io.hydrangea.websocket;

import java.nio.ByteBuffer;

/**
 * Receives the events of one {@link WebSocket}, always on its event loop thread and never concurrently, so a
 * listener can keep per-connection state in plain fields. Do not block. One listener instance per connection.
 * <p>
 * An exception thrown by a callback is reported to {@link #onError} and closes the socket with status 1011.
 */
public interface WebSocketListener {

    /** The handshake finished; the socket is ready for sending. */
    default void onOpen(WebSocket ws) throws Exception {
    }

    /** A complete (reassembled, UTF-8 validated) text message. */
    default void onText(WebSocket ws, String text) throws Exception {
    }

    /** A complete binary message. The buffer belongs to the callee. */
    default void onBinary(WebSocket ws, ByteBuffer data) throws Exception {
    }

    /**
     * Asked at the start of every incoming data message: return true to receive it in parts
     * ({@link #onTextPart}/{@link #onBinaryPart}) as the bytes arrive, instead of whole through
     * {@link #onText}/{@link #onBinary}. A streamed message is never buffered and is not subject to
     * {@link WebSocketConfig#maxMessageBytes()}, so it can be arbitrarily large (only
     * {@link WebSocketConfig#maxStreamedMessageBytes()} applies). Use {@link WebSocket#setAutoRead} to pause.
     */
    default boolean streaming() {
        return false;
    }

    /**
     * A piece of a text message (only for {@link #streaming()} messages), decoded and UTF-8 validated; a character is
     * never split between pieces. Piece boundaries are arbitrary. {@code last} marks the end of the message and is true
     * exactly once (the final piece may be empty).
     */
    default void onTextPart(WebSocket ws, String part, boolean last) throws Exception {
    }

    /** A piece of a binary message (only for {@link #streaming()} messages). The buffer belongs to the callee. */
    default void onBinaryPart(WebSocket ws, ByteBuffer part, boolean last) throws Exception {
    }

    /** A ping arrived; the pong has already been sent automatically. */
    default void onPing(WebSocket ws, ByteBuffer data) throws Exception {
    }

    default void onPong(WebSocket ws, ByteBuffer data) throws Exception {
    }

    /** {@link WebSocket#isWritable()} changed. */
    default void onWritabilityChanged(WebSocket ws) throws Exception {
    }

    /** A protocol violation, I/O failure or callback exception. {@link #onClose} follows. */
    default void onError(WebSocket ws, Throwable error) {
    }

    /**
     * The connection is gone; called exactly once, if the socket was opened.
     *
     * @param code the status of the closing handshake, 1005 if the peer sent none, or 1006 if the connection dropped
     *             without a closing handshake
     */
    default void onClose(WebSocket ws, int code, String reason) {
    }
}
