package io.hydrangea.quic;

/** Application callbacks of a {@link QuicServer}; all run on the connection's event-loop thread. */
public interface QuicHandler {

    /** The handshake finished; the connection can send and receive streams. */
    default void onOpen(QuicConnection connection) throws Exception {
    }

    /**
     * Bytes of a stream, in order. {@code fin} is true on the last call for the stream (possibly with no data). A bidirectional stream opened by the
     * peer is answered with {@link QuicConnection#send}.
     */
    default void onStreamData(QuicConnection connection, long streamId, byte[] data, boolean fin) throws Exception {
    }

    /** The peer abandoned a stream (RESET_STREAM) with an application error code. */
    default void onStreamReset(QuicConnection connection, long streamId, long errorCode) throws Exception {
    }

    /** The connection ended: closed by either side, idle timeout, or a protocol error. {@code errorCode} 0 with {@code application} true is a clean close. */
    default void onClose(QuicConnection connection, long errorCode, boolean application, String reason) {
    }
}
