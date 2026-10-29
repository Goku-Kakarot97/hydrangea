package io.hydrangea.http;

/**
 * Flow control for a streamed response, given to every {@link BodyListener} callback. Thread-safe.
 * <p>
 * {@link #pause()} stops reading from the socket, so TCP flow control slows the server down instead of this process
 * buffering the stream. Data already read before the pause took effect is held back too: after {@code pause()}
 * returns, no further {@code onData} is delivered until {@link #resume()}.
 * <p>
 * Caveat (server side): a connection that is not being read cannot notice that its peer has gone away; a body left
 * paused forever holds the connection until an {@link io.hydrangea.Options#idleTimeout()} closes it. Always resume or
 * cancel, and configure an idle timeout on servers that pause.
 */
public interface BodyControl {

    void pause();

    void resume();

    /** Abandons the response: the connection is closed and the listener gets {@code onError(CancellationException)}. */
    void cancel();
}
