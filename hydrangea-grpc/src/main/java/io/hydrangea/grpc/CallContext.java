package io.hydrangea.grpc;

import java.net.SocketAddress;
import java.time.Instant;

/** What a server-side handler knows about the call it is serving. Thread-safe unless noted. */
public interface CallContext {

    /** The method's path, {@code /package.Service/Method}. */
    String method();

    /** The metadata the client sent. */
    Metadata requestMetadata();

    /**
     * Metadata for the response's initial headers (a request id, a server version). Sent with the first response message, or with the status
     * if there is none; set it before you send the first message. Ignored once the headers have gone out. Not thread-safe.
     */
    Metadata responseHeaders();

    /** Metadata that goes out with the final status. Not thread-safe: fill it before you complete or fail the call. */
    Metadata trailers();

    SocketAddress remoteAddress();

    /** The deadline the client set, or null. The call is cancelled with DEADLINE_EXCEEDED when it passes. */
    Instant deadline();

    /** True once the client cancelled, the connection broke or the deadline passed; results sent after that are dropped. */
    boolean isCancelled();

    /** Runs {@code r} once, when the call is cancelled (immediately if it already was). */
    void onCancel(Runnable r);

    /** False while the stream's output buffer is over its high-water mark; wait for {@link #onReady} before sending more. */
    boolean isReady();

    /** Runs {@code r} each time the call becomes writable again after having not been. */
    void onReady(Runnable r);

    /** Stops (false) or resumes (true) taking request messages from the client; the client is slowed by HTTP/2 flow control. */
    void setAutoRead(boolean enabled);
}
