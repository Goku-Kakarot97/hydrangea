package io.hydrangea.http;

/**
 * The body of a request being received, handed to a streaming route <em>before</em> the body arrives.
 * <p>
 * Call {@link #listen} from inside the route (before it returns): data then flows to the listener as it comes in,
 * with no size limit from {@link HttpConfig#maxBodyBytes()} (the route is responsible for its own limit, e.g. by
 * counting bytes and answering 413 early). Without a listener the body is read and discarded.
 * <p>
 * The listener's {@code onHead} is never called; {@code onData}/{@code onComplete} run on the connection's loop thread.
 * {@code onError} reports a connection that died, or a response that was sent before the body had fully arrived. Use
 * the {@link BodyControl} given to {@code onData} to pause reading (TCP then slows the sender down).
 */
public interface RequestBodyStream {

    void listen(BodyListener listener);

    /** Declared body size, or -1 for a chunked body. */
    long contentLength();
}
