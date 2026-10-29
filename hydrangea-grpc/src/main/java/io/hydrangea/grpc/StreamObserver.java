package io.hydrangea.grpc;

/**
 * Receives a stream of messages: zero or more {@link #onNext} followed by exactly one of {@link #onCompleted} or {@link #onError}. On the
 * receiving side the callbacks run on the connection's event loop, one at a time, and must not block. On the sending side (the observer a
 * handler or a client gets to write into) the methods are thread-safe, and calls after the stream ended are ignored.
 */
public interface StreamObserver<T> {

    void onNext(T value);

    /** The stream failed; for a {@link StatusException} its status is what the peer sees. */
    void onError(Throwable t);

    void onCompleted();
}
