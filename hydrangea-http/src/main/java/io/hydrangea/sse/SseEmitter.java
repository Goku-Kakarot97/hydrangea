package io.hydrangea.sse;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Sends events to one connected client. Thread-safe; events from one thread keep their order. The returned futures
 * complete when the event is queued on the connection, or fail (e.g. {@link java.nio.channels.ClosedChannelException})
 * if the stream is closed. A client that lets more than {@link SseConfig#maxPendingBytes()} pile up is disconnected
 * rather than buffered forever.
 */
public interface SseEmitter {

    CompletableFuture<Void> send(SseEvent event);

    default CompletableFuture<Void> send(String data) {
        return send(SseEvent.data(data));
    }

    /** True from {@link SseListener#onOpen} until the stream ends. */
    boolean isOpen();

    /** False while a lot of data is queued for this client; stop producing until {@link SseListener#onWritabilityChanged}. */
    boolean isWritable();

    /** Ends the stream after the events already sent have gone out. Idempotent. */
    void close();

    /** The {@code Last-Event-ID} header the client sent when reconnecting, or null. */
    String lastEventId();

    SocketAddress remoteAddress();
}
