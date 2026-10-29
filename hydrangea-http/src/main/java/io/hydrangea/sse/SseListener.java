package io.hydrangea.sse;

/**
 * Lifecycle of one SSE stream, always called on its event loop thread; do not block. One instance per client.
 */
public interface SseListener {

    /** The response head is out; start sending. The emitter is also safe to keep and use from any thread. */
    default void onOpen(SseEmitter emitter) throws Exception {
    }

    /** {@link SseEmitter#isWritable()} changed. */
    default void onWritabilityChanged(SseEmitter emitter) throws Exception {
    }

    /** The stream ended: the client disconnected, {@link SseEmitter#close()} was called, or the client was too slow. Exactly once. */
    default void onClose(SseEmitter emitter) {
    }
}
