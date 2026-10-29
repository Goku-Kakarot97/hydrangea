package io.hydrangea.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * The body of a request, produced on demand instead of held in memory.
 * <pre>{@code
 * client.send(ClientRequest.put("https://host/backup.tar").body(RequestBody.file(Path.of("backup.tar"))));
 * client.send(ClientRequest.post(url).body(RequestBody.stream(() -> openSomething(), -1)));   // unknown length: chunked
 * }</pre>
 * With a known {@link #contentLength()} the request carries {@code Content-Length}; with -1 it is sent chunked.
 * <p>
 * {@link #start} is called once the connection is ready (and, with {@code Expect: 100-continue}, once the server
 * agreed). It must not block the event loop: either return after writing everything with plain
 * {@link BodySink#write} calls, or hand the work to another thread. A body that may be sent more than once (retry after a
 * stale connection, a 307/308 redirect) must say so with {@link #isReplayable()}; otherwise the client neither retries
 * nor follows such redirects.
 */
public interface RequestBody {

    /** Exact size in bytes, or -1 if unknown (then the body is sent with chunked transfer encoding). */
    long contentLength();

    /** True if {@link #start} can be called again for a retry or a redirect and sends the same bytes. */
    default boolean isReplayable() {
        return false;
    }

    void start(BodySink sink) throws Exception;

    // ---- factories ----

    static RequestBody bytes(byte[] data) {
        return new RequestBodies.Bytes(data.clone());
    }

    static RequestBody text(String text) {
        return new RequestBodies.Bytes(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Streams a file with backpressure; memory use is a single 64 KiB buffer whatever the size. Replayable. The size is
     * read now and checked again when the upload starts: a file that changed in between fails the request rather
     * than sending a body that disagrees with the declared length.
     */
    static RequestBody file(Path path) throws IOException {
        return RequestBodies.file(path);
    }

    /**
     * Streams whatever {@code source} yields, read on a virtual thread (blocking reads are fine). The supplier is called
     * for every attempt, so the body is replayable only if each call returns a fresh stream with the same content.
     *
     * @param length exact size, or -1 for unknown (sent chunked)
     */
    static RequestBody stream(Supplier<? extends InputStream> source, long length) {
        return RequestBodies.stream(source, length, true);
    }

    /** Like {@link #stream} for a source that can be read only once (not replayable). */
    static RequestBody streamOnce(Supplier<? extends InputStream> source, long length) {
        return RequestBodies.stream(source, length, false);
    }

    /**
     * Runs {@code producer} on a virtual thread; it writes into the sink, using {@link BodySink#whenWritable()} for flow
     * control, and calls {@link BodySink#finish()} at the end (or just returns, which finishes).
     */
    static RequestBody producer(long length, boolean replayable, Producer producer) {
        return new RequestBodies.Produced(length, replayable, producer);
    }

    @FunctionalInterface
    interface Producer {
        void produce(BodySink sink) throws Exception;
    }
}
