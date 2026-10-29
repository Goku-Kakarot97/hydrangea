package io.hydrangea.grpc;

import java.time.Duration;

/**
 * Per-call settings. Immutable: each {@code with...} returns a new instance.
 *
 * @param deadline    how long the whole call may take, counted from when it starts; sent to the server as {@code grpc-timeout}. null = none.
 * @param metadata    custom metadata to send with the request
 * @param compression "gzip" to compress the messages sent (the server must accept it: it says so in {@code grpc-accept-encoding}); null = none
 * @param maxRequestBytes  largest request message this call may send (before compression); null = no limit. Over it, the call fails RESOURCE_EXHAUSTED.
 * @param maxResponseBytes largest response message accepted (after decompression); null = the channel's limit
 */
public record CallOptions(Duration deadline, Metadata metadata, String compression, Integer maxRequestBytes, Integer maxResponseBytes) {

    public static final CallOptions DEFAULT = new CallOptions(null, new Metadata(), null, null, null);

    public CallOptions(Duration deadline, Metadata metadata, String compression) {
        this(deadline, metadata, compression, null, null);
    }

    public CallOptions {
        if (compression != null && !compression.equals("gzip")) {
            throw new IllegalArgumentException("only gzip is supported: " + compression);
        }
    }

    public CallOptions withDeadline(Duration d) {
        return new CallOptions(d, metadata, compression, maxRequestBytes, maxResponseBytes);
    }

    public CallOptions withMetadata(Metadata m) {
        return new CallOptions(deadline, m.copy(), compression, maxRequestBytes, maxResponseBytes);
    }

    public CallOptions withCompression(String c) {
        return new CallOptions(deadline, metadata, c, maxRequestBytes, maxResponseBytes);
    }

    public CallOptions withMaxRequestBytes(Integer n) {
        return new CallOptions(deadline, metadata, compression, n, maxResponseBytes);
    }

    public CallOptions withMaxResponseBytes(Integer n) {
        return new CallOptions(deadline, metadata, compression, maxRequestBytes, n);
    }
}
