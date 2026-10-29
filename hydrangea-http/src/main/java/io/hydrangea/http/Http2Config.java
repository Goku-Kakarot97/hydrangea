package io.hydrangea.http;

import java.time.Duration;

/**
 * HTTP/2 server settings (RFC 9113). Enable with {@link HttpConfig#withHttp2}: cleartext connections are recognised by the
 * client preface ("prior knowledge"), TLS connections by ALPN ({@code h2}).
 *
 * @param maxConcurrentStreams    streams a client may have open at once (SETTINGS_MAX_CONCURRENT_STREAMS); more are refused
 * @param initialWindowSize       flow-control window per stream the client may fill before we credit it (also the cap on
 *                                unconsumed bytes per stream; request bodies are buffered up to {@link HttpConfig#maxBodyBytes()})
 * @param connectionWindowSize    the same for the whole connection
 * @param maxFrameSize            largest frame payload we accept (16384 to 16777215)
 * @param headerTableSize         HPACK dynamic table the client's encoder may use
 * @param maxResetsPerSecond      RST_STREAM frames tolerated per second before the connection is dropped (the "rapid reset" attack)
 * @param maxControlFramesPerSecond PING, SETTINGS, RST_STREAM, PRIORITY and empty DATA frames tolerated per second (floods that cost
 *                                the server work or output but carry nothing)
 */
public record Http2Config(int maxConcurrentStreams, int initialWindowSize, int connectionWindowSize, int maxFrameSize,
                          int headerTableSize, int maxResetsPerSecond, int maxControlFramesPerSecond) {

    public Http2Config {
        if (maxConcurrentStreams < 1 || initialWindowSize < 1 || connectionWindowSize < 65535) {
            throw new IllegalArgumentException("invalid HTTP/2 window or stream limits");
        }
        if (maxFrameSize < 16384 || maxFrameSize > 16_777_215) {
            throw new IllegalArgumentException("maxFrameSize must be in [16384, 16777215]");
        }
        if (headerTableSize < 0 || maxResetsPerSecond < 1 || maxControlFramesPerSecond < 1) {
            throw new IllegalArgumentException("invalid HTTP/2 limits");
        }
    }

    public static Http2Config defaults() {
        return new Http2Config(100, 1 << 20, 1 << 20, 16384, 4096, 200, 2000);
    }

    /**
     * Defaults for gRPC servers: the same as {@link #defaults()} except for the flood limits. A gRPC client that cancels calls (deadlines
     * that expire, hedged calls that lose) resets streams as part of normal operation, so 200 per second per connection would drop healthy
     * clients with ENHANCE_YOUR_CALM; the rapid-reset defence stays on, at 2000 resets and 20000 control frames per second.
     */
    public static Http2Config grpcDefaults() {
        return defaults().withLimits(2000, 20000);
    }

    public Http2Config withMaxConcurrentStreams(int n) {
        return new Http2Config(n, initialWindowSize, connectionWindowSize, maxFrameSize, headerTableSize, maxResetsPerSecond, maxControlFramesPerSecond);
    }

    public Http2Config withWindows(int perStream, int perConnection) {
        return new Http2Config(maxConcurrentStreams, perStream, perConnection, maxFrameSize, headerTableSize, maxResetsPerSecond, maxControlFramesPerSecond);
    }

    public Http2Config withLimits(int resetsPerSecond, int controlFramesPerSecond) {
        return new Http2Config(maxConcurrentStreams, initialWindowSize, connectionWindowSize, maxFrameSize, headerTableSize, resetsPerSecond, controlFramesPerSecond);
    }
}
