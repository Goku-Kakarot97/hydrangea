package io.hydrangea.http;

import java.time.Duration;

/**
 * Limits and timeouts applied by the HTTP server to every connection. They exist so that one peer cannot make the server
 * buffer unbounded data or hold a connection (and its memory) forever.
 * <p>
 * The three timeouts are <b>deadlines, not idle timers</b>: bytes dribbling in slowly (a "slowloris" client) do not extend them.
 * While your handler is working on a request, or while a streamed body, WebSocket or SSE stream is running, none of them applies
 * (use {@link io.hydrangea.Options#idleTimeout()} and the protocols' own keep-alives for those). A {@code null} timeout disables it.
 *
 * @param maxHeaderBytes   request line + headers; exceeded → 431
 * @param maxBodyBytes     buffered request body (also the sum of chunks); exceeded → 413. Streaming routes are exempt.
 * @param headerTimeout    from the first byte of a request until its head is complete; exceeded → 408 and close
 * @param bodyTimeout      from the complete head until a <em>buffered</em> body is complete; exceeded → 408 and close
 * @param keepAliveTimeout how long a connection may wait, from the accept or from the last response, for the first byte of the next
 *                         request; exceeded → closed silently
 * @param accessLog        called for every response; null = none
 * @param http2            HTTP/2 settings, or null (the default) to speak HTTP/1.1 only. When set, cleartext connections that start with the
 *                         HTTP/2 preface and TLS connections that negotiate {@code h2} are served as HTTP/2.
 */
public record HttpConfig(int maxHeaderBytes, int maxBodyBytes, Duration headerTimeout, Duration bodyTimeout,
                         Duration keepAliveTimeout, AccessLog accessLog, Http2Config http2) {

    public HttpConfig {
        if (maxHeaderBytes < 1 || maxBodyBytes < 0) {
            throw new IllegalArgumentException("invalid limits");
        }
    }

    public HttpConfig(int maxHeaderBytes, int maxBodyBytes, Duration headerTimeout, Duration bodyTimeout,
                      Duration keepAliveTimeout, AccessLog accessLog) {
        this(maxHeaderBytes, maxBodyBytes, headerTimeout, bodyTimeout, keepAliveTimeout, accessLog, null);
    }

    /** Size limits with the default timeouts (10 s head, 30 s body, 60 s keep-alive). */
    public HttpConfig(int maxHeaderBytes, int maxBodyBytes) {
        this(maxHeaderBytes, maxBodyBytes, Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(60), null, null);
    }

    public static HttpConfig defaults() {
        return new HttpConfig(16 * 1024, 1024 * 1024);
    }

    public HttpConfig withHeaderTimeout(Duration d) {
        return new HttpConfig(maxHeaderBytes, maxBodyBytes, d, bodyTimeout, keepAliveTimeout, accessLog, http2);
    }

    public HttpConfig withBodyTimeout(Duration d) {
        return new HttpConfig(maxHeaderBytes, maxBodyBytes, headerTimeout, d, keepAliveTimeout, accessLog, http2);
    }

    public HttpConfig withKeepAliveTimeout(Duration d) {
        return new HttpConfig(maxHeaderBytes, maxBodyBytes, headerTimeout, bodyTimeout, d, accessLog, http2);
    }

    public HttpConfig withAccessLog(AccessLog log) {
        return new HttpConfig(maxHeaderBytes, maxBodyBytes, headerTimeout, bodyTimeout, keepAliveTimeout, log, http2);
    }

    /** Serves HTTP/2 as well as HTTP/1.1 (see {@link #http2}). */
    public HttpConfig withHttp2(Http2Config config) {
        return new HttpConfig(maxHeaderBytes, maxBodyBytes, headerTimeout, bodyTimeout, keepAliveTimeout, accessLog, config);
    }
}
