package io.hydrangea.http;

/**
 * HTTP/2 settings of an {@link HttpClient}; enable with {@link HttpClientConfig#withHttp2}. Over TLS the client offers {@code h2}
 * and {@code http/1.1} (ALPN) and uses whatever the server picks; the first request to a host waits for that answer before
 * further requests to the same host are sent, so a burst of requests shares one HTTP/2 connection instead of opening many.
 *
 * @param maxStreamsPerConnection requests multiplexed on one connection (further requests open another connection, up to
 *                                {@link HttpClientConfig#maxConnectionsPerHost()}, or wait); the server's own limit applies too
 * @param initialWindowSize       flow-control window per response stream; response bodies are buffered up to
 *                                {@link HttpClientConfig#maxResponseBytes()} or handed to a listener, which can pause
 * @param connectionWindowSize    the same for the whole connection
 * @param headerTableSize         HPACK dynamic table the server's encoder may use
 * @param cleartextPriorKnowledge also speak HTTP/2 over plain {@code http://} (no upgrade, no negotiation: the server <em>must</em>
 *                                speak HTTP/2 on that port, as with {@code curl --http2-prior-knowledge}); otherwise {@code http://}
 *                                stays HTTP/1.1
 * @param cleartextUpgrade        for plain {@code http://} without prior knowledge: offer {@code Upgrade: h2c} (RFC 7540 section 3.2) on the first
 *                                request of a connection and switch to HTTP/2 if the server answers 101; any other answer simply goes on as
 *                                HTTP/1.1. Only requests whose body is in memory make the offer. RFC 9113 dropped this mechanism and most servers
 *                                (nginx, Envoy, Node.js...) do not support it, so it is off by default; not used through a proxy.
 */
public record Http2ClientConfig(int maxStreamsPerConnection, int initialWindowSize, int connectionWindowSize, int headerTableSize,
                                boolean cleartextPriorKnowledge, boolean cleartextUpgrade,
                                java.time.Duration keepAliveInterval, java.time.Duration keepAliveTimeout) {

    public Http2ClientConfig(int maxStreamsPerConnection, int initialWindowSize, int connectionWindowSize, int headerTableSize,
                             boolean cleartextPriorKnowledge) {
        this(maxStreamsPerConnection, initialWindowSize, connectionWindowSize, headerTableSize, cleartextPriorKnowledge, false, null, null);
    }

    public Http2ClientConfig(int maxStreamsPerConnection, int initialWindowSize, int connectionWindowSize, int headerTableSize,
                             boolean cleartextPriorKnowledge, boolean cleartextUpgrade) {
        this(maxStreamsPerConnection, initialWindowSize, connectionWindowSize, headerTableSize, cleartextPriorKnowledge, cleartextUpgrade, null, null);
    }

    public Http2ClientConfig {
        if (maxStreamsPerConnection < 1 || initialWindowSize < 1 || connectionWindowSize < 65535 || headerTableSize < 0) {
            throw new IllegalArgumentException("invalid HTTP/2 client limits");
        }
        if ((keepAliveInterval == null) != (keepAliveTimeout == null)
                || (keepAliveInterval != null && (keepAliveInterval.isNegative() || keepAliveInterval.isZero() || keepAliveTimeout.isNegative() || keepAliveTimeout.isZero()))) {
            throw new IllegalArgumentException("keep-alive needs both a positive interval and a positive timeout, or neither");
        }
    }

    public static Http2ClientConfig defaults() {
        return new Http2ClientConfig(100, 1 << 20, 1 << 20, 4096, false, false, null, null);
    }

    public Http2ClientConfig withCleartextPriorKnowledge(boolean b) {
        return new Http2ClientConfig(maxStreamsPerConnection, initialWindowSize, connectionWindowSize, headerTableSize, b, cleartextUpgrade, keepAliveInterval, keepAliveTimeout);
    }

    public Http2ClientConfig withCleartextUpgrade(boolean b) {
        return new Http2ClientConfig(maxStreamsPerConnection, initialWindowSize, connectionWindowSize, headerTableSize, cleartextPriorKnowledge, b, keepAliveInterval, keepAliveTimeout);
    }

    public Http2ClientConfig withMaxStreamsPerConnection(int n) {
        return new Http2ClientConfig(n, initialWindowSize, connectionWindowSize, headerTableSize, cleartextPriorKnowledge, cleartextUpgrade, keepAliveInterval, keepAliveTimeout);
    }

    public Http2ClientConfig withWindows(int perStream, int perConnection) {
        return new Http2ClientConfig(maxStreamsPerConnection, perStream, perConnection, headerTableSize, cleartextPriorKnowledge, cleartextUpgrade, keepAliveInterval, keepAliveTimeout);
    }

    /**
     * Sends an HTTP/2 PING when nothing has been received from the server for {@code interval} and closes the connection if it is not
     * answered (nor anything else received) within {@code timeout}: finds dead peers (a pulled cable, a silent NAT drop) that TCP alone reports
     * only after minutes or never. Off by default. Servers may close connections that ping too often; seconds-to-minutes is sensible.
     */
    public Http2ClientConfig withKeepAlive(java.time.Duration interval, java.time.Duration timeout) {
        return new Http2ClientConfig(maxStreamsPerConnection, initialWindowSize, connectionWindowSize, headerTableSize, cleartextPriorKnowledge, cleartextUpgrade, interval, timeout);
    }
}
