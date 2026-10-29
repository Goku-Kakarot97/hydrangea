package io.hydrangea.http;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Settings of an {@link HttpClient}.
 *
 * @param connectTimeout       TCP connect timeout
 * @param requestTimeout       default overall deadline per request (queueing, connect, send, full response); null = none
 * @param idleTimeout          a connection with no traffic for this long is closed (this also bounds how long a pooled
 *                             connection is kept and acts as an inactivity timeout for a stalled response)
 * @param maxConnectionsPerHost open connections per scheme+host+port; further requests wait in a queue
 * @param maxIdlePerHost       idle connections kept for reuse per host
 * @param maxResponseBytes     response body limit (after decompression); larger responses fail the request
 * @param maxHeaderBytes       response status line + headers limit
 * @param maxRedirects         redirects followed per request; 0 disables following
 * @param sslContext           for https; null = the JVM default
 * @param http2                HTTP/2 settings, or null (the default) for HTTP/1.1 only
 * @param decompress           send {@code Accept-Encoding: gzip, deflate} (unless the request sets its own) and decode the answer; the
 *                             exposed response then has no {@code Content-Encoding}/{@code Content-Length}, and {@code maxResponseBytes}
 *                             bounds the <em>decoded</em> size, which is what stops a decompression bomb. Off by default.
 * @param proxy                forward proxy, or null for direct connections
 * @param cookieJar            cookie storage, or null for no cookie handling
 */
public record HttpClientConfig(
        Duration connectTimeout,
        Duration requestTimeout,
        Duration idleTimeout,
        int maxConnectionsPerHost,
        int maxIdlePerHost,
        int maxResponseBytes,
        int maxHeaderBytes,
        int maxRedirects,
        SSLContext sslContext,
        String userAgent,
        Http2ClientConfig http2,
        boolean decompress,
        ProxyConfig proxy,
        CookieJar cookieJar) {

    public HttpClientConfig {
        if (maxConnectionsPerHost < 1 || maxIdlePerHost < 0 || maxResponseBytes < 0 || maxHeaderBytes < 1 || maxRedirects < 0) {
            throw new IllegalArgumentException("invalid limits");
        }
    }

    /** HTTP/1.1-only settings without decompression, proxy or cookies. */
    public HttpClientConfig(Duration connectTimeout, Duration requestTimeout, Duration idleTimeout, int maxConnectionsPerHost,
                            int maxIdlePerHost, int maxResponseBytes, int maxHeaderBytes, int maxRedirects, SSLContext sslContext,
                            String userAgent) {
        this(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes,
                maxRedirects, sslContext, userAgent, null, false, null, null);
    }

    /** Settings with HTTP/2 but without decompression, proxy or cookies. */
    public HttpClientConfig(Duration connectTimeout, Duration requestTimeout, Duration idleTimeout, int maxConnectionsPerHost,
                            int maxIdlePerHost, int maxResponseBytes, int maxHeaderBytes, int maxRedirects, SSLContext sslContext,
                            String userAgent, Http2ClientConfig http2) {
        this(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes,
                maxRedirects, sslContext, userAgent, http2, false, null, null);
    }

    public static HttpClientConfig defaults() {
        return new HttpClientConfig(Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(30),
                32, 16, 10 * 1024 * 1024, 16 * 1024, 5, null, "Hydrangea/0.1");
    }

    public HttpClientConfig withConnectTimeout(Duration d) {
        return new HttpClientConfig(d, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withRequestTimeout(Duration d) {
        return new HttpClientConfig(connectTimeout, d, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withIdleTimeout(Duration d) {
        return new HttpClientConfig(connectTimeout, requestTimeout, d, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withMaxConnectionsPerHost(int n) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, n, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withMaxIdlePerHost(int n) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, n, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withMaxResponseBytes(int n) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, n, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withMaxRedirects(int n) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, n, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withSslContext(SSLContext c) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, c, userAgent, http2, decompress, proxy, cookieJar);
    }

    public HttpClientConfig withUserAgent(String ua) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, ua, http2, decompress, proxy, cookieJar);
    }

    /** Speaks HTTP/2 where the server does (see {@link Http2ClientConfig}). */
    public HttpClientConfig withHttp2(Http2ClientConfig config) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, config, decompress, proxy, cookieJar);
    }

    /** Asks for gzip/deflate and decodes the answer transparently (see the class comment). */
    public HttpClientConfig withDecompression(boolean on) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, on, proxy, cookieJar);
    }

    /** Sends requests through a forward proxy (see {@link ProxyConfig}); null = direct. */
    public HttpClientConfig withProxy(ProxyConfig proxy) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, cookieJar);
    }

    /** Keeps cookies between requests (see {@link CookieJar}); null = none. */
    public HttpClientConfig withCookieJar(CookieJar jar) {
        return new HttpClientConfig(connectTimeout, requestTimeout, idleTimeout, maxConnectionsPerHost, maxIdlePerHost, maxResponseBytes, maxHeaderBytes, maxRedirects, sslContext, userAgent, http2, decompress, proxy, jar);
    }
}
