package io.hydrangea.http;

import java.net.URI;
import java.util.List;

/**
 * Where an {@link HttpClient} keeps cookies between requests (RFC 6265). Enable with {@link HttpClientConfig#withCookieJar};
 * {@link InMemoryCookieJar} is the ready-made one. The client calls {@link #store} for every response it receives, redirects
 * included, and {@link #cookieHeader} just before it sends each request (a request that already has a {@code Cookie} header is left
 * alone). Implementations are called from event loop threads and must not block, and must be thread-safe.
 */
public interface CookieJar {

    /** Remembers the cookies a response from {@code requestUri} set; {@code setCookieHeaders} are the raw {@code Set-Cookie} values. */
    void store(URI requestUri, List<String> setCookieHeaders);

    /** The value of the {@code Cookie} header for a request to {@code uri}, or null if no cookie applies. */
    String cookieHeader(URI uri);
}
