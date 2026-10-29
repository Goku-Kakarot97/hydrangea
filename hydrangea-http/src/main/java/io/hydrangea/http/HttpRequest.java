package io.hydrangea.http;

import java.nio.charset.StandardCharsets;

/**
 * A fully received request (body buffered, size-limited by {@link HttpConfig#maxBodyBytes()}).
 *
 * @param target the raw request-target, e.g. {@code /search?q=1}
 * @param version {@code "HTTP/1.1"} or {@code "HTTP/1.0"}
 */
public record HttpRequest(String method, String target, String version, Headers headers, byte[] body) {

    public String path() {
        int q = target.indexOf('?');
        return q < 0 ? target : target.substring(0, q);
    }

    /** Raw query string without the leading '?', or null. */
    public String query() {
        int q = target.indexOf('?');
        return q < 0 ? null : target.substring(q + 1);
    }

    public String bodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }
}
