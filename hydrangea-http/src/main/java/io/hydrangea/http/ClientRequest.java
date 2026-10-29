package io.hydrangea.http;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Set;

/**
 * An immutable HTTP request description; every {@code header/body/timeout} call returns a modified copy.
 * <pre>{@code ClientRequest.post("http://localhost:8080/users").header("Accept", "application/json").body("name=An")}</pre>
 * {@code Host}, {@code Content-Length}, {@code Transfer-Encoding} and {@code Connection} are managed by the client
 * and cannot be set. URLs must already be percent-encoded.
 */
public final class ClientRequest {

    private static final Set<String> MANAGED = Set.of("host", "content-length", "transfer-encoding", "connection");
    private static final RequestBody EMPTY = RequestBody.bytes(new byte[0]);

    private final String method;
    private final URI uri;
    private final Headers headers;
    private final RequestBody body;
    private final Duration timeout;
    private final InetSocketAddress connectAddress;

    private ClientRequest(String method, URI uri, Headers headers, RequestBody body, Duration timeout, InetSocketAddress connectAddress) {
        this.connectAddress = connectAddress;
        this.method = method;
        this.uri = uri;
        this.headers = headers;
        this.body = body;
        this.timeout = timeout;
    }

    /** @throws IllegalArgumentException for a non-http(s) URL or a bad method */
    public static ClientRequest of(String method, String url) {
        String m = method.toUpperCase(Locale.ROOT);
        if (m.isEmpty()) {
            throw new IllegalArgumentException("empty method");
        }
        for (int i = 0; i < m.length(); i++) {
            if (!HttpServerStage.isTokenChar(m.charAt(i))) {
                throw new IllegalArgumentException("illegal method: " + method);
            }
        }
        URI uri = URI.create(url);
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("only http and https URLs are supported: " + url);
        }
        if (uri.getHost() == null || uri.getUserInfo() != null) {
            throw new IllegalArgumentException("URL needs a host and must not contain credentials: " + url);
        }
        return new ClientRequest(m, uri, new Headers(), EMPTY, null, null);
    }

    public static ClientRequest get(String url) {
        return of("GET", url);
    }

    public static ClientRequest head(String url) {
        return of("HEAD", url);
    }

    public static ClientRequest delete(String url) {
        return of("DELETE", url);
    }

    public static ClientRequest post(String url) {
        return of("POST", url);
    }

    public static ClientRequest put(String url) {
        return of("PUT", url);
    }

    /** Adds a header value. @throws IllegalArgumentException for illegal characters or client-managed headers */
    public ClientRequest header(String name, String value) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty header name");
        }
        for (int i = 0; i < name.length(); i++) {
            if (!HttpServerStage.isTokenChar(name.charAt(i))) {
                throw new IllegalArgumentException("illegal character in header name: " + name);
            }
        }
        if (MANAGED.contains(name.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("header is managed by the client: " + name);
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c < 0x20 && c != '\t') || c == 0x7f) {
                throw new IllegalArgumentException("control character in header value (request splitting?)");
            }
        }
        return new ClientRequest(method, uri, headers.copy().add(name, value), body, timeout, connectAddress);
    }

    public ClientRequest body(byte[] body) {
        return new ClientRequest(method, uri, headers, RequestBody.bytes(body), timeout, connectAddress);
    }

    public ClientRequest body(String text) {
        return new ClientRequest(method, uri, headers, RequestBody.text(text), timeout, connectAddress);
    }

    /** A body produced on demand, e.g. {@link RequestBody#file}; see {@link RequestBody} for the rules. */
    public ClientRequest body(RequestBody body) {
        return new ClientRequest(method, uri, headers, body, timeout, connectAddress);
    }

    /**
     * Sends {@code Expect: 100-continue}: the client waits (up to one second) for the server's go-ahead before
     * uploading a streamed body, so a server that rejects the request (401, 413...) is never sent the data.
     */
    public ClientRequest expectContinue() {
        return new ClientRequest(method, uri, headers.copy().add("Expect", "100-continue"), body, timeout, connectAddress);
    }

    /** Overall deadline (queueing + connect + request + response) overriding {@link HttpClientConfig#requestTimeout()}. */
    public ClientRequest timeout(Duration timeout) {
        return new ClientRequest(method, uri, headers, body, timeout, connectAddress);
    }

    /**
     * Connects to this address instead of resolving the URL's host (which still names the server in {@code Host}, {@code :authority} and TLS
     * verification). For clients that do their own name resolution and load balancing; ignored when a proxy is used.
     */
    public ClientRequest connectTo(java.net.InetSocketAddress address) {
        return new ClientRequest(method, uri, headers, body, timeout, address);
    }

    java.net.InetSocketAddress connectAddress() {
        return connectAddress;
    }

    public String method() {
        return method;
    }

    public URI uri() {
        return uri;
    }

    public Headers headers() {
        return headers;
    }

    RequestBody requestBody() {
        return body;
    }

    Duration timeout() {
        return timeout;
    }

    /** Copy for following a redirect to {@code target}. */
    ClientRequest redirected(URI target, String newMethod, boolean keepBody, boolean sameOrigin) {
        Headers h = new Headers();
        headers.forEach((n, v) -> {
            String ln = n.toLowerCase(Locale.ROOT);
            boolean sensitive = ln.equals("authorization") || ln.equals("cookie") || ln.equals("proxy-authorization");
            if (sameOrigin || !sensitive) {
                h.add(n, v);
            }
        });
        return new ClientRequest(newMethod, target, h, keepBody ? body : EMPTY, timeout, sameOrigin ? connectAddress : null);
    }
}
