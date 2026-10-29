package io.hydrangea.http;

import io.hydrangea.pipeline.Stage;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/** A response to send. {@code Content-Length}, {@code Date} and {@code Connection} are managed by the server. */
public final class HttpResponse {

    private static final byte[] EMPTY = new byte[0];

    private final int status;
    private final Headers headers = new Headers();
    private byte[] body = EMPTY;
    private Supplier<? extends Stage> takeover;
    private boolean streaming;
    private boolean headDeferred;
    private final java.util.List<Push> pushes = new java.util.ArrayList<>();

    /** A resource the server offers to send before the client asks for it; see {@link #push}. */
    record Push(String path, Headers requestHeaders) {
    }

    private HttpResponse(int status) {
        if (status < 100 || status > 599) {
            throw new IllegalArgumentException("bad status " + status);
        }
        this.status = status;
    }

    public static HttpResponse status(int status) {
        return new HttpResponse(status);
    }

    public static HttpResponse ok(String text) {
        return status(200).text(text);
    }

    public static HttpResponse notFound() {
        return status(404).text("Not Found");
    }

    public HttpResponse header(String name, String value) {
        checkToken(name, "header name");
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("illegal character in header value (response splitting?)");
        }
        headers.set(name, value);
        return this;
    }

    public HttpResponse body(byte[] body) {
        this.body = body;
        return this;
    }

    public HttpResponse text(String text) {
        headers.set("Content-Type", "text/plain; charset=utf-8");
        this.body = text.getBytes(StandardCharsets.UTF_8);
        return this;
    }

    /**
     * Turns this into a protocol switch (status must be 101): after the response head is sent, the connection
     * stops speaking HTTP and its bytes go to the stage returned by {@code stage}, which is created just before the
     * head is written. Used by WebSocket; see {@code io.hydrangea.websocket.WebSocketUpgrade}.
     */
    public HttpResponse upgrade(Supplier<? extends Stage> stage) {
        if (status != 101) {
            throw new IllegalStateException("only a 101 response can switch protocols");
        }
        this.takeover = stage;
        return this;
    }

    /**
     * Turns this into an open-ended streamed response (status 2xx): the head is sent without a Content-Length, the
     * connection is then handed to the stage returned by {@code stage} (created just before the head is written),
     * and it closes when the stage closes it. The route must set {@code Transfer-Encoding: chunked} itself when the
     * client speaks HTTP/1.1 and frame its output accordingly. Used by Server-Sent Events.
     */
    public HttpResponse stream(Supplier<? extends Stage> stage) {
        if (status < 200 || status > 299) {
            throw new IllegalStateException("a streamed response needs a 2xx status");
        }
        this.takeover = stage;
        this.streaming = true;
        return this;
    }

    /**
     * Offers {@code path} (a GET on the same origin) to the client in advance, HTTP/2 only (RFC 9113 section 8.4): the server sends a
     * PUSH_PROMISE, then runs a synthetic request for the path through the same handler and sends its answer on a stream of its own.
     * Pushes are silently dropped for HTTP/1.1, when the client has disabled push (browsers all do nowadays; Chrome removed support), when
     * its stream limit is reached, when this connection already pushed the same path, or when this response is not a success.
     * The synthetic request carries the original request's {@code accept-encoding}, {@code accept-language}, {@code user-agent} and
     * {@code cookie}; use {@link #push(String, Headers)} to choose.
     */
    public HttpResponse push(String path) {
        return push(path, null);
    }

    /** Like {@link #push(String)} with the request headers the synthetic request should carry (pseudo-headers and {@code host} are added). */
    public HttpResponse push(String path, Headers requestHeaders) {
        if (path == null || path.isEmpty() || path.charAt(0) != '/' || path.length() > 2048) {
            throw new IllegalArgumentException("a pushed path must start with '/' and be at most 2048 characters: " + path);
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c <= 0x20 || c == 0x7f) {
                throw new IllegalArgumentException("illegal character in pushed path");
            }
        }
        pushes.add(new Push(path, requestHeaders == null ? null : requestHeaders.copy()));
        return this;
    }

    java.util.List<Push> pushes() {
        return pushes;
    }

    Supplier<? extends Stage> takeover() {
        return takeover;
    }

    /**
     * HTTP/2 only, for {@link #stream streamed} responses: holds the response head back until the stage writes its first byte or ends the
     * stream, so the stage can still add headers ({@link Http2Stream#addResponseHeaders}) or answer with a single header block (what gRPC
     * calls Trailers-Only). Ignored on HTTP/1.1, where the head goes out first.
     */
    public HttpResponse deferHead() {
        this.headDeferred = true;
        return this;
    }

    boolean isHeadDeferred() {
        return headDeferred;
    }

    boolean isStreaming() {
        return streaming;
    }

    public int status() {
        return status;
    }

    public Headers headers() {
        return headers;
    }

    public byte[] body() {
        return body;
    }

    private static void checkToken(String s, String what) {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("empty " + what);
        }
        for (int i = 0; i < s.length(); i++) {
            if (!HttpServerStage.isTokenChar(s.charAt(i))) {
                throw new IllegalArgumentException("illegal character in " + what + ": " + s);
            }
        }
    }
}
