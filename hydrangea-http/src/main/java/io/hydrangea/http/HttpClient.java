package io.hydrangea.http;

import io.hydrangea.*;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.tls.TlsStage;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous HTTP/1.1 client with a per-host connection pool.
 * <pre>{@code
 * try (var client = new HttpClient()) {
 *     ClientResponse r = client.send(ClientRequest.get("https://example.org/api").header("Accept", "text/plain")).join();
 *     System.out.println(r.status() + " " + r.bodyAsString());
 * }
 * }</pre>
 * <ul>
 *   <li><b>Pooling</b>: keep-alive connections are reused; at most {@code maxConnectionsPerHost} are open per
 *       scheme+host+port and further requests queue. One request at a time per connection (no pipelining).</li>
 *   <li><b>Timeouts</b>: connect timeout, an overall request deadline, and an inactivity timeout
 *       ({@link HttpClientConfig#idleTimeout()}). Cancelling the returned future aborts the request and closes its connection.</li>
 *   <li><b>Retry</b>: if a <em>reused</em> connection turns out to be dead before any response byte arrived (the server closed it
 *       while idle), idempotent requests (GET, HEAD, PUT, DELETE, OPTIONS) are retried once on a fresh connection.
 *       POST/PATCH are never retried.</li>
 *   <li><b>Redirects</b> (301/302/303/307/308) are followed up to {@code maxRedirects}. 301/302/303 turn POST into GET;
 *       307/308 keep method and body. {@code Authorization}/{@code Cookie} are dropped when the origin changes, and an
 *       https → http redirect is never followed (the 3xx response is returned instead).</li>
 *   <li><b>Response safety</b>: strict parsing (CRLF only, Content-Length + Transfer-Encoding rejected), bounded headers and body.</li>
 * </ul>
 * HTTP/2 is available with {@link HttpClientConfig#withHttp2}: requests to a host then share one multiplexed connection.
 * Optional, off unless configured: HTTP proxy ({@link HttpClientConfig#withProxy}), cookies ({@link HttpClientConfig#withCookieJar})
 * and gzip/deflate response decoding ({@link HttpClientConfig#withDecompression}; without it no {@code Accept-Encoding} is sent).
 * Not supported: SOCKS, TLS to a proxy, request bodies streamed from disk, pipelining.
 * Futures complete on an event loop thread: continuations must not block.
 */
public final class HttpClient implements AutoCloseable {

    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final HttpClientConfig config;
    private final SSLContext sslContext;
    private final ConcurrentHashMap<Key, HostPool> pools = new ConcurrentHashMap<>();
    private final ExecutorService dns = Executors.newVirtualThreadPerTaskExecutor(); // name resolution blocks; keep it off the loops
    private volatile boolean closed;
    // metrics (per group, shared by every client on it): requests by outcome, time to the response (to the head for streamed ones), requests under way
    private final Metrics.Count mActive;
    private final Metrics.Count mError;
    private final Metrics.Count[] mClass = new Metrics.Count[4];   // 2xx, 3xx, 4xx, 5xx
    private final Metrics.Count mOtherStatus;
    private final Metrics.Count mRetries;
    private final Metrics.Count mRedirects;
    private final Latency mDuration;

    /** Own event loops (2 threads), default settings. */
    public HttpClient() {
        this(new EventLoopGroup(2), HttpClientConfig.defaults(), true);
    }

    /** Shares {@code group}; closing the client does not shut the group down. */
    public HttpClient(EventLoopGroup group, HttpClientConfig config) {
        this(group, config, false);
    }

    private HttpClient(EventLoopGroup group, HttpClientConfig config, boolean ownsGroup) {
        this.group = group;
        this.config = config;
        this.ownsGroup = ownsGroup;
        Metrics m = group.metrics();
        mActive = m.gauge("http.client.active", null);
        mError = m.counter("http.client.requests", null, "class", "error");
        for (int i = 0; i < 4; i++) {
            mClass[i] = m.counter("http.client.requests", null, "class", (i + 2) + "xx");
        }
        mOtherStatus = m.counter("http.client.requests", null, "class", "other");
        mRetries = m.counter("http.client.retries", null);
        mRedirects = m.counter("http.client.redirects", null);
        mDuration = m.latency("http.client.duration");
        try {
            this.sslContext = config.sslContext() != null ? config.sslContext() : SSLContext.getDefault();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public CompletableFuture<ClientResponse> get(String url) {
        return send(ClientRequest.get(url));
    }

    public CompletableFuture<ClientResponse> post(String url, String body) {
        return send(ClientRequest.post(url).body(body));
    }

    /** Sends the request. The future fails with {@link IOException} subtypes (e.g. {@link SocketTimeoutException}). */
    public CompletableFuture<ClientResponse> send(ClientRequest request) {
        if (closed) {
            return CompletableFuture.failedFuture(new IOException("client is closed"));
        }
        Exchange ex = new Exchange(request, group.next());
        return start(ex, request) ? ex.result : CompletableFuture.failedFuture(new IOException("client is closed"));
    }

    /**
     * Sends the request and hands the response to {@code listener} as it arrives, with no size limit on the body, so
     * it suits large downloads and endless streams (Server-Sent Events, NDJSON feeds).
     * <ul>
     *   <li>The request timeout ({@link ClientRequest#timeout} or the config default) covers queueing, connecting,
     *       sending and waiting for the response <em>head</em>. After that, only
     *       {@link HttpClientConfig#idleTimeout()} applies: a stream may run for hours as long as bytes keep coming.</li>
     *   <li>Redirects are followed before the listener sees anything.</li>
     *   <li>Use the {@link BodyControl} passed to the callbacks to pause and resume.</li>
     * </ul>
     */
    public StreamHandle stream(ClientRequest request, BodyListener listener) {
        Exchange ex = new Exchange(request, group.next());
        ex.stream = listener;
        StreamHandle handle = new StreamHandle(ex);
        if (closed || !start(ex, request)) {
            ex.fail(new IOException("client is closed"));
        }
        return handle;
    }

    /** The loop used for timers; handy for building reconnect logic on top of the client. */
    public io.hydrangea.EventLoop eventLoop() {
        return group.next();
    }

    private boolean start(Exchange ex, ClientRequest request) {
        Duration timeout = request.timeout() != null ? request.timeout() : config.requestTimeout();
        if (timeout != null) {
            try {
                ex.timeout = ex.fallbackLoop.schedule(
                        () -> ex.fail(new SocketTimeoutException("request timed out after " + timeout.toMillis() + " ms")),
                        timeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException e) {
                return false;
            }
        }
        ex.start = mDuration.begin();
        mActive.increment();
        ex.result.whenComplete((r, e) -> {
            mActive.decrement();
            if (e != null) {
                mError.increment();
            } else {
                int c = r.status() / 100;
                (c >= 2 && c <= 5 ? mClass[c - 2] : mOtherStatus).increment();
            }
            mDuration.end(ex.start);
            ex.cancelTimeout();
            if (ex.result.isCancelled()) {
                ex.abortConnection(); // the caller gave up: do not keep a half-used connection around
            }
        });
        dispatch(ex);
        return true;
    }

    /** Closes every connection and fails queued and in-flight requests. */
    @Override
    public void close() {
        closed = true;
        for (HostPool p : pools.values()) {
            p.shutdown();
        }
        dns.shutdownNow();
        if (ownsGroup) {
            group.close();
        }
    }

    // ---- request lifecycle ----

    /** The pool key of the last request's URL and address: requests made from one template (gRPC calls, say) share both objects. */
    private record KeyFor(URI uri, InetSocketAddress connect, Key key) {
    }

    private volatile KeyFor lastKey;

    private void dispatch(Exchange ex) {
        URI u = ex.req.uri();
        InetSocketAddress connect = ex.req.connectAddress();
        KeyFor cached = lastKey;
        Key key;
        if (cached != null && cached.uri == u && cached.connect == connect) {
            key = cached.key;
        } else {
            boolean tls = u.getScheme().equalsIgnoreCase("https");
            int port = u.getPort() != -1 ? u.getPort() : (tls ? 443 : 80);
            key = new Key(tls, u.getHost().toLowerCase(Locale.ROOT), port, connect);
            lastKey = new KeyFor(u, connect, key);
        }
        pools.computeIfAbsent(key, HostPool::new).acquire(ex);
    }

    /** A connection failed while serving {@code ex}. Decides between retry and failure. */
    private void onFailure(Exchange ex, Throwable err, boolean gotResponseBytes) {
        // retry only what cannot have been processed: a reused connection that was dead before any response byte (idempotent methods), or
        // an HTTP/2 request the server provably did not handle (refused stream, beyond the GOAWAY mark: any method)
        boolean provablyUnprocessed = err instanceof UnprocessedException;
        if (!gotResponseBytes && !ex.retried && ex.req.requestBody().isReplayable() && !closed && !ex.result.isDone()
                && (provablyUnprocessed || (ex.reused && isIdempotent(ex.req.method())))) {
            ex.retried = true;
            ex.forceNew = true;
            mRetries.increment();
            dispatch(ex);
        } else {
            ex.fail(err);
        }
    }

    private static boolean isIdempotent(String m) {
        return m.equals("GET") || m.equals("HEAD") || m.equals("PUT") || m.equals("DELETE") || m.equals("OPTIONS");
    }

    /** A response arrived and the connection was handed back. Follows redirects or completes the future. */
    private void onResponse(Exchange ex, ClientResponse resp) {
        if (ex.result.isDone()) {
            return;
        }
        int s = resp.status();
        String location = resp.header("location");
        if (config.maxRedirects() > 0 && location != null && (s == 301 || s == 302 || s == 303 || s == 307 || s == 308)) {
            URI from = ex.req.uri();
            URI target;
            try {
                target = from.resolve(location);
            } catch (IllegalArgumentException e) {
                ex.result.complete(resp); // unusable Location: hand the 3xx to the caller
                return;
            }
            boolean downgrade = from.getScheme().equalsIgnoreCase("https") && "http".equalsIgnoreCase(target.getScheme());
            boolean supported = target.getScheme() != null && (target.getScheme().equalsIgnoreCase("http") || target.getScheme().equalsIgnoreCase("https"))
                    && target.getHost() != null && target.getUserInfo() == null;
            String m0 = ex.req.method();
            boolean toGet0 = s == 303 ? !m0.equals("HEAD") : (s == 301 || s == 302) && m0.equals("POST");
            boolean bodyCanBeResent = toGet0 || ex.req.requestBody().isReplayable() || ex.req.requestBody().contentLength() == 0;
            if (!downgrade && supported && bodyCanBeResent) {
                if (ex.redirects >= config.maxRedirects()) {
                    ex.fail(new IOException("too many redirects (" + config.maxRedirects() + ")"));
                    return;
                }
                String m = ex.req.method();
                boolean toGet = toGet0;
                boolean sameOrigin = from.getScheme().equalsIgnoreCase(target.getScheme())
                        && from.getHost().equalsIgnoreCase(target.getHost()) && portOf(from) == portOf(target);
                ex.req = ex.req.redirected(target, toGet ? "GET" : m, !toGet, sameOrigin);
                ex.redirects++;
                mRedirects.increment();
                ex.forceNew = false;
                ex.retried = false;
                dispatch(ex);
                return;
            }
        }
        if (ex.stream != null) { // a response that had to be buffered (an unfollowable redirect): replay it to the listener
            try {
                ex.stream.onHead(new ResponseHead(resp.status(), resp.reason(), resp.headers(), resp.uri()), NOOP_CONTROL);
                if (resp.body().length > 0) {
                    ex.stream.onData(java.nio.ByteBuffer.wrap(resp.body()).asReadOnlyBuffer(), NOOP_CONTROL);
                }
            } catch (Throwable t) {
                ex.fail(t);
                return;
            }
            ex.endStream(null);
        }
        ex.result.complete(resp);
    }

    private static final BodyControl NOOP_CONTROL = new BodyControl() {
        public void pause() { }
        public void resume() { }
        public void cancel() { }
    };

    /** Streamed exchanges buffer redirect responses so they can be followed before the listener sees a head. */
    boolean buffersForRedirect(int status, Headers headers) {
        return config.maxRedirects() > 0 && (status == 301 || status == 302 || status == 303 || status == 307 || status == 308)
                && headers.contains("location");
    }

    private static int portOf(URI u) {
        return u.getPort() != -1 ? u.getPort() : (u.getScheme().equalsIgnoreCase("https") ? 443 : 80);
    }

    // ---- wire format ----

    static byte[] serialize(HttpClient client, Exchange ex, String userAgent, ProxyConfig.Endpoint plainProxy) {
        ClientRequest r = ex.req;
        URI u = r.uri();
        String path = u.getRawPath();
        String target = (path == null || path.isEmpty() ? "/" : path) + (u.getRawQuery() != null ? "?" + u.getRawQuery() : "");
        for (int i = 0; i < target.length(); i++) {
            char c = target.charAt(i);
            if (c <= 0x20 || c >= 0x7f) {
                throw new IllegalArgumentException("request target must be percent-encoded ASCII: " + target);
            }
        }
        boolean tls = u.getScheme().equalsIgnoreCase("https");
        int port = portOf(u);
        StringBuilder sb = new StringBuilder(256);
        if (plainProxy != null) { // a proxy needs the whole URL: it is not the origin
            target = u.getScheme().toLowerCase(Locale.ROOT) + "://" + u.getHost() + (u.getPort() != -1 ? ":" + u.getPort() : "") + target;
        }
        sb.append(r.method()).append(' ').append(target).append(" HTTP/1.1\r\n");
        sb.append("Host: ").append(u.getHost());
        if (port != (tls ? 443 : 80)) {
            sb.append(':').append(port);
        }
        sb.append("\r\n");
        boolean[] seen = new boolean[2]; // user-agent, accept
        r.headers().forEach((n, v) -> {
            if (n.equalsIgnoreCase("user-agent")) seen[0] = true;
            if (n.equalsIgnoreCase("accept")) seen[1] = true;
            sb.append(n).append(": ").append(v).append("\r\n");
        });
        if (!seen[0]) sb.append("User-Agent: ").append(userAgent).append("\r\n");
        if (!seen[1]) sb.append("Accept: */*\r\n");
        if (client.addsAcceptEncoding(r)) sb.append("Accept-Encoding: gzip, deflate\r\n");
        String cookies = client.cookieHeaderFor(r);
        if (cookies != null) sb.append("Cookie: ").append(cookies).append("\r\n");
        if (plainProxy != null && plainProxy.authorization() != null) {
            sb.append("Proxy-Authorization: ").append(plainProxy.authorization()).append("\r\n");
        }
        if (ex.offerUpgrade) {
            sb.append("Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: ")
                    .append(java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                            Http2ClientStage.settingsPayload(client.http2Config(), client.maxHeaderBytes()))).append("\r\n");
        }
        RequestBody rb = r.requestBody();
        String m = r.method();
        byte[] inline = rb instanceof RequestBodies.Bytes b ? b.data() : null;
        long length = rb.contentLength();
        if (length < 0) {
            sb.append("Transfer-Encoding: chunked\r\n"); // unknown size: streamed in chunks
        } else if (length > 0 || m.equals("POST") || m.equals("PUT") || m.equals("PATCH")) {
            sb.append("Content-Length: ").append(length).append("\r\n");
        }
        sb.append("\r\n");
        byte[] head = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
        if (inline == null || inline.length == 0) {
            return head; // a streamed body is written afterwards, by the stage
        }
        byte[] out = new byte[head.length + inline.length];
        System.arraycopy(head, 0, out, 0, head.length);
        System.arraycopy(inline, 0, out, head.length, inline.length);
        return out;
    }

    /** True if the stage must write the body after the head (anything but an in-memory body). */
    static boolean hasStreamedBody(ClientRequest r) {
        RequestBody rb = r.requestBody();
        return !(rb instanceof RequestBodies.Bytes) && !(rb.contentLength() == 0 && rb.isReplayable());
    }

    String userAgent() {
        return config.userAgent();
    }

    Http2ClientConfig http2Config() {
        return config.http2();
    }

    /** Hands the cookies a response set to the cookie jar (all hops of a redirect chain, interim responses excluded). */
    void storeCookies(URI uri, Headers responseHeaders) {
        CookieJar jar = config.cookieJar();
        if (jar == null) {
            return;
        }
        List<String> set = responseHeaders.getAll("set-cookie");
        if (!set.isEmpty()) {
            try {
                jar.store(uri, set);
            } catch (RuntimeException e) {
                // a faulty jar must not fail the request
            }
        }
    }

    /** The {@code Cookie} header to add to this request, or null (no jar, nothing matches, or the request set its own). */
    String cookieHeaderFor(ClientRequest r) {
        CookieJar jar = config.cookieJar();
        if (jar == null || r.headers().contains("cookie")) {
            return null;
        }
        try {
            String v = jar.cookieHeader(r.uri());
            return v == null || v.isEmpty() || v.indexOf('\r') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('\0') >= 0 ? null : v;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** True if this client adds {@code Accept-Encoding} and decodes the answer for requests that did not set the header themselves. */
    boolean addsAcceptEncoding(ClientRequest r) {
        return config.decompress() && !r.headers().contains("accept-encoding");
    }

    /** The decoder for a response body, or null if it is to be passed on as received. */
    ContentDecoder contentDecoder(Exchange ex, Headers responseHeaders, boolean streaming) {
        if (!addsAcceptEncoding(ex.req)) {
            return null;
        }
        return ContentDecoder.forEncoding(responseHeaders.get("content-encoding"), streaming ? Long.MAX_VALUE : config.maxResponseBytes());
    }

    /** Response headers as the application sees them once the body has been decoded for it. */
    static Headers withoutEncoding(Headers h) {
        Headers c = h.copy();
        c.remove("content-encoding");
        c.remove("content-length");
        return c;
    }

    int maxResponseBytes() {
        return config.maxResponseBytes();
    }

    int maxHeaderBytes() {
        return config.maxHeaderBytes();
    }

    // ---- internals ----

    private record Key(boolean tls, String host, int port, InetSocketAddress connect) {
    }

    /** One logical request (which may span several connections: retries, redirects). */
    static final class Exchange {
        final CompletableFuture<ClientResponse> result = new CompletableFuture<>();
        final io.hydrangea.EventLoop fallbackLoop;
        volatile ClientRequest req;
        volatile PooledConn conn; // set only while a connection serves this exchange
        volatile io.hydrangea.EventLoop callbackLoop; // loop of the current connection: where listener callbacks run
        volatile Scheduled timeout;
        volatile boolean offerUpgrade; // this request carries "Upgrade: h2c"; a 101 answer turns the connection into HTTP/2 with it as stream 1
        volatile boolean h2Aborted;   // the HTTP/2 stream of this exchange was reset by abortConnection
        volatile Runnable h2Cancel; // set while the exchange runs as an HTTP/2 stream: aborting resets that stream, not the shared connection
        BodyListener stream;      // non-null for streamed exchanges
        private final java.util.concurrent.atomic.AtomicBoolean ended = new java.util.concurrent.atomic.AtomicBoolean();
        boolean reused;
        boolean forceNew;
        boolean retried;
        int redirects;
        long start;               // Latency.begin() when the request was sent (0 = not timed)

        Exchange(ClientRequest req, EventLoop fallbackLoop) {
            this.req = req;
            this.fallbackLoop = fallbackLoop;
        }

        void fail(Throwable t) {
            if (result.completeExceptionally(t)) {
                abortConnection();
                if (stream != null) {
                    // callbacks stay serial with data callbacks only if they run on the connection's loop
                    EventLoop loop = callbackLoop != null ? callbackLoop : fallbackLoop;
                    try {
                        loop.execute(() -> endStream(t));
                    } catch (RejectedExecutionException shutDown) {
                        endStream(t);
                    }
                }
            }
        }

        void cancelTimeout() {
            Scheduled t = timeout;
            if (t != null) {
                t.cancel();
            }
        }

        /** Calls onComplete (err == null) or onError, once. */
        void endStream(Throwable err) {
            if (stream == null || !ended.compareAndSet(false, true)) {
                return;
            }
            try {
                if (err == null) {
                    stream.onComplete();
                } else {
                    stream.onError(err);
                }
            } catch (Throwable ignored) {
                // a failing terminal callback has nowhere to go
            }
        }

        void abortConnection() {
            Runnable cancelStream = h2Cancel;
            if (cancelStream != null) {
                h2Cancel = null;
                h2Aborted = true;
                cancelStream.run();
                return;
            }
            if (h2Aborted) {
                return;   // the stream was reset already; the connection is shared with other streams and must stay
            }
            PooledConn c = conn;
            if (c != null && c.h2) {
                // The stream has not started yet (its h2Cancel is set on the loop): the connection is shared with other streams and must stay.
                // The stage sees h2Aborted when it gets to this exchange and does not start it.
                h2Aborted = true;
                return;
            }
            conn = null;
            if (c != null) {
                c.closeConnection();
            }
        }
    }

    /** A connection plus the parser bound to it. */
    final class PooledConn {
        final HostPool pool;
        final HttpClientStage stage;              // HTTP/1.1
        final Http2ClientStage h2stage;           // null unless HTTP/2 is enabled
        final ClientProtocolStage proto;          // ditto: chooses between the two once the connection is active
        volatile boolean h2;                      // this connection speaks HTTP/2
        final java.util.concurrent.atomic.AtomicInteger h2Active = new java.util.concurrent.atomic.AtomicInteger(); // streams running or reserved
        volatile boolean h2Goaway;
        volatile Connection connection;
        /** Set when this connection goes to the proxy for plain-http requests (absolute-form targets); null for direct and tunnelled connections. */
        volatile ProxyConfig.Endpoint plainProxy;
        /** Why the proxy would not open a tunnel, if it did not: reported instead of the generic "channel closed". */
        volatile IOException tunnelError;
        private boolean accounted; // pool bookkeeping for the close happened (guarded by pool lock)

        PooledConn(HostPool pool, java.util.function.Supplier<String> alpn, boolean http2, boolean upgradeOffer) {
            this.pool = pool;
            this.stage = new HttpClientStage(this, HttpClient.this);
            if (http2) {
                this.h2stage = new Http2ClientStage(this, HttpClient.this, config.http2(), config.maxHeaderBytes());
                this.proto = new ClientProtocolStage(this, alpn, upgradeOffer);
                this.h2Active.set(1); // the request this connection is opened for holds a stream slot from the start (released if the host speaks HTTP/1.1)
            } else {
                this.h2stage = null;
                this.proto = null;
            }
        }

        /** The stage to put last in the pipeline. */
        io.hydrangea.pipeline.Stage lastStage() {
            return proto != null ? proto : stage;
        }

        void attach(Connection c) {
            this.connection = c;
            c.closeFuture().thenRun(() -> {
                if (proto != null) {
                    proto.connectionClosed();
                } else {
                    stage.connectionClosed(); // fails an exchange that never saw onClose (closed before activation)
                }
                pool.onClosed(this);
            });
        }

        boolean usable() {
            Connection c = connection;
            return c != null && c.isOpen() && !h2Goaway;
        }

        /** Reserves one stream on this HTTP/2 connection; false if it is full, going away, or not HTTP/2. */
        boolean tryReserveStream() {
            if (!h2 || !usable()) {
                return false;
            }
            int limit = config.http2().maxStreamsPerConnection();
            for (;;) {
                int a = h2Active.get();
                if (a >= limit) {
                    return false;
                }
                if (h2Active.compareAndSet(a, a + 1)) {
                    return true;
                }
            }
        }

        void run(Exchange ex, boolean reused) {
            ex.conn = this;
            ex.reused = reused;
            ex.callbackLoop = connection.eventLoop();
            try {
                connection.eventLoop().execute(() -> {
                    if (proto != null) {
                        proto.submitFirst(ex);
                    } else {
                        stage.submit(ex);
                    }
                });
            } catch (RejectedExecutionException e) {
                ex.conn = null;
                ex.fail(new IOException("client is closed"));
            }
        }

        /** Runs {@code ex} as one more stream of this HTTP/2 connection; the pool has reserved the slot with {@link #tryReserveStream}. */
        void runH2(Exchange ex) {
            ex.reused = true;
            ex.callbackLoop = connection.eventLoop();
            try {
                connection.eventLoop().execute(() -> proto.submitReserved(ex));
            } catch (RejectedExecutionException e) {
                h2StreamEnded();
                ex.fail(new IOException("client is closed"));
            }
        }

        /** An HTTP/2 stream (or a reserved slot) is over: free it and let the next waiting request use it. */
        void h2StreamEnded() {
            h2Active.decrementAndGet();
            pool.h2Released(this);
        }

        void h2GoingAway() {
            h2Goaway = true;
        }

        /** Loop thread. A response completed on an HTTP/2 stream. */
        void h2Finished(Exchange ex, ClientResponse resp) {
            ex.conn = null;
            ex.h2Cancel = null;
            onResponse(ex, resp);
        }

        void h2FinishedStream(Exchange ex, ClientResponse head) {
            ex.conn = null;
            ex.h2Cancel = null;
            ex.endStream(null);
            ex.result.complete(head);
        }

        /** Loop thread. An HTTP/2 stream failed; {@code gotResponseBytes} false means the request was not processed (retryable). */
        void h2Failed(Exchange ex, Throwable t, boolean gotResponseBytes) {
            ex.conn = null;
            ex.h2Cancel = null;
            onFailure(ex, tunnelError != null ? tunnelError : t, gotResponseBytes);
        }

        void closeConnection() {
            Connection c = connection;
            if (c != null) {
                c.close();
            }
        }

        /** Loop thread. The response is complete. */
        void finished(Exchange ex, ClientResponse resp, boolean reusable) {
            ex.conn = null;
            if (reusable && usable()) {
                pool.release(this);
            } else {
                closeConnection();
            }
            onResponse(ex, resp);
        }

        /** Loop thread. A streamed response ended normally; {@code head} carries status and headers with an empty body. */
        void finishedStream(Exchange ex, ClientResponse head, boolean reusable) {
            ex.conn = null;
            if (reusable && usable()) {
                pool.release(this);
            } else {
                closeConnection();
            }
            ex.endStream(null);
            ex.result.complete(head);
        }

        /** Loop thread. The exchange failed on this connection. */
        void failed(Exchange ex, Throwable t, boolean gotResponseBytes) {
            ex.conn = null;
            closeConnection();
            onFailure(ex, tunnelError != null ? tunnelError : t, gotResponseBytes);
        }
    }

    /** Connections and waiting requests of one scheme+host+port. */
    final class HostPool {
        private final Key key;
        private final ArrayDeque<PooledConn> idle = new ArrayDeque<>();
        private final Set<PooledConn> all = new HashSet<>();
        /** The connections of {@link #all} that speak HTTP/2: what {@link #acquire} searches for a free stream (an HTTP/1.1 host has none, so no search at all). */
        private final ArrayList<PooledConn> h2Conns = new ArrayList<>();
        private final ArrayDeque<Exchange> waiters = new ArrayDeque<>();
        private int total; // connections open or being opened
        private boolean shutdown;
        /** What this host speaks, as far as we know: TLS hosts are unknown until the first connection's ALPN answer. */
        private boolean protocolUnknown;

        HostPool(Key key) {
            this.key = key;
            // until the first connection is active nobody knows whether to share it: with cleartext prior knowledge it is HTTP/2 for sure,
            // but a burst must still wait for that one connection instead of opening one per request
            this.protocolUnknown = config.http2() != null && (key.tls || config.http2().cleartextPriorKnowledge() || config.http2().cleartextUpgrade());
        }

        void acquire(Exchange ex) {
            PooledConn reuse = null;
            PooledConn reuseH2 = null;
            boolean open = false;
            synchronized (this) {
                if (shutdown) {
                    ex.fail(new IOException("client is closed"));
                    return;
                }
                if (!ex.forceNew) {
                    for (int i = 0, n = h2Conns.size(); i < n; i++) { // a live HTTP/2 connection with room for one more stream
                        PooledConn c = h2Conns.get(i);
                        if (c.tryReserveStream()) {
                            reuseH2 = c;
                            break;
                        }
                    }
                    PooledConn c;
                    while (reuseH2 == null && (c = idle.pollLast()) != null) {
                        if (c.usable()) {
                            reuse = c;
                            break;
                        }
                    }
                }
                if (reuse == null && reuseH2 == null) {
                    if (protocolUnknown && total > 0) {
                        waiters.add(ex); // the first connection's handshake decides whether everybody can share it
                    } else if (total < config.maxConnectionsPerHost()) {
                        total++;
                        open = true;
                    } else {
                        waiters.add(ex);
                    }
                }
            }
            if (reuseH2 != null) {
                reuseH2.runH2(ex);
            } else if (reuse != null) {
                reuse.run(ex, true);
            } else if (open) {
                openConnection(ex);
            }
        }

        void release(PooledConn c) {
            Exchange next = null;
            boolean close = false;
            synchronized (this) {
                if (shutdown) {
                    close = true;
                } else {
                    Exchange w;
                    while ((w = waiters.poll()) != null && w.result.isDone()) {
                        // skip requests that timed out or were cancelled while queued
                    }
                    if (w != null) {
                        next = w;
                    } else if (idle.size() < config.maxIdlePerHost()) {
                        idle.addLast(c);
                    } else {
                        close = true;
                    }
                }
            }
            if (close) {
                c.closeConnection();
            } else if (next != null) {
                c.run(next, true);
            }
        }

        /** Loop thread: {@code pc} has become an HTTP/2 connection (negotiated, or upgraded from HTTP/1.1); from now on {@link #acquire} may give it more streams. */
        void markH2(PooledConn pc) {
            synchronized (this) {
                if (!shutdown && !pc.accounted && !h2Conns.contains(pc)) {
                    h2Conns.add(pc);
                }
            }
        }

        /** Loop thread, once a connection is active: it speaks HTTP/2 or HTTP/1.1. Requests that waited for the answer go on. */
        void protocolKnown(PooledConn pc, boolean isH2) {
            List<Exchange> toH2 = new ArrayList<>();
            List<Exchange> toOpen = new ArrayList<>();
            synchronized (this) {
                protocolUnknown = false;
                if (shutdown) {
                    return;
                }
                Exchange w;
                if (isH2) {
                    while ((w = waiters.peek()) != null) {
                        if (w.result.isDone()) {
                            waiters.poll();
                        } else if (pc.tryReserveStream()) {
                            waiters.poll();
                            toH2.add(w);
                        } else {
                            break;
                        }
                    }
                } else {
                    while (total < config.maxConnectionsPerHost() && (w = waiters.poll()) != null) {
                        if (!w.result.isDone()) {
                            total++;
                            toOpen.add(w);
                        }
                    }
                }
            }
            toH2.forEach(pc::runH2);
            toOpen.forEach(this::openConnection);
        }

        /** An HTTP/2 stream ended: the connection may take the next waiting request. */
        void h2Released(PooledConn pc) {
            Exchange next = null;
            synchronized (this) {
                if (shutdown) {
                    return;
                }
                Exchange w;
                while ((w = waiters.peek()) != null) {
                    if (w.result.isDone()) {
                        waiters.poll();
                        continue;
                    }
                    if (pc.tryReserveStream()) {
                        waiters.poll();
                        next = w;
                    }
                    break;
                }
            }
            if (next != null) {
                pc.runH2(next);
            }
        }

        void onClosed(PooledConn c) {
            Exchange w = null;
            synchronized (this) {
                if (c.accounted) {
                    return;
                }
                c.accounted = true;
                idle.remove(c);
                all.remove(c);
                h2Conns.remove(c);
                total--;
                w = nextWaiterForNewConnection();
            }
            if (w != null) {
                openConnection(w);
            }
        }

        /** Holding the lock: if there is room and someone is waiting, reserve a slot for a new connection. */
        private Exchange nextWaiterForNewConnection() {
            if (shutdown || total >= config.maxConnectionsPerHost()) {
                return null;
            }
            Exchange w;
            while ((w = waiters.poll()) != null) {
                if (!w.result.isDone()) {
                    total++;
                    return w;
                }
            }
            return null;
        }

        /** The slot reserved for {@code ex} could not be turned into a connection. */
        private void slotFailed(Exchange ex, Throwable err) {
            Exchange w;
            synchronized (this) {
                total--;
                w = nextWaiterForNewConnection();
            }
            ex.fail(err instanceof IOException ? err : new IOException(err));
            if (w != null) {
                openConnection(w);
            }
        }

        private void openConnection(Exchange ex) {
            ProxyConfig.Endpoint proxy = config.proxy() != null ? config.proxy().forUri(ex.req.uri()) : null;
            try {
                dns.execute(() -> {
                    // with a proxy only the proxy is looked up: the origin's name goes to the proxy, which resolves it
                    String lookup = proxy != null ? proxy.host() : key.host;
                    int connectPort = proxy != null ? proxy.port() : key.port;
                    InetSocketAddress target;
                    if (key.connect != null && proxy == null) {
                        target = key.connect;
                    } else {
                        try {
                            target = new InetSocketAddress(InetAddress.getByName(lookup.startsWith("[") ? lookup.substring(1, lookup.length() - 1) : lookup), connectPort);
                        } catch (IOException e) {
                            slotFailed(ex, e);
                            return;
                        }
                    }
                    boolean priorKnowledge = config.http2() != null && !key.tls && config.http2().cleartextPriorKnowledge();
                    boolean upgradeOffer = config.http2() != null && !key.tls && !priorKnowledge && config.http2().cleartextUpgrade() && proxy == null;
                    boolean http2 = config.http2() != null && (key.tls || priorKnowledge || upgradeOffer);
                    boolean tunnel = proxy != null && (key.tls || priorKnowledge);
                    TlsStage tls = key.tls
                            ? (http2 ? TlsStage.client(sslContext, key.host, key.port, List.of("h2", "http/1.1")) : TlsStage.client(sslContext, key.host, key.port))
                            : null;
                    PooledConn pc = new PooledConn(this, tls != null && http2 ? tls::applicationProtocol : null, http2, upgradeOffer);
                    if (proxy != null && !tunnel) {
                        pc.plainProxy = proxy;
                    }
                    java.util.ArrayList<io.hydrangea.pipeline.Stage> stages = new java.util.ArrayList<>();
                    if (tunnel) {
                        stages.add(new ProxyTunnelStage(key.host, key.port, proxy.authorization(), err -> pc.tunnelError = err));
                    }
                    if (tls != null) {
                        stages.add(tls);
                    }
                    stages.add(pc.lastStage());
                    Handler handler = Pipeline.of(stages.toArray(new io.hydrangea.pipeline.Stage[0]));
                    Options options = Options.defaults()
                            .withConnectTimeout(config.connectTimeout())
                            .withIdleTimeout(config.idleTimeout());
                    Client.connect(group, target, options, handler).whenComplete((conn, err) -> {
                        if (err != null) {
                            slotFailed(ex, err instanceof CompletionException && err.getCause() != null ? err.getCause() : err);
                            return;
                        }
                        synchronized (HostPool.this) {
                            all.add(pc);
                        }
                        pc.attach(conn);
                        pc.run(ex, false);
                    });
                });
            } catch (RejectedExecutionException e) {
                slotFailed(ex, new IOException("client is closed"));
            }
        }

        void shutdown() {
            List<PooledConn> conns;
            List<Exchange> pending;
            synchronized (this) {
                shutdown = true;
                conns = new ArrayList<>(all);
                pending = new ArrayList<>(waiters);
                waiters.clear();
                idle.clear();
            }
            pending.forEach(e -> e.fail(new IOException("client is closed")));
            conns.forEach(c -> {
                Exchange cur = c.stage.currentExchange();
                if (cur != null) {
                    cur.fail(new IOException("client is closed"));
                }
                if (c.h2stage != null) {
                    c.h2stage.abortAll(new IOException("client is closed"));
                }
                c.closeConnection();
            });
        }
    }
}
