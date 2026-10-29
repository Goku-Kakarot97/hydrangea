package io.hydrangea.http;

import io.hydrangea.Connection;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import io.hydrangea.Scheduled;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * HTTP/1.1 server codec + dispatcher; place it last in a {@link io.hydrangea.pipeline.Pipeline}
 * (after {@code TlsStage} for HTTPS).
 * <p>
 * Strictness is deliberate (request smuggling defence): only CRLF line endings, no obsolete line folding,
 * no whitespace before a header colon, {@code Transfer-Encoding} together with {@code Content-Length} is rejected,
 * conflicting {@code Content-Length} values are rejected, and a {@code Host} header is required for HTTP/1.1.
 * Protocol errors are answered (400/413/431/501/505) and the connection is closed.
 * <p>
 * Supported: keep-alive, pipelining (answers stay in order), {@code Content-Length} and chunked request bodies,
 * {@code Expect: 100-continue}, HEAD. Not supported: HTTP/2, upgrades, chunked responses.
 */
public final class HttpServerStage implements Stage {

    private static final Logger LOG = System.getLogger(HttpServerStage.class.getName());
    private static final int MAX_CHUNK_LINE = 1024;

    private enum State { HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS }

    /** Protocol violation → status code to answer with. */
    private static final class HttpException extends RuntimeException {
        final int status;

        HttpException(int status, String message) {
            super(message, null, false, false);
            this.status = status;
        }
    }

    private final HttpConfig config;
    private final HttpHandler handler;
    private final Accumulator acc = new Accumulator();

    private State state = State.HEAD;
    private boolean busy;      // a request is being handled; do not parse the next one yet
    private boolean closing;   // an error response was sent, ignore everything further

    // request under construction
    private String method;
    private String target;
    private String version;
    private Headers headers;
    private ByteArrayOutputStream body;
    private long remaining;    // bytes left in the current body / chunk
    private int trailerBytes;
    private boolean keepAlive;

    private boolean protocolChecked; // false until the first bytes are known not to be an HTTP/2 preface (only used when HTTP/2 is enabled)
    private Stage upgraded; // after a 101 switch: receives everything the connection delivers

    // deadlines (see HttpConfig): exactly one is active at a time, and it is never extended by incoming bytes
    private enum Timing { NONE, KEEPALIVE, HEADER, BODY }
    private Timing timing = Timing.NONE;
    private Scheduled timer;
    private boolean shuttingDown;       // graceful shutdown requested: finish what is running, then close
    private boolean inFlight;          // counted in http.server.active: from the head being parsed to the response (or the connection going away)
    private long requestStartNanos;     // when the head of the current request was parsed (0 = none yet), for the access log

    // streamed request body (handler.wantsStreamingBody): the body goes to a listener instead of `body`
    private static final long DRAIN_BUDGET = 8L * 1024 * 1024;
    private static final long DRAIN_MILLIS = 5000;
    private boolean streamBody;
    private StreamingBody sb;
    private HttpRequest streamHead;
    private boolean streamKeepAlive;
    private boolean streamBodyDone;
    private boolean streamResponded;
    private boolean draining;        // a response went out before the body ended: read and discard the rest
    private boolean drainKeepAlive;  // ...and then carry on with the next request (the answer was not an error)
    private long drainBudget;
    private Scheduled drainTimer;

    private final Supplier<String> negotiatedProtocol; // ALPN result; null when there is no TLS

    public HttpServerStage(HttpConfig config, HttpHandler handler) {
        this(config, handler, null);
    }

    /** @param negotiatedProtocol the ALPN protocol agreed in the TLS handshake ({@code "h2"} makes the connection HTTP/2 whatever it sends first) */
    public HttpServerStage(HttpConfig config, HttpHandler handler, Supplier<String> negotiatedProtocol) {
        this.config = config;
        this.handler = handler;
        this.negotiatedProtocol = negotiatedProtocol;
    }

    @Override
    public void onActive(Context ctx) {
        updateTimers(ctx); // a fresh connection must say something within keepAliveTimeout
    }

    @Override
    public void onDrain(Context ctx) throws Exception {
        if (upgraded != null) {
            upgraded.onDrain(ctx);
            return;
        }
        shuttingDown = true;
        updateTimers(ctx); // the server's own deadline takes over
        boolean idle = state == State.HEAD && !busy && !streamBody && !draining && !closing && !acc.buffer().hasRemaining();
        if (idle) { // nothing in progress: a keep-alive connection waiting for its next request
            closing = true;
            closeAfterFlush(ctx);
        }
        // otherwise the request in progress finishes and its response says "Connection: close" (see finish)
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        if (upgraded != null) {
            upgraded.onRead(ctx, msg);
            return;
        }
        if (closing) {
            return;
        }
        acc.append((ByteBuffer) msg);
        if (!protocolChecked && config.http2() != null && sniffHttp2(ctx)) {
            return;
        }
        parse(ctx);
    }

    /**
     * Cleartext HTTP/2 starts with a fixed 24-byte preface that no HTTP/1.1 request can begin with. While the bytes seen so far are a
     * prefix of it we wait; the first byte that differs means HTTP/1.1; the complete preface hands the connection to
     * {@link Http2ServerStage}. (Over TLS the client sends it right after ALPN selected h2.)
     * @return true when the bytes were consumed here (HTTP/2, or still waiting for more of the preface)
     */
    private boolean sniffHttp2(Context ctx) {
        ByteBuffer b = acc.buffer();
        byte[] preface = Http2ServerStage.PREFACE;
        boolean alpnH2 = negotiatedProtocol != null && "h2".equals(negotiatedProtocol.get());
        if (alpnH2) { // TLS said h2: whatever arrives is HTTP/2, so a bad preface is answered as one (GOAWAY), not with an HTTP/1.1 error page
            protocolChecked = true;
            return switchToHttp2(ctx, b);
        }
        int n = Math.min(b.remaining(), preface.length);
        for (int i = 0; i < n; i++) {
            if (b.get(b.position() + i) != preface[i]) {
                protocolChecked = true;
                return false;
            }
        }
        if (b.remaining() < preface.length) {
            return true; // could still turn out to be either: wait (the deadline of the first request applies)
        }
        protocolChecked = true;
        return switchToHttp2(ctx, b);
    }

    private boolean switchToHttp2(Context ctx, ByteBuffer b) {
        Http2ServerStage h2 = new Http2ServerStage(config, handler);
        upgraded = h2;
        updateTimers(ctx); // HTTP/1.1 deadlines no longer apply
        ByteBuffer copy = ByteBuffer.allocate(b.remaining()).put(b).flip();
        try {
            h2.onActive(ctx);
            h2.onRead(ctx, copy);
        } catch (Exception e) {
            ctx.connection().close();
        }
        return true;
    }

    @Override
    public void onWritabilityChanged(Context ctx) throws Exception {
        if (upgraded != null) {
            upgraded.onWritabilityChanged(ctx);
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        endInFlight(ctx);
        if (upgraded != null) {
            upgraded.onClose(ctx, cause);
        }
        if (sb != null && !streamBodyDone) {
            sb.abort(new IOException("connection closed before the request body was complete", cause));
        }
        if (drainTimer != null) {
            drainTimer.cancel();
        }
        cancelTimer();
        acc.release();
        ctx.fireClose(cause);
    }

    // ---- parsing ----

    private void parse(Context ctx) {
        parseLoop(ctx);
        updateTimers(ctx);
    }

    private void parseLoop(Context ctx) {
        try {
            while (!busy && !closing) {
                if (sb != null && sb.paused && !draining) {
                    return; // the listener asked for a pause; unread bytes stay in acc
                }
                ByteBuffer in = acc.buffer();
                boolean progressed = switch (state) {
                    case HEAD -> parseHead(ctx, in);
                    case BODY -> readBody(ctx, in);
                    case CHUNK_SIZE -> readChunkSize(in);
                    case CHUNK_DATA -> readChunkData(in);
                    case CHUNK_CRLF -> readChunkCrlf(in);
                    case TRAILERS -> readTrailers(ctx, in);
                };
                if (!progressed) {
                    return; // need more bytes
                }
            }
        } catch (HttpException e) {
            fail(ctx, e.status, e.getMessage());
        } catch (Exception e) {
            LOG.log(Logger.Level.WARNING, "http handling failed", e);
            fail(ctx, 500, "Internal Server Error");
        }
    }

    private boolean parseHead(Context ctx, ByteBuffer in) throws Exception {
        method = null; // fields of the previous request must not leak into the log of a malformed one
        target = null;
        version = null;
        headers = null;
        while (in.remaining() >= 2 && in.get(in.position()) == '\r' && in.get(in.position() + 1) == '\n') {
            in.position(in.position() + 2); // tolerate stray CRLFs before the request line (RFC 9112 §2.2)
        }
        int end = indexOf(in, in.position(), "\r\n\r\n");
        if (end < 0) {
            for (int i = in.position(); i < in.limit(); i++) {
                if (in.get(i) == '\n' && (i == in.position() || in.get(i - 1) != '\r')) {
                    throw new HttpException(400, "Bad Request"); // bare LF: do not wait for a terminator that never comes
                }
            }
            if (in.remaining() > config.maxHeaderBytes()) {
                throw new HttpException(431, "Request Header Fields Too Large");
            }
            return false;
        }
        if (end - in.position() > config.maxHeaderBytes()) {
            throw new HttpException(431, "Request Header Fields Too Large");
        }
        byte[] raw = new byte[end - in.position()];
        in.get(raw);
        in.position(in.position() + 4);
        String head = new String(raw, StandardCharsets.ISO_8859_1);
        for (int i = 0; i < head.length(); i++) {
            char c = head.charAt(i);
            if (c == 0 || (c == '\r' && !head.startsWith("\r\n", i)) || (c == '\n' && (i == 0 || head.charAt(i - 1) != '\r'))) {
                throw new HttpException(400, "Bad Request");
            }
        }
        String[] lines = head.split("\r\n", -1);
        parseRequestLine(lines[0]);
        headers = new Headers();
        for (int i = 1; i < lines.length; i++) {
            parseHeaderLine(lines[i]);
        }
        requestStartNanos = System.nanoTime();
        if (!inFlight) {
            inFlight = true;
            ctx.connection().eventLoop().metrics().recordHttpStart();
        }
        decideBody(ctx);
        return true;
    }

    private void parseRequestLine(String line) {
        String[] parts = line.split(" ", -1);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new HttpException(400, "Bad Request");
        }
        for (int i = 0; i < parts[0].length(); i++) {
            if (!isTokenChar(parts[0].charAt(i))) {
                throw new HttpException(400, "Bad Request");
            }
        }
        for (int i = 0; i < parts[1].length(); i++) {
            char c = parts[1].charAt(i);
            if (c <= 0x20 || c == 0x7f) {
                throw new HttpException(400, "Bad Request");
            }
        }
        if (!parts[2].equals("HTTP/1.1") && !parts[2].equals("HTTP/1.0")) {
            throw new HttpException(parts[2].startsWith("HTTP/") ? 505 : 400, "HTTP Version Not Supported");
        }
        method = parts[0];
        target = parts[1];
        version = parts[2];
    }

    private void parseHeaderLine(String line) {
        if (line.isEmpty() || line.charAt(0) == ' ' || line.charAt(0) == '\t') {
            throw new HttpException(400, "Bad Request"); // obsolete line folding / empty line
        }
        int colon = line.indexOf(':');
        if (colon <= 0) {
            throw new HttpException(400, "Bad Request");
        }
        String name = line.substring(0, colon);
        for (int i = 0; i < name.length(); i++) {
            if (!isTokenChar(name.charAt(i))) {
                throw new HttpException(400, "Bad Request"); // also rejects "Name : value"
            }
        }
        String value = line.substring(colon + 1);
        int s = 0;
        int e = value.length();
        while (s < e && (value.charAt(s) == ' ' || value.charAt(s) == '\t')) s++;
        while (e > s && (value.charAt(e - 1) == ' ' || value.charAt(e - 1) == '\t')) e--;
        value = value.substring(s, e);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c < 0x20 && c != '\t') || c == 0x7f) {
                throw new HttpException(400, "Bad Request");
            }
        }
        headers.add(name, value);
    }

    private void decideBody(Context ctx) {
        boolean http11 = version.equals("HTTP/1.1");
        if (http11 && headers.getAll("host").size() != 1) {
            throw new HttpException(400, "Bad Request");
        }
        keepAlive = http11 ? !headers.hasToken("connection", "close") : headers.hasToken("connection", "keep-alive");
        var te = headers.getAll("transfer-encoding");
        var cl = headers.getAll("content-length");
        body = new ByteArrayOutputStream();
        streamBody = false;
        if (!te.isEmpty()) {
            if (!cl.isEmpty() || !http11) {
                throw new HttpException(400, "Bad Request"); // smuggling vector
            }
            if (te.size() != 1 || !te.get(0).equalsIgnoreCase("chunked")) {
                throw new HttpException(501, "Not Implemented");
            }
            state = State.CHUNK_SIZE;
            if (!startStreamingIfWanted(ctx, -1)) {
                maybeContinue(ctx);
            }
        } else if (!cl.isEmpty()) {
            long len = -1;
            for (String v : cl) {
                for (String part : v.split(",", -1)) {
                    long n = parseLength(part.trim());
                    if (len >= 0 && n != len) {
                        throw new HttpException(400, "Bad Request"); // conflicting lengths
                    }
                    len = n;
                }
            }
            if (len == 0) {
                dispatch(ctx);
                return;
            }
            remaining = len;
            state = State.BODY;
            if (!startStreamingIfWanted(ctx, len)) {
                if (len > config.maxBodyBytes()) {
                    throw new HttpException(413, "Payload Too Large");
                }
                maybeContinue(ctx);
            }
        } else {
            dispatch(ctx);
        }
    }

    // ---- deadlines ----

    /** Picks the deadline that fits what the connection is waiting for now, and leaves a running one of the same kind alone. */
    private void updateTimers(Context ctx) {
        Timing want;
        if (closing || upgraded != null || shuttingDown || busy || streamBody || draining) {
            want = Timing.NONE;       // the application is working, a stream is running, or we are already closing
        } else if (state == State.HEAD) {
            want = acc.buffer().hasRemaining() ? Timing.HEADER : Timing.KEEPALIVE;
        } else {
            want = Timing.BODY;
        }
        if (want == timing) {
            return; // the deadline runs from when this phase began: more bytes do not extend it
        }
        cancelTimer();
        timing = want;
        Duration d = switch (want) {
            case KEEPALIVE -> config.keepAliveTimeout();
            case HEADER -> config.headerTimeout();
            case BODY -> config.bodyTimeout();
            case NONE -> null;
        };
        if (d != null) {
            timer = ctx.connection().eventLoop().schedule(() -> onTimeout(ctx, want), d.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private void cancelTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        timing = Timing.NONE;
    }

    private void onTimeout(Context ctx, Timing kind) {
        timer = null;
        timing = Timing.NONE;
        if (closing || !ctx.connection().isOpen()) {
            return;
        }
        ctx.connection().eventLoop().metrics().recordHttpTimeout();
        if (kind == Timing.KEEPALIVE) {
            closing = true;          // an idle connection: nothing to answer, just let go
            closeAfterFlush(ctx);
        } else {
            fail(ctx, 408, "Request Timeout"); // a request that did not arrive in time
        }
    }

    // ---- access log and metrics ----

    private void endInFlight(Context ctx) {
        if (inFlight) {
            inFlight = false;
            ctx.connection().eventLoop().metrics().recordHttpEnd();
        }
    }

    private void recordResponse(Context ctx, String m, String t, String v, String userAgent, int status, long bytes) {
        ctx.connection().eventLoop().metrics().recordHttpResponse(status, requestStartNanos);
        endInFlight(ctx);
        AccessLog log = config.accessLog();
        if (log != null) {
            try {
                Duration took = requestStartNanos == 0 ? Duration.ZERO : Duration.ofNanos(System.nanoTime() - requestStartNanos);
                log.log(new AccessLog.Entry(ctx.connection().remoteAddress(), m, t, v, status, bytes, took, userAgent));
            } catch (Throwable ignored) {
                // a failing logger must not break request handling
            }
        }
    }

    // ---- streamed request bodies ----

    /**
     * If the handler wants this request's body as a stream, calls it now (before the body is read) and sets up
     * delivery. @return true when the request is being streamed (the caller must not buffer it)
     */
    private boolean startStreamingIfWanted(Context ctx, long contentLength) {
        HttpRequest head = new HttpRequest(method, target, version, headers, new byte[0]);
        boolean wanted;
        try {
            wanted = handler.wantsStreamingBody(head);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "wantsStreamingBody failed for " + target, t);
            wanted = false;
        }
        if (!wanted) {
            return false;
        }
        streamBody = true;
        streamHead = head;
        streamKeepAlive = keepAlive;
        streamBodyDone = false;
        streamResponded = false;
        draining = false;
        sb = new StreamingBody(ctx, contentLength);
        CompletionStage<HttpResponse> stage;
        try {
            stage = handler.handleStreaming(head, sb);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "streaming handler threw for " + head.method() + " " + head.target(), t);
            stage = CompletableFuture.failedFuture(t);
        }
        sb.registrationClosed = true;
        boolean alreadyAnswered = isDone(stage);
        if (!alreadyAnswered) {
            maybeContinue(ctx); // accepted so far: tell an Expect: 100-continue client to go ahead
        }
        Connection c = ctx.connection();
        stage.whenComplete((resp, err) -> {
            try {
                c.eventLoop().execute(() -> onStreamResponse(ctx, resp, err));
            } catch (RuntimeException rejected) {
                // loop shut down; the connection is being torn down
            }
        });
        return true;
    }

    private static boolean isDone(CompletionStage<?> stage) {
        try {
            return stage.toCompletableFuture().isDone();
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    /** The streamed request's response is ready (loop thread). */
    private void onStreamResponse(Context ctx, HttpResponse resp, Throwable err) {
        if (streamResponded || closing || !ctx.connection().isOpen()) {
            return;
        }
        HttpResponse r = resp;
        if (err != null) {
            LOG.log(Logger.Level.WARNING, "streaming handler failed for " + streamHead.method() + " " + streamHead.target(), err);
            r = null;
        }
        if (r != null && r.takeover() != null) {
            r = null; // protocol switches and streamed responses cannot start while a request body is in flight
        }
        if (r == null) {
            r = HttpResponse.status(500).text("Internal Server Error");
        }
        streamResponded = true;
        answerStream(ctx, r, streamKeepAlive && err == null && resp != null);
    }

    /** Sends the response of the streamed request: normally if its body is complete, otherwise as an early response. */
    private void answerStream(Context ctx, HttpResponse r, boolean keepAlive) {
        if (streamBodyDone) {
            HttpRequest head = streamHead;
            resetStream();
            finish(ctx, head, r, keepAlive);
        } else {
            earlyResponse(ctx, r);
        }
    }

    /**
     * A body listener failed. The 500 is sent from a fresh task, never from inside the parse loop that is calling the
     * listener: answering re-enters parse(), which would corrupt the loop's own bookkeeping.
     */
    private void failStream(Context ctx) {
        if (streamResponded) {
            return;
        }
        streamResponded = true;
        try {
            ctx.connection().eventLoop().execute(() -> {
                if (!closing && ctx.connection().isOpen()) {
                    answerStream(ctx, HttpResponse.status(500).text("Internal Server Error"), false);
                }
            });
        } catch (RuntimeException shutDown) {
            // loop is going away
        }
    }

    /**
     * The handler answered while the client was still sending (typically a rejection such as 413). The response goes out
     * now; then a bounded amount of the remaining body is read and discarded before closing, because closing a socket
     * that still holds unread data makes the kernel send RST, which can destroy the response before the client reads it.
     */
    private void earlyResponse(Context ctx, HttpResponse r) {
        if (sb != null) {
            sb.abort(new IOException("a response was sent before the request body was fully received"));
        }
        // A success answered early is no reason to give the connection up: discard the (bounded) rest of the body and go on.
        // An error (rejection) is announced as the end of the connection, which the client can rely on.
        drainKeepAlive = streamKeepAlive && r.status() < 400 && !shuttingDown;
        recordResponse(ctx, streamHead.method(), streamHead.target(), streamHead.version(), streamHead.headers().get("user-agent"), r.status(), r.body().length);
        sendResponse(ctx, r, false, drainKeepAlive);
        draining = true;
        drainBudget = DRAIN_BUDGET;
        drainTimer = ctx.connection().eventLoop().schedule(() -> closeAfterFlush(ctx), DRAIN_MILLIS, TimeUnit.MILLISECONDS);
        ctx.connection().setAutoRead(true); // a paused body must flow again so it can be discarded
        if (sb != null) {
            sb.paused = false;
        }
        parse(ctx); // there may be body bytes in the buffer already
    }

    /** The whole body has been read. */
    private void streamBodyDone(Context ctx) {
        state = State.HEAD;
        streamBodyDone = true;
        if (draining) {
            if (drainTimer != null) {
                drainTimer.cancel();
            }
            if (drainKeepAlive) {
                resetStream(); // the body is fully discarded: the next request on this connection can be parsed
                return;
            }
            closing = true;
            closeAfterFlush(ctx);
            return;
        }
        Connection c = ctx.connection();
        busy = true;
        c.setAutoRead(false); // hold the next pipelined request until this one is answered
        sb.complete();
    }

    private void resetStream() {
        streamBody = false;
        sb = null;
        streamBodyDone = false;
        streamResponded = false;
        draining = false;
        drainKeepAlive = false;
    }

    /** What a streaming route sees; also the flow control for its listener. */
    private final class StreamingBody implements RequestBodyStream, BodyControl {
        private final Context ctx;
        private final long contentLength;
        private BodyListener listener;
        boolean registrationClosed;
        boolean paused;
        private boolean ended;

        StreamingBody(Context ctx, long contentLength) {
            this.ctx = ctx;
            this.contentLength = contentLength;
        }

        @Override
        public void listen(BodyListener l) {
            if (registrationClosed) {
                throw new IllegalStateException("listen() must be called from inside the route, before it returns");
            }
            if (listener != null) {
                throw new IllegalStateException("a listener is already registered");
            }
            listener = l;
        }

        @Override
        public long contentLength() {
            return contentLength;
        }

        /** Hands n bytes of {@code in} to the listener (or drops them). */
        void deliver(ByteBuffer in, int n) {
            if (draining) {
                in.position(in.position() + n);
                drainBudget -= n;
                if (drainBudget < 0) {
                    closing = true;
                    closeAfterFlush(ctx);
                }
                return;
            }
            if (listener == null || ended) {
                in.position(in.position() + n);
                return;
            }
            ByteBuffer slice = in.slice(in.position(), n).asReadOnlyBuffer();
            in.position(in.position() + n);
            try {
                listener.onData(slice, this);
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "request body listener failed for " + streamHead.target(), t);
                ended = true;
                failStream(ctx);
            }
        }

        void complete() {
            if (ended) {
                return;
            }
            ended = true;
            if (listener != null) {
                try {
                    listener.onComplete();
                } catch (Throwable t) {
                    LOG.log(Logger.Level.WARNING, "request body listener failed in onComplete", t);
                    failStream(ctx);
                }
            }
        }

        void abort(Throwable cause) {
            if (ended) {
                return;
            }
            ended = true;
            if (listener != null) {
                try {
                    listener.onError(cause);
                } catch (Throwable ignored) {
                    // nowhere to report
                }
            }
        }

        // BodyControl (loop thread or any thread)

        @Override
        public void pause() {
            onLoop(() -> {
                if (!paused && !ended && !draining) {
                    paused = true;
                    ctx.connection().setAutoRead(false);
                }
            });
        }

        @Override
        public void resume() {
            onLoop(() -> {
                if (!paused) {
                    return;
                }
                paused = false;
                if (!busy && !closing && ctx.connection().isOpen()) {
                    ctx.connection().setAutoRead(true);
                    parse(ctx);
                }
            });
        }

        /** Gives up on this request: the connection is closed without a response. */
        @Override
        public void cancel() {
            onLoop(() -> ctx.connection().close());
        }

        private void onLoop(Runnable r) {
            var loop = ctx.connection().eventLoop();
            if (loop.inEventLoop()) {
                r.run();
            } else {
                try {
                    loop.execute(r);
                } catch (RuntimeException shutDown) {
                    // connection is going away
                }
            }
        }
    }

    private static long parseLength(String s) {
        if (s.isEmpty() || s.length() > 18) {
            throw new HttpException(400, "Bad Request");
        }
        long n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new HttpException(400, "Bad Request");
            }
            n = n * 10 + (c - '0');
        }
        return n;
    }

    private void maybeContinue(Context ctx) {
        if (version.equals("HTTP/1.1") && headers.hasToken("expect", "100-continue")) {
            send(ctx, ByteBuffer.wrap("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)));
        }
    }

    private boolean readBody(Context ctx, ByteBuffer in) throws Exception {
        if (!in.hasRemaining()) {
            return false;
        }
        int n = (int) Math.min(remaining, in.remaining());
        appendBody(in, n);
        remaining -= n;
        if (remaining == 0) {
            bodyComplete(ctx);
        }
        return true;
    }

    private boolean readChunkSize(ByteBuffer in) {
        int eol = indexOf(in, in.position(), "\r\n");
        if (eol < 0) {
            if (in.remaining() > MAX_CHUNK_LINE) {
                throw new HttpException(400, "Bad Request");
            }
            return false;
        }
        if (eol - in.position() > MAX_CHUNK_LINE) {
            throw new HttpException(400, "Bad Request");
        }
        byte[] raw = new byte[eol - in.position()];
        in.get(raw);
        in.position(in.position() + 2);
        String line = new String(raw, StandardCharsets.ISO_8859_1);
        int semi = line.indexOf(';'); // chunk extensions are ignored
        String hex = (semi < 0 ? line : line.substring(0, semi)).trim();
        if (hex.isEmpty() || hex.length() > 15) {
            throw new HttpException(400, "Bad Request");
        }
        long size = 0;
        for (int i = 0; i < hex.length(); i++) {
            int d = Character.digit(hex.charAt(i), 16);
            if (d < 0) {
                throw new HttpException(400, "Bad Request");
            }
            size = size * 16 + d;
        }
        if (!streamBody && body.size() + size > config.maxBodyBytes()) {
            throw new HttpException(413, "Payload Too Large");
        }
        if (size == 0) {
            state = State.TRAILERS;
            trailerBytes = 0;
        } else {
            remaining = size;
            state = State.CHUNK_DATA;
        }
        return true;
    }

    private boolean readChunkData(ByteBuffer in) {
        if (!in.hasRemaining()) {
            return false;
        }
        int n = (int) Math.min(remaining, in.remaining());
        appendBody(in, n);
        remaining -= n;
        if (remaining == 0) {
            state = State.CHUNK_CRLF;
        }
        return true;
    }

    private boolean readChunkCrlf(ByteBuffer in) {
        if (in.remaining() < 2) {
            return false;
        }
        if (in.get(in.position()) != '\r' || in.get(in.position() + 1) != '\n') {
            throw new HttpException(400, "Bad Request");
        }
        in.position(in.position() + 2);
        state = State.CHUNK_SIZE;
        return true;
    }

    private boolean readTrailers(Context ctx, ByteBuffer in) throws Exception {
        int eol = indexOf(in, in.position(), "\r\n");
        if (eol < 0) {
            if (in.remaining() + trailerBytes > config.maxHeaderBytes()) {
                throw new HttpException(431, "Request Header Fields Too Large");
            }
            return false;
        }
        int len = eol - in.position();
        trailerBytes += len + 2;
        if (trailerBytes > config.maxHeaderBytes()) {
            throw new HttpException(431, "Request Header Fields Too Large");
        }
        in.position(eol + 2); // trailer fields are read and discarded
        if (len == 0) {
            bodyComplete(ctx);
        }
        return true;
    }

    /** The request body is complete: buffered requests are dispatched, streamed ones report their end. */
    private void bodyComplete(Context ctx) {
        if (streamBody) {
            streamBodyDone(ctx);
        } else {
            dispatch(ctx);
        }
    }

    private void appendBody(ByteBuffer in, int n) {
        if (streamBody) {
            sb.deliver(in, n);
            return;
        }
        byte[] tmp = new byte[n];
        in.get(tmp);
        body.write(tmp, 0, n);
    }

    private static int indexOf(ByteBuffer in, int from, String needle) {
        int n = needle.length();
        outer:
        for (int i = from; i <= in.limit() - n; i++) {
            for (int j = 0; j < n; j++) {
                if (in.get(i + j) != needle.charAt(j)) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    // ---- dispatch & response ----

    private void dispatch(Context ctx) {
        HttpRequest request = new HttpRequest(method, target, version, headers, body.toByteArray());
        if (config.http2() != null && negotiatedProtocol == null) { // never over TLS: RFC 7540 section 3.2 forbids upgrading there
            byte[] settings = h2cUpgradeSettings(request);
            if (settings != null) {
                upgradeToHttp2(ctx, request, settings);
                return;
            }
        }
        boolean ka = keepAlive;
        state = State.HEAD;
        body = null;
        busy = true;
        Connection c = ctx.connection();
        c.setAutoRead(false); // do not buffer pipelined requests beyond what is already read
        CompletionStage<HttpResponse> stage;
        try {
            stage = handler.handle(request);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "handler threw for " + request.method() + " " + request.target(), t);
            finish(ctx, request, null, ka);
            return;
        }
        stage.whenComplete((resp, err) -> {
            if (err != null) {
                LOG.log(Logger.Level.WARNING, "handler failed for " + request.method() + " " + request.target(), err);
            }
            try {
                c.eventLoop().execute(() -> finish(ctx, request, err == null ? resp : null, ka));
            } catch (RuntimeException rejected) {
                // loop shut down; connection is being torn down
            }
        });
    }

    /**
     * The decoded HTTP2-Settings of a valid {@code Upgrade: h2c} request, or null if the request is not an offer we can accept
     * (RFC 7540 section 3.2: HTTP/1.1, {@code Connection: Upgrade, HTTP2-Settings}, exactly one HTTP2-Settings holding a valid SETTINGS payload).
     * An offer that does not qualify is simply served as the HTTP/1.1 request it is.
     */
    private static byte[] h2cUpgradeSettings(HttpRequest request) {
        if (!request.version().equals("HTTP/1.1")) {
            return null;
        }
        Headers h = request.headers();
        if (!h.hasToken("upgrade", "h2c") || !h.hasToken("connection", "upgrade") || !h.hasToken("connection", "http2-settings")) {
            return null;
        }
        List<String> settings = h.getAll("http2-settings");
        if (settings.size() != 1) {
            return null;
        }
        try {
            byte[] payload = Base64.getUrlDecoder().decode(settings.get(0).trim());
            return Http2ServerStage.validClientSettings(payload) ? payload : null;
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
    }

    /** Answers 101 and continues as HTTP/2; the request that asked for it is stream 1. */
    private void upgradeToHttp2(Context ctx, HttpRequest request, byte[] clientSettings) {
        endInFlight(ctx);   // from here the request is stream 1 of the HTTP/2 stage, which counts it
        state = State.HEAD;
        body = null;
        Http2ServerStage h2 = new Http2ServerStage(config, handler);
        upgraded = h2;
        updateTimers(ctx); // HTTP/1.1 deadlines no longer apply
        try {
            ctx.write(ByteBuffer.wrap("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1)));
            h2.adoptUpgrade(ctx, request, clientSettings);
            ByteBuffer rest = acc.buffer();
            if (rest.hasRemaining()) { // the client's preface may already be here
                ByteBuffer copy = ByteBuffer.allocate(rest.remaining()).put(rest).flip();
                h2.onRead(ctx, copy);
            }
        } catch (Exception e) {
            ctx.connection().close();
        }
    }

    private void finish(Context ctx, HttpRequest request, HttpResponse response, boolean ka) {
        Connection c = ctx.connection();
        if (!c.isOpen()) {
            return;
        }
        Stage switchTo = null;
        if (response != null && response.takeover() != null) {
            try {
                switchTo = response.takeover().get();
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "protocol upgrade failed for " + request.target(), t);
                response = null;
            }
        }
        if (response == null) {
            response = HttpResponse.status(500).text("Internal Server Error");
            ka = false;
        }
        if (shuttingDown) {
            ka = false; // this connection ends with the response in hand; the client is told so
        }
        recordResponse(ctx, request.method(), request.target(), request.version(), request.headers().get("user-agent"), response.status(),
                response.isStreaming() || response.status() == 101 || request.method().equals("HEAD") ? 0 : response.body().length);
        if (switchTo != null) {
            sendResponse(ctx, response, false, true);
            upgraded = switchTo;
            busy = false;
            updateTimers(ctx); // protocol switched: HTTP deadlines no longer apply
            c.setAutoRead(true);
            try {
                switchTo.onActive(ctx);
                ByteBuffer rest = acc.buffer();
                if (rest.hasRemaining()) { // bytes the client sent right behind its upgrade request
                    ByteBuffer copy = ByteBuffer.allocate(rest.remaining()).put(rest).flip();
                    switchTo.onRead(ctx, copy);
                }
            } catch (Exception e) {
                c.close();
            }
            return;
        }
        sendResponse(ctx, response, request.method().equals("HEAD"), ka);
        busy = false;
        if (!ka) {
            closing = true;
            closeAfterFlush(ctx);
            return;
        }
        c.setAutoRead(true);
        parse(ctx); // a pipelined request may already be buffered
    }

    private void fail(Context ctx, int status, String reason) {
        closing = true;
        cancelTimer();
        recordResponse(ctx, method, target, version, headers == null ? null : headers.get("user-agent"), status, reason.length());
        sendResponse(ctx, HttpResponse.status(status).text(reason), false, false);
        closeLingering(ctx);
    }

    /**
     * Closes after a rejection. The client is probably still sending (that is why we reject), and closing a socket that holds
     * unread data makes the kernel send RST, which can erase the error response before the client reads it. So: finish with
     * a FIN, read and discard for a moment, then close.
     */
    private static void closeLingering(Context ctx) {
        try {
            ctx.closeAfterFlush(Duration.ofSeconds(2));
        } catch (Exception e) {
            ctx.connection().close();
        }
    }

    private static void closeAfterFlush(Context ctx) {
        try {
            ctx.closeAfterFlush();
        } catch (Exception e) {
            ctx.connection().close();
        }
    }

    /** Writes through the pipeline (not straight to the connection) so a preceding TlsStage encrypts it. */
    private static void send(Context ctx, ByteBuffer data) {
        try {
            ctx.write(data);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Bodies at least this large are sent as their own buffer instead of being copied behind the head (the copy costs more than a second iovec). */
    private static final int SEPARATE_BODY_BYTES = 16 * 1024;

    private static void sendResponse(Context ctx, HttpResponse r, boolean head, boolean keepAlive) {
        ByteBuffer[] parts = serialize(r, head, keepAlive);
        for (ByteBuffer part : parts) {
            send(ctx, part);
        }
    }

    private static ByteBuffer[] serialize(HttpResponse r, boolean head, boolean keepAlive) {
        boolean bodyAllowed = r.status() >= 200 && r.status() != 204 && r.status() != 304;
        StringBuilder sb = new StringBuilder(256);
        sb.append("HTTP/1.1 ").append(r.status()).append(' ').append(reason(r.status())).append("\r\n");
        boolean switching = r.status() == 101;
        boolean streaming = r.isStreaming();
        r.headers().forEach((n, v) -> {
            if (!n.equalsIgnoreCase("content-length") && (streaming || !n.equalsIgnoreCase("transfer-encoding"))
                    && (switching || !n.equalsIgnoreCase("connection")) && !n.equalsIgnoreCase("date")) {
                sb.append(n).append(": ").append(v).append("\r\n");
            }
        });
        sb.append("Date: ").append(date()).append("\r\n");
        if (bodyAllowed && !streaming) {
            sb.append("Content-Length: ").append(r.body().length).append("\r\n");
        }
        if ((!keepAlive && !switching) || streaming) {
            sb.append("Connection: close\r\n"); // a stream ends when the connection does
        }
        sb.append("\r\n");
        byte[] headBytes = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
        byte[] bodyBytes = bodyAllowed && !head ? r.body() : new byte[0];
        if (bodyBytes.length >= SEPARATE_BODY_BYTES) {
            // no copy of the body: the connection only reads it, so the response's array can be wrapped as it is
            return new ByteBuffer[] {ByteBuffer.wrap(headBytes), ByteBuffer.wrap(bodyBytes)};
        }
        return new ByteBuffer[] {ByteBuffer.allocate(headBytes.length + bodyBytes.length).put(headBytes).put(bodyBytes).flip()};
    }

    /** The current time as an HTTP date (cached per second), for other protocol stages of this package. */
    static String httpDate() {
        return date();
    }

    private static volatile long cachedSecond;
    private static volatile String cachedDate = "";

    private static String date() {
        long now = System.currentTimeMillis() / 1000;
        if (now != cachedSecond) {
            cachedDate = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC));
            cachedSecond = now;
        }
        return cachedDate;
    }

    static boolean isTokenChar(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || "!#$%&'*+-.^_`|~".indexOf(c) >= 0;
    }

    private static String reason(int status) {
        return switch (status) {
            case 100 -> "Continue";
            case 200 -> "OK";
            case 201 -> "Created";
            case 202 -> "Accepted";
            case 204 -> "No Content";
            case 301 -> "Moved Permanently";
            case 302 -> "Found";
            case 304 -> "Not Modified";
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized";
            case 403 -> "Forbidden";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 408 -> "Request Timeout";
            case 409 -> "Conflict";
            case 413 -> "Payload Too Large";
            case 431 -> "Request Header Fields Too Large";
            case 500 -> "Internal Server Error";
            case 501 -> "Not Implemented";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            case 505 -> "HTTP Version Not Supported";
            default -> "Status " + status;
        };
    }
}
