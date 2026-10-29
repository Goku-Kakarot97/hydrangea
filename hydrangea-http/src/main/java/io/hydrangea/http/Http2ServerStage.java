package io.hydrangea.http;

import io.hydrangea.Connection;
import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.http2.HpackDecoder;
import io.hydrangea.http2.HpackEncoder;
import io.hydrangea.http2.HpackException;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Server side of HTTP/2 (RFC 9113) for one connection. Entered from {@link HttpServerStage} once the client preface shows up, on
 * cleartext (prior knowledge) and on TLS (ALPN {@code h2}) alike. Everything runs on the connection's event loop.
 * <p>
 * Each request stream is answered through the same {@link HttpHandler} as HTTP/1.1. A request body is buffered up to
 * {@link HttpConfig#maxBodyBytes()}, or streamed to the application when {@link HttpHandler#wantsStreamingBody} asks for it
 * (the stream's flow-control window is then the backpressure: nothing is credited while the application has paused).
 * <p>
 * A response that takes over its connection ({@link HttpResponse#stream}: Server-Sent Events, and WebSocket per RFC 8441) is given a
 * {@link StreamConnection}, so the SSE and WebSocket stages run on an HTTP/2 stream exactly as on a socket. A 101 upgrade has no
 * HTTP/2 form and answers 501.
 * <p>
 * What it enforces: frame sizes and stream state rules, both levels of flow control, the header block rules (pseudo-headers, lower
 * case names, no connection-specific fields, Content-Length against the body), bounds on the header block and the decoded header
 * list, a limit on concurrent streams, and rate limits on RST_STREAM (rapid reset) and on frames that cost work but carry nothing
 * (PING, SETTINGS, PRIORITY, empty DATA).
 */
final class Http2ServerStage implements Stage {

    private static final Logger LOG = System.getLogger(Http2ServerStage.class.getName());

    static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

    // frame types
    private static final int DATA = 0, HEADERS = 1, PRIORITY = 2, RST_STREAM = 3, SETTINGS = 4, PUSH_PROMISE = 5, PING = 6,
            GOAWAY = 7, WINDOW_UPDATE = 8, CONTINUATION = 9;
    // flags
    private static final int END_STREAM = 0x1, ACK = 0x1, END_HEADERS = 0x4, PADDED = 0x8, PRIORITY_FLAG = 0x20;
    // error codes
    private static final int NO_ERROR = 0, PROTOCOL_ERROR = 1, INTERNAL_ERROR = 2, FLOW_CONTROL_ERROR = 3, STREAM_CLOSED = 5,
            FRAME_SIZE_ERROR = 6, REFUSED_STREAM = 7, CANCEL = 8, COMPRESSION_ERROR = 9, ENHANCE_YOUR_CALM = 11;
    // settings
    private static final int S_HEADER_TABLE_SIZE = 1, S_ENABLE_PUSH = 2, S_MAX_CONCURRENT_STREAMS = 3, S_INITIAL_WINDOW_SIZE = 4,
            S_MAX_FRAME_SIZE = 5, S_MAX_HEADER_LIST_SIZE = 6, S_ENABLE_CONNECT_PROTOCOL = 8;

    private static final long MAX_WINDOW = 0x7fffffffL;
    private static final long ONE_SECOND = 1_000_000_000L;
    /** A takeover stream is not writable above this much queued output, and writable again below the low mark (like Options' watermarks). */
    private static final long HIGH_WATER = 64 * 1024, LOW_WATER = 32 * 1024;
    private static final int MAX_EARLY_TUNNEL_BYTES = 64 * 1024;

    /** How a stream that is no longer in {@link #streams} ended; decides how late frames on it are treated. */
    private enum Closed { NORMAL, RESET_BY_PEER, RESET_BY_US }

    /** One queued piece of output of a stream; {@code done} (nullable) runs once the buffer is no longer referenced. */
    private static final int COALESCE_BELOW = 4096;    // writes smaller than this are candidates for merging into one DATA frame
    private static final int GATHER_PREFIX_MAX = 16;      // a header up to this size followed by a big write is sent as gathered buffers, not copied
    private static final int COALESCE_FRAME = 16384;   // and a merged frame stays within the default frame size

    private record Out(ByteBuffer buf, Runnable done) {
    }

    private static final class Stream {
        byte[] deferredHead;      // the encoded response head, held back until the first write or the end (HttpResponse.deferHead)
        Headers extraHead;        // headers the application added meanwhile
        final int id;
        long sendWindow;
        long recvWindow;
        long recvConsumed;
        boolean remoteClosed;   // the client has sent END_STREAM
        boolean closed;
        boolean rejected;       // answered early (too large, timed out): the rest of the request is ignored
        boolean queued;
        boolean headOnly;
        long declaredLength = -1;
        long received;
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        Headers headers;
        String method;
        String path;
        String userAgent;
        long startNanos = System.nanoTime();
        boolean inFlight;               // counted in http.server.active until answered or closed
        Scheduled bodyTimer;

        // response side: output queue drained by pump() within the flow-control windows
        final ArrayDeque<Out> outQ = new ArrayDeque<>();
        volatile long outPending;   // read by Connection.pendingBytes() from any thread; written on the loop only
        boolean outEnd;         // END_STREAM follows the queued data
        boolean endSent;
        volatile boolean writable = true;   // read by Connection.isWritable() from any thread

        // extended CONNECT (WebSocket) and takeover responses (SSE, WebSocket): the stream is a Connection for a pipeline
        boolean tunnelRequest;
        Pipeline tunnel;
        StreamConnection conn;
        boolean tunnelNotified;
        ByteArrayOutputStream early;           // DATA that arrived before the application took the stream over
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        // flow control towards the application: while paused, window credit is held back
        boolean autoRead = true;
        long heldCredit;

        // streamed request body
        StreamingBody streaming;

        // the application took the whole exchange at HEADERS time (HttpHandler.wantsStream): body arrives via the tunnel, response may end in trailers
        boolean bidi;
        HalfCloseListener halfClose;
        Headers trailers;

        // what a PUSH_PROMISE for this request needs to name its origin, and whether this stream is itself a pushed one
        String scheme = "http";
        String authority;
        boolean pushed;

        Stream(int id, long sendWindow, long recvWindow) {
            this.id = id;
            this.sendWindow = sendWindow;
            this.recvWindow = recvWindow;
        }
    }

    private final HttpConfig httpConfig;
    private final Http2Config cfg;
    private final HttpHandler handler;
    private final Accumulator in = new Accumulator();
    private final HpackDecoder decoder;
    private final HpackEncoder encoder = new HpackEncoder();

    private Context ctx;
    private io.hydrangea.Metrics.Count peerResets;   // streams the client reset (RST_STREAM), looked up once
    private final Map<Integer, Stream> streams = new HashMap<>();
    private final ArrayDeque<Stream> sendQueue = new ArrayDeque<>();
    private final Map<Integer, Closed> recentlyClosed = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, Closed> eldest) {
            return size() > 1024;
        }
    };

    private boolean prefaceSeen;
    private boolean settingsSeen;
    private boolean dead;           // connection error sent or connection closed: ignore everything
    private boolean goingAway;      // graceful shutdown: no new streams
    private int lastClientStreamId;

    // the peer's settings, as far as they bind us
    private int peerMaxFrameSize = 16384;
    private long peerInitialWindow = 65535;
    private long connSendWindow = 65535;
    private boolean peerEnablePush = true;                 // SETTINGS_ENABLE_PUSH, initially 1
    private long peerMaxConcurrent = Long.MAX_VALUE;       // SETTINGS_MAX_CONCURRENT_STREAMS: bounds the streams WE open (pushes)
    private int nextPushId = 2;
    private int openPushes;
    private final java.util.LinkedHashSet<String> pushedPaths = new java.util.LinkedHashSet<>();
    private static final int MAX_PUSHES_PER_RESPONSE = 16;

    // our receive side
    private long connRecvWindow = 65535;
    private long connConsumed;

    // a header block being assembled (HEADERS + CONTINUATION*)
    private int hbStream;
    private boolean hbEndStream;
    private int hbReject;           // error to answer with once the block is decoded (the block must still go through HPACK)
    private ByteArrayOutputStream hbBuf;
    private Scheduled hbTimer;
    private final int maxHeaderBlock;

    private Scheduled idleTimer;

    // flood protection
    private long windowStart = System.nanoTime();
    private int controlFrames;
    private int resets;

    Http2ServerStage(HttpConfig httpConfig, HttpHandler handler) {
        this.httpConfig = httpConfig;
        this.cfg = httpConfig.http2();
        this.handler = handler;
        int maxList = httpConfig.maxHeaderBytes();
        this.maxHeaderBlock = Math.max(64 * 1024, 4 * maxList);
        // one string can be no longer than the whole block allows (Huffman coding expands at most 8/5); a header list over maxList is
        // answered 431 on its stream, so a long single value must not be treated as a broken connection
        this.decoder = new HpackDecoder(cfg.headerTableSize(), 2 * maxHeaderBlock, maxList);
    }

    // ---- lifecycle ----

    @Override
    public void onActive(Context ctx) {
        this.ctx = ctx;
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        putSetting(s, S_MAX_CONCURRENT_STREAMS, cfg.maxConcurrentStreams());
        putSetting(s, S_INITIAL_WINDOW_SIZE, cfg.initialWindowSize());
        putSetting(s, S_MAX_FRAME_SIZE, cfg.maxFrameSize());
        putSetting(s, S_MAX_HEADER_LIST_SIZE, httpConfig.maxHeaderBytes());
        putSetting(s, S_HEADER_TABLE_SIZE, cfg.headerTableSize());
        putSetting(s, S_ENABLE_PUSH, 0);
        putSetting(s, S_ENABLE_CONNECT_PROTOCOL, 1); // RFC 8441: WebSocket over HTTP/2
        frame(ctx, SETTINGS, 0, 0, s.toByteArray());
        if (cfg.connectionWindowSize() > 65535) {
            frame(ctx, WINDOW_UPDATE, 0, 0, int32(cfg.connectionWindowSize() - 65535));
            connRecvWindow = cfg.connectionWindowSize();
        }
        armIdle(ctx);
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        if (dead) {
            return;
        }
        in.append((ByteBuffer) msg);
        try {
            parse(ctx);
        } catch (RuntimeException e) {
            LOG.log(Logger.Level.WARNING, "http/2 handling failed", e);
            connectionError(ctx, INTERNAL_ERROR, "internal error");
        }
    }

    @Override
    public void onWritabilityChanged(Context ctx) {
        if (!dead) {
            pump(ctx);
        }
    }

    @Override
    public void onDrain(Context ctx) {
        if (dead || goingAway) {
            return;
        }
        goingAway = true;
        frame(ctx, GOAWAY, 0, 0, goaway(lastClientStreamId, NO_ERROR, ""));
        // long-lived streams (event streams, WebSockets) would hold the shutdown until its deadline: ask them to wind down
        for (Stream s : new ArrayList<>(streams.values())) {
            if (s.tunnel != null && !s.closed) {
                try {
                    s.tunnel.onDrain(s.conn);
                } catch (Exception e) {
                    LOG.log(Logger.Level.DEBUG, "stream drain failed", e);
                }
            }
        }
        if (streams.isEmpty()) {
            closeConnection(ctx, Duration.ofMillis(200));
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        dead = true;
        cancelTimers();
        abortAllStreams(ctx, cause);
        in.release();
        ctx.fireClose(cause);
    }

    /** Every stream ends with the connection: tell the applications that run on them. */
    private void abortAllStreams(Context ctx, Throwable cause) {
        List<Stream> all = new ArrayList<>(streams.values());
        streams.clear();
        sendQueue.clear();
        for (Stream s : all) {
            s.closed = true;
            endInFlight(ctx, s);
            cancel(s.bodyTimer);
            releaseOutput(s);
            if (s.streaming != null && !s.remoteClosed) {
                s.streaming.abort(cause != null ? cause : new IOException("connection closed"));
            }
            notifyTunnelClosed(s, cause);
        }
    }

    private void cancelTimers() {
        cancel(idleTimer);
        cancel(hbTimer);
        idleTimer = null;
        hbTimer = null;
    }

    private static void cancel(Scheduled t) {
        if (t != null) {
            t.cancel();
        }
    }

    // ---- frame parsing ----

    private void parse(Context ctx) {
        while (!dead) {
            ByteBuffer b = in.buffer();
            if (!prefaceSeen) {
                int n = Math.min(b.remaining(), PREFACE.length);
                for (int i = 0; i < n; i++) {
                    if (b.get(b.position() + i) != PREFACE[i]) {
                        connectionError(ctx, PROTOCOL_ERROR, "bad connection preface");
                        return;
                    }
                }
                if (b.remaining() < PREFACE.length) {
                    return;
                }
                b.position(b.position() + PREFACE.length);
                prefaceSeen = true;
                continue;
            }
            if (b.remaining() < 9) {
                return;
            }
            int p = b.position();
            int len = (b.get(p) & 0xff) << 16 | (b.get(p + 1) & 0xff) << 8 | (b.get(p + 2) & 0xff);
            int type = b.get(p + 3) & 0xff;
            int flags = b.get(p + 4) & 0xff;
            int sid = b.getInt(p + 5) & 0x7fffffff;
            if (len > cfg.maxFrameSize()) {
                connectionError(ctx, FRAME_SIZE_ERROR, "frame of " + len + " bytes exceeds " + cfg.maxFrameSize());
                return;
            }
            if (b.remaining() < 9 + len) {
                return; // wait for the rest of the frame
            }
            byte[] payload = new byte[len];
            b.position(p + 9);
            b.get(payload);
            if (!settingsSeen && !(type == SETTINGS && (flags & ACK) == 0)) {
                connectionError(ctx, PROTOCOL_ERROR, "the first frame must be SETTINGS");
                return;
            }
            if (hbStream != 0 && (type != CONTINUATION || sid != hbStream)) {
                connectionError(ctx, PROTOCOL_ERROR, "a header block is open: only CONTINUATION may follow");
                return;
            }
            switch (type) {
                case DATA -> onData(ctx, sid, flags, payload);
                case HEADERS -> onHeaders(ctx, sid, flags, payload);
                case PRIORITY -> onPriority(ctx, sid, payload);
                case RST_STREAM -> onRst(ctx, sid, payload);
                case SETTINGS -> onSettings(ctx, sid, flags, payload);
                case PUSH_PROMISE -> connectionError(ctx, PROTOCOL_ERROR, "clients must not send PUSH_PROMISE");
                case PING -> onPing(ctx, sid, flags, payload);
                case GOAWAY -> onGoaway(ctx, sid, payload);
                case WINDOW_UPDATE -> onWindowUpdate(ctx, sid, payload);
                case CONTINUATION -> onContinuation(ctx, sid, flags, payload);
                default -> { /* unknown frame types are ignored (RFC 9113 section 4.1) */ }
            }
        }
    }

    // ---- SETTINGS / PING / GOAWAY / WINDOW_UPDATE / PRIORITY / RST_STREAM ----

    private void onSettings(Context ctx, int sid, int flags, byte[] p) {
        if (sid != 0) {
            connectionError(ctx, PROTOCOL_ERROR, "SETTINGS on a stream");
            return;
        }
        if ((flags & ACK) != 0) {
            if (p.length != 0) {
                connectionError(ctx, FRAME_SIZE_ERROR, "SETTINGS ack with a payload");
            }
            return;
        }
        if (p.length % 6 != 0) {
            connectionError(ctx, FRAME_SIZE_ERROR, "SETTINGS length is not a multiple of 6");
            return;
        }
        if (floodControl(ctx)) {
            return;
        }
        settingsSeen = true;
        if (!applySettings(ctx, p)) {
            return;
        }
        frame(ctx, SETTINGS, ACK, 0, new byte[0]);
        pump(ctx);
    }

    /** Applies a client's settings (a SETTINGS payload, or the HTTP2-Settings of an upgrade). @return false after a connection error */
    private boolean applySettings(Context ctx, byte[] p) {
        for (int i = 0; i < p.length; i += 6) {
            int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
            long v = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8 | (p[i + 5] & 0xff);
            switch (id) {
                case S_ENABLE_PUSH -> {
                    if (v > 1) {
                        connectionError(ctx, PROTOCOL_ERROR, "ENABLE_PUSH must be 0 or 1");
                        return false;
                    }
                    peerEnablePush = v == 1;
                }
                case S_INITIAL_WINDOW_SIZE -> {
                    if (v > MAX_WINDOW) {
                        connectionError(ctx, FLOW_CONTROL_ERROR, "INITIAL_WINDOW_SIZE above 2^31-1");
                        return false;
                    }
                    long delta = v - peerInitialWindow;
                    peerInitialWindow = v;
                    for (Stream s : streams.values()) { // RFC 9113 section 6.9.2: open streams' windows move by the difference
                        s.sendWindow += delta;
                        if (s.sendWindow > MAX_WINDOW) {
                            connectionError(ctx, FLOW_CONTROL_ERROR, "stream window overflow");
                            return false;
                        }
                        requeue(s);
                    }
                }
                case S_MAX_FRAME_SIZE -> {
                    if (v < 16384 || v > 16_777_215) {
                        connectionError(ctx, PROTOCOL_ERROR, "MAX_FRAME_SIZE out of range");
                        return false;
                    }
                    peerMaxFrameSize = (int) v;
                }
                case S_ENABLE_CONNECT_PROTOCOL -> {
                    if (v > 1) {
                        connectionError(ctx, PROTOCOL_ERROR, "ENABLE_CONNECT_PROTOCOL must be 0 or 1");
                        return false;
                    }
                }
                case S_MAX_CONCURRENT_STREAMS -> peerMaxConcurrent = v;
                case S_HEADER_TABLE_SIZE, S_MAX_HEADER_LIST_SIZE -> {
                    // our encoder uses no dynamic table and response headers are small. Nothing to record.
                }
                default -> { /* unknown settings are ignored */ }
            }
        }
        return true;
    }

    /** True if these bytes are an acceptable SETTINGS payload (used to vet the HTTP2-Settings header before agreeing to an upgrade). */
    static boolean validClientSettings(byte[] p) {
        if (p.length % 6 != 0) {
            return false;
        }
        for (int i = 0; i < p.length; i += 6) {
            int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
            long v = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8 | (p[i + 5] & 0xff);
            boolean bad = switch (id) {
                case S_ENABLE_PUSH, S_ENABLE_CONNECT_PROTOCOL -> v > 1;
                case S_INITIAL_WINDOW_SIZE -> v > MAX_WINDOW;
                case S_MAX_FRAME_SIZE -> v < 16384 || v > 16_777_215;
                default -> false;
            };
            if (bad) {
                return false;
            }
        }
        return true;
    }

    /**
     * Enters HTTP/2 from an HTTP/1.1 "Upgrade: h2c" request (RFC 7540 section 3.2): our connection preface goes out, the client's
     * HTTP2-Settings count as its first SETTINGS, and the request itself becomes stream 1, already complete from the client's side.
     * The client's own preface and SETTINGS frame follow on the wire as usual.
     */
    void adoptUpgrade(Context ctx, HttpRequest request, byte[] clientSettings) {
        onActive(ctx);
        applySettings(ctx, clientSettings);
        Headers h = new Headers();
        request.headers().forEach((n, v) -> {
            switch (n.toLowerCase(Locale.ROOT)) {
                case "connection", "upgrade", "http2-settings", "keep-alive", "proxy-connection", "transfer-encoding", "te" -> { }
                default -> h.add(n.toLowerCase(Locale.ROOT), v);
            }
        });
        Stream s = new Stream(1, peerInitialWindow, cfg.initialWindowSize());
        s.inFlight = true;
        ctx.connection().eventLoop().metrics().recordHttpStart();
        s.remoteClosed = true;
        s.headers = h;
        s.method = request.method();
        s.path = request.target();
        s.userAgent = h.get("user-agent");
        s.headOnly = request.method().equals("HEAD");
        s.authority = h.get("host");
        s.body.write(request.body(), 0, request.body().length);
        lastClientStreamId = 1;
        streams.put(1, s);
        cancel(idleTimer);
        idleTimer = null;
        dispatch(ctx, s);
    }

    private void onPing(Context ctx, int sid, int flags, byte[] p) {
        if (sid != 0) {
            connectionError(ctx, PROTOCOL_ERROR, "PING on a stream");
        } else if (p.length != 8) {
            connectionError(ctx, FRAME_SIZE_ERROR, "PING length");
        } else if ((flags & ACK) == 0 && !floodControl(ctx)) {
            frame(ctx, PING, ACK, 0, p);
        }
    }

    private void onGoaway(Context ctx, int sid, byte[] p) {
        if (sid != 0) {
            connectionError(ctx, PROTOCOL_ERROR, "GOAWAY on a stream");
        } else if (p.length < 8) {
            connectionError(ctx, FRAME_SIZE_ERROR, "GOAWAY length");
        }
        // otherwise nothing to do: we open no streams, and the peer closes when it is done
    }

    private void onWindowUpdate(Context ctx, int sid, byte[] p) {
        if (p.length != 4) {
            connectionError(ctx, FRAME_SIZE_ERROR, "WINDOW_UPDATE length");
            return;
        }
        long inc = ((p[0] & 0x7fL) << 24) | (p[1] & 0xff) << 16 | (p[2] & 0xff) << 8 | (p[3] & 0xff);
        if (sid == 0) {
            if (inc == 0) {
                connectionError(ctx, PROTOCOL_ERROR, "zero WINDOW_UPDATE");
                return;
            }
            connSendWindow += inc;
            if (connSendWindow > MAX_WINDOW) {
                connectionError(ctx, FLOW_CONTROL_ERROR, "connection window overflow");
                return;
            }
            pump(ctx);
            return;
        }
        Stream s = streams.get(sid);
        if (s == null) {
            if (idle(sid)) {
                connectionError(ctx, PROTOCOL_ERROR, "WINDOW_UPDATE on an idle stream");
            }
            return; // closed: late window updates are harmless
        }
        if (inc == 0) {
            streamError(ctx, s.id, PROTOCOL_ERROR);
            return;
        }
        s.sendWindow += inc;
        if (s.sendWindow > MAX_WINDOW) {
            streamError(ctx, s.id, FLOW_CONTROL_ERROR);
            return;
        }
        requeue(s);
        pump(ctx);
    }

    /** A stream that was never opened: ahead of the client's last odd id, or an even id we have not promised yet. */
    private boolean idle(int sid) {
        return (sid & 1) == 1 ? sid > lastClientStreamId : sid >= nextPushId;
    }

    private void onPriority(Context ctx, int sid, byte[] p) {
        if (sid == 0) {
            connectionError(ctx, PROTOCOL_ERROR, "PRIORITY on stream 0");
        } else if (p.length != 5) {
            streamError(ctx, sid, FRAME_SIZE_ERROR);
        } else if (floodControl(ctx)) {
            // dropped
        } else if (((p[0] & 0x7f) << 24 | (p[1] & 0xff) << 16 | (p[2] & 0xff) << 8 | (p[3] & 0xff)) == sid) {
            streamError(ctx, sid, PROTOCOL_ERROR); // a stream cannot depend on itself
        }
        // priorities themselves are deprecated (RFC 9113 section 5.3) and not used
    }

    private void onRst(Context ctx, int sid, byte[] p) {
        if (sid == 0) {
            connectionError(ctx, PROTOCOL_ERROR, "RST_STREAM on stream 0");
        } else if (p.length != 4) {
            connectionError(ctx, FRAME_SIZE_ERROR, "RST_STREAM length");
        } else if (idle(sid)) {
            connectionError(ctx, PROTOCOL_ERROR, "RST_STREAM on an idle stream");
        } else if (rapidReset(ctx)) {
            // connection dropped
        } else {
            Stream s = streams.get(sid);
            if (s != null) {
                closeStream(ctx, s, Closed.RESET_BY_PEER);
            }
        }
    }

    // ---- DATA ----

    private void onData(Context ctx, int sid, int flags, byte[] p) {
        if (sid == 0) {
            connectionError(ctx, PROTOCOL_ERROR, "DATA on stream 0");
            return;
        }
        int off = 0;
        int end = p.length;
        if ((flags & PADDED) != 0) {
            if (p.length < 1) {
                connectionError(ctx, FRAME_SIZE_ERROR, "padded DATA without a pad length");
                return;
            }
            int pad = p[0] & 0xff;
            if (pad >= p.length) {
                connectionError(ctx, PROTOCOL_ERROR, "padding longer than the frame");
                return;
            }
            off = 1;
            end = p.length - pad;
        }
        // flow control counts the whole payload, padding included, even when the frame is then rejected
        if (p.length > connRecvWindow) {
            connectionError(ctx, FLOW_CONTROL_ERROR, "DATA beyond the connection window");
            return;
        }
        connRecvWindow -= p.length;
        connConsumed += p.length;
        if (connConsumed >= cfg.connectionWindowSize() / 2) {
            frame(ctx, WINDOW_UPDATE, 0, 0, int32((int) connConsumed));
            connRecvWindow += connConsumed;
            connConsumed = 0;
        }
        if (idle(sid)) {
            connectionError(ctx, PROTOCOL_ERROR, "DATA on an idle stream");
            return;
        }
        Stream s = streams.get(sid);
        if (s == null) {
            if (recentlyClosed.get(sid) != Closed.RESET_BY_US) {
                sendRst(ctx, sid, STREAM_CLOSED);
            }
            return;
        }
        if (s.rejected) {
            return; // answered already; the reset that finishes the response is on its way
        }
        if (s.remoteClosed) {
            streamError(ctx, sid, STREAM_CLOSED);
            return;
        }
        if (p.length > s.recvWindow) {
            streamError(ctx, sid, FLOW_CONTROL_ERROR);
            return;
        }
        s.recvWindow -= p.length;
        boolean last = (flags & END_STREAM) != 0;
        int n = end - off;
        if (n == 0 && !last && floodControl(ctx)) {
            return;
        }
        s.received += n;
        if (s.declaredLength >= 0 && s.received > s.declaredLength) {
            streamError(ctx, sid, PROTOCOL_ERROR); // more body than Content-Length announced
            return;
        }
        if (s.tunnel != null) {
            try {
                if (n > 0) {
                    s.tunnel.onRead(s.conn, ByteBuffer.wrap(p, off, n).slice());
                }
            } catch (Exception e) {
                LOG.log(Logger.Level.WARNING, "stream " + sid + " failed while handling data", e);
                streamError(ctx, sid, INTERNAL_ERROR);
                return;
            }
            if (!s.closed) {
                creditStream(ctx, s, p.length, last);
            }
        } else if (s.tunnelRequest || s.bidi) {
            if (s.early == null) {
                s.early = new ByteArrayOutputStream();
            }
            if (s.early.size() + n > MAX_EARLY_TUNNEL_BYTES) {
                streamError(ctx, sid, ENHANCE_YOUR_CALM);
                return;
            }
            s.early.write(p, off, n);
            creditStream(ctx, s, p.length, last);
        } else if (s.streaming != null) {
            s.streaming.deliver(p, off, n);
            if (!s.closed) {
                creditStream(ctx, s, p.length, last);
            }
        } else {
            if (s.received > httpConfig.maxBodyBytes()) {
                rejectStream(ctx, s, 413, "Payload Too Large");
                return;
            }
            s.body.write(p, off, n);
            creditStream(ctx, s, p.length, last);
        }
        if (last && !s.closed) {
            s.remoteClosed = true;
            cancel(s.bodyTimer);
            if (s.declaredLength >= 0 && s.received != s.declaredLength) {
                streamError(ctx, sid, PROTOCOL_ERROR);
                return;
            }
            if (s.tunnelRequest || s.bidi) {
                peerFinishedTunnel(ctx, s);
            } else if (s.streaming != null) {
                s.streaming.complete();
            } else {
                dispatch(ctx, s);
            }
        }
    }

    /** Gives the client back window for bytes the application has taken (held back while the application is paused). */
    private void creditStream(Context ctx, Stream s, int bytes, boolean last) {
        s.recvConsumed += bytes;
        if (!s.autoRead) {
            s.heldCredit = s.recvConsumed;
            return;
        }
        if (last || s.recvConsumed < cfg.initialWindowSize() / 2) {
            return;
        }
        frame(ctx, WINDOW_UPDATE, 0, s.id, int32((int) s.recvConsumed));
        s.recvWindow += s.recvConsumed;
        s.recvConsumed = 0;
    }

    // ---- HEADERS / CONTINUATION ----

    private void onHeaders(Context ctx, int sid, int flags, byte[] p) {
        if (sid == 0 || (sid & 1) == 0) {
            connectionError(ctx, PROTOCOL_ERROR, "HEADERS on stream " + sid);
            return;
        }
        int off = 0;
        int end = p.length;
        if ((flags & PADDED) != 0) {
            if (p.length < 1) {
                connectionError(ctx, FRAME_SIZE_ERROR, "padded HEADERS without a pad length");
                return;
            }
            int pad = p[0] & 0xff;
            off = 1;
            end = p.length - pad;
            if (end < off) {
                connectionError(ctx, PROTOCOL_ERROR, "padding longer than the frame");
                return;
            }
        }
        int reject = 0;
        if ((flags & PRIORITY_FLAG) != 0) {
            if (end - off < 5) {
                connectionError(ctx, FRAME_SIZE_ERROR, "HEADERS priority fields truncated");
                return;
            }
            int dep = (p[off] & 0x7f) << 24 | (p[off + 1] & 0xff) << 16 | (p[off + 2] & 0xff) << 8 | (p[off + 3] & 0xff);
            if (dep == sid) {
                reject = PROTOCOL_ERROR; // depends on itself; the block still has to be decoded
            }
            off += 5;
        }
        Stream existing = streams.get(sid);
        if (existing != null) {
            if (existing.remoteClosed) {
                reject = STREAM_CLOSED;
            } else if ((flags & END_STREAM) == 0) {
                reject = PROTOCOL_ERROR; // trailers must end the stream
            }
        } else if (sid <= lastClientStreamId) {
            connectionError(ctx, recentlyClosed.containsKey(sid) ? STREAM_CLOSED : PROTOCOL_ERROR, "HEADERS on a stream that is not new");
            return;
        } else {
            lastClientStreamId = sid;
            if (goingAway || streams.size() >= cfg.maxConcurrentStreams()) {
                reject = REFUSED_STREAM;
            }
        }
        cancel(idleTimer);
        idleTimer = null;
        hbStream = sid;
        hbEndStream = (flags & END_STREAM) != 0;
        hbReject = reject;
        hbBuf = new ByteArrayOutputStream(Math.max(64, end - off));
        hbBuf.write(p, off, end - off);
        if ((flags & END_HEADERS) != 0) {
            completeHeaderBlock(ctx);
        } else {
            Duration t = httpConfig.headerTimeout();
            if (t != null) {
                hbTimer = ctx.connection().eventLoop().schedule(() -> connectionError(ctx, ENHANCE_YOUR_CALM, "header block not completed in time"),
                        t.toNanos(), TimeUnit.NANOSECONDS);
            }
        }
    }

    private void onContinuation(Context ctx, int sid, int flags, byte[] p) {
        if (hbStream == 0 || sid != hbStream) {
            connectionError(ctx, PROTOCOL_ERROR, "unexpected CONTINUATION");
            return;
        }
        if (hbBuf.size() + p.length > maxHeaderBlock) {
            connectionError(ctx, ENHANCE_YOUR_CALM, "header block too large");
            return;
        }
        hbBuf.write(p, 0, p.length);
        if ((flags & END_HEADERS) != 0) {
            completeHeaderBlock(ctx);
        }
    }

    private void completeHeaderBlock(Context ctx) {
        int sid = hbStream;
        boolean endStream = hbEndStream;
        int reject = hbReject;
        byte[] block = hbBuf.toByteArray();
        hbStream = 0;
        hbBuf = null;
        cancel(hbTimer);
        hbTimer = null;

        List<String[]> headers = new ArrayList<>();
        try {
            decoder.decode(block, (n, v) -> headers.add(new String[]{n, v}));
        } catch (HpackException e) {
            connectionError(ctx, COMPRESSION_ERROR, e.getMessage());
            return;
        }
        Stream existing = streams.get(sid);
        if (reject != 0) {
            if (existing != null) {
                streamError(ctx, sid, reject);
            } else {
                sendRst(ctx, sid, reject);
                recentlyClosed.put(sid, Closed.RESET_BY_US);
                armIdleIfQuiet(ctx);
            }
            return;
        }
        boolean tooLarge = decoder.lastBlockTooLarge();
        if (existing != null && existing.rejected) {
            return;
        }
        if (existing != null) { // trailers
            if (tooLarge || !validTrailers(headers)) {
                streamError(ctx, sid, PROTOCOL_ERROR);
                return;
            }
            existing.remoteClosed = true;
            cancel(existing.bodyTimer);
            if (existing.declaredLength >= 0 && existing.received != existing.declaredLength) {
                streamError(ctx, sid, PROTOCOL_ERROR);
                return;
            }
            if (existing.tunnelRequest || existing.bidi) {
                peerFinishedTunnel(ctx, existing);
            } else if (existing.streaming != null) {
                existing.streaming.complete();
            } else {
                dispatch(ctx, existing);
            }
            return;
        }
        Stream s = new Stream(sid, peerInitialWindow, cfg.initialWindowSize());
        s.inFlight = true;
        ctx.connection().eventLoop().metrics().recordHttpStart();
        streams.put(sid, s);
        cancel(idleTimer);
        idleTimer = null;
        if (tooLarge) {
            s.method = "-";
            s.path = "-";
            s.remoteClosed = endStream;
            rejectStream(ctx, s, 431, "Request Header Fields Too Large");
            return;
        }
        if (!buildRequestHead(s, headers)) {
            streamError(ctx, sid, PROTOCOL_ERROR);
            return;
        }
        if (s.tunnelRequest) {
            // extended CONNECT (RFC 8441): there is no request body to wait for; the application decides now whether to take the stream over
            s.remoteClosed = endStream; // a tunnel the client already closed again is answered and reset
            dispatch(ctx, s);
            return;
        }
        if (wantsStream(s)) {
            s.bidi = true;            // gRPC-style exchange: the application takes the stream now and the body comes to it as it arrives
            s.remoteClosed = endStream;
            dispatch(ctx, s);
            return;
        }
        if (endStream) {
            s.remoteClosed = true;
            if (s.declaredLength > 0) {
                streamError(ctx, sid, PROTOCOL_ERROR); // announced a body, sent none
                return;
            }
            dispatch(ctx, s);
            return;
        }
        if (startStreamingIfWanted(ctx, s)) {
            return;
        }
        Duration bt = httpConfig.bodyTimeout();
        if (bt != null) {
            s.bodyTimer = ctx.connection().eventLoop().schedule(() -> {
                if (!s.closed && !s.remoteClosed) {
                    rejectStream(ctx, s, 408, "Request Timeout");
                }
            }, bt.toNanos(), TimeUnit.NANOSECONDS);
        }
        if (s.headers.hasToken("expect", "100-continue")) {
            if (s.declaredLength > httpConfig.maxBodyBytes()) {
                rejectStream(ctx, s, 413, "Payload Too Large");
            } else {
                encoder.reset();
                encoder.header(":status", "100");
                headerFrames(ctx, sid, encoder.toByteArray(), false);
            }
        }
    }

    /** Validates a request header block (RFC 9113 section 8.2 and 8.3, RFC 8441) and fills in the stream. @return false when malformed */
    private boolean buildRequestHead(Stream s, List<String[]> list) {
        String method = null, scheme = null, authority = null, path = null, protocol = null;
        boolean pseudoAllowed = true;
        Headers h = new Headers();
        List<String> cookies = null;
        long contentLength = -1;
        for (String[] e : list) {
            String name = e[0];
            String value = e[1];
            if (name.isEmpty() || !validFieldValue(value)) {
                return false;
            }
            if (name.charAt(0) == ':') {
                if (!pseudoAllowed) {
                    return false;
                }
                switch (name) {
                    case ":method" -> {
                        if (method != null) return false;
                        method = value;
                    }
                    case ":scheme" -> {
                        if (scheme != null) return false;
                        scheme = value;
                    }
                    case ":authority" -> {
                        if (authority != null) return false;
                        authority = value;
                    }
                    case ":path" -> {
                        if (path != null) return false;
                        path = value;
                    }
                    case ":protocol" -> {
                        if (protocol != null) return false;
                        protocol = value;
                    }
                    default -> {
                        return false; // unknown pseudo-header, or :status in a request
                    }
                }
                continue;
            }
            pseudoAllowed = false;
            if (!validFieldName(name)) {
                return false;
            }
            switch (name) {
                case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade" -> {
                    return false;
                }
                case "te" -> {
                    if (!value.trim().equalsIgnoreCase("trailers")) {
                        return false;
                    }
                }
                case "content-length" -> {
                    long n = parseLength(value);
                    if (n < 0 || (contentLength >= 0 && contentLength != n)) {
                        return false;
                    }
                    contentLength = n;
                }
                case "cookie" -> {
                    if (cookies == null) cookies = new ArrayList<>();
                    cookies.add(value);
                    continue;
                }
                default -> { }
            }
            h.add(name, value);
        }
        if (method == null || method.isEmpty()) {
            return false;
        }
        for (int i = 0; i < method.length(); i++) {
            if (!HttpServerStage.isTokenChar(method.charAt(i))) {
                return false;
            }
        }
        if (method.equals("CONNECT")) {
            // only the extended form of RFC 8441 (":protocol") is offered; it needs every other pseudo-header too
            if (protocol == null || protocol.isEmpty() || scheme == null || path == null || path.isEmpty() || authority == null) {
                return false;
            }
            s.tunnelRequest = true;
            method = "GET";               // the route sees an ordinary GET; the marker below tells it this is a tunnel request
            h.add(":protocol", protocol); // can only be here if it arrived as a validated pseudo-header of a CONNECT
        } else if (protocol != null) {
            return false;                 // :protocol is only defined for CONNECT
        } else if (scheme == null || scheme.isEmpty() || path == null || path.isEmpty()) {
            return false;
        }
        if (cookies != null) {
            h.add("cookie", String.join("; ", cookies)); // RFC 9113 section 8.2.3
        }
        if (authority != null && !h.contains("host")) {
            h.add("host", authority);
        }
        s.headers = h;
        s.method = method;
        s.path = path;
        s.userAgent = h.get("user-agent");
        s.declaredLength = contentLength;
        s.headOnly = method.equals("HEAD");
        s.scheme = scheme == null ? "http" : scheme;
        s.authority = authority != null ? authority : h.get("host");
        return true;
    }

    private static boolean validTrailers(List<String[]> list) {
        for (String[] e : list) {
            String name = e[0];
            if (name.isEmpty() || name.charAt(0) == ':' || !validFieldName(name) || !validFieldValue(e[1])) {
                return false;
            }
            switch (name) {
                case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "te", "content-length" -> {
                    return false;
                }
                default -> { }
            }
        }
        return true;
    }

    private static boolean validFieldName(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c >= 'A' && c <= 'Z' || !HttpServerStage.isTokenChar(c)) {
                return false; // upper case names are malformed in HTTP/2 (RFC 9113 section 8.2.1)
            }
        }
        return true;
    }

    private static boolean validFieldValue(String v) {
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == 0 || c == '\r' || c == '\n') {
                return false;
            }
        }
        return true;
    }

    private static long parseLength(String v) {
        if (v.isEmpty() || v.length() > 18) {
            return -1;
        }
        long n = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            n = n * 10 + (c - '0');
        }
        return n;
    }

    // ---- running the application ----

    private void dispatch(Context ctx, Stream s) {
        HttpRequest request = new HttpRequest(s.method, s.path, "HTTP/2", s.headers, s.body.toByteArray());
        s.body = null;
        CompletionStage<HttpResponse> result;
        try {
            result = handler.handle(request);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "handler failed for " + s.path, t);
            result = CompletableFuture.completedFuture(HttpResponse.status(500).text("Internal Server Error"));
        }
        deliverWhenDone(ctx, s, result);
    }

    private void deliverWhenDone(Context ctx, Stream s, CompletionStage<HttpResponse> result) {
        EventLoop loop = ctx.connection().eventLoop();
        result.whenComplete((resp, err) -> {
            Runnable deliver = () -> {
                if (dead || s.closed) {
                    return; // the client reset the stream or the connection is gone
                }
                HttpResponse r = resp;
                if (err != null || r == null) {
                    Throwable cause = err instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null ? ce.getCause() : err;
                    if (cause != null) {
                        LOG.log(Logger.Level.WARNING, "handler failed for " + s.path, cause);
                    }
                    r = HttpResponse.status(500).text("Internal Server Error");
                }
                respond(ctx, s, r);
            };
            if (loop.inEventLoop()) {
                deliver.run();
            } else {
                try {
                    loop.execute(deliver);
                } catch (RuntimeException closed) {
                    // the loop is shutting down; the connection goes with it
                }
            }
        });
    }

    /** Answers a request that cannot be served before it has been fully received; finishing the response then resets the stream. */
    private void rejectStream(Context ctx, Stream s, int status, String text) {
        if (s.rejected || s.closed) {
            return;
        }
        s.rejected = true;
        cancel(s.bodyTimer);
        s.body = null;
        respond(ctx, s, HttpResponse.status(status).text(text));
    }

    private void respond(Context ctx, Stream s, HttpResponse r) {
        boolean takeover = r.takeover() != null && r.isStreaming();
        if (r.takeover() != null && !takeover) {
            r = HttpResponse.status(501).text("HTTP/2 has no 101 Switching Protocols (RFC 9113 section 8.6); use an extended CONNECT (RFC 8441) for WebSocket");
        } else if (s.tunnelRequest && !takeover && r.status() >= 200 && r.status() < 300) {
            r = HttpResponse.status(400).text("Not a tunnel endpoint"); // a 2xx to a CONNECT would claim a tunnel that does not exist
        }
        int status = r.status();
        boolean bodyAllowed = status >= 200 && status != 204 && status != 304;
        byte[] body = bodyAllowed && !takeover ? r.body() : new byte[0];
        ctx.connection().eventLoop().metrics().recordHttpResponse(status, s.startNanos);
        endInFlight(ctx, s);
        AccessLog log = httpConfig.accessLog();
        if (log != null) {
            try {
                log.log(new AccessLog.Entry(ctx.connection().remoteAddress(), s.method, s.path, "HTTP/2", status, s.headOnly ? 0 : body.length,
                        Duration.ofNanos(System.nanoTime() - s.startNanos), s.userAgent));
            } catch (Throwable ignored) {
                // a failing logger must not break request handling
            }
        }
        List<Stream> promised = takeover || r.pushes().isEmpty() ? List.of() : sendPromises(ctx, s, r);
        encoder.reset();
        encoder.header(":status", Integer.toString(status));
        r.headers().forEach((n, v) -> {
            String name = n.toLowerCase(Locale.ROOT);
            switch (name) {
                case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "content-length", "date", "host" -> { }
                default -> encoder.header(name, v);
            }
        });
        encoder.header("date", HttpServerStage.httpDate());
        if (bodyAllowed && !takeover) {
            encoder.header("content-length", Integer.toString(body.length));
        }
        if (takeover) {
            if (r.isHeadDeferred()) {
                s.deferredHead = encoder.toByteArray();
            } else {
                headerFrames(ctx, s.id, encoder.toByteArray(), false);
            }
            installTunnel(ctx, s, r);
            return;
        }
        boolean noData = s.headOnly || body.length == 0 || !bodyAllowed;
        headerFrames(ctx, s.id, encoder.toByteArray(), noData);
        for (Stream ps : promised) { // the promised requests run now that the response they belong to is on its way
            if (!ps.closed) {
                dispatch(ctx, ps);
            }
        }
        if (noData) {
            s.endSent = true;
            finishLocal(ctx, s);
        } else {
            s.outQ.add(new Out(ByteBuffer.wrap(body), null));
            s.outPending = body.length;
            s.outEnd = true;
            requeue(s);
            pump(ctx);
        }
    }

    // ---- server push (RFC 9113 section 8.4) ----

    /**
     * Sends a PUSH_PROMISE for each resource {@code r} offers, within what the client allows, and returns the promised streams (reserved,
     * not yet answered). A client that disabled push, a full stream limit, a repeated path or a response that is not a success mean no promise.
     */
    private List<Stream> sendPromises(Context ctx, Stream parent, HttpResponse r) {
        if (!peerEnablePush || goingAway || parent.pushed || parent.tunnelRequest || parent.authority == null || (parent.id & 1) == 0
                || r.status() < 200 || r.status() >= 300) {
            return List.of();
        }
        List<Stream> out = new ArrayList<>();
        for (HttpResponse.Push push : r.pushes()) {
            if (out.size() >= MAX_PUSHES_PER_RESPONSE || openPushes >= peerMaxConcurrent || nextPushId < 0) {
                break;
            }
            if (!pushedPaths.add(push.path())) {
                continue; // promised before on this connection
            }
            if (pushedPaths.size() > 1024) {
                var it = pushedPaths.iterator();
                it.next();
                it.remove();
            }
            Headers rh = new Headers();
            if (push.requestHeaders() != null) {
                push.requestHeaders().forEach((n, v) -> rh.add(n.toLowerCase(Locale.ROOT), v));
            } else {
                for (String name : new String[]{"accept-encoding", "accept-language", "user-agent", "cookie"}) {
                    for (String v : parent.headers.getAll(name)) {
                        rh.add(name, v);
                    }
                }
            }
            int id = nextPushId;
            nextPushId = id + 2 < 0 ? -1 : id + 2;
            encoder.reset();
            encoder.header(":method", "GET");
            encoder.header(":scheme", parent.scheme);
            encoder.header(":authority", parent.authority);
            encoder.header(":path", push.path());
            rh.forEach((n, v) -> {
                switch (n) {
                    case "host", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "content-length", "te" -> { }
                    default -> encoder.header(n, v);
                }
            });
            pushPromiseFrames(ctx, parent.id, id, encoder.toByteArray());
            Stream ps = new Stream(id, peerInitialWindow, cfg.initialWindowSize());
            ps.pushed = true;
            ps.remoteClosed = true; // nothing will come from the client on a pushed stream
            ps.method = "GET";
            ps.path = push.path();
            ps.scheme = parent.scheme;
            ps.authority = parent.authority;
            rh.set("host", parent.authority);
            ps.headers = rh;
            ps.userAgent = rh.get("user-agent");
            streams.put(id, ps);
            openPushes++;
            out.add(ps);
        }
        return out;
    }

    /** PUSH_PROMISE on the associated stream, continued with CONTINUATION frames if the header block is large. */
    private void pushPromiseFrames(Context ctx, int associated, int promisedId, byte[] block) {
        int max = peerMaxFrameSize;
        int firstLen = Math.min(block.length, max - 4);
        byte[] first = new byte[4 + firstLen];
        System.arraycopy(int32(promisedId), 0, first, 0, 4);
        System.arraycopy(block, 0, first, 4, firstLen);
        frame(ctx, PUSH_PROMISE, firstLen == block.length ? END_HEADERS : 0, associated, first);
        int pos = firstLen;
        while (pos < block.length) {
            int n = Math.min(max, block.length - pos);
            frame(ctx, CONTINUATION, pos + n == block.length ? END_HEADERS : 0, associated, java.util.Arrays.copyOfRange(block, pos, pos + n));
            pos += n;
        }
    }

    // ---- takeover: a stream as a Connection (SSE, WebSocket) ----

    private void installTunnel(Context ctx, Stream s, HttpResponse r) {
        try {
            Stage stage = r.takeover().get();
            s.conn = new StreamConnection(ctx, s);
            s.tunnel = Pipeline.of(stage);
            s.tunnel.onActive(s.conn);
            if (s.early != null) {
                byte[] early = s.early.toByteArray();
                s.early = null;
                if (early.length > 0 && !s.closed) {
                    s.tunnel.onRead(s.conn, ByteBuffer.wrap(early));
                }
            }
            if (stage instanceof HalfCloseListener l) {
                s.halfClose = l;
            }
            if (s.remoteClosed && (s.tunnelRequest || s.bidi) && !s.closed) {
                peerFinishedTunnel(ctx, s);
            }
        } catch (Exception e) {
            LOG.log(Logger.Level.WARNING, "taking over stream " + s.id + " failed", e);
            streamError(ctx, s.id, INTERNAL_ERROR);
        }
    }

    /** The client ended its side of a tunnel: to the application that is the connection closing; ours follows once what is queued is sent. */
    private void peerFinishedTunnel(Context ctx, Stream s) {
        if (s.closed || s.tunnel == null) {
            return; // not taken over yet: installTunnel looks at remoteClosed when it is
        }
        if (s.halfClose != null) { // a request/response protocol: the client is done talking, the response is still to come
            try {
                s.halfClose.onPeerHalfClose();
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "half-close callback failed for stream " + s.id, t);
                streamError(ctx, s.id, INTERNAL_ERROR);
            }
            return;
        }
        notifyTunnelClosed(s, null);
        s.outEnd = true;
        requeue(s);
        pump(ctx);
    }

    private void notifyTunnelClosed(Stream s, Throwable cause) {
        if (s.tunnel == null || s.tunnelNotified) {
            return;
        }
        s.tunnelNotified = true;
        try {
            s.tunnel.onClose(s.conn, cause);
        } catch (Throwable t) {
            LOG.log(Logger.Level.DEBUG, "stream close callback failed", t);
        } finally {
            s.closeFuture.complete(null);
        }
    }

    /** One HTTP/2 stream presented as a {@link Connection}: what the SSE and WebSocket stages write to and read from. */
    private final class StreamConnection implements Http2Stream {
        private final Context ctx;
        private final Stream s;

        StreamConnection(Context ctx, Stream s) {
            this.ctx = ctx;
            this.s = s;
        }

        @Override
        public EventLoop eventLoop() {
            return ctx.connection().eventLoop();
        }

        @Override
        public SocketAddress remoteAddress() {
            return ctx.connection().remoteAddress();
        }

        @Override
        public SocketAddress localAddress() {
            return ctx.connection().localAddress();
        }

        @Override
        public int streamId() {
            return s.id;
        }

        @Override
        public Headers requestHeaders() {
            return s.headers;
        }

        @Override
        public void addResponseHeaders(Headers headers) {
            Headers copy = headers.copy();
            runInLoop(() -> {
                if (s.closed || s.deferredHead == null) {
                    return;
                }
                if (s.extraHead == null) {
                    s.extraHead = new Headers();
                }
                copy.forEach((n, v) -> s.extraHead.add(n, v));
            });
        }

        @Override
        public void sendTrailers(Headers trailers) {
            Headers copy = trailers.copy();
            runInLoop(() -> {
                if (s.closed || s.outEnd) {
                    return;
                }
                s.trailers = copy;
                s.outEnd = true;
                requeue(s);
                pump(ctx);
            });
        }

        @Override
        public boolean isOpen() {
            return !s.closed && !dead;
        }

        @Override
        public boolean isWritable() {
            return s.writable;
        }

        @Override
        public long pendingBytes() {
            return s.outPending;
        }

        @Override
        public CompletableFuture<Void> closeFuture() {
            return s.closeFuture;
        }

        @Override
        public void write(ByteBuffer data) {
            write(data, null);
        }

        @Override
        public void write(ByteBuffer data, Runnable done) {
            runInLoop(() -> {
                if (s.closed || s.outEnd || data.remaining() == 0) {
                    runDone(done);
                    return;
                }
                s.outQ.add(new Out(data, done));
                s.outPending += data.remaining();
                if (s.writable && s.outPending > HIGH_WATER) {
                    s.writable = false;
                }
                requeue(s);
                pump(ctx);
            });
        }

        @Override
        public void setAutoRead(boolean enabled) {
            runInLoop(() -> {
                if (s.closed || s.autoRead == enabled) {
                    return;
                }
                s.autoRead = enabled;
                if (enabled && s.heldCredit > 0 && !s.remoteClosed) {
                    frame(ctx, WINDOW_UPDATE, 0, s.id, int32((int) s.heldCredit));
                    s.recvWindow += s.heldCredit;
                    s.recvConsumed = 0;
                    s.heldCredit = 0;
                }
            });
        }

        @Override
        public void close() {
            runInLoop(() -> {
                if (s.closed) {
                    return;
                }
                sendRst(ctx, s.id, CANCEL);
                closeStream(ctx, s, Closed.RESET_BY_US);
            });
        }

        @Override
        public void closeAfterFlush() {
            runInLoop(() -> {
                if (s.closed || s.outEnd) {
                    return;
                }
                s.outEnd = true;
                requeue(s);
                pump(ctx);
            });
        }

        @Override
        public void closeAfterFlush(Duration linger) {
            closeAfterFlush(); // nothing to linger for: the stream has its own end marker
        }

        private void runInLoop(Runnable r) {
            EventLoop loop = eventLoop();
            if (loop.inEventLoop()) {
                r.run();
            } else {
                try {
                    loop.execute(r);
                } catch (RuntimeException shutDown) {
                    // the connection is going away
                }
            }
        }

        @Override
        public String toString() {
            return "Http2Stream[" + s.id + " on " + remoteAddress() + "]";
        }
    }

    private static void runDone(Runnable done) {
        if (done != null) {
            try {
                done.run();
            } catch (Throwable ignored) {
                // a failing completion callback must not break the connection
            }
        }
    }

    private static void releaseOutput(Stream s) {
        for (Out o : s.outQ) {
            runDone(o.done());
        }
        s.outQ.clear();
        s.outPending = 0;
    }

    // ---- streamed request bodies ----

    private boolean wantsStream(Stream s) {
        try {
            return handler.wantsStream(new HttpRequest(s.method, s.path, "HTTP/2", s.headers, new byte[0]));
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "wantsStream failed for " + s.path, t);
            return false;
        }
    }

    private boolean startStreamingIfWanted(Context ctx, Stream s) {
        HttpRequest head = new HttpRequest(s.method, s.path, "HTTP/2", s.headers, new byte[0]);
        boolean wanted;
        try {
            wanted = handler.wantsStreamingBody(head);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "wantsStreamingBody failed for " + s.path, t);
            wanted = false;
        }
        if (!wanted) {
            return false;
        }
        StreamingBody sb = new StreamingBody(ctx, s);
        s.streaming = sb;
        CompletionStage<HttpResponse> result;
        try {
            result = handler.handleStreaming(head, sb);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "streaming handler failed for " + s.path, t);
            result = CompletableFuture.completedFuture(HttpResponse.status(500).text("Internal Server Error"));
        }
        sb.registrationClosed = true;
        if (s.headers.hasToken("expect", "100-continue")) {
            encoder.reset();
            encoder.header(":status", "100");
            headerFrames(ctx, s.id, encoder.toByteArray(), false);
        }
        deliverWhenDone(ctx, s, result);
        return true;
    }

    /** The request body of a stream, handed to the application piece by piece. Window credit is the backpressure. */
    private final class StreamingBody implements RequestBodyStream, BodyControl {
        private final Context ctx;
        private final Stream s;
        private BodyListener listener;
        boolean registrationClosed;
        private boolean ended;
        private ByteArrayOutputStream held;  // data that arrived while paused (at most one flow-control window: nothing is credited meanwhile)
        private boolean completePending;

        StreamingBody(Context ctx, Stream s) {
            this.ctx = ctx;
            this.s = s;
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
            return s.declaredLength;
        }

        void deliver(byte[] p, int off, int n) {
            if (listener == null || ended || n == 0) {
                return;
            }
            if (!s.autoRead) { // paused: no onData until resume() (BodyControl's contract)
                if (held == null) {
                    held = new ByteArrayOutputStream();
                }
                held.write(p, off, n);
                return;
            }
            try {
                listener.onData(ByteBuffer.wrap(p, off, n).asReadOnlyBuffer(), this);
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "request body listener failed for " + s.path, t);
                ended = true;
                streamError(ctx, s.id, INTERNAL_ERROR);
            }
        }

        void complete() {
            if (ended) {
                return;
            }
            if (!s.autoRead) {
                completePending = true; // the end of the body waits behind the data that is held
                return;
            }
            ended = true;
            if (listener != null) {
                try {
                    listener.onComplete();
                } catch (Throwable t) {
                    LOG.log(Logger.Level.WARNING, "request body listener failed in onComplete", t);
                    streamError(ctx, s.id, INTERNAL_ERROR);
                }
            }
        }

        /** The body will never complete: the stream was reset, ended by our response, or the connection died. */
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

        @Override
        public void pause() {
            onLoop(() -> {
                if (!ended && !s.closed) {
                    s.autoRead = false;
                }
            });
        }

        @Override
        public void resume() {
            onLoop(() -> {
                if (s.autoRead || s.closed) {
                    return;
                }
                s.autoRead = true;
                if (held != null && held.size() > 0 && !ended) {
                    byte[] data = held.toByteArray();
                    held = null;
                    deliver(data, 0, data.length);
                }
                if (s.closed) {
                    return;
                }
                if (s.heldCredit > 0 && !s.remoteClosed) {
                    frame(ctx, WINDOW_UPDATE, 0, s.id, int32((int) s.heldCredit));
                    s.recvWindow += s.heldCredit;
                    s.recvConsumed = 0;
                    s.heldCredit = 0;
                }
                if (completePending && s.autoRead) {
                    completePending = false;
                    complete();
                }
            });
        }

        @Override
        public void cancel() {
            onLoop(() -> {
                if (!s.closed) {
                    abort(new CancellationException("request body cancelled"));
                    streamError(ctx, s.id, CANCEL);
                }
            });
        }

        private void onLoop(Runnable r) {
            EventLoop loop = ctx.connection().eventLoop();
            if (loop.inEventLoop()) {
                r.run();
            } else {
                try {
                    loop.execute(r);
                } catch (RuntimeException shutDown) {
                    // the connection is going away
                }
            }
        }
    }

    // ---- sending ----

    private void headerFrames(Context ctx, int sid, byte[] block, boolean endStream) {
        int max = peerMaxFrameSize;
        if (block.length <= max) {
            frame(ctx, HEADERS, END_HEADERS | (endStream ? END_STREAM : 0), sid, block);
            return;
        }
        frame(ctx, HEADERS, endStream ? END_STREAM : 0, sid, java.util.Arrays.copyOfRange(block, 0, max));
        int pos = max;
        while (pos < block.length) {
            int n = Math.min(max, block.length - pos);
            boolean lastPart = pos + n == block.length;
            frame(ctx, CONTINUATION, lastPart ? END_HEADERS : 0, sid, java.util.Arrays.copyOfRange(block, pos, pos + n));
            pos += n;
        }
    }

    private void requeue(Stream s) {
        if (s.queued || s.closed || s.endSent) {
            return;
        }
        boolean hasData = !s.outQ.isEmpty() && s.sendWindow > 0;
        boolean needsEnd = s.outQ.isEmpty() && s.outEnd;
        if (hasData || needsEnd) {
            s.queued = true;
            sendQueue.add(s);
        }
    }

    /** Writes DATA frames round-robin while both windows have room and the connection is not backed up. */
    private void pump(Context ctx) {
        Connection c = ctx.connection();
        while (!dead && !sendQueue.isEmpty() && c.isWritable()) {
            Stream s = sendQueue.peek();
            if (s.closed || s.endSent) {
                sendQueue.poll();
                s.queued = false;
                continue;
            }
            if (s.outQ.isEmpty()) {
                sendQueue.poll();
                s.queued = false;
                if (s.outEnd) { // nothing left to send but the end of the stream
                    if (s.trailers != null) {
                        sendTrailerBlock(ctx, s);
                    } else {
                        flushDeferredHead(ctx, s);
                        dataFrame(ctx, 0, END_STREAM, s.id, null);
                    }
                    s.endSent = true;
                    finishLocal(ctx, s);
                }
                continue;
            }
            if (connSendWindow <= 0) {
                return; // every stream waits for the connection window
            }
            sendQueue.poll();
            s.queued = false;
            Out o = s.outQ.peek();
            ByteBuffer b = o.buf();
            long limit = Math.min(Math.min(s.sendWindow, connSendWindow), peerMaxFrameSize);
            long n = Math.min(b.remaining(), limit);
            if (n <= 0) {
                continue; // this stream's window is empty: its next WINDOW_UPDATE queues it again
            }
            boolean bufferDone = n == b.remaining();
            int merged = 1;   // buffers this frame takes from the queue
            ByteBuffer payload;
            ByteBuffer payload2 = null;
            Out second = null;
            if (bufferDone && n <= GATHER_PREFIX_MAX && s.outQ.size() > 1 && limit > n) {
                java.util.Iterator<Out> it = s.outQ.iterator();
                it.next();
                Out nx = it.next();
                if (nx.buf().remaining() >= COALESCE_BELOW) {
                    second = nx;
                }
            }
            if (second != null) {
                // a small header (a gRPC message prefix) written ahead of a big body: one frame, two buffers, no copy of the body
                ByteBuffer nb = second.buf();
                int take = (int) Math.min(nb.remaining(), limit - n);
                payload = o.done() != null ? copyOf(b.slice(b.position(), (int) n)) : b.slice(b.position(), (int) n);
                ByteBuffer part = nb.slice(nb.position(), take);
                payload2 = second.done() != null ? copyOf(part) : part;
                b.position(b.position() + (int) n);
                nb.position(nb.position() + take);
                if (!nb.hasRemaining()) {
                    merged = 2;
                }
                n += take;
            } else if (bufferDone && n < COALESCE_BELOW && s.outQ.size() > 1) {
                // a run of small writes (messages, events) goes out as one frame instead of one frame, one buffer and one queue pass each
                long total = n;
                for (Out next : s.outQ) {
                    if (next == o) {
                        continue;
                    }
                    int r = next.buf().remaining();
                    if (total + r > Math.min(limit, COALESCE_FRAME) || r >= COALESCE_BELOW) {
                        break;
                    }
                    total += r;
                    merged++;
                }
                if (merged > 1) {
                    payload = ByteBuffer.allocate((int) total);
                    int k = 0;
                    for (Out part : s.outQ) {
                        if (k++ == merged) {
                            break;
                        }
                        payload.put(part.buf());
                    }
                    payload.flip();
                    n = total;
                } else {
                    payload = null;
                }
            } else {
                payload = null;
            }
            boolean last = bufferDone && s.outQ.size() == merged && s.outEnd;
            if (payload == null) {
                if (o.done() != null) {
                    payload = ByteBuffer.allocate((int) n);   // the owner will recycle its buffer once told: do not hand out a view of it
                    payload.put(b.slice(b.position(), (int) n)).flip();
                } else {
                    payload = b.slice(b.position(), (int) n);
                }
                b.position(b.position() + (int) n);
            }
            flushDeferredHead(ctx, s);
            dataFrame(ctx, (int) n, last && s.trailers == null ? END_STREAM : 0, s.id, payload);
            if (payload2 != null) {
                write(ctx, payload2);
            }
            s.outPending -= n;
            s.sendWindow -= n;
            connSendWindow -= n;
            if (bufferDone) {
                for (int k = 0; k < merged; k++) {
                    runDone(s.outQ.poll().done());
                }
            }
            if (!s.writable && s.outPending <= LOW_WATER) {
                s.writable = true;
                if (s.tunnel != null && !s.closed) {
                    try {
                        s.tunnel.onWritabilityChanged(s.conn);
                    } catch (Exception e) {
                        LOG.log(Logger.Level.DEBUG, "writability callback failed", e);
                    }
                }
            }
            if (last) {
                if (s.trailers != null) { // the data is out: the trailing header block ends the stream (it is not flow controlled)
                    sendTrailerBlock(ctx, s);
                }
                s.endSent = true;
                finishLocal(ctx, s);
            } else {
                requeue(s);
            }
        }
    }

    /** Sends a held-back response head now (without END_STREAM), including what the application added. No-op if none is held. */
    private void flushDeferredHead(Context ctx, Stream s) {
        if (s.deferredHead == null) {
            return;
        }
        byte[] head = s.deferredHead;
        s.deferredHead = null;
        headerFrames(ctx, s.id, withExtra(head, s), false);
    }

    private byte[] withExtra(byte[] head, Stream s) {
        if (s.extraHead == null) {
            return head;
        }
        encoder.reset();
        s.extraHead.forEach((n, v) -> encoder.header(n.toLowerCase(Locale.ROOT), v));
        byte[] extra = encoder.toByteArray();
        s.extraHead = null;
        byte[] all = java.util.Arrays.copyOf(head, head.length + extra.length);   // header blocks without a dynamic table concatenate
        System.arraycopy(extra, 0, all, head.length, extra.length);
        return all;
    }

    /** A HEADERS frame (continued if large) carrying the trailers, with END_STREAM. A head still held back goes in the same block. */
    private void sendTrailerBlock(Context ctx, Stream s) {
        byte[] held = s.deferredHead != null ? withExtra(s.deferredHead, s) : null;
        s.deferredHead = null;
        encoder.reset();
        s.trailers.forEach((n, v) -> {
            String name = n.toLowerCase(Locale.ROOT);
            if (!name.startsWith(":")) {
                encoder.header(name, v);
            }
        });
        byte[] trailerBytes = encoder.toByteArray();
        if (held != null) {
            byte[] all = java.util.Arrays.copyOf(held, held.length + trailerBytes.length);
            System.arraycopy(trailerBytes, 0, all, held.length, trailerBytes.length);
            trailerBytes = all;
        }
        headerFrames(ctx, s.id, trailerBytes, true);
    }

    /** A DATA frame: the 9-byte header, then (without copying) the payload. */
    private static ByteBuffer copyOf(ByteBuffer src) {
        ByteBuffer c = ByteBuffer.allocate(src.remaining());
        c.put(src).flip();
        return c;
    }

    private void dataFrame(Context ctx, int n, int flags, int sid, ByteBuffer payload) {
        ByteBuffer head = ByteBuffer.allocate(9);
        head.put((byte) (n >>> 16)).put((byte) (n >>> 8)).put((byte) n).put((byte) DATA).put((byte) flags).putInt(sid).flip();
        write(ctx, head);
        if (payload != null) {
            write(ctx, payload);
        }
    }

    /** The response is complete. If the request is not, tell the client to stop sending it (RFC 9113 section 8.1). */
    private void finishLocal(Context ctx, Stream s) {
        if (!s.remoteClosed) {
            sendRst(ctx, s.id, NO_ERROR);
            closeStream(ctx, s, Closed.RESET_BY_US);
        } else {
            closeStream(ctx, s, Closed.NORMAL);
        }
    }

    /** Takes the stream out of http.server.active, once. */
    private static void endInFlight(Context ctx, Stream s) {
        if (s.inFlight) {
            s.inFlight = false;
            ctx.connection().eventLoop().metrics().recordHttpEnd();
        }
    }

    private void closeStream(Context ctx, Stream s, Closed how) {
        if (s.closed) {
            return;
        }
        s.closed = true;
        endInFlight(ctx, s);
        if (how == Closed.RESET_BY_PEER) {
            if (peerResets == null) {
                peerResets = ctx.connection().eventLoop().metrics().counter("http2.resets", null, "by", "peer");
            }
            peerResets.increment();
        }
        cancel(s.bodyTimer);
        streams.remove(s.id);
        recentlyClosed.put(s.id, how);
        if (s.pushed) {
            openPushes--;
        }
        releaseOutput(s);
        s.body = null;
        if (s.streaming != null && !s.remoteClosed) {
            s.streaming.abort(new IOException("the stream ended before the request body was complete"));
        }
        notifyTunnelClosed(s, null);
        if (streams.isEmpty() && !dead) {
            if (goingAway) {
                closeConnection(ctx, Duration.ofMillis(200));
            } else {
                armIdle(ctx);
            }
        }
    }

    private void streamError(Context ctx, int sid, int code) {
        sendRst(ctx, sid, code);
        Stream s = streams.get(sid);
        if (s != null) {
            closeStream(ctx, s, Closed.RESET_BY_US);
        } else {
            recentlyClosed.put(sid, Closed.RESET_BY_US);
        }
    }

    private void sendRst(Context ctx, int sid, int code) {
        frame(ctx, RST_STREAM, 0, sid, int32(code));
    }

    // ---- connection-level errors, idle handling, abuse limits ----

    private void connectionError(Context ctx, int code, String why) {
        if (dead) {
            return;
        }
        dead = true;
        cancelTimers();
        LOG.log(Logger.Level.DEBUG, "http/2 connection error {0}: {1}", code, why);
        frame(ctx, GOAWAY, 0, 0, goaway(lastClientStreamId, code, why));
        abortAllStreams(ctx, new IOException("HTTP/2 connection error " + code + ": " + why));
        closeConnection(ctx, Duration.ofMillis(500));
    }

    private void closeConnection(Context ctx, Duration linger) {
        try {
            ctx.closeAfterFlush(linger);
        } catch (Exception e) {
            ctx.connection().close();
        }
    }

    private void armIdle(Context ctx) {
        armIdleIfQuiet(ctx);
    }

    private void armIdleIfQuiet(Context ctx) {
        Duration t = httpConfig.keepAliveTimeout();
        if (t == null || dead || !streams.isEmpty() || idleTimer != null || hbStream != 0) {
            return;
        }
        idleTimer = ctx.connection().eventLoop().schedule(() -> {
            idleTimer = null;
            if (!dead && streams.isEmpty() && hbStream == 0) {
                frame(ctx, GOAWAY, 0, 0, goaway(lastClientStreamId, NO_ERROR, "idle"));
                dead = true;
                closeConnection(ctx, Duration.ofMillis(200));
            }
        }, t.toNanos(), TimeUnit.NANOSECONDS);
    }

    private boolean floodControl(Context ctx) {
        long now = System.nanoTime();
        if (now - windowStart > ONE_SECOND) {
            windowStart = now;
            controlFrames = 0;
            resets = 0;
        }
        if (++controlFrames > cfg.maxControlFramesPerSecond()) {
            connectionError(ctx, ENHANCE_YOUR_CALM, "too many control frames");
            return true;
        }
        return false;
    }

    private boolean rapidReset(Context ctx) {
        if (floodControl(ctx)) {
            return true;
        }
        if (++resets > cfg.maxResetsPerSecond()) {
            connectionError(ctx, ENHANCE_YOUR_CALM, "too many stream resets");
            return true;
        }
        return false;
    }

    // ---- frame output ----

    private void frame(Context ctx, int type, int flags, int sid, byte[] payload) {
        if (dead && type != GOAWAY) {
            return;
        }
        int n = payload.length;
        ByteBuffer b = ByteBuffer.allocate(9 + n);
        b.put((byte) (n >>> 16)).put((byte) (n >>> 8)).put((byte) n).put((byte) type).put((byte) flags).putInt(sid).put(payload).flip();
        write(ctx, b);
        // a peer that sends control frames but never reads what we answer must not make us buffer without bound
        if (ctx.connection().pendingBytes() > 8L * 1024 * 1024 && type != GOAWAY) {
            dead = true;
            ctx.connection().close();
        }
    }

    private void write(Context ctx, ByteBuffer b) {
        try {
            ctx.write(b);
        } catch (Exception e) {
            dead = true;
            ctx.connection().close();
        }
    }

    private static void putSetting(ByteArrayOutputStream out, int id, int value) {
        out.write(id >>> 8);
        out.write(id);
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }

    private static byte[] int32(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    private static byte[] goaway(int lastStream, int code, String debug) {
        byte[] d = debug.getBytes(StandardCharsets.UTF_8);
        int n = Math.min(d.length, 200);
        byte[] out = new byte[8 + n];
        System.arraycopy(int32(lastStream), 0, out, 0, 4);
        System.arraycopy(int32(code), 0, out, 4, 4);
        System.arraycopy(d, 0, out, 8, n);
        return out;
    }
}
