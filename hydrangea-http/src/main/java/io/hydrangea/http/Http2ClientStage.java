package io.hydrangea.http;

import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.http2.HpackDecoder;
import io.hydrangea.http2.HpackEncoder;
import io.hydrangea.http2.HpackException;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client side of HTTP/2 (RFC 9113) for one connection: many requests at once, one stream each. Last stage of the pipeline; everything
 * runs on the connection's event loop. Created by {@link HttpClient} when HTTP/2 is enabled and the connection turns out to speak it
 * (ALPN {@code h2}, or prior knowledge on cleartext).
 * <p>
 * Request bodies are in memory or produced by a {@link RequestBody} through a {@link BodySink} whose writability follows the stream's
 * output queue. Responses are buffered (bounded by {@link HttpClientConfig#maxResponseBytes()}) or handed to a {@link BodyListener}
 * whose {@link BodyControl#pause()} holds back the stream's window credit, so the server stops sending. A GOAWAY, a refused stream or a
 * connection that dies before any response byte are reported as "not processed", which lets {@link HttpClient} retry idempotent
 * requests on a fresh connection.
 */
final class Http2ClientStage implements Stage {

    private static final Logger LOG = System.getLogger(Http2ClientStage.class.getName());

    private static final int DATA = 0, HEADERS = 1, PRIORITY = 2, RST_STREAM = 3, SETTINGS = 4, PUSH_PROMISE = 5, PING = 6,
            GOAWAY = 7, WINDOW_UPDATE = 8, CONTINUATION = 9;
    private static final int END_STREAM = 0x1, ACK = 0x1, END_HEADERS = 0x4, PADDED = 0x8, PRIORITY_FLAG = 0x20;
    private static final int NO_ERROR = 0, PROTOCOL_ERROR = 1, INTERNAL_ERROR = 2, FLOW_CONTROL_ERROR = 3, FRAME_SIZE_ERROR = 6,
            REFUSED_STREAM = 7, CANCEL = 8, COMPRESSION_ERROR = 9, ENHANCE_YOUR_CALM = 11;
    private static final int S_HEADER_TABLE_SIZE = 1, S_ENABLE_PUSH = 2, S_MAX_CONCURRENT_STREAMS = 3, S_INITIAL_WINDOW_SIZE = 4,
            S_MAX_FRAME_SIZE = 5, S_MAX_HEADER_LIST_SIZE = 6;
    private static final long MAX_WINDOW = 0x7fffffffL;
    private static final long HIGH_WATER = 64 * 1024, LOW_WATER = 32 * 1024;
    private static final long CONTINUE_WAIT_MILLIS = 1000;

    private record Out(ByteBuffer buf) {
    }

    private static final class CStream {
        final int id;
        final HttpClient.Exchange ex;
        long sendWindow;
        long recvWindow;
        long recvConsumed;
        boolean localClosed;     // we sent END_STREAM
        boolean remoteClosed;    // the server sent END_STREAM
        boolean closed;
        boolean headSeen;        // the final response head arrived (the exchange can no longer be retried)
        boolean headRequest;
        ContentDecoder decoder;  // non-null while the body is decoded for the application
        Headers trailers;        // the trailing header block, delivered to a listener after its last data
        int status;
        Headers headers;
        long declaredLength = -1;
        long received;
        ByteArrayOutputStream body = new ByteArrayOutputStream();

        // response handed to a listener
        boolean streaming;
        BodyListener listener;
        boolean paused;
        long heldCredit;
        ByteArrayOutputStream held;
        boolean completePending;

        // request side
        final ArrayDeque<Out> outQ = new ArrayDeque<>();
        volatile long outPending;
        boolean outEnd;
        boolean endSent;
        boolean queued;
        volatile boolean writable = true;
        UploadSink upload;
        boolean awaitingContinue;
        Scheduled continueTimer;
        boolean uploadComplete = true;

        CStream(int id, HttpClient.Exchange ex, long sendWindow, long recvWindow) {
            this.id = id;
            this.ex = ex;
            this.sendWindow = sendWindow;
            this.recvWindow = recvWindow;
        }
    }

    private final HttpClient.PooledConn owner;
    private final HttpClient client;
    private final Http2ClientConfig cfg;
    private final int maxHeaderBytes;
    private final Accumulator in = new Accumulator();
    private final HpackEncoder encoder = new HpackEncoder();
    private final HpackDecoder decoder;
    private final int maxHeaderBlock;

    private Context ctx;
    private boolean dead;
    private boolean prefaceSent;
    private boolean settingsSeen;
    private final Map<Integer, CStream> streams = new HashMap<>();
    private final ArrayDeque<CStream> sendQueue = new ArrayDeque<>();
    private final ArrayDeque<HttpClient.Exchange> waitingForStreamSlot = new ArrayDeque<>();
    private int nextStreamId = 1;
    private boolean goaway;          // received GOAWAY: no new streams
    private int goawayLastId = Integer.MAX_VALUE;

    private int peerMaxFrameSize = 16384;
    private long peerInitialWindow = 65535;
    private long connSendWindow = 65535;
    private long peerMaxConcurrent = Long.MAX_VALUE;
    private long peerMaxHeaderList = Long.MAX_VALUE;

    private long connRecvWindow = 65535;
    private long connConsumed;

    private int hbStream;
    private boolean hbEndStream;
    private ByteArrayOutputStream hbBuf;

    Http2ClientStage(HttpClient.PooledConn owner, HttpClient client, Http2ClientConfig cfg, int maxHeaderBytes) {
        this.owner = owner;
        this.client = client;
        this.cfg = cfg;
        this.maxHeaderBytes = maxHeaderBytes;
        this.maxHeaderBlock = Math.max(64 * 1024, 4 * maxHeaderBytes);
        this.decoder = new HpackDecoder(cfg.headerTableSize(), 2 * maxHeaderBlock, maxHeaderBytes);
    }

    // ---- lifecycle ----

    /** The SETTINGS payload we announce: also what the HTTP2-Settings header of an h2c upgrade offer carries. */
    static byte[] settingsPayload(Http2ClientConfig cfg, int maxHeaderBytes) {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        putSetting(s, S_ENABLE_PUSH, 0);
        putSetting(s, S_INITIAL_WINDOW_SIZE, cfg.initialWindowSize());
        putSetting(s, S_HEADER_TABLE_SIZE, cfg.headerTableSize());
        putSetting(s, S_MAX_HEADER_LIST_SIZE, maxHeaderBytes);
        return s.toByteArray();
    }

    @Override
    public void onActive(Context ctx) {
        this.ctx = ctx;
        lastReceived = System.nanoTime();
        if (cfg.keepAliveInterval() != null) {
            scheduleKeepAlive(cfg.keepAliveInterval().toNanos());
        }
        write(ByteBuffer.wrap(Http2ServerStage.PREFACE));
        prefaceSent = true;
        frame(SETTINGS, 0, 0, settingsPayload(cfg, maxHeaderBytes));
        if (cfg.connectionWindowSize() > 65535) {
            frame(WINDOW_UPDATE, 0, 0, int32(cfg.connectionWindowSize() - 65535));
            connRecvWindow = cfg.connectionWindowSize();
        }
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        if (dead) {
            return;
        }
        lastReceived = System.nanoTime();
        pingOutstanding = false;   // anything from the server proves it is alive, the ACK or not
        in.append((ByteBuffer) msg);
        try {
            parse();
        } catch (RuntimeException e) {
            LOG.log(Logger.Level.WARNING, "http/2 client failed", e);
            connectionError(INTERNAL_ERROR, "internal error");
        }
    }

    @Override
    public void onWritabilityChanged(Context ctx) {
        if (!dead) {
            pump();
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        connectionGone(cause);
    }

    /** From the connection's closeFuture: covers a close before activation, when onClose is not delivered. */
    void connectionClosed() {
        connectionGone(null);
    }

    // ---- keep-alive ----

    private long lastReceived;
    private boolean pingOutstanding;
    private io.hydrangea.Scheduled keepAliveTimer;

    private void scheduleKeepAlive(long nanos) {
        keepAliveTimer = ctx.connection().eventLoop().schedule(this::keepAliveTick, nanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    private void keepAliveTick() {
        if (dead) {
            return;
        }
        long interval = cfg.keepAliveInterval().toNanos();
        long timeout = cfg.keepAliveTimeout().toNanos();
        long quiet = System.nanoTime() - lastReceived;
        if (pingOutstanding) {
            if (quiet >= timeout) {
                LOG.log(Logger.Level.DEBUG, "http/2 keep-alive ping unanswered for {0} ms: closing", quiet / 1_000_000);
                dead = true;
                ctx.connection().close();
                return;
            }
            scheduleKeepAlive(timeout - quiet);
        } else if (quiet >= interval) {
            pingOutstanding = true;
            frame(PING, 0, 0, new byte[8]);
            scheduleKeepAlive(timeout);
        } else {
            scheduleKeepAlive(interval - quiet);
        }
    }

    private void connectionGone(Throwable cause) {
        dead = true;
        if (keepAliveTimer != null) {
            keepAliveTimer.cancel();
        }
        in.release();
        List<CStream> all = new ArrayList<>(streams.values());
        streams.clear();
        sendQueue.clear();
        for (CStream s : all) {
            failStream(s, new IOException("connection closed before the response was complete", cause), false);
        }
        List<HttpClient.Exchange> waiting = new ArrayList<>(waitingForStreamSlot);
        waitingForStreamSlot.clear();
        for (HttpClient.Exchange ex : waiting) {
            owner.h2StreamEnded();
            owner.h2Failed(ex, new ClosedChannelException(), false);
        }
    }

    /** Any thread. Fails every running and waiting request with {@code error} (the client is shutting down). */
    void abortAll(Throwable error) {
        onLoop(() -> {
            for (CStream st : new ArrayList<>(streams.values())) {
                failStream(st, error, true);
            }
            List<HttpClient.Exchange> waiting = new ArrayList<>(waitingForStreamSlot);
            waitingForStreamSlot.clear();
            for (HttpClient.Exchange ex : waiting) {
                owner.h2StreamEnded();
                owner.h2Failed(ex, error, true);
            }
        });
    }

    // ---- starting requests ----

    /**
     * The server agreed to an h2c upgrade (101): the HTTP/1.1 request already sent for {@code ex} is stream 1, complete from our side, and
     * its answer arrives as frames. The slot for it was reserved when the connection was created.
     */
    void adoptUpgraded(HttpClient.Exchange ex) {
        CStream s = new CStream(1, ex, peerInitialWindow, cfg.initialWindowSize());
        s.headRequest = ex.req.method().equals("HEAD");
        s.localClosed = true;
        s.endSent = true;
        nextStreamId = 3;
        streams.put(1, s);
        ex.h2Cancel = () -> onLoop(() -> {
            if (!s.closed) {
                sendRst(1, CANCEL);
                closeStream(s);
            }
        });
    }

    /** Loop thread. Starts {@code ex} as a new stream, or queues it until the server's stream limit allows it. */
    void submit(HttpClient.Exchange ex) {
        if (dead || goaway || nextStreamId < 0) {
            owner.h2StreamEnded();
            owner.h2Failed(ex, new UnprocessedException("connection cannot take new streams"), false);   // nothing was sent: safe to try another connection
            return;
        }
        if (streams.size() >= peerMaxConcurrent) {
            waitingForStreamSlot.add(ex);
            return;
        }
        start(ex);
    }

    private void start(HttpClient.Exchange ex) {
        if (ex.h2Aborted || ex.result.isDone()) {   // cancelled while it waited for the loop or for a stream slot: nothing to send
            owner.h2StreamEnded();
            return;
        }
        ClientRequest r = ex.req;
        byte[] block;
        try {
            block = requestHeaders(r);
        } catch (RuntimeException e) {
            owner.h2StreamEnded();
            owner.h2Failed(ex, e, true);   // a bad request is not retryable
            return;
        }
        int id = nextStreamId;
        nextStreamId = id + 2 < 0 ? -1 : id + 2;
        CStream s = new CStream(id, ex, peerInitialWindow, cfg.initialWindowSize());
        s.headRequest = r.method().equals("HEAD");
        streams.put(id, s);
        ex.h2Cancel = () -> onLoop(() -> {
            if (!s.closed) {
                sendRst(id, CANCEL);
                closeStream(s);
            }
        });
        RequestBody rb = r.requestBody();
        boolean streamedBody = HttpClient.hasStreamedBody(r);
        byte[] inline = rb instanceof RequestBodies.Bytes b ? b.data() : null;
        boolean hasInline = inline != null && inline.length > 0;
        boolean headOnly = !streamedBody && !hasInline;
        headerFrames(id, block, headOnly);
        if (headOnly) {
            s.endSent = true;
            s.localClosed = true;
            return;
        }
        if (hasInline) {
            s.outQ.add(new Out(ByteBuffer.wrap(inline)));
            s.outPending = inline.length;
            s.outEnd = true;
            requeue(s);
            pump();
            return;
        }
        s.uploadComplete = false;
        if (r.headers().hasToken("expect", "100-continue")) {
            s.awaitingContinue = true;
            s.continueTimer = ctx.connection().eventLoop().schedule(() -> {
                if (s.awaitingContinue && !s.closed) {  // no answer in time: send anyway, as curl does
                    s.awaitingContinue = false;
                    startBody(s);
                }
            }, CONTINUE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } else {
            startBody(s);
        }
    }

    private void startBody(CStream s) {
        if (s.closed || s.upload != null) {
            return;
        }
        RequestBody rb = s.ex.req.requestBody();
        s.upload = new UploadSink(s, rb.contentLength());
        try {
            rb.start(s.upload);
        } catch (Throwable t) {
            failStream(s, t instanceof IOException io ? io : new IOException("request body failed to start", t), true);
            sendRst(s.id, CANCEL);
        }
    }

    private byte[] requestHeaders(ClientRequest r) {
        URI u = r.uri();
        String path = u.getRawPath();
        String target = (path == null || path.isEmpty() ? "/" : path) + (u.getRawQuery() != null ? "?" + u.getRawQuery() : "");
        for (int i = 0; i < target.length(); i++) {
            char c = target.charAt(i);
            if (c <= 0x20 || c >= 0x7f) {
                throw new IllegalArgumentException("request target must be percent-encoded ASCII: " + target);
            }
        }
        String method = r.method();
        for (int i = 0; i < method.length(); i++) {
            if (!HttpServerStage.isTokenChar(method.charAt(i))) {
                throw new IllegalArgumentException("bad method " + method);
            }
        }
        boolean tls = u.getScheme().equalsIgnoreCase("https");
        int port = u.getPort() != -1 ? u.getPort() : (tls ? 443 : 80);
        String authority = u.getHost() + (port != (tls ? 443 : 80) ? ":" + port : "");
        encoder.reset();
        encoder.header(":method", method);
        encoder.header(":scheme", tls ? "https" : "http");
        encoder.header(":authority", authority);
        encoder.header(":path", target);
        long size = method.length() + target.length() + authority.length() + 40;
        boolean[] seen = new boolean[2];
        List<String[]> user = new ArrayList<>();
        r.headers().forEach((n, v) -> {
            String name = Headers.lower(n);
            if (v.indexOf('\r') >= 0 || v.indexOf('\n') >= 0 || v.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("illegal character in header " + n);
            }
            switch (name) {
                case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "host", "content-length" -> { }
                case "te" -> {
                    if (v.trim().equalsIgnoreCase("trailers")) user.add(new String[]{name, "trailers"});
                }
                default -> {
                    if (name.equals("user-agent")) seen[0] = true;
                    if (name.equals("accept")) seen[1] = true;
                    user.add(new String[]{name, v});
                }
            }
        });
        for (String[] h : user) {
            encoder.header(h[0], h[1]);
            size += h[0].length() + h[1].length() + 32;
        }
        if (!seen[0]) {
            encoder.header("user-agent", client.userAgent());
            size += 12 + client.userAgent().length() + 32;
        }
        if (!seen[1]) {
            encoder.header("accept", "*/*");
            size += 40;
        }
        if (client.addsAcceptEncoding(r)) {
            encoder.header("accept-encoding", "gzip, deflate");
            size += 60;
        }
        String cookies = client.cookieHeaderFor(r);
        if (cookies != null) {
            encoder.header("cookie", cookies);
            size += 40 + cookies.length();
        }
        RequestBody rb = r.requestBody();
        long length = rb.contentLength();
        if (length > 0 || (length == 0 && (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")))) {
            encoder.header("content-length", Long.toString(length));
            size += 50;
        }
        if (size > peerMaxHeaderList) {
            throw new IllegalArgumentException("request headers exceed the server's SETTINGS_MAX_HEADER_LIST_SIZE of " + peerMaxHeaderList);
        }
        return encoder.toByteArray();
    }

    // ---- frame parsing ----

    private void parse() {
        while (!dead) {
            ByteBuffer b = in.buffer();
            if (b.remaining() < 9) {
                return;
            }
            int p = b.position();
            int len = (b.get(p) & 0xff) << 16 | (b.get(p + 1) & 0xff) << 8 | (b.get(p + 2) & 0xff);
            int type = b.get(p + 3) & 0xff;
            int flags = b.get(p + 4) & 0xff;
            int sid = b.getInt(p + 5) & 0x7fffffff;
            if (len > 16384) { // we never raise SETTINGS_MAX_FRAME_SIZE above the default
                connectionError(FRAME_SIZE_ERROR, "frame of " + len + " bytes");
                return;
            }
            if (b.remaining() < 9 + len) {
                return;
            }
            byte[] payload = new byte[len];
            b.position(p + 9);
            b.get(payload);
            if (!settingsSeen && !(type == SETTINGS && (flags & ACK) == 0)) {
                connectionError(PROTOCOL_ERROR, "the server's first frame must be SETTINGS");
                return;
            }
            if (hbStream != 0 && (type != CONTINUATION || sid != hbStream)) {
                connectionError(PROTOCOL_ERROR, "a header block is open: only CONTINUATION may follow");
                return;
            }
            switch (type) {
                case DATA -> onData(sid, flags, payload);
                case HEADERS -> onHeaders(sid, flags, payload);
                case PRIORITY -> { /* ignored */ }
                case RST_STREAM -> onRst(sid, payload);
                case SETTINGS -> onSettings(sid, flags, payload);
                case PUSH_PROMISE -> connectionError(PROTOCOL_ERROR, "PUSH_PROMISE although push is disabled");
                case PING -> onPing(sid, flags, payload);
                case GOAWAY -> onGoaway(sid, payload);
                case WINDOW_UPDATE -> onWindowUpdate(sid, payload);
                case CONTINUATION -> onContinuation(sid, flags, payload);
                default -> { /* unknown frame types are ignored */ }
            }
        }
    }

    private void onSettings(int sid, int flags, byte[] p) {
        if (sid != 0) {
            connectionError(PROTOCOL_ERROR, "SETTINGS on a stream");
            return;
        }
        if ((flags & ACK) != 0) {
            if (p.length != 0) {
                connectionError(FRAME_SIZE_ERROR, "SETTINGS ack with a payload");
            }
            return;
        }
        if (p.length % 6 != 0) {
            connectionError(FRAME_SIZE_ERROR, "SETTINGS length");
            return;
        }
        settingsSeen = true;
        for (int i = 0; i < p.length; i += 6) {
            int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
            long v = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8 | (p[i + 5] & 0xff);
            switch (id) {
                case S_ENABLE_PUSH -> {
                    // the server's value for its own receiving side; irrelevant to us
                }
                case S_INITIAL_WINDOW_SIZE -> {
                    if (v > MAX_WINDOW) {
                        connectionError(FLOW_CONTROL_ERROR, "INITIAL_WINDOW_SIZE above 2^31-1");
                        return;
                    }
                    long delta = v - peerInitialWindow;
                    peerInitialWindow = v;
                    for (CStream s : streams.values()) {
                        s.sendWindow += delta;
                        if (s.sendWindow > MAX_WINDOW) {
                            connectionError(FLOW_CONTROL_ERROR, "stream window overflow");
                            return;
                        }
                        requeue(s);
                    }
                }
                case S_MAX_FRAME_SIZE -> {
                    if (v < 16384 || v > 16_777_215) {
                        connectionError(PROTOCOL_ERROR, "MAX_FRAME_SIZE out of range");
                        return;
                    }
                    peerMaxFrameSize = (int) v;
                }
                case S_MAX_CONCURRENT_STREAMS -> peerMaxConcurrent = Math.min(v, cfg.maxStreamsPerConnection());
                case S_MAX_HEADER_LIST_SIZE -> peerMaxHeaderList = v;
                case S_HEADER_TABLE_SIZE -> { /* our encoder uses no dynamic table */ }
                default -> { }
            }
        }
        frame(SETTINGS, ACK, 0, new byte[0]);
        startWaiting();
        pump();
    }

    private void onPing(int sid, int flags, byte[] p) {
        if (sid != 0) {
            connectionError(PROTOCOL_ERROR, "PING on a stream");
        } else if (p.length != 8) {
            connectionError(FRAME_SIZE_ERROR, "PING length");
        } else if ((flags & ACK) == 0) {
            frame(PING, ACK, 0, p);
        } else {
            pingOutstanding = false;
        }
    }

    private void onGoaway(int sid, byte[] p) {
        if (sid != 0 || p.length < 8) {
            connectionError(sid != 0 ? PROTOCOL_ERROR : FRAME_SIZE_ERROR, "GOAWAY");
            return;
        }
        int last = ByteBuffer.wrap(p).getInt() & 0x7fffffff;
        int code = ByteBuffer.wrap(p).getInt(4);
        goaway = true;
        goawayLastId = Math.min(goawayLastId, last);
        owner.h2GoingAway();
        // streams above the last one the server processed were not (and will not be) handled: they can be retried elsewhere
        for (CStream s : new ArrayList<>(streams.values())) {
            if (s.id > last) {
                failStream(s, new UnprocessedException("server is going away (GOAWAY, last stream " + last + ")"), false);
                sendRstQuiet(s.id);
            } else if (code != NO_ERROR) {
                failStream(s, new IOException("server closed the connection with error " + code), s.headSeen);
            }
        }
        List<HttpClient.Exchange> waiting = new ArrayList<>(waitingForStreamSlot);
        waitingForStreamSlot.clear();
        for (HttpClient.Exchange ex : waiting) {
            owner.h2StreamEnded();
            owner.h2Failed(ex, new UnprocessedException("server is going away"), false);
        }
        if (streams.isEmpty()) {
            closeConnection();
        }
    }

    private void onWindowUpdate(int sid, byte[] p) {
        if (p.length != 4) {
            connectionError(FRAME_SIZE_ERROR, "WINDOW_UPDATE length");
            return;
        }
        long inc = ((p[0] & 0x7fL) << 24) | (p[1] & 0xff) << 16 | (p[2] & 0xff) << 8 | (p[3] & 0xff);
        if (sid == 0) {
            if (inc == 0) {
                connectionError(PROTOCOL_ERROR, "zero WINDOW_UPDATE");
                return;
            }
            connSendWindow += inc;
            if (connSendWindow > MAX_WINDOW) {
                connectionError(FLOW_CONTROL_ERROR, "connection window overflow");
                return;
            }
            pump();
            return;
        }
        CStream s = streams.get(sid);
        if (s == null) {
            return;
        }
        if (inc == 0) {
            streamError(s, PROTOCOL_ERROR, "zero WINDOW_UPDATE");
            return;
        }
        s.sendWindow += inc;
        if (s.sendWindow > MAX_WINDOW) {
            streamError(s, FLOW_CONTROL_ERROR, "stream window overflow");
            return;
        }
        requeue(s);
        pump();
    }

    private void onRst(int sid, byte[] p) {
        if (sid == 0 || p.length != 4) {
            connectionError(sid == 0 ? PROTOCOL_ERROR : FRAME_SIZE_ERROR, "RST_STREAM");
            return;
        }
        CStream s = streams.get(sid);
        if (s == null) {
            return;
        }
        int code = ByteBuffer.wrap(p).getInt();
        if (code == NO_ERROR && s.remoteClosed) {
            closeStream(s);
            return;
        }
        // REFUSED_STREAM promises the request was not processed at all: safe to retry
        failStream(s, code == REFUSED_STREAM && !s.headSeen ? new UnprocessedException("stream refused by the server")
                : new IOException("stream reset by the server (error " + code + ")"), s.headSeen);
    }

    // ---- HEADERS / CONTINUATION ----

    private void onHeaders(int sid, int flags, byte[] p) {
        if (sid == 0 || (sid & 1) == 0) {
            connectionError(PROTOCOL_ERROR, "HEADERS on stream " + sid);
            return;
        }
        int off = 0;
        int end = p.length;
        if ((flags & PADDED) != 0) {
            if (p.length < 1) {
                connectionError(FRAME_SIZE_ERROR, "padded HEADERS without a pad length");
                return;
            }
            off = 1;
            end = p.length - (p[0] & 0xff);
            if (end < off) {
                connectionError(PROTOCOL_ERROR, "padding longer than the frame");
                return;
            }
        }
        if ((flags & PRIORITY_FLAG) != 0) {
            if (end - off < 5) {
                connectionError(FRAME_SIZE_ERROR, "priority fields truncated");
                return;
            }
            off += 5;
        }
        if (sid >= nextStreamId && nextStreamId > 0) {
            connectionError(PROTOCOL_ERROR, "HEADERS on a stream we never opened");
            return;
        }
        hbStream = sid;
        hbEndStream = (flags & END_STREAM) != 0;
        hbBuf = new ByteArrayOutputStream(Math.max(64, end - off));
        hbBuf.write(p, off, end - off);
        if ((flags & END_HEADERS) != 0) {
            completeHeaderBlock();
        }
    }

    private void onContinuation(int sid, int flags, byte[] p) {
        if (hbStream == 0 || sid != hbStream) {
            connectionError(PROTOCOL_ERROR, "unexpected CONTINUATION");
            return;
        }
        if (hbBuf.size() + p.length > maxHeaderBlock) {
            connectionError(ENHANCE_YOUR_CALM, "header block too large");
            return;
        }
        hbBuf.write(p, 0, p.length);
        if ((flags & END_HEADERS) != 0) {
            completeHeaderBlock();
        }
    }

    private void completeHeaderBlock() {
        int sid = hbStream;
        boolean endStream = hbEndStream;
        byte[] block = hbBuf.toByteArray();
        hbStream = 0;
        hbBuf = null;
        List<String[]> list = new ArrayList<>();
        try {
            decoder.decode(block, (n, v) -> list.add(new String[]{n, v}));
        } catch (HpackException e) {
            connectionError(COMPRESSION_ERROR, e.getMessage());
            return;
        }
        CStream s = streams.get(sid);
        if (s == null) {
            return; // a stream we cancelled: the block only had to go through HPACK
        }
        if (decoder.lastBlockTooLarge()) {
            streamError(s, CANCEL, "response headers exceed " + maxHeaderBytes + " bytes");
            return;
        }
        if (s.remoteClosed) {
            streamError(s, PROTOCOL_ERROR, "HEADERS after the end of the response");
            return;
        }
        if (s.headSeen) { // trailers
            for (String[] h : list) {
                if (h[0].startsWith(":")) {
                    streamError(s, PROTOCOL_ERROR, "pseudo-header in trailers");
                    return;
                }
            }
            if (!endStream) {
                streamError(s, PROTOCOL_ERROR, "trailers must end the stream");
                return;
            }
            Headers t = new Headers();
            for (String[] h : list) {
                t.add(h[0], h[1]);
            }
            s.trailers = t;
            endOfResponse(s);
            return;
        }
        int status = -1;
        Headers h = new Headers();
        boolean pseudoAllowed = true;
        long contentLength = -1;
        for (String[] e : list) {
            String name = e[0];
            String value = e[1];
            if (name.startsWith(":")) {
                if (!pseudoAllowed || !name.equals(":status") || status != -1) {
                    streamError(s, PROTOCOL_ERROR, "bad pseudo-header " + name);
                    return;
                }
                status = parseStatus(value);
                if (status < 100) {
                    streamError(s, PROTOCOL_ERROR, "bad :status " + value);
                    return;
                }
                continue;
            }
            pseudoAllowed = false;
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);
                if (c >= 'A' && c <= 'Z' || !HttpServerStage.isTokenChar(c)) {
                    streamError(s, PROTOCOL_ERROR, "bad header name");
                    return;
                }
            }
            switch (name) {
                case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade" -> {
                    streamError(s, PROTOCOL_ERROR, "connection-specific header " + name);
                    return;
                }
                case "content-length" -> {
                    long n = parseLength(value);
                    if (n < 0 || (contentLength >= 0 && n != contentLength)) {
                        streamError(s, PROTOCOL_ERROR, "bad content-length");
                        return;
                    }
                    contentLength = n;
                }
                default -> { }
            }
            h.add(name, value);
        }
        if (status == -1) {
            streamError(s, PROTOCOL_ERROR, "response without :status");
            return;
        }
        if (status >= 100 && status < 200) { // interim
            if (endStream) {
                streamError(s, PROTOCOL_ERROR, "interim response ends the stream");
                return;
            }
            if (status == 100 && s.awaitingContinue) {
                s.awaitingContinue = false;
                cancelContinueTimer(s);
                startBody(s);
            }
            return;
        }
        if (s.awaitingContinue) { // a final answer instead of the go-ahead: the body is never sent
            s.awaitingContinue = false;
            cancelContinueTimer(s);
        }
        s.headSeen = true;
        s.status = status;
        s.declaredLength = contentLength;
        HttpClient.Exchange ex = s.ex;
        s.streaming = ex.stream != null && !client.buffersForRedirect(status, h);
        boolean noBody = s.headRequest || status == 204 || status == 304;
        s.decoder = noBody ? null : client.contentDecoder(ex, h, s.streaming);
        s.headers = s.decoder != null ? HttpClient.withoutEncoding(h) : h; // what the application sees
        client.storeCookies(ex.req.uri(), h);
        if (s.streaming) {
            s.listener = ex.stream;
            ex.cancelTimeout(); // from here on only the idle timeout applies
            try {
                s.listener.onHead(new ResponseHead(status, "", s.headers, ex.req.uri()), new Control(s));
            } catch (Throwable t) {
                failStream(s, new IOException("stream listener failed in onHead", t), true);
                sendRst(sid, CANCEL);
                return;
            }
        } else if (contentLength > client.maxResponseBytes() && !s.headRequest) {
            streamError(s, CANCEL, "response body of " + contentLength + " bytes exceeds the limit of " + client.maxResponseBytes());
            return;
        }
        if (endStream) {
            if (s.declaredLength > 0 && !s.headRequest && status != 204 && status != 304) {
                streamError(s, PROTOCOL_ERROR, "response ended without the body its content-length announced");
                return;
            }
            endOfResponse(s);
        }
    }

    private static int parseStatus(String v) {
        if (v.length() != 3) {
            return -1;
        }
        int n = 0;
        for (int i = 0; i < 3; i++) {
            char c = v.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            n = n * 10 + (c - '0');
        }
        return n;
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

    // ---- DATA ----

    private void onData(int sid, int flags, byte[] p) {
        if (sid == 0) {
            connectionError(PROTOCOL_ERROR, "DATA on stream 0");
            return;
        }
        int off = 0;
        int end = p.length;
        if ((flags & PADDED) != 0) {
            if (p.length < 1 || (p[0] & 0xff) >= p.length) {
                connectionError(p.length < 1 ? FRAME_SIZE_ERROR : PROTOCOL_ERROR, "bad padding");
                return;
            }
            off = 1;
            end = p.length - (p[0] & 0xff);
        }
        if (p.length > connRecvWindow) {
            connectionError(FLOW_CONTROL_ERROR, "DATA beyond the connection window");
            return;
        }
        connRecvWindow -= p.length;
        connConsumed += p.length;
        if (connConsumed >= cfg.connectionWindowSize() / 2) {
            frame(WINDOW_UPDATE, 0, 0, int32((int) connConsumed));
            connRecvWindow += connConsumed;
            connConsumed = 0;
        }
        CStream s = streams.get(sid);
        if (s == null) {
            return; // a stream we cancelled; its bytes were still charged to the connection window above
        }
        if (!s.headSeen || s.remoteClosed) {
            streamError(s, PROTOCOL_ERROR, "DATA outside a response body");
            return;
        }
        if (p.length > s.recvWindow) {
            streamError(s, FLOW_CONTROL_ERROR, "DATA beyond the stream window");
            return;
        }
        s.recvWindow -= p.length;
        boolean last = (flags & END_STREAM) != 0;
        int n = end - off;
        s.received += n;
        if (!s.headRequest && s.declaredLength >= 0 && s.received > s.declaredLength) {
            streamError(s, PROTOCOL_ERROR, "more body than content-length");
            return;
        }
        if (s.decoder != null) {
            try {
                if (n > 0) {
                    s.decoder.write(p, off, n, (b, o, l) -> emitDecoded(s, b, o, l));
                }
            } catch (IOException e) {
                streamError(s, CANCEL, e.getMessage());
                return;
            }
            if (s.closed) {
                return;
            }
        } else if (s.streaming) {
            if (n > 0) {
                deliver(s, p, off, n);
            }
            if (s.closed) {
                return;
            }
        } else {
            if (s.received > client.maxResponseBytes()) {
                streamError(s, CANCEL, "response body exceeds the limit of " + client.maxResponseBytes());
                return;
            }
            s.body.write(p, off, n);
        }
        creditStream(s, p.length, last);
        if (last) {
            if (!s.headRequest && s.declaredLength >= 0 && s.received != s.declaredLength) {
                streamError(s, PROTOCOL_ERROR, "body shorter than content-length");
                return;
            }
            endOfResponse(s);
        }
    }

    /** Decoded bytes: to the listener (held while it is paused) or into the buffered body. */
    private void emitDecoded(CStream s, byte[] data, int off, int len) throws IOException {
        if (s.streaming) {
            deliver(s, data, off, len);
        } else {
            s.body.write(data, off, len);
        }
        if (s.closed) {
            throw new IOException("stream ended while decoding");
        }
    }

    private void deliver(CStream s, byte[] p, int off, int n) {
        if (s.paused) { // BodyControl contract: nothing after pause() until resume()
            if (s.held == null) {
                s.held = new ByteArrayOutputStream();
            }
            s.held.write(p, off, n);
            return;
        }
        try {
            s.listener.onData(ByteBuffer.wrap(p, off, n).asReadOnlyBuffer(), new Control(s));
        } catch (Throwable t) {
            failStream(s, new IOException("stream listener failed in onData", t), true);
            sendRst(s.id, CANCEL);
        }
    }

    private void creditStream(CStream s, int bytes, boolean last) {
        s.recvConsumed += bytes;
        if (s.paused) {
            s.heldCredit = s.recvConsumed;
            return;
        }
        if (last || s.recvConsumed < cfg.initialWindowSize() / 2) {
            return;
        }
        frame(WINDOW_UPDATE, 0, s.id, int32((int) s.recvConsumed));
        s.recvWindow += s.recvConsumed;
        s.recvConsumed = 0;
    }

    /** END_STREAM from the server: the response is complete (unless the application has paused behind held data). */
    private void endOfResponse(CStream s) {
        s.remoteClosed = true;
        if (s.decoder != null) {
            try {
                s.decoder.finish();
            } catch (IOException e) {
                s.decoder = null;
                streamError(s, CANCEL, e.getMessage());
                return;
            }
            s.decoder = null;
        }
        if (s.streaming && s.paused) {
            s.completePending = true;
            return;
        }
        completeResponse(s);
    }

    private void completeResponse(CStream s) {
        HttpClient.Exchange ex = s.ex;
        boolean unfinishedUpload = !s.localClosed;
        ClientResponse resp = new ClientResponse(s.status, "", s.headers, s.streaming ? new byte[0] : s.body.toByteArray(), ex.req.uri());
        boolean wasStreaming = s.streaming;
        if (unfinishedUpload) { // the server answered before it had all of the request: stop sending the rest
            sendRst(s.id, CANCEL);
            if (s.upload != null) {
                s.upload.kill(new IOException("the server answered before the request body was fully sent"));
            }
        }
        closeStream(s);
        if (wasStreaming) {
            if (s.trailers != null && s.listener != null) {
                try {
                    s.listener.onTrailers(s.trailers);
                } catch (Throwable t) {
                    owner.h2Failed(ex, new IOException("stream listener failed in onTrailers", t), true);
                    return;
                }
            }
            owner.h2FinishedStream(ex, resp);
        } else {
            owner.h2Finished(ex, resp);
        }
    }

    // ---- flow control handed to a response listener ----

    private final class Control implements BodyControl {
        private final CStream s;

        Control(CStream s) {
            this.s = s;
        }

        @Override
        public void pause() {
            onLoop(() -> {
                if (!s.closed && s.streaming && !s.paused) {
                    s.paused = true;
                }
            });
        }

        @Override
        public void resume() {
            onLoop(() -> {
                if (!s.paused) {
                    return;
                }
                s.paused = false;
                if (s.closed) {
                    return;
                }
                if (s.held != null && s.held.size() > 0) {
                    byte[] data = s.held.toByteArray();
                    s.held = null;
                    deliver(s, data, 0, data.length);
                    if (s.closed) {
                        return;
                    }
                }
                if (s.heldCredit > 0 && !s.remoteClosed) {
                    frame(WINDOW_UPDATE, 0, s.id, int32((int) s.heldCredit));
                    s.recvWindow += s.heldCredit;
                    s.recvConsumed = 0;
                    s.heldCredit = 0;
                }
                if (s.completePending && !s.paused) {
                    s.completePending = false;
                    completeResponse(s);
                }
            });
        }

        @Override
        public void cancel() {
            s.ex.fail(new CancellationException("stream cancelled"));
        }
    }

    // ---- sending ----

    private void headerFrames(int sid, byte[] block, boolean endStream) {
        int max = peerMaxFrameSize;
        if (block.length <= max) {
            frame(HEADERS, END_HEADERS | (endStream ? END_STREAM : 0), sid, block);
            return;
        }
        frame(HEADERS, endStream ? END_STREAM : 0, sid, java.util.Arrays.copyOfRange(block, 0, max));
        int pos = max;
        while (pos < block.length) {
            int n = Math.min(max, block.length - pos);
            frame(CONTINUATION, pos + n == block.length ? END_HEADERS : 0, sid, java.util.Arrays.copyOfRange(block, pos, pos + n));
            pos += n;
        }
    }

    private void requeue(CStream s) {
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

    private void pump() {
        while (!dead && !sendQueue.isEmpty() && ctx.connection().isWritable()) {
            CStream s = sendQueue.peek();
            if (s.closed || s.endSent) {
                sendQueue.poll();
                s.queued = false;
                continue;
            }
            if (s.outQ.isEmpty()) {
                sendQueue.poll();
                s.queued = false;
                if (s.outEnd) {
                    dataFrame(0, END_STREAM, s.id, null);
                    s.endSent = true;
                    s.localClosed = true;
                    afterLocalEnd(s);
                }
                continue;
            }
            if (connSendWindow <= 0) {
                return;
            }
            sendQueue.poll();
            s.queued = false;
            ByteBuffer b = s.outQ.peek().buf();
            long n = Math.min(Math.min(b.remaining(), s.sendWindow), Math.min(connSendWindow, peerMaxFrameSize));
            if (n <= 0) {
                continue;
            }
            boolean bufferDone = n == b.remaining();
            boolean last = bufferDone && s.outQ.size() == 1 && s.outEnd;
            ByteBuffer payload = b.slice(b.position(), (int) n);
            b.position(b.position() + (int) n);
            dataFrame((int) n, last ? END_STREAM : 0, s.id, payload);
            s.outPending -= n;
            s.sendWindow -= n;
            connSendWindow -= n;
            if (bufferDone) {
                s.outQ.poll();
            }
            if (!s.writable && s.outPending <= LOW_WATER) {
                s.writable = true;
                if (s.upload != null) {
                    s.upload.notifyWritable();
                }
            }
            if (last) {
                s.endSent = true;
                s.localClosed = true;
                afterLocalEnd(s);
            } else {
                requeue(s);
            }
        }
    }

    private void afterLocalEnd(CStream s) {
        s.uploadComplete = true;
        if (s.remoteClosed && !s.closed) {
            closeStream(s);
        }
    }

    private void dataFrame(int n, int flags, int sid, ByteBuffer payload) {
        ByteBuffer head = ByteBuffer.allocate(9);
        head.put((byte) (n >>> 16)).put((byte) (n >>> 8)).put((byte) n).put((byte) DATA).put((byte) flags).putInt(sid).flip();
        write(head);
        if (payload != null) {
            write(payload);
        }
    }

    // ---- the request body producer's view of a stream ----

    private final class UploadSink implements BodySink {
        private final CStream s;
        private final long declared;
        private long written;
        private boolean finished;
        private volatile boolean dead;
        private volatile Throwable deadCause;
        private final ArrayList<CompletableFuture<Void>> waiters = new ArrayList<>();

        UploadSink(CStream s, long declared) {
            this.s = s;
            this.declared = declared;
        }

        @Override
        public CompletableFuture<Void> write(ByteBuffer data) {
            byte[] copy = new byte[data.remaining()];
            data.get(copy);
            return queue(ByteBuffer.wrap(copy));
        }

        @Override
        public CompletableFuture<Void> writeOwned(ByteBuffer data) {
            return queue(data);
        }

        private CompletableFuture<Void> queue(ByteBuffer buf) {
            int length = buf.remaining();
            CompletableFuture<Void> f = new CompletableFuture<>();
            onLoop(() -> {
                if (dead || finished || s.closed) {
                    f.completeExceptionally(dead ? stopped() : new IllegalStateException("the body is already finished"));
                    return;
                }
                if (declared >= 0 && written + length > declared) {
                    IOException e = new IOException("the request body is longer than the declared " + declared + " bytes");
                    f.completeExceptionally(e);
                    failStream(s, e, true);
                    sendRst(s.id, CANCEL);
                    return;
                }
                written += length;
                if (length > 0) {
                    s.outQ.add(new Out(buf));
                    s.outPending += length;
                    if (s.writable && s.outPending > HIGH_WATER) {
                        s.writable = false;
                    }
                    requeue(s);
                    pump();
                }
                f.complete(null);
            });
            return f;
        }

        @Override
        public boolean isWritable() {
            return !dead && s.writable;
        }

        @Override
        public CompletableFuture<Void> whenWritable() {
            CompletableFuture<Void> f = new CompletableFuture<>();
            onLoop(() -> {
                if (dead) {
                    f.completeExceptionally(stopped());
                } else if (s.writable) {
                    f.complete(null);
                } else {
                    waiters.add(f);
                }
            });
            return f;
        }

        @Override
        public void finish() {
            onLoop(() -> {
                if (dead || finished || s.closed) {
                    return;
                }
                if (declared >= 0 && written != declared) {
                    failStream(s, new IOException("the request body ended after " + written + " of the declared " + declared + " bytes"), true);
                    sendRst(s.id, CANCEL);
                    return;
                }
                finished = true;
                s.outEnd = true;
                requeue(s);
                pump();
            });
        }

        @Override
        public void fail(Throwable cause) {
            onLoop(() -> {
                if (!dead && !s.closed) {
                    failStream(s, cause instanceof IOException ? cause : new IOException("request body failed", cause), true);
                    sendRst(s.id, CANCEL);
                }
            });
        }

        void kill(Throwable reason) {
            dead = true;
            deadCause = reason;
            for (CompletableFuture<Void> w : waiters) {
                w.completeExceptionally(stopped());
            }
            waiters.clear();
        }

        void notifyWritable() {
            if (waiters.isEmpty()) {
                return;
            }
            var ready = new ArrayList<>(waiters);
            waiters.clear();
            ready.forEach(w -> w.complete(null));
        }

        private IOException stopped() {
            Throwable c = deadCause;
            return new IOException("the request is no longer being sent" + (c != null && c.getMessage() != null ? ": " + c.getMessage() : ""), c);
        }
    }

    // ---- ending streams and connections ----

    private void cancelContinueTimer(CStream s) {
        if (s.continueTimer != null) {
            s.continueTimer.cancel();
            s.continueTimer = null;
        }
    }

    /** Removes the stream and gives its slot back to the pool. */
    private void closeStream(CStream s) {
        if (s.closed) {
            return;
        }
        s.closed = true;
        streams.remove(s.id);
        cancelContinueTimer(s);
        if (s.decoder != null) {
            s.decoder.abort();
            s.decoder = null;
        }
        s.ex.h2Cancel = null;
        if (s.upload != null) {
            s.upload.kill(null);
        }
        s.outQ.clear();
        s.outPending = 0;
        owner.h2StreamEnded();
        if (!dead) {
            startWaiting();
            if (goaway && streams.isEmpty()) {
                closeConnection();
            }
        }
    }

    /** The exchange failed on this stream. {@code gotResponse} false = nothing was processed: the caller may retry. */
    private void failStream(CStream s, Throwable err, boolean gotResponse) {
        if (s.closed) {
            return;
        }
        closeStream(s);
        owner.h2Failed(s.ex, err, gotResponse);
    }

    private void streamError(CStream s, int code, String why) {
        LOG.log(Logger.Level.DEBUG, "http/2 stream {0} error {1}: {2}", s.id, code, why);
        sendRst(s.id, code);
        failStream(s, new IOException(why), true);
    }

    private void connectionError(int code, String why) {
        if (dead) {
            return;
        }
        dead = true;
        LOG.log(Logger.Level.DEBUG, "http/2 client connection error {0}: {1}", code, why);
        byte[] d = why.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[8 + Math.min(d.length, 200)];
        System.arraycopy(int32(0), 0, payload, 0, 4);
        System.arraycopy(int32(code), 0, payload, 4, 4);
        System.arraycopy(d, 0, payload, 8, payload.length - 8);
        write(HttpFrames.frameBytes(GOAWAY, 0, 0, payload));
        List<CStream> all = new ArrayList<>(streams.values());
        streams.clear();
        sendQueue.clear();
        for (CStream s : all) {
            s.closed = true;
            cancelContinueTimer(s);
            s.ex.h2Cancel = null;
            if (s.upload != null) {
                s.upload.kill(null);
            }
            owner.h2StreamEnded();
            owner.h2Failed(s.ex, new IOException("HTTP/2 protocol error: " + why), s.headSeen);
        }
        closeConnection();
    }

    private void closeConnection() {
        ctx.connection().closeAfterFlush();
    }

    private void startWaiting() {
        while (!waitingForStreamSlot.isEmpty() && streams.size() < peerMaxConcurrent && !dead && !goaway) {
            HttpClient.Exchange ex = waitingForStreamSlot.poll();
            if (!ex.result.isDone()) {
                start(ex);
            } else {
                owner.h2StreamEnded(); // it never became a stream, but it had reserved a slot
            }
        }
    }

    private void onLoop(Runnable r) {
        if (ctx == null) {
            r.run();
            return;
        }
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

    // ---- frame output ----

    private void frame(int type, int flags, int sid, byte[] payload) {
        if (dead) {
            return;
        }
        write(HttpFrames.frameBytes(type, flags, sid, payload));
    }

    private void sendRst(int sid, int code) {
        frame(RST_STREAM, 0, sid, int32(code));
    }

    /** RST_STREAM(REFUSED_STREAM) for a stream the server will not process (above its GOAWAY mark): tells it nothing is wanted there. */
    private void sendRstQuiet(int sid) {
        sendRst(sid, CANCEL);
    }

    private void write(ByteBuffer b) {
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
}
