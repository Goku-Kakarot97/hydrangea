package io.hydrangea.http;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.http2.Qpack;
import io.hydrangea.http2.QpackException;
import io.hydrangea.quic.QuicConfig;
import io.hydrangea.quic.QuicConnection;
import io.hydrangea.quic.QuicHandler;
import io.hydrangea.quic.QuicServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP/3 (RFC 9114) over {@link QuicServer}: the same {@link HttpHandler} (so {@link HttpRouter} too) that serves HTTP/1.1 and HTTP/2.
 * <p>
 * Experimental, phase 2 of HTTP/3: requests and responses are buffered (no streaming handlers, no upgrades, no server push, no trailers in
 * responses), QPACK runs without a dynamic table (capacity 0 is advertised), a stream-level error is answered with a 400 instead of RESET_STREAM,
 * and there is no access log or header/body timeout beyond the QUIC idle timeout.
 */
public final class Http3Server implements AutoCloseable {

    private static final Logger LOG = System.getLogger(Http3Server.class.getName());

    // frame types
    private static final int DATA = 0x00, HEADERS = 0x01, CANCEL_PUSH = 0x03, SETTINGS = 0x04, PUSH_PROMISE = 0x05, GOAWAY = 0x07, MAX_PUSH_ID = 0x0d;
    // error codes (RFC 9114 section 8.1, RFC 9204 section 6)
    private static final long H3_GENERAL_PROTOCOL_ERROR = 0x101, H3_STREAM_CREATION_ERROR = 0x103, H3_CLOSED_CRITICAL_STREAM = 0x104,
            H3_FRAME_UNEXPECTED = 0x105, H3_FRAME_ERROR = 0x106, H3_EXCESSIVE_LOAD = 0x107, H3_SETTINGS_ERROR = 0x109, H3_MISSING_SETTINGS = 0x10a,
            QPACK_DECOMPRESSION_FAILED = 0x200, QPACK_ENCODER_STREAM_ERROR = 0x201;
    private static final int MAX_CONTROL_PAYLOAD = 4096;
    private static final Set<String> CONNECTION_SPECIFIC = Set.of("connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade");

    private final QuicServer quic;
    private final HttpHandler handler;
    private final HttpConfig config;
    private final Map<QuicConnection, Conn> connections = new ConcurrentHashMap<>();
    private final io.hydrangea.Metrics metrics;

    private Http3Server(EventLoopGroup group, SocketAddress address, QuicConfig quicConfig, HttpHandler handler, HttpConfig config) throws IOException {
        this.handler = handler;
        this.config = config;
        this.metrics = group.metrics();
        QuicConfig qc = quicConfig.withAlpn(List.of("h3"));
        if (qc.maxUniStreams() < 3) {
            qc = qc.withMaxStreams(qc.maxBidiStreams(), 3);
        }
        this.quic = QuicServer.bind(group, address, qc, new Adapter());
    }

    /** Serves {@code handler} over HTTP/3 on a UDP port; {@code quic} supplies the certificate and transport limits (its ALPN is replaced by "h3"). */
    public static Http3Server bind(EventLoopGroup group, SocketAddress address, QuicConfig quic, HttpConfig config, HttpHandler handler) throws IOException {
        return new Http3Server(group, address, quic, handler, config);
    }

    public SocketAddress localAddress() {
        return quic.localAddress();
    }

    /** Connections with a finished handshake that have not ended. */
    public int activeConnections() {
        return connections.size();
    }

    @Override
    public void close() {
        quic.close();
    }

    // ---- errors ----

    /** A connection error: the QUIC connection is closed with this application error code. */
    private static final class H3Error extends Exception {
        final long code;

        H3Error(long code, String message) {
            super(message, null, false, false);
            this.code = code;
        }
    }

    // ---- varints ----

    private static void putVarint(ByteArrayOutputStream out, long v) {
        if (v < 1 << 6) {
            out.write((int) v);
        } else if (v < 1 << 14) {
            out.write(0x40 | (int) (v >> 8));
            out.write((int) v);
        } else if (v < 1 << 30) {
            out.write(0x80 | (int) (v >> 24));
            out.write((int) (v >> 16));
            out.write((int) (v >> 8));
            out.write((int) v);
        } else {
            out.write(0xc0 | (int) (v >> 56));
            for (int s = 48; s >= 0; s -= 8) out.write((int) (v >> s));
        }
    }

    private static void putFrame(ByteArrayOutputStream out, int type, byte[] payload) {
        putVarint(out, type);
        putVarint(out, payload.length);
        out.write(payload, 0, payload.length);
    }

    // ---- incremental frame reader shared by request streams and the control stream ----

    private abstract static class FrameReader {
        private byte[] pend = new byte[0];
        private int pos, lim;
        private boolean inFrame, discarding, passThrough;
        private long type, remaining;
        private final int payloadCap;
        private int varintLen;

        FrameReader(int payloadCap) {
            this.payloadCap = payloadCap;
        }

        /** A whole frame whose payload is buffered (everything except DATA). */
        abstract void frame(long type, byte[] payload, int off, int len) throws H3Error;

        /** A piece of a DATA frame. */
        abstract void data(byte[] b, int off, int len) throws H3Error;

        /** A frame type that is never legal on this kind of stream, checked before its payload is read. */
        abstract void checkType(long type) throws H3Error;

        /** A buffered frame bigger than the cap; its payload is skipped. */
        abstract void oversize(long type) throws H3Error;

        boolean midFrame() {
            return inFrame || lim > pos;
        }

        void feed(byte[] data) throws H3Error {
            if (pos == lim) {
                pend = data;
                pos = 0;
                lim = data.length;
            } else {
                byte[] n = new byte[lim - pos + data.length];
                System.arraycopy(pend, pos, n, 0, lim - pos);
                System.arraycopy(data, 0, n, lim - pos, data.length);
                pend = n;
                pos = 0;
                lim = n.length;
            }
            while (true) {
                if (!inFrame) {
                    long t = varint();
                    if (t < 0) break;
                    int save = pos;
                    long l = varint();
                    if (l < 0) {
                        pos = save - varintLen;
                        break;
                    }
                    type = t;
                    remaining = l;
                    checkType(t);
                    inFrame = true;
                    discarding = t != DATA && l > payloadCap;
                    passThrough = t == DATA;
                    if (discarding) {
                        oversize(t);
                    }
                }
                if (passThrough || discarding || isIgnorable(type)) {
                    int n = (int) Math.min(remaining, lim - pos);
                    if (passThrough && n > 0) {
                        data(pend, pos, n);
                    }
                    pos += n;
                    remaining -= n;
                    if (remaining == 0) {
                        inFrame = false;
                    } else {
                        break;
                    }
                } else {
                    if (lim - pos < remaining) break;
                    int len = (int) remaining;
                    int start = pos;
                    pos += len;
                    inFrame = false;
                    frame(type, pend, start, len);
                }
            }
            if (pos == lim) {
                pend = new byte[0];
                pos = lim = 0;
            }
        }

        /** Unknown types are skipped (RFC 9114 section 9); the known ones are buffered. */
        private boolean isIgnorable(long t) {
            return !(t == HEADERS || t == SETTINGS || t == GOAWAY || t == MAX_PUSH_ID || t == CANCEL_PUSH || t == PUSH_PROMISE);
        }

        /** Reads a varint at {@code pos}; -1 (and nothing consumed) if incomplete. */
        private long varint() {
            if (pos >= lim) return -1;
            int first = pend[pos] & 0xff;
            int len = 1 << (first >> 6);
            if (lim - pos < len) {
                return -1;
            }
            long v = first & 0x3f;
            for (int i = 1; i < len; i++) v = (v << 8) | (pend[pos + i] & 0xff);
            pos += len;
            varintLen = len;
            return v;
        }
    }

    // ---- per connection ----

    private final class Adapter implements QuicHandler {
        @Override
        public void onOpen(QuicConnection c) {
            Conn conn = new Conn(c);
            connections.put(c, conn);
            conn.open();
        }

        @Override
        public void onStreamData(QuicConnection c, long streamId, byte[] data, boolean fin) {
            Conn conn = connections.get(c);
            if (conn != null) {
                conn.stream(streamId, data, fin);
            }
        }

        @Override
        public void onStreamReset(QuicConnection c, long streamId, long errorCode) {
            Conn conn = connections.get(c);
            if (conn != null) {
                conn.reset(streamId);
            }
        }

        @Override
        public void onClose(QuicConnection c, long errorCode, boolean application, String reason) {
            Conn conn = connections.remove(c);
            if (conn != null) {
                conn.requests.values().forEach(Request::abandon);   // asked but never answered: no longer in flight
            }
        }
    }

    private final class Conn {
        final QuicConnection qc;
        final Map<Long, Request> requests = new HashMap<>();
        final Map<Long, Uni> unis = new HashMap<>();
        final Set<Long> uniTypesSeen = new HashSet<>();

        Conn(QuicConnection qc) {
            this.qc = qc;
        }

        void open() {
            long control = qc.openUniStream(), enc = qc.openUniStream(), dec = qc.openUniStream();
            if (control < 0 || enc < 0 || dec < 0) {
                qc.close(H3_STREAM_CREATION_ERROR, "peer allows too few unidirectional streams");
                return;
            }
            ByteArrayOutputStream settings = new ByteArrayOutputStream();
            putVarint(settings, 0x01);   // QPACK_MAX_TABLE_CAPACITY
            putVarint(settings, 0);
            putVarint(settings, 0x07);   // QPACK_BLOCKED_STREAMS
            putVarint(settings, 0);
            putVarint(settings, 0x06);   // MAX_FIELD_SECTION_SIZE
            putVarint(settings, config.maxHeaderBytes());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0x00);
            putFrame(out, SETTINGS, settings.toByteArray());
            qc.send(control, out.toByteArray(), false);
            qc.send(enc, new byte[]{0x02}, false);
            qc.send(dec, new byte[]{0x03}, false);
        }

        void fail(H3Error e) {
            LOG.log(Level.DEBUG, "http3 connection error {0}: {1}", Long.toHexString(e.code), e.getMessage());
            qc.close(e.code, e.getMessage());
        }

        void stream(long id, byte[] data, boolean fin) {
            try {
                if ((id & 3) == 0) {
                    Request r = requests.get(id);
                    if (r == null) {
                        r = new Request(this, id);
                        requests.put(id, r);
                    }
                    r.feed(data, fin);
                } else if ((id & 3) == 2) {
                    Uni u = unis.get(id);
                    if (u == null) {
                        u = new Uni(this, id);
                        unis.put(id, u);
                    }
                    u.feed(data, fin);
                } else {
                    throw new H3Error(H3_STREAM_CREATION_ERROR, "stream " + id + " is server initiated");
                }
            } catch (H3Error e) {
                fail(e);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "http3 stream handling failed", e);
                qc.close(H3_GENERAL_PROTOCOL_ERROR, "internal error");
            }
        }

        void reset(long id) {
            requests.remove(id);
            Uni u = unis.remove(id);
            if (u != null && u.critical()) {
                fail(new H3Error(H3_CLOSED_CRITICAL_STREAM, "critical stream reset"));
            }
        }
    }

    // ---- peer unidirectional streams ----

    private final class Uni {
        final Conn conn;
        final long id;
        long streamType = -1;
        FrameReader control;
        boolean settingsSeen;
        byte[] typeBuf = new byte[0];

        Uni(Conn conn, long id) {
            this.conn = conn;
            this.id = id;
        }

        boolean critical() {
            return streamType == 0 || streamType == 2 || streamType == 3;
        }

        void feed(byte[] data, boolean fin) throws H3Error {
            if (streamType < 0) {
                byte[] all = Arrays.copyOf(typeBuf, typeBuf.length + data.length);
                System.arraycopy(data, 0, all, typeBuf.length, data.length);
                if (all.length == 0) {
                    return;
                }
                int len = 1 << ((all[0] & 0xff) >> 6);
                if (all.length < len) {
                    typeBuf = all;
                    return;
                }
                long t = all[0] & 0x3f;
                for (int i = 1; i < len; i++) t = (t << 8) | (all[i] & 0xff);
                streamType = t;
                data = Arrays.copyOfRange(all, len, all.length);
                if (t == 1) {
                    throw new H3Error(H3_STREAM_CREATION_ERROR, "a client cannot push");
                }
                if ((t == 0 || t == 2 || t == 3) && !conn.uniTypesSeen.add(t)) {
                    throw new H3Error(H3_STREAM_CREATION_ERROR, "second stream of type " + t);
                }
                if (t == 0) {
                    control = new FrameReader(MAX_CONTROL_PAYLOAD) {
                        @Override
                        void frame(long type, byte[] p, int off, int len) throws H3Error {
                            controlFrame(type, p, off, len);
                        }

                        @Override
                        void data(byte[] b, int off, int len) throws H3Error {
                            throw new H3Error(H3_FRAME_UNEXPECTED, "DATA on the control stream");
                        }

                        @Override
                        void checkType(long type) throws H3Error {
                            if (!settingsSeen && type != SETTINGS) {
                                throw new H3Error(H3_MISSING_SETTINGS, "first control frame is not SETTINGS");
                            }
                            if (type == DATA || type == HEADERS || type == PUSH_PROMISE || type == 2 || type == 6 || type == 8 || type == 9
                                    || (settingsSeen && type == SETTINGS)) {
                                throw new H3Error(H3_FRAME_UNEXPECTED, "frame " + type + " on the control stream");
                            }
                        }

                        @Override
                        void oversize(long type) throws H3Error {
                            throw new H3Error(H3_EXCESSIVE_LOAD, "control frame too large");
                        }
                    };
                }
            }
            if (fin && critical()) {
                throw new H3Error(H3_CLOSED_CRITICAL_STREAM, "critical stream closed");
            }
            if (streamType == 0) {
                control.feed(data);
            } else if (streamType == 2) {
                for (byte b : data) {
                    if (b != 0x20) {   // only "set dynamic table capacity 0" is meaningful when the capacity is 0
                        throw new H3Error(QPACK_ENCODER_STREAM_ERROR, "encoder instruction with a disabled dynamic table");
                    }
                }
            }
            // decoder stream and unknown stream types: nothing to do
        }

        void controlFrame(long type, byte[] p, int off, int len) throws H3Error {
            if (type == SETTINGS) {
                settingsSeen = true;
                Set<Long> seen = new HashSet<>();
                int i = off, end = off + len;
                while (i < end) {
                    long[] k = readVarint(p, i, end);
                    long[] v = k == null ? null : readVarint(p, (int) k[1], end);
                    if (v == null) {
                        throw new H3Error(H3_FRAME_ERROR, "truncated SETTINGS");
                    }
                    i = (int) v[1];
                    if (k[0] == 0 || k[0] == 2 || k[0] == 3 || k[0] == 4 || k[0] == 5) {
                        throw new H3Error(H3_SETTINGS_ERROR, "HTTP/2 setting " + k[0]);
                    }
                    if (!seen.add(k[0])) {
                        throw new H3Error(H3_SETTINGS_ERROR, "duplicate setting " + k[0]);
                    }
                    if (k[0] == 0x01 && v[0] != 0) {
                        // the peer may keep a dynamic table for what it receives; we never send dynamic references, so nothing to do
                    }
                }
            }
            // GOAWAY, MAX_PUSH_ID, CANCEL_PUSH: we never push and do not open requests, so there is nothing to act on
        }
    }

    private static boolean isToken(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!HttpServerStage.isTokenChar(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** {value, next position}, or null if truncated. */
    private static long[] readVarint(byte[] b, int pos, int end) {
        if (pos >= end) return null;
        int len = 1 << ((b[pos] & 0xff) >> 6);
        if (end - pos < len) return null;
        long v = b[pos] & 0x3f;
        for (int i = 1; i < len; i++) v = (v << 8) | (b[pos + i] & 0xff);
        return new long[]{v, pos + len};
    }

    // ---- request streams ----

    private final class Request extends FrameReader {
        final Conn conn;
        final long id;
        int state;                       // 0: waiting for HEADERS, 1: head received, 2: trailers received
        boolean responded;
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        String method;
        String path;
        Headers headers;
        long contentLength = -1;
        long start;                      // the clock when the request was dispatched (0 = latency is not being recorded)
        final java.util.concurrent.atomic.AtomicBoolean inFlight = new java.util.concurrent.atomic.AtomicBoolean();

        Request(Conn conn, long id) {
            super(Math.max(config.maxHeaderBytes(), 1));
            this.conn = conn;
            this.id = id;
        }

        void feed(byte[] data, boolean fin) throws H3Error {
            feed(data);
            if (!fin) {
                return;
            }
            conn.requests.remove(id);
            if (midFrame()) {
                throw new H3Error(H3_FRAME_ERROR, "request stream ended inside a frame");
            }
            if (responded) {
                return;
            }
            if (state == 0) {
                respondError(400, "request without headers");
                return;
            }
            if (contentLength >= 0 && contentLength != body.size()) {
                respondError(400, "content-length does not match the body");
                return;
            }
            dispatch();
        }

        @Override
        void checkType(long type) throws H3Error {
            if (type == SETTINGS || type == GOAWAY || type == MAX_PUSH_ID || type == CANCEL_PUSH || type == PUSH_PROMISE || type == 2 || type == 6
                    || type == 8 || type == 9) {
                throw new H3Error(H3_FRAME_UNEXPECTED, "frame " + type + " on a request stream");
            }
            if (state == 0 && type == DATA) {
                throw new H3Error(H3_FRAME_UNEXPECTED, "DATA before HEADERS");
            }
            if (state == 2 && (type == DATA || type == HEADERS)) {
                throw new H3Error(H3_FRAME_UNEXPECTED, "frame after trailers");
            }
        }

        @Override
        void oversize(long type) throws H3Error {
            if (!responded) {
                respondError(431, "header section too large");
            }
        }

        @Override
        void data(byte[] b, int off, int len) {
            if (responded) {
                return;
            }
            if ((long) body.size() + len > config.maxBodyBytes()) {
                respondError(413, "request body too large");
                return;
            }
            body.write(b, off, len);
        }

        @Override
        void frame(long type, byte[] p, int off, int len) throws H3Error {
            if (type != HEADERS) {
                return;
            }
            List<Qpack.Field> fields;
            try {
                fields = Qpack.decode(p, off, len, config.maxHeaderBytes());
            } catch (QpackException e) {
                throw new H3Error(QPACK_DECOMPRESSION_FAILED, e.getMessage());
            }
            if (state == 0) {
                state = 1;
                if (!responded) {
                    parseHead(fields);
                }
            } else {
                state = 2;   // trailers: parsed for validity above, not exposed
            }
        }

        void parseHead(List<Qpack.Field> fields) {
            String m = null, scheme = null, p = null, authority = null;
            Headers h = new Headers();
            List<String> cookies = new ArrayList<>();
            boolean regular = false;
            for (Qpack.Field f : fields) {
                String name = f.name(), value = f.value();
                if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\0') >= 0) {
                    respondError(400, "illegal character in a field value");
                    return;
                }
                if (name.isEmpty() || !name.equals(Headers.lower(name))) {
                    respondError(400, "field names must be lower case");
                    return;
                }
                if (name.charAt(0) == ':') {
                    if (regular) {
                        respondError(400, "pseudo-header after a regular header");
                        return;
                    }
                    switch (name) {
                        case ":method" -> {
                            if (m != null) { respondError(400, "duplicate :method"); return; }
                            m = value;
                        }
                        case ":scheme" -> {
                            if (scheme != null) { respondError(400, "duplicate :scheme"); return; }
                            scheme = value;
                        }
                        case ":path" -> {
                            if (p != null) { respondError(400, "duplicate :path"); return; }
                            p = value;
                        }
                        case ":authority" -> {
                            if (authority != null) { respondError(400, "duplicate :authority"); return; }
                            authority = value;
                        }
                        default -> {
                            respondError(400, "unknown pseudo-header " + name);
                            return;
                        }
                    }
                    continue;
                }
                regular = true;
                if (CONNECTION_SPECIFIC.contains(name) || (name.equals("te") && !value.equalsIgnoreCase("trailers"))) {
                    respondError(400, "connection-specific header " + name);
                    return;
                }
                if (!isToken(name)) {
                    respondError(400, "illegal header name");
                    return;
                }
                if (name.equals("cookie")) {
                    cookies.add(value);
                } else if (name.equals("content-length")) {
                    try {
                        long cl = Long.parseLong(value.trim());
                        if (cl < 0 || (contentLength >= 0 && cl != contentLength)) throw new NumberFormatException();
                        contentLength = cl;
                    } catch (NumberFormatException e) {
                        respondError(400, "bad content-length");
                        return;
                    }
                    h.set(name, value);
                } else {
                    h.add(name, value);
                }
            }
            if (m == null || p == null || (scheme == null && !"CONNECT".equals(m)) || (p.isEmpty() && !"OPTIONS".equals(m))) {
                respondError(400, "missing or empty :method, :scheme or :path");
                return;
            }
            if (!cookies.isEmpty()) {
                h.set("cookie", String.join("; ", cookies));
            }
            if (authority != null && !h.contains("host")) {
                h.set("host", authority);
            }
            method = m;
            path = p;
            headers = h;
        }

        void dispatch() {
            if (headers == null) {
                return;
            }
            if (method.equals("CONNECT")) {
                respondError(501, "CONNECT is not supported over HTTP/3");
                return;
            }
            start = metrics.httpDuration().begin();
            if (inFlight.compareAndSet(false, true)) {
                metrics.recordHttpStart();
            }
            HttpRequest request = new HttpRequest(method, path, "HTTP/3", headers, body.toByteArray());
            CompletionStage<HttpResponse> stage;
            try {
                if (handler.wantsStream(request) || handler.wantsStreamingBody(request)) {
                    respondError(501, "streaming handlers are not supported over HTTP/3 yet");
                    return;
                }
                stage = handler.handle(request);
            } catch (Throwable t) {
                LOG.log(Level.DEBUG, "http3 handler failed", t);
                respondError(500, "Internal Server Error");
                return;
            }
            responded = true;
            stage.whenComplete((resp, err) -> {
                if (err != null || resp == null) {
                    if (err != null) {
                        LOG.log(Level.DEBUG, "http3 handler failed", err);
                    }
                    send(HttpResponse.status(500).text("Internal Server Error"), method.equals("HEAD"));
                } else if (resp.takeover() != null || resp.isHeadDeferred()) {
                    send(HttpResponse.status(501).text("streaming responses are not supported over HTTP/3 yet"), method.equals("HEAD"));
                } else {
                    send(resp, method.equals("HEAD"));
                }
            });
        }

        void respondError(int status, String message) {
            if (responded) {
                return;
            }
            responded = true;
            send(HttpResponse.status(status).text(message), method != null && method.equals("HEAD"));
        }

        /** The request ended without an answer (its connection closed). */
        void abandon() {
            if (inFlight.compareAndSet(true, false)) {
                metrics.recordHttpEnd();
            }
        }

        void send(HttpResponse resp, boolean head) {
            metrics.recordHttpResponse(resp.status(), start);
            if (inFlight.compareAndSet(true, false)) {
                metrics.recordHttpEnd();
            }
            List<Qpack.Field> fields = new ArrayList<>();
            fields.add(new Qpack.Field(":status", Integer.toString(resp.status())));
            for (Map.Entry<String, String> e : resp.headers().entries()) {
                String name = Headers.lower(e.getKey());
                if (CONNECTION_SPECIFIC.contains(name) || name.equals("content-length")) {
                    continue;
                }
                fields.add(new Qpack.Field(name, e.getValue()));
            }
            byte[] bytes = resp.body();
            boolean bodyless = resp.status() < 200 || resp.status() == 204 || resp.status() == 304;
            if (!bodyless) {
                fields.add(new Qpack.Field("content-length", Integer.toString(bytes.length)));
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length + 128);
            putFrame(out, HEADERS, Qpack.encode(fields));
            if (!bodyless && !head && bytes.length > 0) {
                putVarint(out, DATA);
                putVarint(out, bytes.length);
                out.write(bytes, 0, bytes.length);
            }
            conn.qc.send(id, out.toByteArray(), true);
        }
    }
}
