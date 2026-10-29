package io.hydrangea.quic;

import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.UdpSocket;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * One QUIC connection (RFC 9000) as seen by a server: packet protection, the three packet number spaces, loss detection and congestion control
 * (RFC 9002, NewReno), flow control, and streams. Everything runs on the connection's event-loop thread; {@link #send}, {@link #close} and
 * {@link #openUniStream} may be called from any thread.
 */
public final class QuicConnection {

    private static final Logger LOG = System.getLogger(QuicConnection.class.getName());

    /** -Dquic.trace=true prints every packet and recovery event to stderr (development aid). */
    private static final boolean TRACE = Boolean.getBoolean("quic.trace");

    private void trace(String what) {
        if (TRACE) {
            System.err.printf("%.3f %s %s%n", System.nanoTime() / 1e9, java.util.HexFormat.of().formatHex(scid, 0, 2), what);
        }
    }

    static final int MAX_DATAGRAM = 1200;
    private static final int INITIAL = 0, HANDSHAKE = 1, APP = 2;
    private static final int ACK_DELAY_EXPONENT = 3;
    private static final long MAX_ACK_DELAY_MS = 25;
    private static final long GRANULARITY_NANOS = 1_000_000L;
    private static final long INITIAL_RTT_NANOS = 333_000_000L;

    // transport error codes (RFC 9000 section 20.1)
    static final long NO_ERROR = 0, INTERNAL_ERROR = 1, FLOW_CONTROL_ERROR = 3, STREAM_LIMIT_ERROR = 4, STREAM_STATE_ERROR = 5, FINAL_SIZE_ERROR = 6,
            FRAME_ENCODING_ERROR = 7, TRANSPORT_PARAMETER_ERROR = 8, PROTOCOL_VIOLATION = 10;

    /** A connection-level error: closes the connection with this code. */
    static final class QuicError extends Exception {
        final long code;

        QuicError(long code, String message) {
            super(message);
            this.code = code;
        }
    }

    // ---- bookkeeping records ----

    private record CryptoChunk(long offset, int length) {
    }

    private record StreamChunk(long id, long offset, int length, boolean fin) {
    }

    /** A small frame that is sent again if its packet is lost: HANDSHAKE_DONE, MAX_DATA, MAX_STREAM_DATA, MAX_STREAMS. */
    private record Control(int type, long a, long b) {
    }

    private static final class Sent {
        final long pn;
        final long time;
        int size;
        boolean ackEliciting;
        boolean inFlight;
        boolean requeued;
        final List<Object> frames = new ArrayList<>();

        Sent(long pn, long time) {
            this.pn = pn;
            this.time = time;
        }
    }

    private static final class Space {
        long nextPn;
        long largestAcked = -1;
        long largestRecv = -1;
        long largestRecvTime;
        final Ranges received = new Ranges();
        boolean ackPending;
        final TreeMap<Long, Sent> sent = new TreeMap<>();
        long lossTimeNanos;           // 0 = none
        long lastAckElicitingSent;    // nanoTime
        int probes;                   // ack-eliciting probe packets still owed
        boolean discarded;
        // CRYPTO out
        byte[] cryptoBuf = new byte[0];
        long cryptoSentUpTo;
        final Ranges cryptoAcked = new Ranges();
        final ArrayList<long[]> cryptoRetx = new ArrayList<>();
        // CRYPTO in
        long cryptoRecvOffset;
        final TreeMap<Long, byte[]> cryptoRecvPending = new TreeMap<>();
    }

    private static final class Stream {
        final long id;
        // receive
        long recvDelivered;
        final TreeMap<Long, byte[]> recvPending = new TreeMap<>();
        long recvMax;
        long recvLimit;
        long finalSize = -1;
        boolean finDelivered;
        boolean resetByPeer;
        // send
        byte[] sendBuf = new byte[0];
        long sendBase;
        int sendLen;
        long sendNext;
        boolean finQueued;
        boolean finNeeded;
        boolean finAcked;
        final Ranges sendAcked = new Ranges();
        final ArrayList<long[]> retx = new ArrayList<>();
        long peerLimit;

        Stream(long id) {
            this.id = id;
        }

        long sendEnd() {
            return sendBase + sendLen;
        }
    }

    // ---- identity ----

    private final QuicServer server;
    private final UdpSocket socket;
    private final EventLoop loop;
    private final QuicConfig config;
    private final QuicHandler handler;
    private final SocketAddress peer;
    final byte[] scid;
    private final byte[] odcid;
    private final boolean retried;
    private byte[] dcid;
    private final Tls13Server tls;

    // ---- crypto state ----
    private final QuicCrypto.Keys[] rx = new QuicCrypto.Keys[3];
    private final QuicCrypto.Keys[] tx = new QuicCrypto.Keys[3];
    private QuicCrypto.Keys rxPrevious;
    private int keyPhase;
    private final Space[] spaces = {new Space(), new Space(), new Space()};
    private QuicCrypto.Keys pendingClientApp, pendingServerApp;

    // ---- state ----
    private boolean handshakeComplete;
    private boolean handshakeDoneQueued;
    private boolean addressValidated;
    private boolean opened;
    private boolean discardInitialAfterFlush, discardHandshakeAfterFlush;
    private enum State { OPEN, CLOSING, DRAINING, CLOSED }
    private State state = State.OPEN;
    private long closeCode;
    private boolean closeApplication;
    private String closeReason = "";
    private long lastCloseSent;
    private long drainDeadline;
    private boolean closeNotified;

    private long bytesReceived, bytesSent;
    private long lastActivity;
    private long idleNanos;

    // peer transport parameters
    private long peerMaxData, peerInitialStreamBidiLocal, peerInitialStreamBidiRemote, peerInitialStreamUni;
    private long peerMaxStreamsBidi, peerMaxStreamsUni, peerMaxUdpPayload = 65527, peerMaxIdleMillis;
    private int peerAckDelayExponent = 3;
    private long peerMaxAckDelayMs = 25;

    // flow control (ours)
    private long recvMaxData;
    private long connRecvTotal;
    private long connDelivered;
    private long maxStreamsBidi, maxStreamsUni;
    private long peerBidiOpened, peerUniOpened;
    private long dataSent;
    private long nextServerUni = 3, nextServerBidi = 1;

    private final Map<Long, Stream> streams = new HashMap<>();
    private final Ranges retiredBidi = new Ranges(), retiredUni = new Ranges();   // indexes of finished peer-initiated streams: late copies of their frames are ignored
    private final ArrayList<Long> streamOrder = new ArrayList<>();
    private int streamCursor;
    private final ArrayDeque<Control> pendingControl = new ArrayDeque<>();
    private final ArrayDeque<Long> pathResponses = new ArrayDeque<>();

    // recovery
    private long latestRtt, minRtt = Long.MAX_VALUE, smoothedRtt = 0, rttVar;
    private boolean haveRtt;
    private boolean receiving;
    private int ptoCount;
    private long bytesInFlight;
    private long cwnd = Math.min(10L * MAX_DATAGRAM, Math.max(2L * MAX_DATAGRAM, 14720));
    private long ssthresh = Long.MAX_VALUE;
    private long recoveryStart;
    private Scheduled timer;
    private long timerDeadline;

    QuicConnection(QuicServer server, UdpSocket socket, QuicConfig config, QuicHandler handler, SocketAddress peer, byte[] scid, byte[] odcid, byte[] initialDcid, boolean retried, byte[] clientScid) {
        this.server = server;
        this.socket = socket;
        this.loop = socket.eventLoop();
        this.config = config;
        this.handler = handler;
        this.peer = peer;
        this.scid = scid;
        this.odcid = odcid;
        this.retried = retried;
        this.dcid = clientScid;
        this.recvMaxData = config.connectionWindow();
        this.maxStreamsBidi = config.maxBidiStreams();
        this.maxStreamsUni = config.maxUniStreams();
        this.idleNanos = config.idleTimeout().toNanos();
        QuicCrypto.Keys[] initial = QuicCrypto.initialKeys(initialDcid);
        rx[INITIAL] = initial[0];
        tx[INITIAL] = initial[1];
        this.lastActivity = System.nanoTime();
        this.tls = new Tls13Server(config.privateKey(), config.certificateChain(), config.alpn(), encodeTransportParameters(), new Tls13Server.Output() {
            @Override
            public void crypto(Tls13Server.Level level, byte[] data) {
                Space s = spaces[level.ordinal()];
                byte[] n = Arrays.copyOf(s.cryptoBuf, s.cryptoBuf.length + data.length);
                System.arraycopy(data, 0, n, s.cryptoBuf.length, data.length);
                s.cryptoBuf = n;
            }

            @Override
            public void handshakeKeys(QuicCrypto.Keys client, QuicCrypto.Keys serverKeys) {
                rx[HANDSHAKE] = client;
                tx[HANDSHAKE] = serverKeys;
            }

            @Override
            public void applicationKeys(QuicCrypto.Keys client, QuicCrypto.Keys serverKeys) {
                pendingClientApp = client;
                tx[APP] = serverKeys;
            }

            @Override
            public void handshakeComplete() {
                QuicConnection.this.onHandshakeComplete();
            }
        });
    }

    // ---- public API ----

    byte[] originalDestinationConnectionId() {
        return odcid;
    }

    public SocketAddress remoteAddress() {
        return peer;
    }

    public String alpn() {
        return tls.alpn();
    }

    public String serverName() {
        return tls.serverName();
    }

    public boolean isOpen() {
        return state == State.OPEN;
    }

    /** Queues bytes on a stream (the peer's bidirectional streams, or one from {@link #openUniStream}); {@code fin} ends the stream. Any thread. */
    public void send(long streamId, byte[] data, boolean fin) {
        byte[] copy = data.clone();
        if (loop.inEventLoop()) {
            doSend(streamId, copy, fin);
        } else {
            loop.execute(() -> doSend(streamId, copy, fin));
        }
    }

    /** A new unidirectional stream to the peer (HTTP/3 control and QPACK streams); -1 if the peer's limit is reached. Loop thread. */
    public long openUniStream() {
        if ((nextServerUni >> 2) >= peerMaxStreamsUni) {
            return -1;
        }
        long id = nextServerUni;
        nextServerUni += 4;
        stream(id);
        return id;
    }

    /** Ends the connection with an application error code (0 = clean). Any thread. */
    public void close(long errorCode, String reason) {
        Runnable r = () -> startClosing(errorCode, true, reason == null ? "" : reason);
        if (loop.inEventLoop()) {
            r.run();
        } else {
            loop.execute(r);
        }
    }

    // ---- transport parameters (RFC 9000 section 18) ----

    private byte[] encodeTransportParameters() {
        QuicWire.Writer w = new QuicWire.Writer(128);
        param(w, 0x00, odcid);
        param(w, 0x01, varint(config.idleTimeout().toMillis()));
        param(w, 0x02, randomBytes(16));                    // stateless_reset_token (not used to reset, but a server may send one)
        param(w, 0x03, varint(1472));
        param(w, 0x04, varint(config.connectionWindow()));
        param(w, 0x05, varint(config.streamWindow()));      // initial_max_stream_data_bidi_local
        param(w, 0x06, varint(config.streamWindow()));      // initial_max_stream_data_bidi_remote
        param(w, 0x07, varint(config.streamWindow()));      // initial_max_stream_data_uni
        param(w, 0x08, varint(config.maxBidiStreams()));
        param(w, 0x09, varint(config.maxUniStreams()));
        param(w, 0x0c, new byte[0]);                        // disable_active_migration
        param(w, 0x0e, varint(2));                          // active_connection_id_limit
        param(w, 0x0f, scid);                               // initial_source_connection_id
        if (retried) {
            param(w, 0x10, scid);                           // retry_source_connection_id
        }
        return w.toByteArray();
    }

    private static byte[] varint(long v) {
        return new QuicWire.Writer(8).varint(v).toByteArray();
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        new java.security.SecureRandom().nextBytes(b);
        return b;
    }

    private static void param(QuicWire.Writer w, long id, byte[] value) {
        w.varint(id).varint(value.length).bytes(value);
    }

    private void parsePeerParameters(byte[] raw) throws QuicError {
        try {
            QuicWire.Reader r = new QuicWire.Reader(raw, 0, raw.length);
            boolean sawInitialScid = false;
            while (r.more()) {
                long id = r.varint();
                long len = r.varint();
                if (len > r.remaining()) {
                    throw new QuicWire.Malformed();
                }
                int end = r.pos + (int) len;
                QuicWire.Reader v = new QuicWire.Reader(raw, r.pos, end);
                r.skip(len);
                if (id == 0x01) peerMaxIdleMillis = v.varint();
                else if (id == 0x03) peerMaxUdpPayload = v.varint();
                else if (id == 0x04) peerMaxData = v.varint();
                else if (id == 0x05) peerInitialStreamBidiLocal = v.varint();
                else if (id == 0x06) peerInitialStreamBidiRemote = v.varint();
                else if (id == 0x07) peerInitialStreamUni = v.varint();
                else if (id == 0x08) peerMaxStreamsBidi = v.varint();
                else if (id == 0x09) peerMaxStreamsUni = v.varint();
                else if (id == 0x0a) peerAckDelayExponent = (int) v.varint();
                else if (id == 0x0b) peerMaxAckDelayMs = v.varint();
                else if (id == 0x0f) sawInitialScid = true;
                else if (id == 0x00 || id == 0x02 || id == 0x10) throw new QuicError(TRANSPORT_PARAMETER_ERROR, "client sent a server-only transport parameter");
            }
            if (!sawInitialScid) {
                throw new QuicError(TRANSPORT_PARAMETER_ERROR, "missing initial_source_connection_id");
            }
            if (peerAckDelayExponent > 20 || peerMaxAckDelayMs >= (1 << 14) || peerMaxUdpPayload < 1200) {
                throw new QuicError(TRANSPORT_PARAMETER_ERROR, "invalid transport parameter value");
            }
            if (peerMaxIdleMillis > 0) {
                idleNanos = Math.min(idleNanos, TimeUnit.MILLISECONDS.toNanos(peerMaxIdleMillis));
            }
        } catch (QuicWire.Malformed e) {
            throw new QuicError(TRANSPORT_PARAMETER_ERROR, "malformed transport parameters");
        }
    }

    // ---- receiving ----

    /** A datagram for this connection. */
    void onDatagram(byte[] dg, SocketAddress from) {
        if (state == State.CLOSED) {
            return;
        }
        bytesReceived += dg.length;
        boolean progress = false;
        receiving = true;   // sends made by handlers while the datagram is processed wait for afterReceive, so they share a datagram with the ACK
        try {
            int off = 0;
            while (off < dg.length) {
                int used = onPacket(dg, off);
                if (used <= 0) {
                    break;
                }
                progress = true;
                off += used;
            }
        } catch (QuicError e) {
            startClosing(e.code, false, e.getMessage());
        } catch (Tls13Server.TlsAlert a) {
            startClosing(0x100 + a.alert, false, a.getMessage());
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "quic packet handling failed", e);
            startClosing(INTERNAL_ERROR, false, "internal error");
        } finally {
            receiving = false;
        }
        if (progress) {
            lastActivity = System.nanoTime();
        }
        if (state == State.CLOSING) {
            long now = System.nanoTime();
            if (now - lastCloseSent > 50_000_000L) {
                sendClose();
            }
        } else if (state == State.OPEN) {
            afterReceive();
        }
        rearm();
    }

    private void afterReceive() {
        flush();
        if (discardInitialAfterFlush) {
            discardInitialAfterFlush = false;
            discard(INITIAL);
        }
        if (discardHandshakeAfterFlush) {
            discardHandshakeAfterFlush = false;
            discard(HANDSHAKE);
        }
    }

    private void discard(int level) {
        Space s = spaces[level];
        if (s.discarded) {
            return;
        }
        s.discarded = true;
        for (Sent p : s.sent.values()) {
            if (p.inFlight) {
                bytesInFlight -= p.size;
            }
        }
        s.sent.clear();
        s.lossTimeNanos = 0;
        s.cryptoRetx.clear();
        rx[level] = null;
        tx[level] = null;
    }

    /** Processes one packet starting at {@code off}; returns its length in the datagram, or -1 to drop the rest. */
    private int onPacket(byte[] dg, int off) throws QuicError, Tls13Server.TlsAlert {
        int b0 = dg[off] & 0xff;
        boolean longHeader = (b0 & 0x80) != 0;
        int level;
        int pnOffset;
        int packetEnd;
        if (longHeader) {
            QuicWire.Reader r = new QuicWire.Reader(dg, off + 1, dg.length);
            try {
                long version = r.u32();
                int dl = r.u8();
                r.skip(dl);
                int sl = r.u8();
                r.skip(sl);
                int type = (b0 >> 4) & 3;
                if (version != 1) {
                    return -1;
                }
                if (type == 0) {
                    long tokenLen = r.varint();
                    r.skip(tokenLen);
                    level = INITIAL;
                } else if (type == 2) {
                    level = HANDSHAKE;
                } else {
                    // 0-RTT (we do not accept it) and Retry (clients do not send it): skip over the packet when it has a length
                    if (type == 1) {
                        long len = r.varint();
                        r.skip(len);
                        return r.pos - off;
                    }
                    return -1;
                }
                long length = r.varint();
                pnOffset = r.pos;
                if (length > dg.length - pnOffset || length < 4 + 16) {
                    return -1;
                }
                packetEnd = pnOffset + (int) length;
            } catch (QuicWire.Malformed e) {
                return -1;
            }
        } else {
            level = APP;
            pnOffset = off + 1 + scid.length;
            packetEnd = dg.length;
            if (pnOffset + 4 + 16 > packetEnd) {
                return -1;
            }
        }
        Space space = spaces[level];
        QuicCrypto.Keys keys = rx[level];
        if (space.discarded || keys == null) {
            return packetEnd - off;
        }
        // remove header protection
        byte[] mask = keys.mask(dg, pnOffset + 4);
        int first = b0 ^ (mask[0] & (longHeader ? 0x0f : 0x1f));
        int pnLen = (first & 3) + 1;
        if (pnOffset + pnLen > packetEnd) {
            return -1;
        }
        long truncated = 0;
        for (int i = 0; i < pnLen; i++) {
            truncated = (truncated << 8) | ((dg[pnOffset + i] ^ mask[1 + i]) & 0xff);
        }
        long pn = decodePacketNumber(space.largestRecv, truncated, pnLen * 8);
        byte[] header = Arrays.copyOfRange(dg, off, pnOffset + pnLen);
        header[0] = (byte) first;
        for (int i = 0; i < pnLen; i++) {
            header[(pnOffset - off) + i] = (byte) (truncated >> (8 * (pnLen - 1 - i)));
        }
        int payloadOffset = pnOffset + pnLen;
        int payloadLength = packetEnd - payloadOffset;
        byte[] plain = null;
        boolean rotated = false;
        if (level == APP) {
            int phase = (first >> 2) & 1;
            if (phase == keyPhase) {
                plain = keys.open(pn, header, dg, payloadOffset, payloadLength);
            } else {
                if (rxPrevious != null) {
                    plain = rxPrevious.open(pn, header, dg, payloadOffset, payloadLength);
                }
                if (plain == null) {
                    QuicCrypto.Keys next = keys.next();
                    plain = next.open(pn, header, dg, payloadOffset, payloadLength);
                    if (plain != null) {
                        rxPrevious = keys;
                        rx[APP] = next;
                        tx[APP] = tx[APP].next();
                        keyPhase ^= 1;
                        rotated = true;
                    }
                }
            }
        } else {
            plain = keys.open(pn, header, dg, payloadOffset, payloadLength);
        }
        if (plain == null) {
            return packetEnd - off;      // not authentic (or a copy that arrived after we dropped the keys): ignore silently
        }
        if (space.received.contains(pn)) {
            return packetEnd - off;      // duplicate
        }
        space.received.add(pn, pn + 1);
        if (pn > space.largestRecv) {
            space.largestRecv = pn;
            space.largestRecvTime = System.nanoTime();
        }
        if (level == HANDSHAKE) {
            addressValidated = true;
            discardInitialAfterFlush = true;
        }
        if (level == APP && !handshakeComplete) {
            return packetEnd - off;      // cannot happen: rx[APP] is installed when the handshake completes
        }
        trace("rx " + "IHA".charAt(level) + " pn=" + pn + " len=" + plain.length);
        boolean ackEliciting = processFrames(level, plain);
        if (ackEliciting) {
            space.ackPending = true;
        }
        return packetEnd - off;
    }

    static long decodePacketNumber(long largest, long truncated, int bits) {
        long expected = largest + 1;
        long win = 1L << bits;
        long half = win / 2;
        long mask = win - 1;
        long candidate = (expected & ~mask) | truncated;
        if (candidate <= expected - half && candidate < (1L << 62) - win) {
            return candidate + win;
        }
        if (candidate > expected + half && candidate >= win) {
            return candidate - win;
        }
        return candidate;
    }

    // ---- frames ----

    private boolean processFrames(int level, byte[] payload) throws QuicError, Tls13Server.TlsAlert {
        QuicWire.Reader r = new QuicWire.Reader(payload, 0, payload.length);
        boolean ackEliciting = false;
        try {
            while (r.more()) {
                long type = r.varint();
                if (level != APP && !(type == 0 || type == 1 || type == 2 || type == 3 || type == 6 || type == 0x1c)) {
                    throw new QuicError(PROTOCOL_VIOLATION, "frame type " + type + " not allowed at this encryption level");
                }
                if (type == 0x00) {
                    continue;
                }
                if (type != 0x02 && type != 0x03 && type != 0x1c && type != 0x1d) {
                    ackEliciting = true;
                }
                if (type == 0x01) {
                    // PING
                } else if (type == 0x02 || type == 0x03) {
                    ackFrame(level, r, type == 0x03);
                } else if (type == 0x04) {
                    long id = r.varint();
                    long code = r.varint();
                    long finalSize = r.varint();
                    resetStream(id, code, finalSize);
                } else if (type == 0x05) {
                    r.varint();
                    r.varint();
                } else if (type == 0x06) {
                    long off = r.varint();
                    long len = r.varint();
                    if (len > r.remaining()) {
                        throw new QuicWire.Malformed();
                    }
                    cryptoFrame(level, off, r.bytes(len));
                } else if (type == 0x07 || type == 0x1e) {
                    throw new QuicError(PROTOCOL_VIOLATION, "a client cannot send this frame");
                } else if (type >= 0x08 && type <= 0x0f) {
                    streamFrame((int) type, r);
                } else if (type == 0x10) {
                    peerMaxData = Math.max(peerMaxData, r.varint());
                } else if (type == 0x11) {
                    long id = r.varint();
                    long limit = r.varint();
                    Stream s = streams.get(id);
                    if (s != null) {
                        s.peerLimit = Math.max(s.peerLimit, limit);
                    }
                } else if (type == 0x12) {
                    peerMaxStreamsBidi = Math.max(peerMaxStreamsBidi, r.varint());
                } else if (type == 0x13) {
                    peerMaxStreamsUni = Math.max(peerMaxStreamsUni, r.varint());
                } else if (type == 0x14) {
                    r.varint();
                } else if (type == 0x15) {
                    r.varint();
                    r.varint();
                } else if (type == 0x16 || type == 0x17) {
                    r.varint();
                } else if (type == 0x18) {
                    r.varint();
                    r.varint();
                    int l = r.u8();
                    if (l < 1 || l > 20) {
                        throw new QuicError(FRAME_ENCODING_ERROR, "bad connection id length");
                    }
                    r.skip(l + 16);
                } else if (type == 0x19) {
                    r.varint();
                } else if (type == 0x1a) {
                    long challenge = 0;
                    byte[] data = r.bytes(8);
                    for (byte b : data) {
                        challenge = (challenge << 8) | (b & 0xff);
                    }
                    pathResponses.add(challenge);
                } else if (type == 0x1b) {
                    r.skip(8);
                } else if (type == 0x1c || type == 0x1d) {
                    long code = r.varint();
                    if (type == 0x1c) {
                        r.varint();
                    }
                    long rl = r.varint();
                    String reason = new String(r.bytes(rl), StandardCharsets.UTF_8);
                    peerClosed(code, type == 0x1d, reason);
                    return ackEliciting;
                } else {
                    throw new QuicError(FRAME_ENCODING_ERROR, "unknown frame type " + type);
                }
            }
        } catch (QuicWire.Malformed e) {
            throw new QuicError(FRAME_ENCODING_ERROR, "malformed frame");
        }
        return ackEliciting;
    }

    private void peerClosed(long code, boolean application, String reason) {
        if (state == State.OPEN || state == State.CLOSING) {
            state = State.DRAINING;
            closeCode = code;
            closeApplication = application;
            closeReason = reason;
            drainDeadline = System.nanoTime() + 3 * Math.max(ptoNanos(APP), 100_000_000L);
            notifyClosed();
        }
    }

    private void notifyClosed() {
        if (closeNotified) {
            return;
        }
        closeNotified = true;
        try {
            handler.onClose(this, closeCode, closeApplication, closeReason);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "onClose failed", t);
        }
    }

    private void cryptoFrame(int level, long off, byte[] data) throws QuicError, Tls13Server.TlsAlert {
        Space s = spaces[level];
        if (off + data.length > 1 << 20) {
            throw new QuicError(0x0d, "too much CRYPTO data");   // CRYPTO_BUFFER_EXCEEDED
        }
        if (off + data.length > s.cryptoRecvOffset) {
            byte[] old = s.cryptoRecvPending.get(off);
            if (old == null || old.length < data.length) {
                s.cryptoRecvPending.put(off, data);
            }
        }
        for (Map.Entry<Long, byte[]> e = s.cryptoRecvPending.firstEntry(); e != null && e.getKey() <= s.cryptoRecvOffset; e = s.cryptoRecvPending.firstEntry()) {
            s.cryptoRecvPending.remove(e.getKey());
            long skip = s.cryptoRecvOffset - e.getKey();
            if (e.getValue().length > skip) {
                byte[] chunk = Arrays.copyOfRange(e.getValue(), (int) skip, e.getValue().length);
                s.cryptoRecvOffset += chunk.length;
                Tls13Server.Level tlsLevel = Tls13Server.Level.values()[level];
                boolean had = tls.peerTransportParameters() != null;
                tls.onCrypto(tlsLevel, chunk);
                if (!had && tls.peerTransportParameters() != null) {
                    parsePeerParameters(tls.peerTransportParameters());
                }
            }
        }
    }

    private void onHandshakeComplete() {
        handshakeComplete = true;
        addressValidated = true;
        rx[APP] = pendingClientApp;
        handshakeDoneQueued = true;
        pendingControl.add(new Control(0x1e, 0, 0));
        discardHandshakeAfterFlush = true;
        opened = true;
        try {
            handler.onOpen(this);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "onOpen failed", t);
            startClosing(INTERNAL_ERROR, false, "internal error");
        }
    }

    // ---- streams ----

    private Stream stream(long id) {
        Stream s = streams.get(id);
        if (s == null) {
            s = new Stream(id);
            boolean clientInitiated = (id & 1) == 0;
            boolean bidi = (id & 2) == 0;
            s.recvLimit = config.streamWindow();
            // RFC 9000 section 18.2: the peer's "bidi_local" limits the streams the peer opened, "bidi_remote" those we opened
            if (clientInitiated) {
                s.peerLimit = bidi ? peerInitialStreamBidiLocal : 0;
            } else {
                s.peerLimit = bidi ? peerInitialStreamBidiRemote : peerInitialStreamUni;
            }
            streams.put(id, s);
            streamOrder.add(id);
        }
        return s;
    }

    private void streamFrame(int type, QuicWire.Reader r) throws QuicError {
        long id = r.varint();
        long off = (type & 0x04) != 0 ? r.varint() : 0;
        long len = (type & 0x02) != 0 ? r.varint() : r.remaining();
        boolean fin = (type & 0x01) != 0;
        if (len > r.remaining()) {
            throw new QuicWire.Malformed();
        }
        byte[] data = r.bytes(len);
        boolean clientInitiated = (id & 1) == 0;
        boolean bidi = (id & 2) == 0;
        if (!clientInitiated && !bidi) {
            throw new QuicError(STREAM_STATE_ERROR, "STREAM frame for a stream the server opened for sending only");
        }
        if (!clientInitiated) {
            if (!streams.containsKey(id)) {
                throw new QuicError(STREAM_STATE_ERROR, "STREAM frame for an unopened server stream");
            }
        } else {
            long index = id >> 2;
            long limit = bidi ? maxStreamsBidi : maxStreamsUni;
            if (index >= limit) {
                throw new QuicError(STREAM_LIMIT_ERROR, "stream limit exceeded");
            }
            if ((bidi ? retiredBidi : retiredUni).contains(index)) {
                return;   // a late copy of a frame of a stream that is already finished
            }
            if (!streams.containsKey(id)) {
                if (bidi) peerBidiOpened = Math.max(peerBidiOpened, index + 1);
                else peerUniOpened = Math.max(peerUniOpened, index + 1);
            }
        }
        if (off + len >= (1L << 62)) {
            throw new QuicError(FRAME_ENCODING_ERROR, "stream offset too large");
        }
        Stream s = stream(id);
        if (s.resetByPeer) {
            return;
        }
        long end = off + len;
        if (end > s.recvLimit) {
            throw new QuicError(FLOW_CONTROL_ERROR, "stream flow control limit exceeded");
        }
        if (s.finalSize != -1 && (end > s.finalSize || (fin && end != s.finalSize))) {
            throw new QuicError(FINAL_SIZE_ERROR, "data beyond the final size");
        }
        if (fin) {
            if (end < s.recvMax) {
                throw new QuicError(FINAL_SIZE_ERROR, "final size below data already received");
            }
            s.finalSize = end;
        }
        if (end > s.recvMax) {
            connRecvTotal += end - s.recvMax;
            if (connRecvTotal > recvMaxData) {
                throw new QuicError(FLOW_CONTROL_ERROR, "connection flow control limit exceeded");
            }
            s.recvMax = end;
        }
        if (len > 0 && end > s.recvDelivered) {
            byte[] old = s.recvPending.get(off);
            if (old == null || old.length < data.length) {
                s.recvPending.put(off, data);
            }
        }
        deliver(s);
    }

    private void deliver(Stream s) {
        List<byte[]> chunks = new ArrayList<>();
        for (Map.Entry<Long, byte[]> e = s.recvPending.firstEntry(); e != null && e.getKey() <= s.recvDelivered; e = s.recvPending.firstEntry()) {
            s.recvPending.remove(e.getKey());
            long skip = s.recvDelivered - e.getKey();
            if (e.getValue().length > skip) {
                byte[] chunk = skip == 0 ? e.getValue() : Arrays.copyOfRange(e.getValue(), (int) skip, e.getValue().length);
                s.recvDelivered += chunk.length;
                chunks.add(chunk);
            }
        }
        boolean finNow = s.finalSize != -1 && s.recvDelivered == s.finalSize && !s.finDelivered;
        if (finNow) {
            s.finDelivered = true;
        }
        long delivered = 0;
        for (int i = 0; i < chunks.size(); i++) {
            boolean last = i == chunks.size() - 1;
            delivered += chunks.get(i).length;
            callData(s.id, chunks.get(i), finNow && last);
        }
        if (finNow && chunks.isEmpty()) {
            callData(s.id, new byte[0], true);
        }
        connDelivered += delivered;
        if (recvMaxData - connDelivered < config.connectionWindow() / 2) {
            recvMaxData = connDelivered + config.connectionWindow();
            pendingControl.add(new Control(0x10, recvMaxData, 0));
        }
        if (!s.finDelivered && s.recvLimit - s.recvDelivered < config.streamWindow() / 2) {
            s.recvLimit = s.recvDelivered + config.streamWindow();
            pendingControl.add(new Control(0x11, s.id, s.recvLimit));
        }
        maybeRetire(s);
    }

    private void callData(long id, byte[] data, boolean fin) {
        try {
            handler.onStreamData(this, id, data, fin);
        } catch (Throwable t) {
            LOG.log(Level.WARNING, "onStreamData failed", t);
            startClosing(INTERNAL_ERROR, false, "internal error");
        }
    }

    private void resetStream(long id, long code, long finalSize) throws QuicError {
        boolean clientInitiated = (id & 1) == 0;
        if (!clientInitiated && (id & 2) != 0) {
            throw new QuicError(STREAM_STATE_ERROR, "RESET_STREAM for a send-only stream");
        }
        if (clientInitiated) {
            long index = id >> 2;
            if (index >= ((id & 2) == 0 ? maxStreamsBidi : maxStreamsUni)) {
                throw new QuicError(STREAM_LIMIT_ERROR, "stream limit exceeded");
            }
            if (((id & 2) == 0 ? retiredBidi : retiredUni).contains(index)) {
                return;
            }
        }
        Stream s = stream(id);
        if (s.finalSize != -1 && s.finalSize != finalSize) {
            throw new QuicError(FINAL_SIZE_ERROR, "RESET_STREAM changes the final size");
        }
        if (finalSize > s.recvLimit) {
            throw new QuicError(FLOW_CONTROL_ERROR, "RESET_STREAM beyond the stream limit");
        }
        if (finalSize > s.recvMax) {
            connRecvTotal += finalSize - s.recvMax;
            if (connRecvTotal > recvMaxData) {
                throw new QuicError(FLOW_CONTROL_ERROR, "connection flow control limit exceeded");
            }
            s.recvMax = finalSize;
        }
        if (!s.resetByPeer) {
            s.resetByPeer = true;
            s.finalSize = finalSize;
            s.recvPending.clear();
            // the bytes the peer will never send still count as consumed, or the connection window would shrink for good
            connDelivered += Math.max(0, finalSize - s.recvDelivered);
            s.recvDelivered = finalSize;
            try {
                handler.onStreamReset(this, id, code);
            } catch (Throwable t) {
                LOG.log(Level.WARNING, "onStreamReset failed", t);
            }
            maybeRetire(s);
        }
    }

    /** A peer-initiated stream with both directions finished frees one slot of the peer's stream limit. */
    private void maybeRetire(Stream s) {
        boolean clientInitiated = (s.id & 1) == 0;
        boolean bidi = (s.id & 2) == 0;
        boolean recvDone = s.finDelivered || s.resetByPeer;
        boolean sendDone = !bidi || (s.finQueued && s.finAcked && s.sendLen == 0);   // the FIN can be acknowledged before earlier data that was lost
        if (clientInitiated && recvDone && sendDone && streams.remove(s.id) != null) {
            streamOrder.remove(Long.valueOf(s.id));
            (bidi ? retiredBidi : retiredUni).add(s.id >> 2, (s.id >> 2) + 1);
            if (bidi) {
                maxStreamsBidi++;
                pendingControl.add(new Control(0x12, maxStreamsBidi, 0));
            } else {
                maxStreamsUni++;
                pendingControl.add(new Control(0x13, maxStreamsUni, 0));
            }
        }
    }

    private void doSend(long id, byte[] data, boolean fin) {
        if (state != State.OPEN) {
            return;
        }
        Stream s = streams.get(id);
        if (s == null || ((id & 3) == 2)) {
            return;   // unknown stream, or one the peer opened for sending only
        }
        if (s.finQueued) {
            return;
        }
        if (data.length > 0) {
            if (s.sendLen + data.length > s.sendBuf.length) {
                s.sendBuf = Arrays.copyOf(s.sendBuf, Math.max(s.sendBuf.length * 2, s.sendLen + data.length));
            }
            System.arraycopy(data, 0, s.sendBuf, s.sendLen, data.length);
            s.sendLen += data.length;
        }
        if (fin) {
            s.finQueued = true;
            s.finNeeded = true;
        }
        if (handshakeComplete && !receiving) {
            flush();
            rearm();
        }
    }

    // ---- ACK handling and loss detection (RFC 9002) ----

    private void ackFrame(int level, QuicWire.Reader r, boolean ecn) throws QuicError {
        long largest = r.varint();
        long delay = r.varint();
        long rangeCount = r.varint();
        long firstRange = r.varint();
        if (firstRange > largest) {
            throw new QuicError(FRAME_ENCODING_ERROR, "bad ACK range");
        }
        Ranges acked = new Ranges();
        long hi = largest;
        long lo = largest - firstRange;
        acked.add(lo, hi + 1);
        if (rangeCount > 256) {
            throw new QuicError(PROTOCOL_VIOLATION, "too many ACK ranges");
        }
        for (long i = 0; i < rangeCount; i++) {
            long gap = r.varint();
            long len = r.varint();
            if (gap + 2 > lo) {
                throw new QuicError(FRAME_ENCODING_ERROR, "bad ACK range");
            }
            hi = lo - gap - 2;
            if (len > hi) {
                throw new QuicError(FRAME_ENCODING_ERROR, "bad ACK range");
            }
            lo = hi - len;
            acked.add(lo, hi + 1);
        }
        if (ecn) {
            r.varint();
            r.varint();
            r.varint();
        }
        Space s = spaces[level];
        if (largest >= s.nextPn) {
            throw new QuicError(PROTOCOL_VIOLATION, "ACK of a packet that was never sent");
        }
        onAck(level, s, acked, largest, delay);
    }

    private void onAck(int level, Space s, Ranges acked, long largest, long delayField) {
        long now = System.nanoTime();
        List<Sent> newly = new ArrayList<>();
        for (long[] range : acked.descending(Integer.MAX_VALUE)) {
            for (Sent p : s.sent.subMap(range[0], range[1]).values()) {
                newly.add(p);
            }
        }
        if (newly.isEmpty()) {
            return;
        }
        s.largestAcked = Math.max(s.largestAcked, largest);
        Sent largestSent = s.sent.get(largest);
        if (largestSent != null && largestSent.ackEliciting) {
            long delay = TimeUnit.MICROSECONDS.toNanos(delayField << peerAckDelayExponent);
            updateRtt(now - largestSent.time, level == APP && handshakeComplete ? Math.min(delay, TimeUnit.MILLISECONDS.toNanos(peerMaxAckDelayMs)) : 0);
        }
        for (Sent p : newly) {
            s.sent.remove(p.pn);
            if (p.inFlight) {
                bytesInFlight -= p.size;
                if (p.time > recoveryStart) {
                    if (cwnd < ssthresh) {
                        cwnd += p.size;
                    } else {
                        cwnd += (long) MAX_DATAGRAM * p.size / cwnd;
                    }
                }
            }
            for (Object f : p.frames) {
                if (f instanceof CryptoChunk c) {
                    s.cryptoAcked.add(c.offset, c.offset + c.length);
                } else if (f instanceof StreamChunk c) {
                    streamAcked(c);
                }
            }
        }
        ptoCount = 0;
        detectLoss(level, s, now);
    }

    private void streamAcked(StreamChunk c) {
        Stream s = streams.get(c.id);
        if (s == null) {
            return;
        }
        s.sendAcked.add(c.offset, c.offset + c.length);
        if (c.fin) {
            s.finAcked = true;
        }
        long end = s.sendAcked.contiguousEnd(s.sendBase);
        if (end > s.sendBase) {
            int drop = (int) (end - s.sendBase);
            System.arraycopy(s.sendBuf, drop, s.sendBuf, 0, s.sendLen - drop);
            s.sendLen -= drop;
            s.sendBase = end;
        }
        maybeRetire(s);
    }

    private void updateRtt(long sample, long ackDelay) {
        latestRtt = sample;
        minRtt = Math.min(minRtt, sample);
        long adjusted = sample;
        if (sample >= minRtt + ackDelay) {
            adjusted = sample - ackDelay;
        }
        if (!haveRtt) {
            haveRtt = true;
            smoothedRtt = adjusted;
            rttVar = adjusted / 2;
        } else {
            rttVar = (3 * rttVar + Math.abs(smoothedRtt - adjusted)) / 4;
            smoothedRtt = (7 * smoothedRtt + adjusted) / 8;
        }
    }

    private long rtt() {
        return haveRtt ? smoothedRtt : INITIAL_RTT_NANOS;
    }

    private void detectLoss(int level, Space s, long now) {
        s.lossTimeNanos = 0;
        long lossDelay = Math.max(9 * Math.max(haveRtt ? smoothedRtt : INITIAL_RTT_NANOS, latestRtt) / 8, GRANULARITY_NANOS);
        long lostSentAfter = Long.MIN_VALUE;
        List<Sent> lost = new ArrayList<>();
        for (Sent p : s.sent.headMap(s.largestAcked).values()) {
            if (p.pn + 3 <= s.largestAcked || p.time + lossDelay <= now) {
                lost.add(p);
            } else {
                long at = p.time + lossDelay;
                s.lossTimeNanos = s.lossTimeNanos == 0 ? at : Math.min(s.lossTimeNanos, at);
            }
        }
        for (Sent p : lost) {
            trace("lost pn=" + p.pn + " frames=" + p.frames.size());
            s.sent.remove(p.pn);
            if (p.inFlight) {
                bytesInFlight -= p.size;
                lostSentAfter = Math.max(lostSentAfter, p.time);
            }
            if (!p.requeued) {
                requeue(s, p);
            }
        }
        if (lostSentAfter != Long.MIN_VALUE && lostSentAfter > recoveryStart) {
            recoveryStart = now;
            ssthresh = Math.max(cwnd / 2, 2L * MAX_DATAGRAM);
            cwnd = ssthresh;
        }
    }

    private void requeue(Space s, Sent p) {
        for (Object f : p.frames) {
            if (f instanceof CryptoChunk c) {
                s.cryptoRetx.add(new long[]{c.offset, c.length});
            } else if (f instanceof StreamChunk c) {
                Stream st = streams.get(c.id);
                if (st != null) {
                    if (c.length > 0) {
                        st.retx.add(new long[]{c.offset, c.length});
                    }
                    if (c.fin) {
                        st.finNeeded = true;
                    }
                }
            } else if (f instanceof Control c) {
                pendingControl.addFirst(c);
            }
        }
    }

    // ---- timers ----

    private long ptoNanos(int level) {
        long base = rtt() + Math.max(4 * rttVar, GRANULARITY_NANOS);
        if (level == APP && handshakeComplete) {
            base += TimeUnit.MILLISECONDS.toNanos(peerMaxAckDelayMs);
        }
        return base << Math.min(ptoCount, 16);
    }

    private void rearm() {
        if (state == State.CLOSED) {
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
            return;
        }
        long deadline = Long.MAX_VALUE;
        if (state == State.DRAINING || state == State.CLOSING) {
            deadline = drainDeadline;
        } else {
            deadline = lastActivity + idleNanos;
            for (int lvl = 0; lvl < 3; lvl++) {
                Space s = spaces[lvl];
                if (s.discarded) {
                    continue;
                }
                if (s.lossTimeNanos != 0) {
                    deadline = Math.min(deadline, s.lossTimeNanos);
                } else if (hasAckElicitingInFlight(s) && (lvl != APP || handshakeComplete)) {
                    deadline = Math.min(deadline, s.lastAckElicitingSent + ptoNanos(lvl));
                }
            }
        }
        long now = System.nanoTime();
        if (timer != null && Math.abs(deadline - timerDeadline) < 1_000_000L) {
            return;
        }
        trace("rearm in " + (deadline - now) / 1000 + "us state=" + state + " handshake=" + handshakeComplete + " loss=" + (spaces[APP].lossTimeNanos != 0)
                + " eliciting=" + hasAckElicitingInFlight(spaces[APP]) + " pto=" + ptoNanos(APP) / 1000 + "us");
        if (timer != null) {
            timer.cancel();
        }
        timerDeadline = deadline;
        timer = loop.schedule(this::onTimer, Math.max(0, deadline - now), TimeUnit.NANOSECONDS);
    }

    private boolean hasAckElicitingInFlight(Space s) {
        for (Sent p : s.sent.values()) {
            if (p.ackEliciting) {
                return true;
            }
        }
        return false;
    }

    private void onTimer() {
        timer = null;
        timerDeadline = 0;
        long now = System.nanoTime();
        trace("timer fired");
        try {
            if (state == State.DRAINING || state == State.CLOSING) {
                if (now >= drainDeadline) {
                    finish();
                    return;
                }
                rearm();
                return;
            }
            if (now >= lastActivity + idleNanos) {
                if (TRACE) {
                    trace("idle: dataSent=" + dataSent + " peerMaxData=" + peerMaxData + " connRecvTotal=" + connRecvTotal + " recvMaxData=" + recvMaxData + " inflight=" + bytesInFlight
                            + " cwnd=" + cwnd + " probes=" + spaces[APP].probes + " sent=" + spaces[APP].sent.size());
                    for (Stream st : streams.values()) {
                        trace("  stream " + st.id + " recvDelivered=" + st.recvDelivered + " recvMax=" + st.recvMax + " finalSize=" + st.finalSize + " pending=" + st.recvPending.keySet()
                                + " sendBase=" + st.sendBase + " sendLen=" + st.sendLen + " sendNext=" + st.sendNext + " finQueued=" + st.finQueued + " finNeeded=" + st.finNeeded
                                + " finAcked=" + st.finAcked + " retx=" + st.retx.size() + " peerLimit=" + st.peerLimit + " acked=" + st.sendAcked.size());
                    }
                }
                closeCode = 0;
                closeApplication = false;
                closeReason = "idle timeout";
                state = State.DRAINING;
                notifyClosed();
                finish();
                return;
            }
            for (int lvl = 0; lvl < 3; lvl++) {
                Space s = spaces[lvl];
                if (!s.discarded && s.lossTimeNanos != 0 && s.lossTimeNanos <= now) {
                    detectLoss(lvl, s, now);
                }
            }
            // probe timeout: the earliest space whose deadline passed
            int pick = -1;
            long best = Long.MAX_VALUE;
            for (int lvl = 0; lvl < 3; lvl++) {
                Space s = spaces[lvl];
                if (s.discarded || s.lossTimeNanos != 0 || (lvl == APP && !handshakeComplete) || !hasAckElicitingInFlight(s)) {
                    continue;
                }
                long at = s.lastAckElicitingSent + ptoNanos(lvl);
                if (at <= now && at < best) {
                    best = at;
                    pick = lvl;
                }
            }
            trace("timer pick=" + pick + " ptoCount=" + ptoCount + " rtt=" + rtt() / 1000 + "us");
            if (pick >= 0) {
                ptoCount++;
                Space s = spaces[pick];
                s.probes = 2;
                for (Sent p : s.sent.values()) {
                    if (p.ackEliciting && !p.requeued) {
                        p.requeued = true;
                        requeue(s, p);
                    }
                }
            }
            flush();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "quic timer failed", e);
        }
        rearm();
    }

    private void finish() {
        state = State.CLOSED;
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        notifyClosed();
        server.remove(this);
    }

    // ---- closing ----

    private void startClosing(long code, boolean application, String reason) {
        if (state != State.OPEN) {
            return;
        }
        state = State.CLOSING;
        closeCode = code;
        closeApplication = application && handshakeComplete;
        closeReason = reason;
        drainDeadline = System.nanoTime() + 3 * Math.max(ptoNanos(APP), 100_000_000L);
        notifyClosed();
        sendClose();
        rearm();
    }

    private void sendClose() {
        lastCloseSent = System.nanoTime();
        int level = tx[APP] != null && handshakeComplete ? APP : tx[HANDSHAKE] != null && !spaces[HANDSHAKE].discarded ? HANDSHAKE : INITIAL;
        if (tx[level] == null) {
            return;
        }
        QuicWire.Writer f = new QuicWire.Writer(64);
        boolean app = closeApplication && level == APP;
        f.varint(app ? 0x1d : 0x1c).varint(app ? closeCode : closeApplication ? 0x0c : closeCode);
        if (!app) {
            f.varint(0);
        }
        byte[] reason = closeReason.getBytes(StandardCharsets.UTF_8);
        int n = Math.min(reason.length, 100);
        f.varint(n).bytes(reason, 0, n);
        Pending p = new Pending(level, f);
        QuicWire.Writer dgram = new QuicWire.Writer(MAX_DATAGRAM);
        seal(p, dgram, null);
        transmit(dgram);
    }

    // ---- sending ----

    private static final class Pending {
        final int level;
        final QuicWire.Writer frames;
        Sent sent;
        boolean ackEliciting;

        Pending(int level, QuicWire.Writer frames) {
            this.level = level;
            this.frames = frames;
        }
    }

    private int headerOverhead(int level) {
        if (level == APP) {
            return 1 + dcid.length + 4 + 16;
        }
        return 1 + 4 + 1 + dcid.length + 1 + scid.length + (level == INITIAL ? 1 : 0) + 2 + 4 + 16;
    }

    private long amplificationBudget() {
        if (addressValidated) {
            return Long.MAX_VALUE;
        }
        return Math.max(0, 3 * bytesReceived - bytesSent);
    }

    private void flush() {
        if (state != State.OPEN) {
            return;
        }
        for (int guard = 0; guard < 200; guard++) {
            if (!sendDatagram()) {
                break;
            }
        }
    }

    private boolean sendDatagram() {
        long amp = amplificationBudget();
        int budget = (int) Math.min(MAX_DATAGRAM, amp);
        if (budget < 80) {
            return false;
        }
        List<Pending> packets = new ArrayList<>();
        int used = 0;
        for (int level = 0; level < 3; level++) {
            Space s = spaces[level];
            if (s.discarded || tx[level] == null) {
                continue;
            }
            int room = budget - used - headerOverhead(level);
            if (room < 20) {
                break;
            }
            Pending p = buildPacket(level, s, room);
            if (p == null) {
                continue;
            }
            packets.add(p);
            used += headerOverhead(level) + p.frames.n;
        }
        if (packets.isEmpty()) {
            return false;
        }
        Pending last = packets.get(packets.size() - 1);
        boolean initialAckEliciting = false;
        for (Pending p : packets) {
            initialAckEliciting |= p.level == INITIAL && p.ackEliciting;
        }
        if (initialAckEliciting && budget >= MAX_DATAGRAM) {
            int pad = MAX_DATAGRAM - used;
            for (int i = 0; i < pad; i++) {
                last.frames.u8(0);
            }
        }
        QuicWire.Writer dgram = new QuicWire.Writer(MAX_DATAGRAM);
        long now = System.nanoTime();
        for (Pending p : packets) {
            int size = seal(p, dgram, p.sent);
            trace("tx " + "IHA".charAt(p.level) + " pn=" + p.sent.pn + " size=" + size + " eliciting=" + p.ackEliciting + " frames=" + p.sent.frames.size()
                    + " inflight=" + bytesInFlight + " cwnd=" + cwnd);
            if (p.ackEliciting) {   // packets with only ACK (and padding) are not tracked: nobody acknowledges them and they do not count against the window
                Space s = spaces[p.level];
                p.sent.size = size;
                s.sent.put(p.sent.pn, p.sent);
                s.lastAckElicitingSent = now;
                s.probes = Math.max(0, s.probes - 1);
                bytesInFlight += size;
            }
        }
        transmit(dgram);
        return true;
    }

    private void transmit(QuicWire.Writer dgram) {
        bytesSent += dgram.n;
        socket.send(ByteBuffer.wrap(dgram.b, 0, dgram.n).slice(), peer);
    }

    /** Frames for one packet at {@code level}, or null if there is nothing to send. */
    private Pending buildPacket(int level, Space s, int room) {
        QuicWire.Writer f = new QuicWire.Writer(room + 16);
        Sent sent = new Sent(s.nextPn, System.nanoTime());
        boolean ackEliciting = false;
        boolean mayProbe = s.probes > 0;
        boolean canSend = mayProbe || bytesInFlight < cwnd;

        if (s.ackPending && s.received.size() > 0) {
            writeAck(f, s, level);
            s.ackPending = false;
        }
        // CRYPTO
        while (canSend && room - f.n > 20) {
            long[] chunk = nextCryptoChunk(s, room - f.n - 12);
            if (chunk == null) {
                break;
            }
            int len = (int) chunk[1];
            f.varint(0x06).varint(chunk[0]).varint(len).bytes(s.cryptoBuf, (int) chunk[0], len);
            sent.frames.add(new CryptoChunk(chunk[0], len));
            ackEliciting = true;
        }
        if (level == APP && handshakeComplete && canSend) {
            while (!pathResponses.isEmpty() && room - f.n > 10) {
                long challenge = pathResponses.poll();
                f.varint(0x1b);
                for (int i = 7; i >= 0; i--) {
                    f.u8((int) (challenge >> (8 * i)));
                }
                ackEliciting = true;
            }
            while (!pendingControl.isEmpty() && room - f.n > 20) {
                Control c = pendingControl.poll();
                f.varint(c.type);
                if (c.type == 0x10 || c.type == 0x12 || c.type == 0x13) {
                    f.varint(c.a);
                } else if (c.type == 0x11) {
                    f.varint(c.a).varint(c.b);
                }
                sent.frames.add(c);
                ackEliciting = true;
            }
            ackEliciting |= writeStreams(f, sent, room);
        }
        if (!ackEliciting && mayProbe) {
            f.varint(0x01);
            ackEliciting = true;
        }
        if (f.n == 0) {
            return null;
        }
        sent.ackEliciting = ackEliciting;
        sent.inFlight = ackEliciting;
        Pending p = new Pending(level, f);
        p.sent = sent;
        p.ackEliciting = ackEliciting;
        s.nextPn++;
        return p;
    }

    private long[] nextCryptoChunk(Space s, int max) {
        while (!s.cryptoRetx.isEmpty()) {
            long[] r = s.cryptoRetx.get(0);
            long[] gap = s.cryptoAcked.firstGap(r[0], r[0] + r[1]);
            if (gap == null) {
                s.cryptoRetx.remove(0);
                continue;
            }
            int len = (int) Math.min(gap[1] - gap[0], max);
            if (gap[0] + len >= r[0] + r[1]) {
                s.cryptoRetx.remove(0);
            } else {
                s.cryptoRetx.set(0, new long[]{gap[0] + len, r[0] + r[1] - gap[0] - len});
            }
            return new long[]{gap[0], len};
        }
        if (s.cryptoSentUpTo < s.cryptoBuf.length) {
            int len = (int) Math.min(s.cryptoBuf.length - s.cryptoSentUpTo, max);
            long off = s.cryptoSentUpTo;
            s.cryptoSentUpTo += len;
            return new long[]{off, len};
        }
        return null;
    }

    private void writeAck(QuicWire.Writer f, Space s, int level) {
        List<long[]> ranges = s.received.descending(32);
        long largest = ranges.get(0)[1] - 1;
        long delayMicros = Math.max(0, (System.nanoTime() - s.largestRecvTime) / 1000);
        f.varint(0x02).varint(largest).varint(delayMicros >> ACK_DELAY_EXPONENT).varint(ranges.size() - 1).varint(ranges.get(0)[1] - 1 - ranges.get(0)[0]);
        for (int i = 1; i < ranges.size(); i++) {
            long prevStart = ranges.get(i - 1)[0];
            long[] cur = ranges.get(i);
            long gap = prevStart - cur[1] - 1;
            f.varint(gap).varint(cur[1] - 1 - cur[0]);
        }
    }

    private boolean writeStreams(QuicWire.Writer f, Sent sent, int room) {
        boolean any = false;
        int n = streamOrder.size();
        for (int i = 0; i < n; i++) {
            Stream s = streams.get(streamOrder.get((streamCursor + i) % n));
            if (s == null) {
                continue;
            }
            while (room - f.n > 24) {
                int max = room - f.n - 24;
                long off;
                int len;
                boolean newData = false;
                long[] gap = nextRetransmitGap(s);
                if (gap != null) {
                    off = gap[0];
                    len = (int) Math.min(gap[1] - gap[0], max);
                    consumeRetransmit(s, off + len);
                } else {
                    long available = s.sendEnd() - s.sendNext;
                    long allowed = Math.max(0, Math.min(available, Math.min(s.peerLimit - s.sendNext, peerMaxData - dataSent)));
                    off = s.sendNext;
                    len = (int) Math.min(allowed, max);
                    newData = true;
                }
                boolean fin = s.finQueued && s.finNeeded && off + len == s.sendEnd();
                if (len == 0 && !fin) {
                    break;
                }
                f.varint(0x08 | 0x02 | (off > 0 ? 0x04 : 0) | (fin ? 0x01 : 0)).varint(s.id);
                if (off > 0) {
                    f.varint(off);
                }
                f.varint(len);
                if (len > 0) {
                    f.bytes(s.sendBuf, (int) (off - s.sendBase), len);
                }
                if (newData) {
                    s.sendNext += len;
                    dataSent += len;
                }
                if (fin) {
                    s.finNeeded = false;
                }
                sent.frames.add(new StreamChunk(s.id, off, len, fin));
                any = true;
            }
        }
        if (n > 0) {
            streamCursor = (streamCursor + 1) % n;
        }
        return any;
    }

    /** The first stretch of lost stream data that is still unacknowledged, as {from, to}. */
    private long[] nextRetransmitGap(Stream s) {
        while (!s.retx.isEmpty()) {
            long[] head = s.retx.get(0);
            long[] gap = s.sendAcked.firstGap(Math.max(head[0], s.sendBase), head[0] + head[1]);
            if (gap != null) {
                return gap;
            }
            s.retx.remove(0);
        }
        return null;
    }

    private void consumeRetransmit(Stream s, long sentUpTo) {
        long[] head = s.retx.get(0);
        long end = head[0] + head[1];
        if (sentUpTo >= end) {
            s.retx.remove(0);
        } else {
            s.retx.set(0, new long[]{sentUpTo, end - sentUpTo});
        }
    }

    /** Header, AEAD and header protection for one packet, appended to {@code dgram}. */
    private int seal(Pending p, QuicWire.Writer dgram, Sent sent) {
        Space s = spaces[p.level];
        QuicCrypto.Keys keys = tx[p.level];
        long pn = sent != null ? sent.pn : s.nextPn++;
        int payloadLen = p.frames.n;
        QuicWire.Writer h = new QuicWire.Writer(64);
        int pnOffset;
        if (p.level == APP) {
            h.u8(0x40 | (keyPhase << 2) | 3);
            h.bytes(dcid);
            pnOffset = h.n;
        } else {
            int type = p.level == INITIAL ? 0 : 2;
            h.u8(0xc0 | (type << 4) | 3);
            h.u32(1);
            h.u8(dcid.length).bytes(dcid);
            h.u8(scid.length).bytes(scid);
            if (p.level == INITIAL) {
                h.varint(0);
            }
            int length = 4 + payloadLen + 16;
            h.u16(0x4000 | length);
            pnOffset = h.n;
        }
        h.u32(pn & 0xffffffffL);
        byte[] header = h.toByteArray();
        byte[] ct = keys.seal(pn, header, p.frames.b, 0, payloadLen);
        byte[] packet = new byte[header.length + ct.length];
        System.arraycopy(header, 0, packet, 0, header.length);
        System.arraycopy(ct, 0, packet, header.length, ct.length);
        byte[] mask = keys.mask(packet, pnOffset + 4);
        packet[0] ^= (byte) (mask[0] & (p.level == APP ? 0x1f : 0x0f));
        for (int i = 0; i < 4; i++) {
            packet[pnOffset + i] ^= mask[1 + i];
        }
        dgram.bytes(packet);
        return packet.length;
    }
}
