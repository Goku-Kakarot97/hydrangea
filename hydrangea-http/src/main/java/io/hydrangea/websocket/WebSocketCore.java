package io.hydrangea.websocket;

import io.hydrangea.Connection;
import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * RFC 6455 frame codec + connection state machine, shared by server and client. Loop-thread confined except for the
 * thread-safe {@link WebSocket} methods, which hop onto the loop.
 * <p>
 * Enforced (violations close the connection with the status in parentheses):
 * RSV bits must be 0 as no extension is negotiated (1002); unknown opcodes (1002); client frames masked and server
 * frames unmasked (1002); minimal length encodings (1002); control frames ≤ 125 bytes and unfragmented (1002);
 * continuation frames only inside a fragmented message (1002); text is valid UTF-8 (1007); close codes and reasons
 * valid (1002/1007); frames and messages ≤ maxMessageBytes (1009).
 */
final class WebSocketCore implements WebSocket {

    static final int OP_CONT = 0, OP_TEXT = 1, OP_BINARY = 2, OP_CLOSE = 8, OP_PING = 9, OP_PONG = 10;

    /** A protocol violation by the peer; {@code code} is the Close status to answer with. */
    private static final class ProtocolException extends Exception {
        final int code;

        ProtocolException(int code, String message) {
            super(message, null, false, false);
            this.code = code;
        }
    }

    private final boolean client;
    private final WebSocketListener listener;
    private final WebSocketConfig config;
    private String subprotocol;
    private final SecureRandom random = new SecureRandom(); // masking keys must be unpredictable (client only)
    private final Accumulator acc = new Accumulator();

    private Context ctx;
    // metrics (tag side=client|server), set when the core is attached to its connection; the handles are looked up once
    private io.hydrangea.Metrics metrics;
    private io.hydrangea.Metrics.Count mOpen;
    private io.hydrangea.Metrics.Count mRecv;
    private io.hydrangea.Metrics.Count mSent;
    private io.hydrangea.Metrics.Count mRecvBytes;
    private io.hydrangea.Metrics.Count mSentBytes;
    private boolean counted;      // this connection is in websocket.connections
    private Connection conn;

    private boolean opened;
    private boolean sentClose;
    private boolean inputStopped;
    private boolean closeNotified;
    private volatile boolean openFlag;       // true between open and the first close
    private int closeCode = 1006;
    private String closeReason = "";

    // the frame being received: a header is parsed first, then its payload is consumed piece by piece
    private boolean inPayload;
    private int fOp;
    private boolean fFin;
    private boolean fMasked;
    private long fLen;
    private long fRead;
    private final byte[] fKey = new byte[4];
    private ByteArrayOutputStream ctrl;         // payload of the control frame being received (at most 125 bytes)

    // the data message being received (may span several frames)
    private int msgType = -1;                   // OP_TEXT or OP_BINARY while a message is in progress
    private boolean msgStreaming;               // the listener takes it in parts instead of whole
    private long msgBytes;
    private Utf8StreamDecoder msgText;          // validates and decodes text as it arrives
    private StringBuilder msgChars;             // buffered mode: the text so far
    private ByteArrayOutputStream msgBinary;    // buffered mode: the bytes so far
    private boolean paused;

    // permessage-deflate (null = not negotiated)
    private Extensions.Agreed deflate;
    private java.util.zip.Deflater deflater;    // created on first use, released on close
    private java.util.zip.Inflater inflater;
    private boolean msgCompressed;              // the message being received has RSV1 set
    private boolean inflateActive;              // the inflater holds input or output that has not been drained yet
    private boolean msgInputDone;               // every compressed byte of the message has been read
    private boolean tailFed;                    // the 00 00 ff ff trailer was appended to finish the message
    private static final byte[] TAIL = {0, 0, (byte) 0xff, (byte) 0xff};

    // the data message being sent in parts
    private boolean outOpen;
    private boolean outCompressed;
    private int outType;
    private char outHighSurrogate;              // a high surrogate waiting for its low half in the next text part
    private final java.util.ArrayList<CompletableFuture<Void>> writableWaiters = new java.util.ArrayList<>();
    private Scheduled closeTimer;
    private Scheduled pingTimer;
    private long lastReceived = System.nanoTime();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();

    WebSocketCore(boolean client, WebSocketListener listener, WebSocketConfig config, String subprotocol) {
        this(client, listener, config, subprotocol, null);
    }

    WebSocketCore(boolean client, WebSocketListener listener, WebSocketConfig config, String subprotocol, Extensions.Agreed deflate) {
        this.client = client;
        this.listener = listener;
        this.config = config;
        this.subprotocol = subprotocol;
        this.deflate = deflate;
    }

    /** Client side: the extension is only known once the handshake response has been read. */
    void setDeflate(Extensions.Agreed deflate) {
        this.deflate = deflate;
    }

    // ---- lifecycle (loop thread) ----

    void setSubprotocol(String subprotocol) {
        this.subprotocol = subprotocol;
    }

    void attach(Context ctx) {
        this.ctx = ctx;
        this.conn = ctx.connection();
        metrics = conn.eventLoop().metrics();
        String side = client ? "client" : "server";
        mOpen = metrics.gauge("websocket.connections", null, "side", side);
        mRecv = metrics.counter("websocket.messages", null, "side", side, "direction", "received");
        mSent = metrics.counter("websocket.messages", null, "side", side, "direction", "sent");
        mRecvBytes = metrics.counter("websocket.bytes", "bytes", "side", side, "direction", "received");
        mSentBytes = metrics.counter("websocket.bytes", "bytes", "side", side, "direction", "sent");
        conn.closeFuture().thenRun(() -> closed.complete(null));
    }

    void opened() {
        opened = true;
        openFlag = true;
        counted = true;
        mOpen.increment();
        lastReceived = System.nanoTime();
        schedulePing();
        callback(() -> listener.onOpen(this));
    }

    void connectionClosed(Throwable cause) {
        openFlag = false;
        if (counted) {
            counted = false;
            mOpen.decrement();
        }
        if (deflater != null) {
            deflater.end(); // native memory: do not wait for the garbage collector
            deflater = null;
        }
        if (inflater != null) {
            inflater.end();
            inflater = null;
        }
        for (CompletableFuture<Void> w : writableWaiters) {
            w.completeExceptionally(new ClosedChannelException());
        }
        writableWaiters.clear();
        cancel(closeTimer);
        cancel(pingTimer);
        acc.release();
        if (closeNotified || !opened) {
            return;
        }
        closeNotified = true;
        if (cause != null && closeCode == 1006) {
            safely(() -> listener.onError(this, cause));
        }
        metrics.counter("websocket.closes", null, "side", client ? "client" : "server", "code", Integer.toString(closeCode)).increment();
        safely(() -> listener.onClose(this, closeCode, closeReason));
    }

    /** The server is shutting down gracefully: tell the peer with status 1001 and run the normal closing handshake. */
    void drain() {
        if (opened && !sentClose && !closeNotified) {
            close(1001, "server is shutting down"); // the closeTimeout bounds the wait for the peer's answer
        }
    }

    void writabilityChanged() {
        if (conn.isWritable() && !writableWaiters.isEmpty()) {
            var ready = new java.util.ArrayList<>(writableWaiters);
            writableWaiters.clear();
            ready.forEach(w -> w.complete(null));
        }
        callback(() -> listener.onWritabilityChanged(this));
    }

    // ---- receiving ----

    void feed(ByteBuffer data) {
        if (inputStopped) {
            return;
        }
        acc.append(data);
        lastReceived = System.nanoTime();
        pump();
    }

    /** Decodes as much buffered input as allowed (not while paused). */
    private void pump() {
        try {
            while (!inputStopped && !paused && decodeOne()) {
                // one header or one piece of payload per iteration
            }
        } catch (ProtocolException e) {
            safely(() -> listener.onError(this, new IOException("websocket protocol error: " + e.getMessage())));
            fail(e.code, e.getMessage());
        }
    }

    private boolean decodeOne() throws ProtocolException {
        if (inflateActive) {
            return inflateStep(); // output of the previous piece first: keeps delivery in wire order, and lets a pause stop mid-expansion
        }
        ByteBuffer in = acc.buffer();
        return inPayload ? readPayload(in) : readHeader(in);
    }

    private boolean readHeader(ByteBuffer in) throws ProtocolException {
        if (in.remaining() < 2) {
            return false;
        }
        int p = in.position();
        int b0 = in.get(p) & 0xFF;
        int b1 = in.get(p + 1) & 0xFF;
        boolean fin = (b0 & 0x80) != 0;
        int op = b0 & 0x0F;
        boolean masked = (b1 & 0x80) != 0;
        int len7 = b1 & 0x7F;

        if ((b0 & 0x30) != 0) {
            throw new ProtocolException(1002, "reserved bits set");
        }
        if ((op >= 3 && op <= 7) || op > OP_PONG) {
            throw new ProtocolException(1002, "unknown opcode " + op);
        }
        boolean rsv1 = (b0 & 0x40) != 0;
        if (rsv1 && (deflate == null || (op != OP_TEXT && op != OP_BINARY))) {
            // RSV1 means "this message is compressed": only valid on the first frame of a data message, and only if negotiated
            throw new ProtocolException(1002, deflate == null ? "reserved bits set" : "RSV1 is only valid on the first frame of a data message");
        }
        if (masked == client) {
            throw new ProtocolException(1002, client ? "server frames must not be masked" : "client frames must be masked");
        }
        boolean control = op >= OP_CLOSE;
        if (control && (!fin || len7 > 125)) {
            throw new ProtocolException(1002, "control frames must be unfragmented and at most 125 bytes");
        }
        int header = 2;
        long len = len7;
        if (len7 == 126) {
            if (in.remaining() < 4) {
                return false;
            }
            len = in.getShort(p + 2) & 0xFFFF;
            header = 4;
            if (len < 126) {
                throw new ProtocolException(1002, "non-minimal length encoding");
            }
        } else if (len7 == 127) {
            if (in.remaining() < 10) {
                return false;
            }
            len = in.getLong(p + 2);
            header = 10;
            if (len < 0 || len < 65536) {
                throw new ProtocolException(1002, len < 0 ? "payload length has the top bit set" : "non-minimal length encoding");
            }
        }
        int maskAt = p + header;
        if (masked) {
            header += 4;
        }
        if (in.remaining() < header) {
            return false; // the header itself (including the mask key) is still incomplete
        }

        // rules that do not depend on the payload are checked now, before any of it is read
        if ((op == OP_TEXT || op == OP_BINARY) && msgType != -1) {
            throw new ProtocolException(1002, "new data frame while a fragmented message is in progress");
        }
        if (op == OP_CONT && msgType == -1) {
            throw new ProtocolException(1002, "continuation frame without a message to continue");
        }
        if (op == OP_TEXT || op == OP_BINARY) { // a new message starts here
            msgType = op;
            msgBytes = 0;
            msgStreaming = streamsMessages();
            msgCompressed = rsv1;
            inflateActive = false;
            msgInputDone = false;
            tailFed = false;
            msgText = op == OP_TEXT ? new Utf8StreamDecoder() : null;
            msgChars = !msgStreaming && op == OP_TEXT ? new StringBuilder() : null;
            msgBinary = !msgStreaming && op == OP_BINARY ? new ByteArrayOutputStream() : null;
        }
        if (control) {
            if (len > config.maxMessageBytes()) {
                throw new ProtocolException(1009, "frame of " + len + " bytes exceeds " + config.maxMessageBytes());
            }
            ctrl = new ByteArrayOutputStream((int) len);
        } else if (!msgCompressed) { // a compressed message is measured after it has been inflated
            if (msgStreaming) {
                long limit = config.maxStreamedMessageBytes();
                if (limit >= 0 && msgBytes + len > limit) {
                    throw new ProtocolException(1009, "streamed message exceeds " + limit + " bytes");
                }
            } else if (msgBytes + len > config.maxMessageBytes()) {
                throw new ProtocolException(1009, "message exceeds " + config.maxMessageBytes() + " bytes");
            }
        }

        if (masked) {
            for (int i = 0; i < 4; i++) {
                fKey[i] = in.get(maskAt + i);
            }
        }
        fOp = op;
        fFin = fin;
        fMasked = masked;
        fLen = len;
        fRead = 0;
        inPayload = true;
        in.position(p + header);
        return true;
    }

    /** Consumes whatever part of the current frame's payload is available. */
    private boolean readPayload(ByteBuffer in) throws ProtocolException {
        long left = fLen - fRead;
        int n = (int) Math.min(left, in.remaining());
        if (n == 0 && left > 0) {
            return false;
        }
        byte[] chunk = new byte[n];
        in.get(chunk);
        if (fMasked) {
            for (int i = 0; i < n; i++) {
                chunk[i] ^= fKey[(int) ((fRead + i) & 3)];
            }
        }
        fRead += n;
        boolean frameDone = fRead == fLen;
        if (msgCompressed && fOp < OP_CLOSE) { // data frame of a compressed message: feed the inflater
            if (frameDone) {
                inPayload = false;
                if (fFin) {
                    msgInputDone = true;
                }
            }
            if (n > 0) {
                inflater().setInput(chunk);
            }
            inflateActive = true;
            return inflateStep();
        }
        if (fOp >= OP_CLOSE) {
            ctrl.write(chunk, 0, n);
            if (frameDone) {
                byte[] payload = ctrl.toByteArray();
                ctrl = null;
                inPayload = false;
                handleControl(fOp, payload);
            }
            return true;
        }
        boolean messageDone = frameDone && fFin;
        if (frameDone) {
            inPayload = false;
        }
        dataPiece(chunk, messageDone);
        return true;
    }

    private void handleControl(int op, byte[] payload) throws ProtocolException {
        switch (op) {
            case OP_CLOSE -> onCloseFrame(payload);
            case OP_PING -> {
                if (!sentClose) {
                    writeFrame(OP_PONG, payload, null);
                }
                callback(() -> listener.onPing(this, ByteBuffer.wrap(payload)));
            }
            default -> callback(() -> listener.onPong(this, ByteBuffer.wrap(payload)));
        }
    }

    private java.util.zip.Inflater inflater() {
        if (inflater == null) {
            inflater = new java.util.zip.Inflater(true); // raw deflate, as RFC 7692 requires
        }
        return inflater;
    }

    /**
     * Produces the next piece of decompressed data. Each call yields at most one buffer of output so the pump loop can honour
     * a pause between pieces; the inflater keeps the rest of its input until the next call.
     */
    private boolean inflateStep() throws ProtocolException {
        java.util.zip.Inflater inf = inflater();
        byte[] out = new byte[16 * 1024];
        int r;
        try {
            r = inf.inflate(out);
        } catch (java.util.zip.DataFormatException e) {
            throw new ProtocolException(1007, "invalid compressed data: " + e.getMessage());
        }
        if (r > 0) {
            dataPiece(r == out.length ? out : java.util.Arrays.copyOf(out, r), false);
            return true;
        }
        if (inf.needsDictionary()) {
            throw new ProtocolException(1007, "invalid compressed data: preset dictionary is not allowed");
        }
        if (!inf.needsInput() && !inf.finished()) {
            return true; // produced nothing yet, but there is more to chew on
        }
        inflateActive = false;
        if (inf.finished()) {
            inf.reset(); // the sender ended its deflate stream (BFINAL); a new one starts with the next message
        }
        if (!msgInputDone) {
            return true; // waiting for the next frame of this message
        }
        if (!tailFed) { // all frames read: append the stripped 00 00 ff ff so the last block is flushed, per RFC 7692 §7.2.2
            inf.setInput(TAIL);
            tailFed = true;
            inflateActive = true;
            return true;
        }
        // the message is complete
        if (deflate.decompressorResets()) {
            inf.reset();
        }
        msgCompressed = false;
        dataPiece(new byte[0], true);
        return true;
    }

    /** A piece of a data message: validated as it comes, then handed on in parts or collected for delivery as a whole. */
    private void dataPiece(byte[] chunk, boolean messageDone) throws ProtocolException {
        msgBytes += chunk.length;
        mRecvBytes.add(chunk.length);
        // measured on what the application would receive, i.e. after decompression: the guard against decompression bombs
        if (msgStreaming) {
            long limit = config.maxStreamedMessageBytes();
            if (limit >= 0 && msgBytes > limit) {
                throw new ProtocolException(1009, "streamed message exceeds " + limit + " bytes");
            }
        } else if (msgBytes > config.maxMessageBytes()) {
            throw new ProtocolException(1009, "message exceeds " + config.maxMessageBytes() + " bytes");
        }
        boolean text = msgType == OP_TEXT;
        String piece = null;
        if (text) {
            piece = msgText.accept(chunk, messageDone);
            if (piece == null) {
                throw new ProtocolException(1007, "text message is not valid UTF-8");
            }
        }
        if (msgStreaming) {
            if (text) {
                if (!piece.isEmpty() || messageDone) {
                    String part = piece;
                    callback(() -> listener.onTextPart(this, part, messageDone));
                }
            } else if (chunk.length > 0 || messageDone) {
                callback(() -> listener.onBinaryPart(this, ByteBuffer.wrap(chunk), messageDone));
            }
        } else {
            if (text) {
                msgChars.append(piece);
            } else {
                msgBinary.write(chunk, 0, chunk.length);
            }
            if (messageDone) {
                if (text) {
                    String whole = msgChars.toString();
                    callback(() -> listener.onText(this, whole));
                } else {
                    byte[] whole = msgBinary.toByteArray();
                    callback(() -> listener.onBinary(this, ByteBuffer.wrap(whole)));
                }
            }
        }
        if (messageDone) {
            mRecv.increment();
            msgType = -1;
            msgText = null;
            msgChars = null;
            msgBinary = null;
        }
    }

    private boolean streamsMessages() {
        try {
            return listener.streaming();
        } catch (Throwable t) {
            return false;
        }
    }

    private void onCloseFrame(byte[] payload) throws ProtocolException {
        inputStopped = true;
        int code = 1005;
        String reason = "";
        if (payload.length == 1) {
            throw new ProtocolException(1002, "close frame with a 1-byte payload");
        }
        if (payload.length >= 2) {
            code = (payload[0] & 0xFF) << 8 | (payload[1] & 0xFF);
            if (!validCloseCode(code)) {
                throw new ProtocolException(1002, "invalid close code " + code);
            }
            reason = strictUtf8(java.util.Arrays.copyOfRange(payload, 2, payload.length));
            if (reason == null) {
                throw new ProtocolException(1007, "close reason is not valid UTF-8");
            }
        }
        closeCode = code;
        closeReason = reason;
        openFlag = false;
        if (!sentClose) {
            sentClose = true;
            writeFrame(OP_CLOSE, payload.length >= 2 ? java.util.Arrays.copyOf(payload, 2) : new byte[0], null); // echo the status
        }
        closeAfterFlush();
    }

    private static boolean validCloseCode(int c) {
        return (c >= 1000 && c <= 1003) || (c >= 1007 && c <= 1014) || (c >= 3000 && c <= 4999);
    }

    private static String strictUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    // ---- failing and closing ----

    /** Local decision to abort: tell the peer why (if still possible) and close once that is flushed. */
    private void fail(int code, String reason) {
        inputStopped = true;
        if (closeCode == 1006) {
            closeCode = code;
            closeReason = reason == null ? "" : reason;
        }
        openFlag = false;
        if (!sentClose) {
            sentClose = true;
            writeFrame(OP_CLOSE, closePayload(code, reason), null);
        }
        closeAfterFlushLingering();
    }

    /** Closes after queued frames are out, going through the pipeline so TLS flushes and says close_notify first. */
    private void closeAfterFlush() {
        try {
            ctx.closeAfterFlush();
        } catch (Exception e) {
            conn.close();
        }
    }

    /**
     * Closes after a failure of ours (protocol error, limit exceeded). The peer may still be sending, so linger: finish with
     * a FIN and discard its remaining bytes, or the RST that follows an abrupt close could erase the Close frame we just sent.
     */
    private void closeAfterFlushLingering() {
        try {
            ctx.closeAfterFlush(config.closeTimeout().compareTo(java.time.Duration.ofSeconds(2)) < 0 ? config.closeTimeout() : java.time.Duration.ofSeconds(2));
        } catch (Exception e) {
            conn.close();
        }
    }

    private static byte[] closePayload(int code, String reason) {
        byte[] r = reason == null ? new byte[0] : reason.getBytes(StandardCharsets.UTF_8);
        if (r.length > 123) {
            r = java.util.Arrays.copyOf(r, 123); // may cut a multi-byte character; fine for a diagnostic text
            String s = strictUtf8(r);
            while (s == null && r.length > 0) {
                r = java.util.Arrays.copyOf(r, r.length - 1);
                s = strictUtf8(r);
            }
        }
        byte[] out = new byte[2 + r.length];
        out[0] = (byte) (code >> 8);
        out[1] = (byte) code;
        System.arraycopy(r, 0, out, 2, r.length);
        return out;
    }

    // ---- sending ----

    @Override
    public CompletableFuture<Void> sendText(String text) {
        return sendData(OP_TEXT, text.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public CompletableFuture<Void> sendBinary(ByteBuffer data) {
        byte[] copy = new byte[data.remaining()];
        data.duplicate().get(copy);
        return sendData(OP_BINARY, copy);
    }

    @Override
    public CompletableFuture<Void> sendPing(ByteBuffer data) {
        if (data.remaining() > 125) {
            throw new IllegalArgumentException("ping payload is limited to 125 bytes");
        }
        byte[] copy = new byte[data.remaining()];
        data.duplicate().get(copy);
        return sendData(OP_PING, copy);
    }

    private CompletableFuture<Void> sendData(int op, byte[] payload) {
        if (payload.length > config.maxMessageBytes()) {
            throw new IllegalArgumentException("message of " + payload.length + " bytes exceeds maxMessageBytes=" + config.maxMessageBytes());
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        runInLoop(() -> {
            if (!opened || sentClose || closeNotified || !conn.isOpen()) {
                f.completeExceptionally(new ClosedChannelException());
            } else if (outOpen && (op == OP_TEXT || op == OP_BINARY)) {
                f.completeExceptionally(new IllegalStateException("a fragmented message is in progress; finish it with last = true first"));
            } else if (deflate != null && (op == OP_TEXT || op == OP_BINARY) && payload.length >= deflate.thresholdBytes()) {
                countSent(op, payload.length, true);
                try {
                    writeFrame(op, true, true, compress(payload, true), f);
                } catch (RuntimeException e) {
                    f.completeExceptionally(e);
                    conn.close();
                }
            } else {
                countSent(op, payload.length, true);
                writeFrame(op, payload, f);
            }
        });
        return f;
    }

    /** A data message (not a ping) is being sent: {@code whole} when this is all of it or the last part. */
    private void countSent(int op, int bytes, boolean whole) {
        if (op == OP_TEXT || op == OP_BINARY || op == OP_CONT) {
            mSentBytes.add(bytes);
            if (whole) {
                mSent.increment();
            }
        }
    }

    @Override
    public CompletableFuture<Void> sendTextPart(String part, boolean last) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        runInLoop(() -> {
            if (!canSend(f)) {
                return;
            }
            if (outOpen && outType != OP_TEXT) {
                f.completeExceptionally(new IllegalStateException("a binary message is in progress; finish it first"));
                return;
            }
            String s = outHighSurrogate != 0 ? outHighSurrogate + part : part;
            outHighSurrogate = 0;
            if (!s.isEmpty() && Character.isHighSurrogate(s.charAt(s.length() - 1))) {
                if (last) {
                    f.completeExceptionally(new IllegalArgumentException("the text ends in the middle of a surrogate pair"));
                    return;
                }
                outHighSurrogate = s.charAt(s.length() - 1); // its low half comes with the next part
                s = s.substring(0, s.length() - 1);
            }
            writePart(OP_TEXT, last, s.getBytes(StandardCharsets.UTF_8), f);
        });
        return f;
    }

    @Override
    public CompletableFuture<Void> sendBinaryPart(ByteBuffer part, boolean last) {
        byte[] copy = new byte[part.remaining()];
        part.duplicate().get(copy);
        CompletableFuture<Void> f = new CompletableFuture<>();
        runInLoop(() -> {
            if (!canSend(f)) {
                return;
            }
            if (outOpen && outType != OP_BINARY) {
                f.completeExceptionally(new IllegalStateException("a text message is in progress; finish it first"));
                return;
            }
            writePart(OP_BINARY, last, copy, f);
        });
        return f;
    }

    private boolean canSend(CompletableFuture<Void> f) {
        if (!opened || sentClose || closeNotified || !conn.isOpen()) {
            f.completeExceptionally(new ClosedChannelException());
            return false;
        }
        return true;
    }

    /** One fragment of a data message sent in parts. Loop thread. */
    private void writePart(int type, boolean last, byte[] bytes, CompletableFuture<Void> f) {
        if (bytes.length == 0 && !last) {
            f.complete(null); // nothing to put on the wire yet (e.g. only a held-back surrogate)
            return;
        }
        countSent(type, bytes.length, last);
        int op = outOpen ? OP_CONT : type;
        if (!outOpen) {
            outCompressed = deflate != null; // decided once per message, up front: see compress()
        }
        if (outCompressed) {
            try {
                writeFrame(op, last, !outOpen, compress(bytes, last), f); // RSV1 only on the first fragment
            } catch (RuntimeException e) {
                f.completeExceptionally(e);
                conn.close();
                return;
            }
        } else {
            writeFrame(op, last, bytes, f);
        }
        outOpen = !last;
        outType = type;
    }

    /**
     * Compresses one message, or one part of it. The deflater keeps its window between messages unless the extension was
     * negotiated without context takeover, so the decision whether a message is compressed at all must be taken before
     * its bytes go in (the caller does): the peer's window has to see exactly the messages ours saw.
     * <p>
     * Every call ends with a sync flush so the bytes can leave immediately; only at the end of the message the final
     * {@code 00 00 ff ff} marker is cut off, as RFC 7692 §7.2.1 prescribes (the receiver puts it back).
     */
    private byte[] compress(byte[] input, boolean lastOfMessage) {
        if (deflater == null) {
            deflater = new java.util.zip.Deflater(deflate.level(), true); // raw deflate, no zlib header
        }
        deflater.setInput(input);
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, input.length / 2));
        byte[] buf = new byte[8192];
        int n;
        do {
            n = deflater.deflate(buf, 0, buf.length, java.util.zip.Deflater.SYNC_FLUSH);
            out.write(buf, 0, n);
        } while (n == buf.length);
        byte[] result = out.toByteArray();
        if (lastOfMessage) {
            int len = result.length;
            if (len >= 4 && result[len - 4] == 0 && result[len - 3] == 0 && result[len - 2] == (byte) 0xff && result[len - 1] == (byte) 0xff) {
                result = java.util.Arrays.copyOf(result, len - 4);
            }
            if (deflate.compressorResets()) {
                deflater.reset(); // no context takeover: the next message starts with an empty window
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        runInLoop(() -> {
            if (closeNotified || !conn.isOpen()) {
                f.completeExceptionally(new ClosedChannelException());
            } else if (conn.isWritable()) {
                f.complete(null);
            } else {
                writableWaiters.add(f);
            }
        });
        return f;
    }

    @Override
    public CompletableFuture<Void> close(int code, String reason) {
        if (!validCloseCode(code)) {
            throw new IllegalArgumentException("invalid close status " + code);
        }
        if (reason != null && reason.getBytes(StandardCharsets.UTF_8).length > 123) {
            throw new IllegalArgumentException("close reason is limited to 123 bytes");
        }
        runInLoop(() -> {
            if (sentClose || !opened) {
                return;
            }
            sentClose = true;
            openFlag = false;
            // closeCode stays 1006 until the peer answers with its own Close frame (RFC 6455 §7.1.5)
            writeFrame(OP_CLOSE, closePayload(code, reason), null);
            // if the peer answered, the connection is long gone and this is a no-op; otherwise stop waiting
            closeTimer = conn.eventLoop().schedule(conn::close, config.closeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        });
        return closed;
    }

    /** Builds one frame (always FIN, never fragmented) and pushes it down the pipeline. Loop thread. */
    private void writeFrame(int op, byte[] payload, CompletableFuture<Void> done) {
        writeFrame(op, true, false, payload, done);
    }

    private void writeFrame(int op, boolean fin, byte[] payload, CompletableFuture<Void> done) {
        writeFrame(op, fin, false, payload, done);
    }

    private void writeFrame(int op, boolean fin, boolean rsv1, byte[] payload, CompletableFuture<Void> done) {
        int n = payload.length;
        int header = 2 + (n < 126 ? 0 : n < 65536 ? 2 : 8) + (client ? 4 : 0);
        ByteBuffer out = ByteBuffer.allocate(header + n);
        out.put((byte) ((fin ? 0x80 : 0) | (rsv1 ? 0x40 : 0) | op));
        int mask = client ? 0x80 : 0;
        if (n < 126) {
            out.put((byte) (mask | n));
        } else if (n < 65536) {
            out.put((byte) (mask | 126)).putShort((short) n);
        } else {
            out.put((byte) (mask | 127)).putLong(n);
        }
        if (client) {
            byte[] key = new byte[4];
            random.nextBytes(key);
            out.put(key);
            for (int i = 0; i < n; i++) {
                out.put((byte) (payload[i] ^ key[i & 3]));
            }
        } else {
            out.put(payload);
        }
        out.flip();
        try {
            ctx.write(out);
            if (done != null) {
                done.complete(null);
            }
        } catch (Exception e) {
            if (done != null) {
                done.completeExceptionally(e);
            }
            conn.close();
        }
    }

    // ---- keep-alive ----

    private void schedulePing() {
        if (config.pingInterval() == null) {
            return;
        }
        long interval = config.pingInterval().toNanos();
        pingTimer = conn.eventLoop().schedule(() -> {
            if (!conn.isOpen() || sentClose) {
                return;
            }
            if (System.nanoTime() - lastReceived > 2 * interval) {
                safely(() -> listener.onError(this, new SocketTimeoutException("no websocket traffic for " + 2 * config.pingInterval().toMillis() + " ms")));
                conn.close();
                return;
            }
            writeFrame(OP_PING, new byte[0], null);
            schedulePing();
        }, interval, TimeUnit.NANOSECONDS);
    }

    // ---- WebSocket accessors ----

    @Override
    public boolean isOpen() {
        return openFlag;
    }

    @Override
    public boolean isWritable() {
        return conn != null && conn.isWritable();
    }

    @Override
    public void setAutoRead(boolean enabled) {
        runInLoop(() -> {
            if (enabled) {
                boolean wasPaused = paused;
                paused = false;
                conn.setAutoRead(true);
                if (wasPaused) {
                    pump(); // frames that were already read while we were paused
                }
            } else {
                paused = true;
                conn.setAutoRead(false);
            }
        });
    }

    @Override
    public String subprotocol() {
        return subprotocol;
    }

    @Override
    public SocketAddress remoteAddress() {
        return conn.remoteAddress();
    }

    @Override
    public EventLoop eventLoop() {
        return conn.eventLoop();
    }

    // ---- helpers ----

    private interface Callback {
        void run() throws Exception;
    }

    /** Runs a listener callback; an exception becomes onError + close 1011. */
    private void callback(Callback c) {
        try {
            c.run();
        } catch (Throwable t) {
            safely(() -> listener.onError(this, t));
            fail(1011, "internal error");
        }
    }

    private static void safely(Callback c) {
        try {
            c.run();
        } catch (Throwable ignored) {
            // a throwing error/close callback must not take the loop down
        }
    }

    private void runInLoop(Runnable r) {
        EventLoop loop = conn != null ? conn.eventLoop() : null;
        if (loop == null || loop.inEventLoop()) {
            r.run();
        } else {
            try {
                loop.execute(r);
            } catch (RuntimeException rejected) {
                // loop shut down: the connection is already being closed
            }
        }
    }

    private static void cancel(Scheduled s) {
        if (s != null) {
            s.cancel();
        }
    }
}
