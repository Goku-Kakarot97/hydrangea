package io.hydrangea.http;

import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;

/**
 * Client side of one connection: writes the request, parses the response, and reports back to the pool.
 * Last stage of the pipeline; all methods run on the connection's loop thread (except {@link #currentExchange}).
 * <p>
 * Parsing is as strict as the server's: CRLF only, no obsolete folding, no whitespace before a header colon,
 * {@code Transfer-Encoding} together with {@code Content-Length} and conflicting lengths are protocol errors, and
 * headers and body are size-limited. A connection is reused only if the response was fully framed
 * (not close-delimited), keep-alive, and no extra bytes followed it.
 */
final class HttpClientStage implements Stage {

    private enum State { IDLE, HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, UNTIL_CLOSE }

    private static final int MAX_CHUNK_LINE = 1024;

    private final HttpClient.PooledConn owner;
    private final HttpClient client;
    private final Accumulator acc = new Accumulator();

    private State state = State.IDLE;
    private Context ctx;
    private boolean dead;
    private volatile HttpClient.Exchange current;
    private byte[] unsent;
    private boolean gotBytes;

    // response under construction
    private int status;
    private String reason;
    private Headers headers;
    private ByteArrayOutputStream body;
    private long remaining;
    private int trailerBytes;
    private boolean keepAlive;

    // streamed request body state (loop thread)
    private UploadSink upload;              // non-null while a streamed body is being produced
    private boolean uploadPending;         // head not yet written; body to start right after it
    private boolean awaitingContinue;      // Expect: 100-continue sent, waiting for the go-ahead
    private boolean uploadComplete = true; // false while a streamed body has not been completely sent
    private io.hydrangea.Scheduled continueTimer;

    // streamed response state (loop thread)
    private boolean streaming;
    private boolean paused;
    private ContentDecoder decoder;                       // non-null while the body is being decoded for the application
    private java.io.ByteArrayOutputStream pendingDecoded; // decoded bytes produced after pause(): held until resume()
    private boolean deferredFinish;                       // the body ended while decoded bytes were still held
    private boolean deferredCloseDelimited;
    private BodyListener listener;
    private final Control control = new Control();

    HttpClientStage(HttpClient.PooledConn owner, HttpClient client) {
        this.owner = owner;
        this.client = client;
    }

    HttpClient.Exchange currentExchange() {
        return current;
    }

    // ---- Stage ----

    @Override
    public void onActive(Context ctx) {
        this.ctx = ctx; // last stage: nothing to forward
        if (unsent != null) {
            flush();
        }
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        if (current == null) { // the server spoke without being asked
            dead = true;
            ctx.connection().close();
            return;
        }
        gotBytes = true;
        acc.append((ByteBuffer) msg);
        try {
            parse();
        } catch (IOException | RuntimeException e) {
            failCurrent(e instanceof IOException ? e : new IOException("bad response", e));
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        dead = true;
        acc.release();
        if (current != null) {
            if (state == State.UNTIL_CLOSE && cause == null) {
                finish(true); // end of a close-delimited body
            } else {
                failCurrent(new IOException("connection closed before the response was complete", cause));
            }
        }
    }

    /** From the connection's closeFuture: covers a close before activation, when onClose is not delivered. */
    void connectionClosed() {
        dead = true;
        if (current != null) {
            failCurrent(new ClosedChannelException());
        }
    }

    // ---- sending ----

    void submit(HttpClient.Exchange ex) {
        if (dead) {
            owner.failed(ex, new ClosedChannelException(), false);
            return;
        }
        byte[] bytes;
        try {
            bytes = HttpClient.serialize(client, ex, client.userAgent(), owner.plainProxy);
        } catch (RuntimeException e) {
            owner.failed(ex, e, true); // a bad request is not retryable
            return;
        }
        current = ex;
        state = State.HEAD;
        gotBytes = false;
        headers = null;
        body = null;
        unsent = bytes;
        uploadPending = HttpClient.hasStreamedBody(ex.req);
        uploadComplete = !uploadPending;
        if (ctx != null) {
            flush();
        }
    }

    private void flush() {
        byte[] bytes = unsent;
        unsent = null;
        try {
            ctx.write(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            failCurrent(e instanceof IOException io ? io : new IOException(e));
            return;
        }
        if (uploadPending) {
            uploadPending = false;
            beginBody();
        }
    }

    // ---- sending a streamed request body ----

    /** The head is out. Start the body now, or after the server's go-ahead if the request asked for 100-continue. */
    private void beginBody() {
        HttpClient.Exchange ex = current;
        if (ex.req.headers().hasToken("expect", "100-continue")) {
            awaitingContinue = true;
            continueTimer = ctx.connection().eventLoop().schedule(() -> {
                if (awaitingContinue) { // no answer in time: send anyway, as curl does
                    awaitingContinue = false;
                    startBody();
                }
            }, 1, java.util.concurrent.TimeUnit.SECONDS);
        } else {
            startBody();
        }
    }

    private void startBody() {
        HttpClient.Exchange ex = current;
        if (ex == null || upload != null) {
            return;
        }
        RequestBody rb = ex.req.requestBody();
        upload = new UploadSink(rb.contentLength());
        try {
            rb.start(upload);
        } catch (Throwable t) {
            failCurrent(t instanceof IOException io ? io : new IOException("request body failed to start", t));
        }
    }

    /** Stops any upload bookkeeping for the finished or failed exchange and frees producers that are waiting. */
    private void endUpload(Throwable reason) {
        awaitingContinue = false;
        uploadPending = false;
        if (continueTimer != null) {
            continueTimer.cancel();
            continueTimer = null;
        }
        UploadSink u = upload;
        upload = null;
        if (u != null) {
            u.kill(reason);
        }
    }

    @Override
    public void onWritabilityChanged(Context ctx) {
        if (upload != null) {
            upload.notifyWritable();
        }
    }

    /** The {@link BodySink} handed to the request body of the current exchange. */
    private final class UploadSink implements BodySink {
        private final long declared;               // -1 = chunked
        private long written;                      // loop thread
        private boolean finished;                  // loop thread
        private volatile boolean dead;
        private volatile Throwable deadCause;
        private final java.util.ArrayList<CompletableFuture<Void>> waiters = new java.util.ArrayList<>(); // loop thread

        UploadSink(long declared) {
            this.declared = declared;
        }

        @Override
        public CompletableFuture<Void> write(ByteBuffer data) {
            byte[] copy = new byte[data.remaining()];
            data.get(copy);
            CompletableFuture<Void> f = new CompletableFuture<>();
            onLoop(() -> {
                if (dead || finished) {
                    f.completeExceptionally(dead ? stopped() : new IllegalStateException("the body is already finished"));
                    return;
                }
                if (declared >= 0 && written + copy.length > declared) {
                    IOException e = new IOException("the request body is longer than the declared " + declared + " bytes");
                    f.completeExceptionally(e);
                    failCurrent(e);
                    return;
                }
                written += copy.length;
                if (copy.length == 0) {
                    f.complete(null);
                    return;
                }
                try {
                    if (declared >= 0) {
                        ctx.write(ByteBuffer.wrap(copy));
                    } else {
                        byte[] size = (Integer.toHexString(copy.length) + "\r\n").getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
                        ByteBuffer framed = ByteBuffer.allocate(size.length + copy.length + 2);
                        framed.put(size).put(copy).put((byte) '\r').put((byte) '\n').flip();
                        ctx.write(framed);
                    }
                    f.complete(null);
                } catch (Exception e) {
                    f.completeExceptionally(e);
                    failCurrent(e instanceof IOException io ? io : new IOException(e));
                }
            });
            return f;
        }

        @Override
        public boolean isWritable() {
            return !dead && ctx != null && ctx.connection().isWritable();
        }

        @Override
        public CompletableFuture<Void> whenWritable() {
            CompletableFuture<Void> f = new CompletableFuture<>();
            onLoop(() -> {
                if (dead) {
                    f.completeExceptionally(stopped());
                } else if (ctx.connection().isWritable()) {
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
                if (dead || finished) {
                    return;
                }
                if (declared >= 0 && written != declared) {
                    failCurrent(new IOException("the request body ended after " + written + " of the declared " + declared + " bytes"));
                    return;
                }
                finished = true;
                try {
                    if (declared < 0) {
                        ctx.write(ByteBuffer.wrap("0\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)));
                    }
                } catch (Exception e) {
                    failCurrent(e instanceof IOException io ? io : new IOException(e));
                    return;
                }
                uploadComplete = true;
            });
        }

        @Override
        public void fail(Throwable cause) {
            onLoop(() -> {
                if (!dead) {
                    failCurrent(cause instanceof IOException ? cause : new IOException("request body failed", cause));
                }
            });
        }

        /** Loop thread: the exchange is over; release everybody waiting on this sink. */
        void kill(Throwable reason) {
            dead = true;
            deadCause = reason;
            for (CompletableFuture<Void> w : waiters) {
                w.completeExceptionally(stopped());
            }
            waiters.clear();
        }

        void notifyWritable() {
            if (!ctx.connection().isWritable() || waiters.isEmpty()) {
                return;
            }
            var ready = new java.util.ArrayList<>(waiters);
            waiters.clear();
            ready.forEach(w -> w.complete(null));
        }

        private IOException stopped() {
            Throwable c = deadCause;
            return new IOException("the request is no longer being sent" + (c != null && c.getMessage() != null ? ": " + c.getMessage() : ""), c);
        }

        private void onLoop(Runnable r) {
            if (ctx == null) {
                r.run();
                return;
            }
            var loop = ctx.connection().eventLoop();
            if (loop.inEventLoop()) {
                r.run();
            } else {
                try {
                    loop.execute(r);
                } catch (RuntimeException shutDown) {
                    dead = true; // no loop left to run anything
                }
            }
        }
    }

    // ---- parsing ----

    private void parse() throws IOException {
        while (current != null) {
            if (streaming && paused) {
                return; // buffered bytes stay in acc until resume()
            }
            ByteBuffer in = acc.buffer();
            boolean progressed = switch (state) {
                case HEAD -> parseHead(in);
                case BODY -> readFixed(in);
                case CHUNK_SIZE -> readChunkSize(in);
                case CHUNK_DATA -> readChunkData(in);
                case CHUNK_CRLF -> readChunkCrlf(in);
                case TRAILERS -> readTrailers(in);
                case UNTIL_CLOSE -> readUntilClose(in);
                case IDLE -> false;
            };
            if (!progressed) {
                return;
            }
        }
    }

    private boolean parseHead(ByteBuffer in) throws IOException {
        int end = indexOf(in, "\r\n\r\n");
        if (end < 0) {
            for (int i = in.position(); i < in.limit(); i++) {
                if (in.get(i) == '\n' && (i == in.position() || in.get(i - 1) != '\r')) {
                    throw new IOException("bare LF in response headers");
                }
            }
            if (in.remaining() > client.maxHeaderBytes()) {
                throw new IOException("response headers exceed " + client.maxHeaderBytes() + " bytes");
            }
            return false;
        }
        if (end - in.position() > client.maxHeaderBytes()) {
            throw new IOException("response headers exceed " + client.maxHeaderBytes() + " bytes");
        }
        byte[] raw = new byte[end - in.position()];
        in.get(raw);
        in.position(in.position() + 4);
        String head = new String(raw, StandardCharsets.ISO_8859_1);
        for (int i = 0; i < head.length(); i++) {
            char c = head.charAt(i);
            if (c == 0 || (c == '\r' && !head.startsWith("\r\n", i)) || (c == '\n' && (i == 0 || head.charAt(i - 1) != '\r'))) {
                throw new IOException("illegal control character in response headers");
            }
        }
        String[] lines = head.split("\r\n", -1);
        String version = parseStatusLine(lines[0]);
        Headers h = new Headers();
        for (int i = 1; i < lines.length; i++) {
            parseHeaderLine(h, lines[i]);
        }
        if (status >= 100 && status < 200) {
            if (status == 101) {
                HttpClient.Exchange up = current;
                if (up != null && up.offerUpgrade && h.hasToken("upgrade", "h2c") && owner.proto != null) {
                    // accepted: bytes that follow are HTTP/2 frames, and this request is stream 1 of the new protocol
                    ByteBuffer rest = in.hasRemaining() ? ByteBuffer.allocate(in.remaining()).put(in).flip() : ByteBuffer.allocate(0);
                    current = null;
                    state = State.IDLE;
                    gotBytes = false;
                    owner.proto.switchToHttp2(up, rest);
                    return true;
                }
                throw new IOException("protocol upgrade (101) is not supported");
            }
            if (status == 100 && awaitingContinue) { // the go-ahead
                awaitingContinue = false;
                if (continueTimer != null) {
                    continueTimer.cancel();
                }
                startBody();
            }
            return true; // interim response (e.g. 100 Continue): keep waiting for the final one
        }
        if (awaitingContinue) { // a final answer instead of the go-ahead: the body is never sent
            awaitingContinue = false;
            if (continueTimer != null) {
                continueTimer.cancel();
            }
        }
        keepAlive = version.equals("HTTP/1.1") ? !h.hasToken("connection", "close") : h.hasToken("connection", "keep-alive");
        body = new ByteArrayOutputStream();
        HttpClient.Exchange ex = current;
        if (ex.offerUpgrade) { // a final answer to the offer that is not 101: the server does not do h2c, carry on as HTTP/1.1
            ex.offerUpgrade = false;
            owner.proto.upgradeDeclined();
        }
        streaming = ex.stream != null && !client.buffersForRedirect(status, h);
        boolean noBody = ex.req.method().equals("HEAD") || status == 204 || status == 304;
        decoder = noBody ? null : client.contentDecoder(ex, h, streaming);
        headers = decoder != null ? HttpClient.withoutEncoding(h) : h; // what the application sees; decideBody still reads the wire headers
        client.storeCookies(ex.req.uri(), h);
        if (streaming) {
            listener = ex.stream;
            ex.cancelTimeout(); // from here on only the idle timeout applies
            try {
                listener.onHead(new ResponseHead(status, reason, headers, ex.req.uri()), control);
            } catch (Throwable t) {
                throw new IOException("stream listener failed in onHead", t);
            }
        }
        decideBody(h);
        return true;
    }

    private String parseStatusLine(String line) throws IOException {
        if (line.length() < 12 || !(line.startsWith("HTTP/1.1 ") || line.startsWith("HTTP/1.0 "))) {
            throw new IOException("malformed status line: " + printable(line));
        }
        for (int i = 9; i < 12; i++) {
            if (line.charAt(i) < '0' || line.charAt(i) > '9') {
                throw new IOException("malformed status code: " + printable(line));
            }
        }
        if (line.length() > 12 && line.charAt(12) != ' ') {
            throw new IOException("malformed status line: " + printable(line));
        }
        status = Integer.parseInt(line.substring(9, 12));
        if (status < 100) {
            throw new IOException("invalid status code " + status);
        }
        reason = line.length() > 13 ? line.substring(13) : "";
        return line.substring(0, 8);
    }

    private static void parseHeaderLine(Headers h, String line) throws IOException {
        if (line.isEmpty() || line.charAt(0) == ' ' || line.charAt(0) == '\t') {
            throw new IOException("obsolete header folding / empty header line");
        }
        int colon = line.indexOf(':');
        if (colon <= 0) {
            throw new IOException("malformed header line: " + printable(line));
        }
        String name = line.substring(0, colon);
        for (int i = 0; i < name.length(); i++) {
            if (!HttpServerStage.isTokenChar(name.charAt(i))) {
                throw new IOException("illegal header name: " + printable(name));
            }
        }
        int s = colon + 1;
        int e = line.length();
        while (s < e && (line.charAt(s) == ' ' || line.charAt(s) == '\t')) s++;
        while (e > s && (line.charAt(e - 1) == ' ' || line.charAt(e - 1) == '\t')) e--;
        h.add(name, line.substring(s, e));
    }

    private void decideBody(Headers h) throws IOException {
        boolean noBody = current.req.method().equals("HEAD") || status == 204 || status == 304;
        var te = h.getAll("transfer-encoding");
        var cl = h.getAll("content-length");
        if (noBody) {
            finish(false);
        } else if (!te.isEmpty()) {
            if (!cl.isEmpty()) {
                throw new IOException("both Transfer-Encoding and Content-Length present");
            }
            if (te.size() != 1 || !te.get(0).equalsIgnoreCase("chunked")) {
                throw new IOException("unsupported Transfer-Encoding: " + printable(String.join(",", te)));
            }
            state = State.CHUNK_SIZE;
        } else if (!cl.isEmpty()) {
            long len = -1;
            for (String v : cl) {
                for (String part : v.split(",", -1)) {
                    long n = parseLength(part.trim());
                    if (len >= 0 && n != len) {
                        throw new IOException("conflicting Content-Length values");
                    }
                    len = n;
                }
            }
            if (!streaming && len > client.maxResponseBytes()) {
                throw new IOException("response body of " + len + " bytes exceeds the limit of " + client.maxResponseBytes());
            }
            remaining = len;
            if (len == 0) {
                finish(false);
            } else {
                state = State.BODY;
            }
        } else {
            state = State.UNTIL_CLOSE; // body runs until the server closes
        }
    }

    private static long parseLength(String s) throws IOException {
        if (s.isEmpty() || s.length() > 18) {
            throw new IOException("invalid Content-Length");
        }
        long n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new IOException("invalid Content-Length");
            }
            n = n * 10 + (c - '0');
        }
        return n;
    }

    private boolean readFixed(ByteBuffer in) throws IOException {
        if (!in.hasRemaining()) {
            return false;
        }
        int n = (int) Math.min(remaining, in.remaining());
        appendBody(in, n);
        remaining -= n;
        if (remaining == 0) {
            finish(false);
        }
        return true;
    }

    private boolean readUntilClose(ByteBuffer in) throws IOException {
        if (!in.hasRemaining()) {
            return false;
        }
        int n = in.remaining();
        if (!streaming && body.size() + (long) n > client.maxResponseBytes()) {
            throw new IOException("response body exceeds the limit of " + client.maxResponseBytes());
        }
        appendBody(in, n);
        return true;
    }

    private boolean readChunkSize(ByteBuffer in) throws IOException {
        int eol = indexOf(in, "\r\n");
        if (eol < 0) {
            if (in.remaining() > MAX_CHUNK_LINE) {
                throw new IOException("chunk size line too long");
            }
            return false;
        }
        if (eol - in.position() > MAX_CHUNK_LINE) {
            throw new IOException("chunk size line too long");
        }
        byte[] raw = new byte[eol - in.position()];
        in.get(raw);
        in.position(in.position() + 2);
        String line = new String(raw, StandardCharsets.ISO_8859_1);
        int semi = line.indexOf(';'); // chunk extensions are ignored
        String hex = (semi < 0 ? line : line.substring(0, semi)).trim();
        if (hex.isEmpty() || hex.length() > 15) {
            throw new IOException("invalid chunk size");
        }
        long size = 0;
        for (int i = 0; i < hex.length(); i++) {
            int d = Character.digit(hex.charAt(i), 16);
            if (d < 0) {
                throw new IOException("invalid chunk size");
            }
            size = size * 16 + d;
        }
        if (!streaming && body.size() + size > client.maxResponseBytes()) {
            throw new IOException("response body exceeds the limit of " + client.maxResponseBytes());
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

    private boolean readChunkData(ByteBuffer in) throws IOException {
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

    private boolean readChunkCrlf(ByteBuffer in) throws IOException {
        if (in.remaining() < 2) {
            return false;
        }
        if (in.get(in.position()) != '\r' || in.get(in.position() + 1) != '\n') {
            throw new IOException("missing CRLF after chunk data");
        }
        in.position(in.position() + 2);
        state = State.CHUNK_SIZE;
        return true;
    }

    private boolean readTrailers(ByteBuffer in) throws IOException {
        int eol = indexOf(in, "\r\n");
        if (eol < 0) {
            if (in.remaining() + trailerBytes > client.maxHeaderBytes()) {
                throw new IOException("trailers too large");
            }
            return false;
        }
        int len = eol - in.position();
        trailerBytes += len + 2;
        if (trailerBytes > client.maxHeaderBytes()) {
            throw new IOException("trailers too large");
        }
        in.position(eol + 2); // trailer fields are read and discarded
        if (len == 0) {
            finish(false);
        }
        return true;
    }

    private void appendBody(ByteBuffer in, int n) throws IOException {
        if (decoder != null) {
            byte[] tmp = new byte[n];
            in.get(tmp);
            decoder.write(tmp, 0, n, this::emitDecoded);
            return;
        }
        if (streaming) {
            ByteBuffer slice = in.slice(in.position(), n).asReadOnlyBuffer();
            in.position(in.position() + n);
            try {
                listener.onData(slice, control);
            } catch (Throwable t) {
                throw new IOException("stream listener failed in onData", t);
            }
            return;
        }
        byte[] tmp = new byte[n];
        in.get(tmp);
        body.write(tmp, 0, n);
    }

    /** Decoded bytes go to the listener, or into the buffered body; after pause() they wait for resume(). */
    private void emitDecoded(byte[] data, int off, int len) throws IOException {
        if (!streaming) {
            body.write(data, off, len);
            return;
        }
        if (paused) {
            if (pendingDecoded == null) {
                pendingDecoded = new ByteArrayOutputStream();
            }
            pendingDecoded.write(data, off, len);
            return;
        }
        try {
            listener.onData(ByteBuffer.wrap(data, off, len).asReadOnlyBuffer(), control);
        } catch (Throwable t) {
            throw new IOException("stream listener failed in onData", t);
        }
    }

    private static int indexOf(ByteBuffer in, String needle) {
        int n = needle.length();
        outer:
        for (int i = in.position(); i <= in.limit() - n; i++) {
            for (int j = 0; j < n; j++) {
                if (in.get(i + j) != needle.charAt(j)) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    // ---- completion ----

    private void finish(boolean closeDelimited) {
        if (decoder != null && paused && pendingDecoded != null && pendingDecoded.size() > 0) {
            deferredFinish = true; // the listener must see every byte before the end
            deferredCloseDelimited = closeDelimited;
            return;
        }
        if (decoder != null) {
            try {
                decoder.finish();
            } catch (IOException e) {
                failCurrent(e);
                return;
            }
            decoder = null;
        }
        HttpClient.Exchange ex = current;
        boolean wasStreaming = streaming;
        ClientResponse resp = new ClientResponse(status, reason, headers, wasStreaming ? new byte[0] : body.toByteArray(), ex.req.uri());
        boolean reusable = !closeDelimited && keepAlive && !dead && !acc.buffer().hasRemaining() && uploadComplete;
        endUpload(uploadComplete ? null : new IOException("the server answered before the request body was fully sent"));
        endStreamState();
        current = null;
        state = State.IDLE;
        gotBytes = false;
        if (wasStreaming) {
            owner.finishedStream(ex, resp, reusable);
        } else {
            owner.finished(ex, resp, reusable);
        }
    }

    /** Leaves streaming mode; a connection that is paused must read again before it can serve the next request. */
    private void endStreamState() {
        if (paused && ctx != null && !dead) {
            ctx.connection().setAutoRead(true);
        }
        paused = false;
        streaming = false;
        listener = null;
        if (decoder != null) {
            decoder.abort();
            decoder = null;
        }
        pendingDecoded = null;
        deferredFinish = false;
    }

    private void failCurrent(Throwable t) {
        HttpClient.Exchange ex = current;
        if (ex == null) {
            return;
        }
        boolean got = gotBytes;
        endUpload(t);
        endStreamState();
        current = null;
        state = State.IDLE;
        owner.failed(ex, t, got);
    }

    /** Flow control handed to the listener. */
    private final class Control implements BodyControl {

        @Override
        public void pause() {
            onLoop(() -> {
                if (current != null && streaming && !paused) {
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
                if (current == null) {
                    return;
                }
                ctx.connection().setAutoRead(true);
                try {
                    if (pendingDecoded != null && pendingDecoded.size() > 0) { // decoded bytes that came after the pause
                        byte[] held = pendingDecoded.toByteArray();
                        pendingDecoded = null;
                        listener.onData(ByteBuffer.wrap(held).asReadOnlyBuffer(), this);
                    }
                    if (deferredFinish && !paused) {
                        deferredFinish = false;
                        finish(deferredCloseDelimited);
                        return;
                    }
                    parse(); // bytes that were read before the pause
                } catch (Exception e) {
                    failCurrent(e instanceof IOException ? e : new IOException(e));
                }
            });
        }

        @Override
        public void cancel() {
            HttpClient.Exchange ex = current;
            if (ex != null) {
                ex.fail(new java.util.concurrent.CancellationException("stream cancelled"));
            }
        }

        private void onLoop(Runnable r) {
            if (ctx == null) {
                return;
            }
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

    private static String printable(String s) {
        String cut = s.length() > 80 ? s.substring(0, 80) + "..." : s;
        return cut.replaceAll("[^\\x20-\\x7e]", "?");
    }
}
