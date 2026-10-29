package io.hydrangea.tls;

import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TLS for a {@link io.hydrangea.pipeline.Pipeline}: put it first (closest to the socket).
 * <ul>
 *   <li>Inbound: ciphertext {@link ByteBuffer} → plaintext {@link ByteBuffer}.</li>
 *   <li>Outbound: plaintext {@link ByteBuffer} → ciphertext. Writes issued before the handshake completes are queued.</li>
 *   <li>{@code onActive} is forwarded to the next stage only <b>after the handshake finishes</b>, so application
 *       stages can start writing immediately in {@code onActive}.</li>
 * </ul>
 * Delegated handshake tasks (certificate validation etc.) run inline on the event loop; they are short for
 * normal certificate chains, but a custom TrustManager that blocks (OCSP, remote lookups) will stall that loop.
 */
public final class TlsStage implements Stage {

    private final SSLEngine engine;
    private final Accumulator netIn = new Accumulator();
    private final ArrayDeque<ByteBuffer> pendingPlain = new ArrayDeque<>();
    private final ByteBuffer[] gather = new ByteBuffer[16];
    private Context ctx;
    private boolean activated;
    private io.hydrangea.Metrics metrics;
    private io.hydrangea.Latency handshakeTime;
    private long handshakeStart;     // the clock when the handshake began (0 = latency is not being recorded)
    private boolean pumping;
    private boolean repump;
    private boolean closeRequested;
    private Duration linger;     // lingering close requested from a later stage (null = close at once)
    private Duration handshakeTimeout = Duration.ofSeconds(10);
    private Scheduled handshakeTimer;
    private boolean closePending;   // close_notify to be sent once everything queued has been flushed

    private TlsStage(SSLEngine engine) {
        this.engine = engine;
    }

    /** Client side; verifies the server certificate against {@code host} (RFC 2818 / "HTTPS" algorithm). */
    /** Client stage that also offers these ALPN protocols, e.g. {@code List.of("h2", "http/1.1")}; see {@link #applicationProtocol()}. */
    public static TlsStage client(SSLContext context, String host, int port, List<String> applicationProtocols) {
        TlsStage stage = client(context, host, port);
        SSLParameters params = stage.engine.getSSLParameters();
        params.setApplicationProtocols(applicationProtocols.toArray(new String[0]));
        stage.engine.setSSLParameters(params);
        return stage;
    }

    public static TlsStage client(SSLContext context, String host, int port) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        SSLParameters params = engine.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(params);
        return new TlsStage(engine);
    }

    /** Server stage that also offers these ALPN protocols (in order of preference), e.g. {@code List.of("h2", "http/1.1")}. */
    public static TlsStage server(SSLContext context, List<String> applicationProtocols) {
        TlsStage stage = server(context);
        SSLParameters params = stage.engine.getSSLParameters();
        params.setApplicationProtocols(applicationProtocols.toArray(new String[0]));
        stage.engine.setSSLParameters(params);
        return stage;
    }

    /** The ALPN protocol agreed in the handshake (e.g. {@code "h2"}), or null if there was none or the handshake has not finished. */
    public String applicationProtocol() {
        String p = engine.getApplicationProtocol();
        return p == null || p.isEmpty() ? null : p;
    }

    public static TlsStage server(SSLContext context) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        return new TlsStage(engine);
    }

    /** For advanced configuration (ALPN, client auth, protocols). Must be configured before the connection starts. */
    public static TlsStage of(SSLEngine engine) {
        return new TlsStage(engine);
    }

    public SSLEngine engine() {
        return engine;
    }

    /**
     * How long the TLS handshake may take before the connection is dropped (default 10 s; null = no limit). Without a
     * limit a peer that connects and then says nothing would hold the connection and its memory forever.
     */
    public TlsStage handshakeTimeout(java.time.Duration timeout) {
        this.handshakeTimeout = timeout;
        return this;
    }

    /**
     * Flushes data still waiting, then sends close_notify and closes the connection. Loop thread only.
     * Safe to call from inside a callback delivered by this stage.
     */
    public void closeNotify() throws Exception {
        closePending = true;
        pump();
    }

    @Override
    public void onCloseRequest(Context ctx, java.time.Duration linger) throws Exception {
        this.linger = linger;
        closeNotify();
    }

    // ---- Stage ----

    @Override
    public void onActive(Context ctx) throws Exception {
        this.ctx = ctx;
        metrics = ctx.connection().eventLoop().metrics();
        handshakeTime = metrics.latency("tls.handshake.duration");
        handshakeStart = handshakeTime.begin();
        if (handshakeTimeout != null) {
            handshakeTimer = ctx.connection().eventLoop().schedule(() -> {
                if (!activated) {
                    ctx.connection().close(); // never completed the handshake
                }
            }, handshakeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        engine.beginHandshake();
        pump();
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        netIn.append((ByteBuffer) msg);
        pump();
    }

    @Override
    public void onWrite(Context ctx, Object msg) throws Exception {
        pendingPlain.add((ByteBuffer) msg);
        pump();
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        if (!activated && metrics != null) {
            metrics.counter("tls.handshakes", null, "result", "failed").increment();   // closed before the handshake finished: refused, timed out, or cut off
        }
        if (handshakeTimer != null) {
            handshakeTimer.cancel();
        }
        netIn.release();
        ctx.fireClose(cause);
    }

    // ---- engine driver ----

    /** Re-entrancy safe: stages above us may write from inside fireActive/fireRead. */
    private void pump() throws SSLException {
        if (pumping) {
            repump = true;
            return;
        }
        pumping = true;
        try {
            do {
                repump = false;
                drive();
            } while (repump);
        } finally {
            pumping = false;
        }
        if (closeRequested) {
            ctx.connection().closeAfterFlush(linger);
        }
    }

    private void drive() throws SSLException {
        boolean progress = true;
        while (progress) {
            progress = false;
            HandshakeStatus hs = engine.getHandshakeStatus();
            switch (hs) {
                case NEED_TASK -> {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    progress = true;
                }
                case NEED_WRAP -> progress = wrap(EMPTY);
                case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> progress = unwrap();
                default -> { // NOT_HANDSHAKING / FINISHED
                    progress = flushPlain();
                    progress |= unwrap();
                    if (closePending && (pendingPlain.isEmpty() || !activated) && !engine.isOutboundDone()) {
                        engine.closeOutbound(); // everything queued is out: now the close_notify
                        progress = true;
                    }
                }
            }
            if (engine.isOutboundDone() && engine.isInboundDone()) {
                closeRequested = true;
            }
        }
    }

    private boolean flushPlain() throws SSLException {
        if (!activated) {
            return false;
        }
        boolean any = false;
        while (!pendingPlain.isEmpty()) {
            // everything queued goes through one wrap call, so a response written as several small buffers (headers, data, trailers) is one record
            int n = 0;
            for (ByteBuffer b : pendingPlain) {
                if (n == gather.length) {
                    break;
                }
                gather[n++] = b;
            }
            boolean progressed = wrap(gather, n);
            Arrays.fill(gather, 0, n, null);
            if (!progressed) {
                break; // closed or nothing consumed
            }
            any = true;
            ByteBuffer head;
            while ((head = pendingPlain.peek()) != null && !head.hasRemaining()) {
                pendingPlain.poll();
            }
        }
        return any;
    }

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /**
     * Scratch space for the engine's output, one pair per event-loop thread (stages only run there): a TLS record can be 16 KiB and allocating and
     * zeroing that for every wrap and unwrap cost more than the encryption of a small message. What the engine produced is copied out at its real size.
     */
    private static final ThreadLocal<ByteBuffer[]> SCRATCH = ThreadLocal.withInitial(() -> new ByteBuffer[2]);

    private static ByteBuffer scratch(int slot, int size) {
        ByteBuffer[] pair = SCRATCH.get();
        ByteBuffer b = pair[slot];
        if (b == null || b.capacity() < size) {
            b = ByteBuffer.allocate(size);
            pair[slot] = b;
        }
        b.clear();
        return b;
    }

    private static ByteBuffer copyOut(ByteBuffer scratch) {
        ByteBuffer exact = ByteBuffer.allocate(scratch.remaining());
        exact.put(scratch).flip();
        return exact;
    }

    private boolean wrap(ByteBuffer[] srcs, int count) throws SSLException {
        ByteBuffer out = scratch(0, engine.getSession().getPacketBufferSize());
        SSLEngineResult r;
        while ((r = engine.wrap(srcs, 0, count, out)).getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            ByteBuffer bigger = ByteBuffer.allocate(out.capacity() * 2);
            out.flip();
            bigger.put(out);
            out = bigger;
            SCRATCH.get()[0] = out;
        }
        out.flip();
        boolean produced = out.hasRemaining();
        if (produced) {
            try {
                ctx.write(copyOut(out));
            } catch (SSLException e) {
                throw e;
            } catch (Exception e) {
                throw new SSLException(e);
            }
        }
        if (r.getStatus() == SSLEngineResult.Status.CLOSED) {
            closeRequested = true;
        }
        noteHandshake(r);
        return produced || r.bytesConsumed() > 0;
    }

    private boolean wrap(ByteBuffer src) throws SSLException {
        gather[0] = src;
        boolean r = wrap(gather, 1);
        gather[0] = null;
        return r;
    }

    private boolean unwrap() throws SSLException {
        ByteBuffer in = netIn.buffer();
        if (!in.hasRemaining()) {
            return false;
        }
        ByteBuffer out = scratch(1, engine.getSession().getApplicationBufferSize());
        SSLEngineResult r;
        while ((r = engine.unwrap(in, out)).getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            ByteBuffer bigger = ByteBuffer.allocate(out.capacity() * 2);
            out.flip();
            bigger.put(out);
            out = bigger;
            SCRATCH.get()[1] = out;
        }
        noteHandshake(r);
        out.flip();
        if (out.hasRemaining()) {
            ByteBuffer plain = copyOut(out);   // copied before delivery: the handler may write, and write wraps with the same scratch
            try {
                ctx.fireRead(plain);
            } catch (SSLException e) {
                throw e;
            } catch (Exception e) {
                throw new SSLException(e);
            }
        }
        if (r.getStatus() == SSLEngineResult.Status.CLOSED) {
            closePending = true; // answer the peer's close_notify, after flushing what we still owe it
            repump = true;
        }
        return r.bytesConsumed() > 0 || r.bytesProduced() > 0;
    }

    private void noteHandshake(SSLEngineResult r) throws SSLException {
        if (r.getHandshakeStatus() == HandshakeStatus.FINISHED && !activated) {
            activated = true;
            handshakeTime.end(handshakeStart);
            metrics.counter("tls.handshakes", null, "result", "ok", "protocol", engine.getSession().getProtocol()).increment();   // once per handshake: the lookup is no cost next to it
            if (handshakeTimer != null) {
                handshakeTimer.cancel();
            }
            try {
                ctx.fireActive();
            } catch (SSLException e) {
                throw e;
            } catch (Exception e) {
                throw new SSLException(e);
            }
            repump = true; // flush anything written during fireActive / before the handshake ended
        }
    }
}
