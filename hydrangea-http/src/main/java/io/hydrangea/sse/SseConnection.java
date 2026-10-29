package io.hydrangea.sse;

import io.hydrangea.Connection;
import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** The stage that owns an SSE connection after the response head, and the {@link SseEmitter} the application sees. */
final class SseConnection implements SseEmitter, Stage {

    private static final byte[] TERMINATOR = "0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
    private static final byte[] HEARTBEAT = SseEvent.comment("keep-alive").encode();

    private final SseListener listener;
    private final SseConfig config;
    private final boolean chunked;
    private final String lastEventId;

    private Context ctx;
    private io.hydrangea.Metrics.Count mOpen;
    private io.hydrangea.Metrics.Count mSent;
    private io.hydrangea.Metrics.Count mSlow;
    private boolean counted;
    private Connection conn;
    private volatile boolean open;
    private boolean closeNotified;
    private Scheduled heartbeat;
    private long lastWrite = System.nanoTime();

    SseConnection(SseListener listener, SseConfig config, boolean chunked, String lastEventId) {
        this.listener = listener;
        this.config = config;
        this.chunked = chunked;
        this.lastEventId = lastEventId;
    }

    // ---- Stage (loop thread) ----

    @Override
    public void onActive(Context ctx) throws Exception {
        this.ctx = ctx;
        this.conn = ctx.connection();
        io.hydrangea.Metrics metrics = conn.eventLoop().metrics();
        mOpen = metrics.gauge("sse.connections", null, "side", "server");
        mSent = metrics.counter("sse.events", null, "side", "server", "direction", "sent");
        mSlow = metrics.counter("sse.slow.disconnects", null);
        counted = true;
        mOpen.increment();
        open = true;
        if (config.retry() != null) {
            writeFrame(new SseEvent(null, null, null, config.retry(), null).encode());
        }
        scheduleHeartbeat();
        listener.onOpen(this);
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        // an event stream is one-way; whatever the client sends now is ignored
    }

    @Override
    public void onWritabilityChanged(Context ctx) throws Exception {
        listener.onWritabilityChanged(this);
    }

    @Override
    public void onDrain(Context ctx) {
        close(); // end the stream cleanly; EventSource clients reconnect on their own (to another instance)
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        open = false;
        if (counted) {
            counted = false;
            mOpen.decrement();
        }
        if (heartbeat != null) {
            heartbeat.cancel();
        }
        if (!closeNotified) {
            closeNotified = true;
            try {
                listener.onClose(this);
            } catch (Throwable ignored) {
                // a failing callback must not disturb the loop
            }
        }
    }

    // ---- SseEmitter ----

    @Override
    public CompletableFuture<Void> send(SseEvent event) {
        byte[] bytes = event.encode(); // validation and encoding happen on the caller's thread
        CompletableFuture<Void> f = new CompletableFuture<>();
        runInLoop(() -> {
            if (!open) {
                f.completeExceptionally(new ClosedChannelException());
            } else if (conn.pendingBytes() + bytes.length > config.maxPendingBytes()) {
                f.completeExceptionally(new IOException("client is too slow: more than " + config.maxPendingBytes() + " bytes pending"));
                open = false;
                mSlow.increment();
                conn.close(); // onClose notifies the listener
            } else {
                writeFrame(bytes);
                mSent.increment();
                f.complete(null);
            }
        });
        return f;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isWritable() {
        return conn != null && conn.isWritable();
    }

    @Override
    public void close() {
        runInLoop(() -> {
            if (!open) {
                return;
            }
            open = false;
            try {
                if (chunked) {
                    ctx.write(ByteBuffer.wrap(TERMINATOR));
                }
                ctx.closeAfterFlush();
            } catch (Exception e) {
                conn.close();
            }
        });
    }

    @Override
    public String lastEventId() {
        return lastEventId;
    }

    @Override
    public SocketAddress remoteAddress() {
        return conn == null ? null : conn.remoteAddress();
    }

    // ---- internals ----

    /** One event, framed as an HTTP chunk when the client speaks HTTP/1.1. Loop thread. */
    private void writeFrame(byte[] payload) {
        ByteBuffer out;
        if (chunked) {
            byte[] size = (Integer.toHexString(payload.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
            out = ByteBuffer.allocate(size.length + payload.length + 2);
            out.put(size).put(payload).put((byte) '\r').put((byte) '\n').flip();
        } else {
            out = ByteBuffer.wrap(payload);
        }
        lastWrite = System.nanoTime();
        try {
            ctx.write(out);
        } catch (Exception e) {
            open = false;
            conn.close();
        }
    }

    private void scheduleHeartbeat() {
        if (config.heartbeat() == null) {
            return;
        }
        long interval = config.heartbeat().toNanos();
        long wait = Math.max(1, interval - (System.nanoTime() - lastWrite));
        heartbeat = conn.eventLoop().schedule(() -> {
            if (!open) {
                return;
            }
            if (System.nanoTime() - lastWrite >= interval) {
                writeFrame(HEARTBEAT); // a write to a vanished client fails or fills up, which ends the stream
            }
            scheduleHeartbeat();
        }, wait, TimeUnit.NANOSECONDS);
    }

    private void runInLoop(Runnable r) {
        EventLoop loop = conn == null ? null : conn.eventLoop();
        if (loop == null || loop.inEventLoop()) {
            r.run();
        } else {
            try {
                loop.execute(r);
            } catch (RuntimeException rejected) {
                // loop shut down: the connection is being torn down
            }
        }
    }
}
