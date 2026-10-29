package io.hydrangea;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A TCP connection bound to one {@link EventLoop} for its whole life.
 * <p>
 * Thread-safety: {@link #write}, {@link #close()}, {@link #closeAfterFlush()}, {@link #setAutoRead} and the
 * getters may be called from any thread; they hop to the loop when needed. Reads are delivered to the
 * {@link Handler} on the loop thread.
 * <p>
 * Backpressure: outbound data is queued without bound, but {@link #isWritable()} turns false above
 * {@link Options#highWaterMark()} so producers can stop; {@link Handler#onWritabilityChanged} signals the way back.
 */
final class SocketConnection implements Connection, SelectionHandle {

    private static final int MAX_GATHER = 16;
    /**
     * The most bytes of one heap buffer handed to a single write. The JDK copies a heap buffer into a temporary direct buffer
     * before every write, and the copy covers everything that remains, not just what the socket accepts: writing a 1 MiB
     * array into a socket that takes 256 KiB at a time would copy about 2.5 MiB for 1 MiB sent.
     */
    private static final int MAX_HEAP_WRITE = 256 * 1024;
    private static final int MAX_READS_PER_EVENT = 16;

    /** One queued write; {@code done} (nullable) runs when the buffer is fully sent or discarded. */
    private record Out(ByteBuffer buffer, Runnable done) {
    }

    private final EventLoop loop;
    private final SocketChannel channel;
    private final Handler handler;
    private final Options options;
    private final Metrics metrics;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    // loop-thread state
    private SelectionKey key;
    private final ArrayDeque<Out> outbound = new ArrayDeque<>();
    private boolean active;
    private boolean closed;
    private boolean closeAfterFlush;
    private boolean inRead;             // a read callback is running: writes it makes are sent together once it returns
    private boolean flushDeferred;
    private Duration lingerFor;      // non-null: after flushing, half-close and keep draining input instead of closing at once
    private boolean lingering;       // output shut down; incoming bytes are read and discarded until EOF or the timer
    private Scheduled lingerTimer;
    private boolean autoRead;
    private Scheduled idleTimer;
    private long lastActivity = System.nanoTime();

    // cross-thread state
    private final AtomicLong pendingBytes = new AtomicLong();
    private volatile boolean writable = true;
    private volatile boolean closing;

    SocketConnection(EventLoop loop, SocketChannel channel, Handler handler, Options options) {
        this.loop = loop;
        this.channel = channel;
        this.handler = handler;
        this.options = options;
        this.metrics = loop.metrics();
        this.autoRead = options.autoRead();
    }

    /**
     * Registers with the loop and activates the handler. {@code existingKey} is a key of this channel on the
     * loop's selector (client connect), or null.
     */
    void start(SelectionKey existingKey) {
        Runnable init = () -> {
            try {
                channel.configureBlocking(false);
                var supported = channel.supportedOptions(); // Unix domain sockets have no TCP options
                if (supported.contains(StandardSocketOptions.TCP_NODELAY)) {
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, options.tcpNoDelay());
                }
                if (supported.contains(StandardSocketOptions.SO_KEEPALIVE)) {
                    channel.setOption(StandardSocketOptions.SO_KEEPALIVE, options.keepAlive());
                }
                int ops = autoRead ? SelectionKey.OP_READ : 0;
                if (existingKey != null) {
                    key = existingKey;
                    key.attach(this);
                    key.interestOps(ops);
                } else {
                    key = loop.register(channel, ops, this);
                }
                active = true;
                metrics.connectionsOpened.increment();
                scheduleIdleCheck(options.idleTimeout());
                handler.onActive(this);
            } catch (Throwable t) {
                doClose(t);
            }
        };
        if (loop.inEventLoop()) {
            init.run();
        } else {
            loop.execute(init);
        }
    }

    // ---- public API ----

    public EventLoop eventLoop() {
        return loop;
    }

    public SocketAddress remoteAddress() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            return null;
        }
    }

    public SocketAddress localAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    public boolean isOpen() {
        return !closing && channel.isOpen();
    }

    /** False while more than {@link Options#highWaterMark()} bytes are waiting to be sent. */
    public boolean isWritable() {
        return writable;
    }

    private final java.util.ArrayList<CompletableFuture<Void>> writableWaiters = new java.util.ArrayList<>(); // loop thread only

    @Override
    public CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        loop.execute(() -> {
            if (closed) {
                f.completeExceptionally(new java.io.IOException("the connection is closed"));
            } else if (writable) {
                f.complete(null);
            } else {
                writableWaiters.add(f);
            }
        });
        return f;
    }

    private void wakeWritableWaiters(Throwable failure) {
        if (writableWaiters.isEmpty()) {
            return;
        }
        var ready = new java.util.ArrayList<>(writableWaiters);
        writableWaiters.clear();
        for (CompletableFuture<Void> f : ready) {
            if (failure == null) {
                f.complete(null);
            } else {
                f.completeExceptionally(failure);
            }
        }
    }

    /** Bytes accepted by {@link #write} that have not been fully sent yet. */
    public long pendingBytes() {
        return pendingBytes.get();
    }

    /** Completes (never exceptionally) when the connection is closed. */
    public CompletableFuture<Void> closeFuture() {
        return closeFuture;
    }

    /**
     * Queues {@code data} for sending. Ownership of the buffer passes to the connection: do not touch it afterwards.
     * Silently dropped if the connection is closed.
     */
    public void write(ByteBuffer data) {
        write(data, null);
    }

    /**
     * Like {@link #write(ByteBuffer)}, and runs {@code done} on the loop thread once the buffer is no longer
     * referenced by the connection: fully sent, or discarded because the connection closed. Use it to return
     * the buffer to a {@link BufferPool}.
     */
    public void write(ByteBuffer data, Runnable done) {
        int n = data.remaining();
        if (n == 0 || closing) {
            runDone(done);
            return;
        }
        pendingBytes.addAndGet(n);
        Out out = new Out(data, done);
        if (loop.inEventLoop()) {
            enqueue(out);
        } else {
            try {
                loop.execute(() -> enqueue(out));
            } catch (RuntimeException rejected) {
                pendingBytes.addAndGet(-n);
                runDone(done);
            }
        }
    }

    /** Stops reading until re-enabled; the way to apply backpressure to a fast sender. */
    public void setAutoRead(boolean enabled) {
        runInLoop(() -> {
            if (closed || autoRead == enabled) {
                return;
            }
            autoRead = enabled;
            if (key != null && key.isValid()) {
                key.interestOps(enabled ? key.interestOps() | SelectionKey.OP_READ : key.interestOps() & ~SelectionKey.OP_READ);
            }
        });
    }

    /** Asks the handler to wind down (see {@link Handler#onDrain}); used by {@link Server#shutdownGracefully}. */
    void drain() {
        runInLoop(() -> {
            if (!closed && active) {
                fire(() -> handler.onDrain(this));
            }
        });
    }

    /** Closes now, discarding unsent data. Idempotent. */
    public void close() {
        runInLoop(() -> doClose(null));
    }

    /** Sends everything already queued, then closes. */
    public void closeAfterFlush() {
        closeAfterFlush(null);
    }

    /**
     * Sends everything already queued, then closes <em>lingeringly</em>: the output is shut down (the peer sees a clean
     * FIN after our last byte) and incoming bytes are read and thrown away until the peer closes or {@code linger}
     * elapses. Use it when the peer may still be sending data we no longer want (a rejected upload, a protocol error):
     * closing a socket that holds unread data makes the kernel send RST instead of FIN, and the RST can destroy our final
     * message (a close frame, an error response) before the peer has read it.
     *
     * @param linger how long to wait for the peer's close; null for an immediate close after flushing
     */
    public void closeAfterFlush(Duration linger) {
        runInLoop(() -> {
            if (closed || closeAfterFlush) {
                return;
            }
            closeAfterFlush = true;
            lingerFor = linger;
            if (outbound.isEmpty()) {
                finishClosing();
            }
        });
    }

    /** The queue is empty and a close was requested: close now, or half-close and linger. */
    private void finishClosing() {
        if (lingerFor == null) {
            doClose(null);
            return;
        }
        try {
            channel.shutdownOutput();
        } catch (IOException e) {
            doClose(null);
            return;
        }
        lingering = true;
        autoRead = true; // even a paused connection must be drained, or the peer's unread data would cause the RST after all
        if (key != null && key.isValid()) {
            key.interestOps(SelectionKey.OP_READ);
        }
        lingerTimer = loop.schedule(() -> doClose(null), lingerFor.toNanos(), TimeUnit.NANOSECONDS);
    }

    // ---- loop-thread internals ----

    private void runInLoop(Runnable r) {
        if (loop.inEventLoop()) {
            r.run();
        } else {
            try {
                loop.execute(r);
            } catch (RuntimeException rejected) {
                // loop already gone: terminate() has closed or will close the channel
            }
        }
    }

    private static void runDone(Runnable done) {
        if (done != null) {
            try {
                done.run();
            } catch (Throwable ignored) {
                // a failing release callback must not break the connection
            }
        }
    }

    private void enqueue(Out out) {
        if (closed || closeAfterFlush) {
            pendingBytes.addAndGet(-out.buffer.remaining());
            runDone(out.done);
            return;
        }
        outbound.add(out);
        if (writable && pendingBytes.get() > options.highWaterMark()) {
            writable = false;
            fire(() -> handler.onWritabilityChanged(this));
        }
        if (inRead) {
            flushDeferred = true;   // many requests in one read produce many responses: send them with one system call
            return;
        }
        if (loop.batchingTasks()) {   // likewise for work queued with execute(): one system call per connection per batch of tasks
            if (!flushDeferred) {
                flushDeferred = true;
                loop.flushAtEndOfBatch(this);
            }
            return;
        }
        flush();
    }

    private void flush() {
        try {
            while (!outbound.isEmpty()) {
                ByteBuffer[] batch = new ByteBuffer[Math.min(outbound.size(), MAX_GATHER)];
                int i = 0;
                for (Out o : outbound) {
                    batch[i++] = o.buffer;
                    // A heap buffer above MAX_HEAP_WRITE is written in slices (below). It must be the last of the batch: a gather write
                    // that has used up the shortened buffer carries on into the next one, which would put that buffer's bytes in the
                    // middle of this one.
                    if (i == batch.length || (!o.buffer.isDirect() && o.buffer.remaining() > MAX_HEAP_WRITE)) {
                        break;
                    }
                }
                if (i < batch.length) {
                    batch = Arrays.copyOf(batch, i);
                }
                int[] limits = null;
                for (int k = 0; k < batch.length; k++) {
                    ByteBuffer b = batch[k];
                    if (!b.isDirect() && b.remaining() > MAX_HEAP_WRITE) {
                        if (limits == null) {
                            limits = new int[batch.length];
                            Arrays.fill(limits, -1);
                        }
                        limits[k] = b.limit();
                        b.limit(b.position() + MAX_HEAP_WRITE);
                    }
                }
                long written;
                try {
                    written = batch.length == 1 ? channel.write(batch[0]) : channel.write(batch);
                } finally {
                    if (limits != null) {
                        for (int k = 0; k < batch.length; k++) {
                            if (limits[k] >= 0) {
                                batch[k].limit(limits[k]);
                            }
                        }
                    }
                }
                if (written > 0) {
                    lastActivity = System.nanoTime();
                    pendingBytes.addAndGet(-written);
                    metrics.bytesWritten.add(written);
                }
                while (!outbound.isEmpty() && !outbound.peek().buffer.hasRemaining()) {
                    runDone(outbound.poll().done);
                }
                if (written == 0) {
                    break; // socket buffer full
                }
            }
        } catch (IOException e) {
            doClose(e);
            return;
        }
        if (outbound.isEmpty()) {
            setWriteInterest(false);
            if (closeAfterFlush) {
                if (!lingering) {
                    finishClosing();
                }
                return;
            }
        } else {
            setWriteInterest(true);
        }
        if (!writable && pendingBytes.get() <= options.lowWaterMark()) {
            writable = true;
            wakeWritableWaiters(null);
            fire(() -> handler.onWritabilityChanged(this));
        }
    }

    private void setWriteInterest(boolean on) {
        if (key == null || !key.isValid()) {
            return;
        }
        int ops = key.interestOps();
        int wanted = on ? ops | SelectionKey.OP_WRITE : ops & ~SelectionKey.OP_WRITE;
        if (wanted != ops) {
            key.interestOps(wanted);
        }
    }

    @Override
    public void ready(int readyOps) {
        if ((readyOps & SelectionKey.OP_WRITE) != 0) {
            flush();
        }
        if ((readyOps & SelectionKey.OP_READ) != 0 && !closed) {
            read();
        }
    }

    @Override
    public void rebind(SelectionKey newKey) {
        key = newKey;
    }

    private void read() {
        inRead = true;
        try {
            readLoop();
        } finally {
            inRead = false;
            flushDeferredWrites();
        }
    }

    void flushDeferredWrites() {
        if (flushDeferred) {
            flushDeferred = false;
            if (!closed) {
                flush();
            }
        }
    }

    private void readLoop() {
        ByteBuffer buf = loop.readBuffer();
        try {
            for (int i = 0; i < MAX_READS_PER_EVENT && !closed && autoRead; i++) {
                buf.clear();
                int n = channel.read(buf);
                if (n < 0) {
                    doClose(null); // orderly EOF
                    return;
                }
                if (n == 0) {
                    return;
                }
                lastActivity = System.nanoTime();
                metrics.bytesRead.add(n);
                if (lingering) {
                    continue; // draining: the bytes are unwanted; keep reading until EOF so the close is a FIN, not a RST
                }
                buf.flip();
                try {
                    handler.onRead(this, buf);
                } catch (Throwable t) {
                    doClose(t);
                    return;
                }
                flushDeferredWrites();
                if (n < buf.capacity()) {
                    return; // drained; skip the extra read that would return 0
                }
            }
        } catch (IOException e) {
            doClose(e);
        }
    }

    private void fire(ThrowingRunnable callback) {
        try {
            callback.run();
        } catch (Throwable t) {
            doClose(t);
        }
    }

    private void scheduleIdleCheck(Duration timeout) {
        if (timeout == null) {
            return;
        }
        long timeoutNanos = timeout.toNanos();
        long idleFor = System.nanoTime() - lastActivity;
        idleTimer = loop.schedule(() -> {
            long idle = System.nanoTime() - lastActivity;
            if (idle >= timeoutNanos) {
                doClose(new IdleTimeoutException("no activity for " + timeout));
            } else {
                scheduleIdleCheck(timeout);
            }
        }, Math.max(1, timeoutNanos - idleFor), TimeUnit.NANOSECONDS);
    }

    private void doClose(Throwable cause) {
        if (closed) {
            return;
        }
        if (flushDeferred) {   // what the handler wrote before closing still goes out, as it did when every write was sent at once
            flushDeferred = false;
            flush();
            if (closed) {
                return;
            }
        }
        closed = true;
        closing = true;
        if (idleTimer != null) {
            idleTimer.cancel();
        }
        if (lingerTimer != null) {
            lingerTimer.cancel();
        }
        if (key != null) {
            key.cancel();
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // already closing
        }
        for (Out o; (o = outbound.poll()) != null; ) {
            runDone(o.done);
        }
        pendingBytes.set(0);
        writable = false;
        wakeWritableWaiters(cause != null ? cause : new java.io.IOException("the connection is closed"));
        try {
            if (active) {
                metrics.connectionsClosed.increment();
                handler.onClose(this, cause);
            }
        } catch (Throwable ignored) {
            // a failing onClose must not prevent completion
        } finally {
            closeFuture.complete(null);
        }
    }

    @Override
    public void terminate() {
        doClose(null);
    }

    @Override
    public String toString() {
        return "Connection[" + localAddress() + " -> " + remoteAddress() + (closing ? ", closed" : "") + "]";
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
