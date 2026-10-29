package io.hydrangea.amqp;

import io.hydrangea.Client;
import io.hydrangea.Connection;
import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;

import java.io.IOException;
import java.lang.System.Logger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An AMQP 0-9-1 client connection (RabbitMQ and compatible brokers) running on a Hydrangea event loop. Everything is asynchronous:
 * methods return {@link CompletableFuture}s, and consumer callbacks and future continuations run on the connection's loop thread,
 * so they must not block. Open channels with {@link #openChannel()}; one connection carries many channels.
 * <p>
 * This class is one connection: when it is lost, everything pending fails and it stays closed. For a connection that reconnects by itself
 * and puts exchanges, queues, bindings and consumers back, use {@link RecoveringConnection}. Not covered: SASL mechanisms other than PLAIN,
 * AMQP 1.0.
 */
public final class AmqpConnection implements AutoCloseable {

    private static final Logger LOG = System.getLogger(AmqpConnection.class.getName());

    static final int FRAME_METHOD = 1;
    static final int FRAME_HEADER = 2;
    static final int FRAME_BODY = 3;
    static final int FRAME_HEARTBEAT = 8;
    static final int FRAME_END = 0xCE;

    private enum State { AWAIT_START, AWAIT_TUNE, AWAIT_OPEN_OK, OPEN, CLOSING, CLOSED }

    private final AmqpConfig config;
    private final CompletableFuture<AmqpConnection> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final Accumulator acc = new Accumulator();
    private final Map<Integer, AmqpChannel> channels = new HashMap<>(); // loop thread only
    private final Stage stage = new AmqpStage();

    // loop thread only from here on
    private Context ctx;
    private EventLoop loop;
    private State state = State.AWAIT_START;
    private volatile int frameMax = 131072;
    private int channelMax = 65535;
    private int heartbeatSeconds;
    private long maxInbound = 1 << 20; // until Tune says otherwise
    private long lastReadNanos;
    private long lastWriteNanos;
    private Scheduled heartbeatTimer;
    private Scheduled handshakeTimer;
    private Scheduled closeTimer;
    private volatile boolean blocked;
    private volatile Map<String, Object> serverProperties = Map.of();
    private AmqpException closeReason;
    private volatile java.util.function.Consumer<Boolean> writabilityListener;

    private AmqpConnection(AmqpConfig config) {
        this.config = config;
    }

    /** Connects, authenticates and opens the virtual host; the future completes when the broker has sent Connection.OpenOk. */
    public static CompletableFuture<AmqpConnection> connect(EventLoopGroup group, AmqpConfig config) {
        AmqpConnection c = new AmqpConnection(config);
        Pipeline pipeline = config.sslContext() == null
                ? Pipeline.of(c.stage)
                : Pipeline.of(TlsStage.client(config.sslContext(), config.host(), config.port()), c.stage);
        Options options = Options.defaults().withKeepAlive(true);
        if (config.connectTimeout() != null) {
            options = options.withConnectTimeout(config.connectTimeout());
        }
        Client.connect(group, new InetSocketAddress(config.host(), config.port()), options, pipeline).whenComplete((conn, err) -> {
            if (err != null) {
                c.ready.completeExceptionally(err);
            }
        });
        return c.ready;
    }

    /** The metrics of the event loop group this connection runs on; valid once it is connected. */
    io.hydrangea.Metrics metrics() {
        return loop.metrics();
    }

    /** Opens a new channel (the unit of work on a connection: declare, publish, consume). */
    public CompletableFuture<AmqpChannel> openChannel() {
        CompletableFuture<AmqpChannel> f = new CompletableFuture<>();
        onLoop(() -> {
            if (state != State.OPEN) {
                f.completeExceptionally(new AmqpException(AmqpException.CHANNEL_ERROR, "the connection is not open", true));
                return;
            }
            int id = 0;
            for (int i = 1; i <= channelMax; i++) {
                if (!channels.containsKey(i)) {
                    id = i;
                    break;
                }
            }
            if (id == 0) {
                f.completeExceptionally(new AmqpException(AmqpException.RESOURCE_ERROR, "no free channel (channel-max " + channelMax + ")", false));
                return;
            }
            AmqpChannel ch = new AmqpChannel(this, id);
            channels.put(id, ch);
            ch.open(f);
        });
        return f;
    }

    /** Closes the connection politely (Connection.Close, wait for CloseOk, or give up after 5 s). The returned future completes when the socket is closed. */
    public CompletableFuture<Void> closeAsync() {
        onLoop(() -> {
            if (state == State.CLOSED || state == State.CLOSING) {
                return;
            }
            if (state != State.OPEN) {
                ctx.connection().close();
                return;
            }
            state = State.CLOSING;
            for (AmqpChannel ch : channels.values()) {
                ch.connectionClosing();
            }
            sendMethodFrame(0, connectionClose(AmqpException.REPLY_SUCCESS, "OK"));
            closeTimer = loop.schedule(() -> ctx.connection().close(), 5, TimeUnit.SECONDS);
        });
        return closed;
    }

    @Override
    public void close() {
        closeAsync();
    }

    public boolean isOpen() {
        return state == State.OPEN;
    }

    /** True while the broker has blocked this connection (memory or disk alarm): publishing is paused until it clears. */
    public boolean isBlocked() {
        return blocked;
    }

    /** The properties the broker announced in Connection.Start (product, version, capabilities...). */
    public Map<String, Object> serverProperties() {
        return serverProperties;
    }

    /** The heartbeat interval agreed with the broker; zero = none. */
    public Duration heartbeat() {
        return Duration.ofSeconds(heartbeatSeconds);
    }

    /**
     * False when more than the high-water mark of data (64 KiB by default) is queued for the socket: a producer should pause. Publishing
     * never blocks, so a producer that ignores this queues without bound. Use {@link #whenWritable()} to be told when to carry on.
     */
    public boolean isWritable() {
        Context c = ctx;
        return c == null || c.connection().isWritable();
    }

    /**
     * Completes at once if the connection can take more data, otherwise when the queued data has drained below the low-water mark
     * (32 KiB by default). Fails if the connection closes first, so a producer waiting on it cannot hang. The usual loop:
     * <pre>
     * void produce() {
     *     while (connection.isWritable()) { if (!more()) return; channel.basicPublish(...); }
     *     connection.whenWritable().thenRun(this::produce);    // continues on the event loop
     * }
     * </pre>
     * Continuations run on the loop thread; a thread of your own can simply {@code join()} the future.
     */
    public CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            if (state == State.CLOSED) {
                f.completeExceptionally(closeReason != null ? closeReason : new AmqpException(AmqpException.CHANNEL_ERROR, "the connection is closed", true));
            } else if (ctx == null) {
                f.complete(null);
            } else {
                ctx.connection().whenWritable().whenComplete((v, t) -> {
                    if (t == null) {
                        f.complete(null);
                    } else {
                        f.completeExceptionally(closeReason != null ? closeReason : new AmqpException(AmqpException.CHANNEL_ERROR, "the connection was lost", true));
                    }
                });
            }
        });
        return f;
    }

    /**
     * Called on the loop thread with the new state each time {@link #isWritable()} changes (false when the queue passes the high-water mark,
     * true when it drains below the low-water mark). One listener; null removes it. It must not block.
     */
    public void onWritabilityChanged(java.util.function.Consumer<Boolean> listener) {
        this.writabilityListener = listener;
    }

    /** Why the connection ended when that was an error (the broker's or our own protocol error); null otherwise. */
    AmqpException lostReason() {
        return closeReason;
    }

    /** Completes when the connection is gone (for whatever reason). */
    public CompletableFuture<Void> closeFuture() {
        return closed;
    }

    // ---- plumbing used by AmqpChannel (loop thread) ----

    void onLoop(Runnable r) {
        EventLoop l = loop;
        if (l == null) { // not active yet: nothing can have been opened
            r.run();
        } else if (l.inEventLoop()) {
            r.run();
        } else {
            try {
                l.execute(r);
            } catch (RuntimeException shutDown) {
                // the loop is gone; futures of the caller stay unresolved only if they were never registered
            }
        }
    }

    int frameMax() {
        return frameMax;
    }

    int maxMessageBytes() {
        return config.maxMessageBytes();
    }

    void removeChannel(int id) {
        channels.remove(id);
    }

    /** Sends one frame whose payload is {@code payload} (a method or content header). */
    void sendMethodFrame(int channel, Wire.Writer payload) {
        Wire.Writer out = new Wire.Writer(payload.size() + 8);
        frame(out, FRAME_METHOD, channel, payload.array(), payload.size());
        write(out);
    }

    static void frame(Wire.Writer out, int type, int channel, byte[] payload, int n) {
        out.octet(type).shortInt(channel).longInt(n).raw(payload, 0, n).octet(FRAME_END);
    }

    void write(Wire.Writer out) {
        if (state == State.CLOSED) {
            throw new AmqpException(AmqpException.CHANNEL_ERROR, "the connection is closed", true);
        }
        lastWriteNanos = System.nanoTime();
        try {
            ctx.write(ByteBuffer.wrap(out.array(), 0, out.size()));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new AmqpException("write failed", e);
        }
    }

    /** A protocol violation or fatal condition: tell the broker if we still can, fail everything, close. */
    void fatal(AmqpException e) {
        if (state == State.CLOSED) {
            return;
        }
        LOG.log(Logger.Level.WARNING, "closing the AMQP connection: {0}", e.getMessage());
        closeReason = e;
        if (state == State.OPEN || state == State.CLOSING) {
            try {
                sendMethodFrame(0, connectionClose(e.replyCode(), e.getMessage()));
            } catch (RuntimeException ignored) {
                // closing anyway
            }
        }
        state = State.CLOSING;
        ctx.connection().closeAfterFlush();
    }

    private static Wire.Writer connectionClose(int code, String text) {
        String t = text.length() > 200 ? text.substring(0, 200) : text;
        return new Wire.Writer(32).shortInt(10).shortInt(50).shortInt(code).shortStr(t).shortInt(0).shortInt(0);
    }

    // ---- the stage ----

    private final class AmqpStage implements Stage {

        @Override
        public void onActive(Context c) throws Exception {
            ctx = c;
            loop = c.connection().eventLoop();
            lastReadNanos = lastWriteNanos = System.nanoTime();
            Duration limit = config.connectTimeout() != null ? config.connectTimeout() : Duration.ofSeconds(10);
            handshakeTimer = loop.schedule(() -> {
                if (!ready.isDone()) {
                    ready.completeExceptionally(new IOException("AMQP handshake timed out"));
                    ctx.connection().close();
                }
            }, limit.toNanos(), TimeUnit.NANOSECONDS);
            c.write(ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0, 0, 9, 1}));
        }

        @Override
        public void onRead(Context c, Object msg) {
            if (state == State.CLOSED) {
                return;
            }
            acc.append((ByteBuffer) msg);
            try {
                parseFrames();
            } catch (AmqpException e) {
                fatal(e);
            } catch (RuntimeException e) {
                fatal(new AmqpException(AmqpException.INTERNAL_ERROR, "internal error: " + e, true));
                LOG.log(Logger.Level.WARNING, "AMQP frame handling failed", e);
            }
        }

        @Override
        public void onWritabilityChanged(Context c) throws Exception {
            boolean writable = c.connection().isWritable(); // the state now, which may already differ from the event that queued this call
            java.util.function.Consumer<Boolean> l = writabilityListener;
            if (l != null) {
                try {
                    l.accept(writable);
                } catch (Throwable t) {
                    LOG.log(Logger.Level.WARNING, "writability listener failed", t);
                }
            }
            c.fireWritabilityChanged();
        }

        @Override
        public void onClose(Context c, Throwable cause) {
            State before = state;
            state = State.CLOSED;
            if (before == State.OPEN) {
                loop.metrics().counter("amqp.connections.lost", null).increment();
            }
            cancel(heartbeatTimer);
            cancel(handshakeTimer);
            cancel(closeTimer);
            AmqpException why = closeReason != null ? closeReason
                    : new AmqpException(AmqpException.CHANNEL_ERROR, before == State.CLOSING ? "the connection was closed"
                            : "the connection was lost" + (cause != null ? ": " + cause : ""), true);
            for (AmqpChannel ch : new java.util.ArrayList<>(channels.values())) {
                ch.connectionLost(why);
            }
            channels.clear();
            if (!ready.isDone()) {
                ready.completeExceptionally(closeReason != null ? closeReason : cause != null ? cause : new IOException("closed during the AMQP handshake"));
            }
            acc.release();
            closed.complete(null);
            c.fireClose(cause);
        }
    }

    private static void cancel(Scheduled s) {
        if (s != null) {
            s.cancel();
        }
    }

    // ---- inbound frames ----

    private void parseFrames() {
        ByteBuffer in = acc.buffer();
        while (state != State.CLOSED) {
            if (state == State.AWAIT_START && in.remaining() >= 4 && in.get(in.position()) == 'A' && in.get(in.position() + 1) == 'M'
                    && in.get(in.position() + 2) == 'Q' && in.get(in.position() + 3) == 'P') {
                throw new AmqpException(AmqpException.NOT_IMPLEMENTED, "the broker does not speak AMQP 0-9-1 (it offered another protocol version)", true);
            }
            if (in.remaining() < 7) {
                return;
            }
            int p = in.position();
            int type = in.get(p) & 0xff;
            int channel = in.getShort(p + 1) & 0xffff;
            long size = in.getInt(p + 3) & 0xffffffffL;
            if (size > maxInbound) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "frame of " + size + " bytes exceeds the negotiated maximum " + maxInbound, true);
            }
            if (in.remaining() < 7 + size + 1) {
                return;
            }
            if ((in.get(p + 7 + (int) size) & 0xff) != FRAME_END) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "frame does not end with 0xCE", true);
            }
            ByteBuffer payload = in.slice(p + 7, (int) size);
            in.position(p + 7 + (int) size + 1);
            lastReadNanos = System.nanoTime();
            dispatch(type, channel, payload);
        }
    }

    private void dispatch(int type, int channel, ByteBuffer payload) {
        switch (type) {
            case FRAME_HEARTBEAT -> {
                if (channel != 0 || payload.hasRemaining()) {
                    throw new AmqpException(AmqpException.FRAME_ERROR, "malformed heartbeat", true);
                }
            }
            case FRAME_METHOD -> {
                Wire.Reader r = new Wire.Reader(payload);
                int cls = r.shortInt();
                int method = r.shortInt();
                if (channel == 0) {
                    connectionMethod(cls, method, r);
                } else {
                    channelOf(channel).method(cls, method, r);
                }
            }
            case FRAME_HEADER -> channelOf(channel).header(new Wire.Reader(payload));
            case FRAME_BODY -> channelOf(channel).body(payload);
            default -> throw new AmqpException(AmqpException.FRAME_ERROR, "unknown frame type " + type, true);
        }
    }

    private AmqpChannel channelOf(int id) {
        AmqpChannel ch = channels.get(id);
        if (ch == null) {
            throw new AmqpException(AmqpException.CHANNEL_ERROR, "frame for channel " + id + " which is not open", true);
        }
        return ch;
    }

    private void connectionMethod(int cls, int method, Wire.Reader r) {
        if (cls != 10) {
            throw new AmqpException(AmqpException.COMMAND_INVALID, "class " + cls + " on channel 0", true);
        }
        switch (method) {
            case 10 -> onStart(r);
            case 30 -> onTune(r);
            case 41 -> onOpenOk();
            case 50 -> onServerClose(r);
            case 51 -> {
                cancel(closeTimer);
                ctx.connection().closeAfterFlush();
            }
            case 60 -> {
                r.shortStr(); // reason
                blocked = true;
            }
            case 61 -> blocked = false;
            default -> throw new AmqpException(AmqpException.COMMAND_INVALID, "unexpected connection method " + method, true);
        }
    }

    private void onStart(Wire.Reader r) {
        if (state != State.AWAIT_START) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "Connection.Start twice", true);
        }
        int major = r.octet();
        int minor = r.octet();
        if (major != 0 || minor != 9) {
            throw new AmqpException(AmqpException.NOT_IMPLEMENTED, "broker speaks AMQP " + major + "-" + minor, true);
        }
        serverProperties = java.util.Collections.unmodifiableMap(r.table());
        String mechanisms = r.longStr();
        r.longStr(); // locales
        if (!java.util.Arrays.asList(mechanisms.split(" ")).contains("PLAIN")) {
            throw new AmqpException(AmqpException.NOT_IMPLEMENTED, "the broker offers no PLAIN authentication (offers: " + mechanisms + ")", true);
        }
        Map<String, Object> caps = new LinkedHashMap<>();
        caps.put("publisher_confirms", true);
        caps.put("consumer_cancel_notify", true);
        caps.put("basic.nack", true);
        caps.put("connection.blocked", true);
        caps.put("authentication_failure_close", true);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("product", "Hydrangea AMQP");
        props.put("version", "0.1");
        props.put("platform", "Java " + Runtime.version().feature());
        props.put("capabilities", caps);
        if (config.connectionName() != null) {
            props.put("connection_name", config.connectionName());
        }
        byte[] user = config.username().getBytes(StandardCharsets.UTF_8);
        byte[] pass = config.password().getBytes(StandardCharsets.UTF_8);
        byte[] response = new byte[2 + user.length + pass.length];
        System.arraycopy(user, 0, response, 1, user.length);
        System.arraycopy(pass, 0, response, 2 + user.length, pass.length);
        state = State.AWAIT_TUNE;
        sendMethodFrame(0, new Wire.Writer(256).shortInt(10).shortInt(11).table(props).shortStr("PLAIN").longStr(response).shortStr("en_US"));
    }

    private void onTune(Wire.Reader r) {
        if (state != State.AWAIT_TUNE) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "Connection.Tune out of order", true);
        }
        int serverChannelMax = r.shortInt();
        long serverFrameMax = r.longInt();
        int serverHeartbeat = r.shortInt();
        channelMax = (int) negotiate(config.channelMax(), serverChannelMax);
        if (channelMax == 0) {
            channelMax = 65535;
        }
        long fm = negotiate(config.frameMax(), serverFrameMax);
        frameMax = (int) (fm == 0 ? config.frameMax() : Math.min(fm, Integer.MAX_VALUE));
        if (frameMax < 4096) {
            throw new AmqpException(AmqpException.NOT_ALLOWED, "negotiated frame-max " + frameMax + " is below the spec's minimum 4096", true);
        }
        maxInbound = frameMax;
        long wanted = config.heartbeat() == null ? 0 : config.heartbeat().toSeconds();
        heartbeatSeconds = (int) Math.min(65535, negotiate(wanted, serverHeartbeat));
        state = State.AWAIT_OPEN_OK;
        sendMethodFrame(0, new Wire.Writer(16).shortInt(10).shortInt(31).shortInt(channelMax).longInt(frameMax).shortInt(heartbeatSeconds));
        sendMethodFrame(0, new Wire.Writer(32).shortInt(10).shortInt(40).shortStr(config.virtualHost()).shortStr("").octet(0));
    }

    /** The rule of the reference clients: if either side says zero take the other's value, otherwise the lower. */
    private static long negotiate(long client, long server) {
        return client == 0 || server == 0 ? Math.max(client, server) : Math.min(client, server);
    }

    private void onOpenOk() {
        if (state != State.AWAIT_OPEN_OK) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "Connection.OpenOk out of order", true);
        }
        state = State.OPEN;
        cancel(handshakeTimer);
        if (heartbeatSeconds > 0) {
            scheduleHeartbeat();
        }
        ready.complete(this);
    }

    private void scheduleHeartbeat() {
        long periodMs = Math.max(250, heartbeatSeconds * 1000L / 4);
        heartbeatTimer = loop.schedule(() -> {
            if (state != State.OPEN && state != State.CLOSING) {
                return;
            }
            long now = System.nanoTime();
            long hb = heartbeatSeconds * 1_000_000_000L;
            if (now - lastReadNanos > 2 * hb) {
                fatal(new AmqpException(AmqpException.CHANNEL_ERROR, "no data from the broker for two heartbeat intervals", true));
                return;
            }
            if (now - lastWriteNanos >= hb / 2) {
                Wire.Writer out = new Wire.Writer(8);
                out.octet(FRAME_HEARTBEAT).shortInt(0).longInt(0).octet(FRAME_END);
                try {
                    write(out);
                } catch (RuntimeException e) {
                    return;
                }
            }
            scheduleHeartbeat();
        }, periodMs, TimeUnit.MILLISECONDS);
    }

    private void onServerClose(Wire.Reader r) {
        int code = r.shortInt();
        String text = r.shortStr();
        r.shortInt();
        r.shortInt();
        AmqpException e = new AmqpException(code, text, true);
        closeReason = e;
        try {
            sendMethodFrame(0, new Wire.Writer(8).shortInt(10).shortInt(51));
        } catch (RuntimeException ignored) {
            // the socket is going away anyway
        }
        state = State.CLOSING;
        ctx.connection().closeAfterFlush();
    }
}
