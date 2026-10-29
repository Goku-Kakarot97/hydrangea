package io.hydrangea.redis;

import io.hydrangea.Client;
import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;

import java.lang.System.Logger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A Redis client (RESP3, falling back to RESP2) on a Hydrangea event loop. <b>One client is one connection that any number of threads share</b>: every method
 * returns a {@link CompletableFuture} at once, commands from all threads are written together and the replies are matched to them in order (pipelining
 * without asking for it), so a thousand commands in flight cost one round trip, not a thousand. Futures complete, and handlers run, on the client's event-loop
 * thread: they must not block.
 * <p>
 * <b>Values.</b> A reply is a Java value: simple string {@code String}; bulk string {@code byte[]}; integer {@code Long}; array and set {@code List<Object>}; map
 * {@code Map<Object,Object>} with String keys; null {@code null}; boolean, double, big number as {@code Boolean}, {@code Double}, {@code BigInteger}; and an error
 * <em>inside</em> an array {@link RedisError}. The typed methods ({@link #get}, {@link #hgetAll}, ...) convert for you; {@link #command} gives the raw value.
 * A reply that is an error fails the future with a {@link RedisException}.
 * <p>
 * <b>State that belongs to the connection</b> is shared by everyone using it: {@code SELECT}, {@code WATCH}, {@code CLIENT} settings, and blocking commands
 * ({@code BLPOP}, {@code XREAD BLOCK}...) which hold up every reply behind them. Use {@link #duplicate()} for a connection of your own for those.
 * <p>
 * <b>Connection loss.</b> With {@link RedisConfig#reconnect()} (the default) a lost connection is re-established with back-off and the subscriptions are
 * restored. The commands that were waiting for a reply fail with a retriable {@link RedisException}: they may or may not have been executed. Commands made while the
 * client is not connected fail at once.
 * <p>
 * Not covered: Cluster and Sentinel, client-side caching, the {@code MONITOR} command, and pub/sub on RESP2 (it needs RESP3, so Redis 6 or later).
 */
public final class RedisClient implements AutoCloseable {

    private static final Logger LOG = System.getLogger(RedisClient.class.getName());

    private enum State { CONNECTING, ONLINE, OFFLINE, CLOSED }

    /** A command sent and waiting for its reply. */
    private static final class Cmd {
        final byte[] bytes;
        final CompletableFuture<Object> future;     // null for the client's own commands
        final Function<Object, Object> convert;     // applied to a reply that is not an error
        final BiConsumer<Object, RedisException> internal;   // the client's own commands: what to do with the outcome
        Cmd[] chain;                                // commands that must follow this one directly (a transaction); their bytes are included in this one's
        int confirmations;                          // SUBSCRIBE-like commands: pushes still to come before it is done
        long start;                                 // the clock when the loop accepted it (0 = not timed); shared by all commands of one batch

        Cmd(byte[] bytes, CompletableFuture<Object> future, Function<Object, Object> convert, BiConsumer<Object, RedisException> internal) {
            this.bytes = bytes;
            this.future = future;
            this.convert = convert;
            this.internal = internal;
        }
    }

    private final RedisConfig config;
    private final RedisListener listener;
    private final CompletableFuture<RedisClient> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private EventLoopGroup group;
    private EventLoop home;

    // any thread
    private final ConcurrentLinkedQueue<Cmd> inbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private volatile Context ctx;
    private volatile boolean online;
    private volatile int protocol = 2;
    private volatile Map<Object, Object> serverInfo = Map.of();

    // loop thread only
    private State state = State.CONNECTING;
    private int gen;
    private boolean everOnline;
    private int attempt;
    private Scheduled reconnectTimer;
    private Scheduled connectTimer;
    private final ArrayDeque<Cmd> pending = new ArrayDeque<>();
    private long batchClock;            // loop thread: when the batch being accepted started (0 = latency is off)
    private long replyClock;            // loop thread: when the replies being parsed arrived
    private int pendingReported;            // what the "pending" gauge has been told, so it is updated once per batch rather than per command
    private io.hydrangea.Metrics.Count mOk;
    private io.hydrangea.Metrics.Count mError;
    private io.hydrangea.Metrics.Count mFailed;
    private io.hydrangea.Metrics.Count mPending;
    private io.hydrangea.Metrics.Count mLost;
    private io.hydrangea.Metrics.Count mReconnects;
    private io.hydrangea.Latency mLatency;
    private final List<byte[]> batch = new ArrayList<>();
    private Accumulator acc = new Accumulator();
    private RespParser parser;
    private final Map<String, List<MessageHandler>> channels = new LinkedHashMap<>();
    private final Map<String, List<MessageHandler>> patterns = new LinkedHashMap<>();

    private RedisClient(RedisConfig config, RedisListener listener) {
        this.config = config;
        this.listener = listener != null ? listener : new RedisListener() { };
    }

    /** Connects, logs in and completes when the server is ready. A first attempt that fails is not retried. */
    public static CompletableFuture<RedisClient> connect(EventLoopGroup group, RedisConfig config) {
        return connect(group, config, null);
    }

    public static CompletableFuture<RedisClient> connect(EventLoopGroup group, RedisConfig config, RedisListener listener) {
        RedisClient c = new RedisClient(config, listener);
        c.group = group;
        c.home = group.next();
        io.hydrangea.Metrics m = group.metrics();
        c.mOk = m.counter("redis.commands", null, "outcome", "ok");
        c.mError = m.counter("redis.commands", null, "outcome", "error");       // the server answered with an error
        c.mFailed = m.counter("redis.commands", null, "outcome", "failed");     // no answer: connection lost, client closed, too many waiting
        c.mPending = m.gauge("redis.commands.pending", null);
        c.mLost = m.counter("redis.connections.lost", null);
        c.mReconnects = m.counter("redis.reconnects", null);
        c.mLatency = m.latency("redis.command.duration");
        c.onLoop(c::startConnecting);
        return c.ready;
    }

    /** A new connection with the same settings, for what must not share a connection: {@code WATCH}, {@code SELECT}, blocking commands. */
    public CompletableFuture<RedisClient> duplicate() {
        return connect(group, config, null);
    }

    // ---- sending ----

    /** Runs any command: {@code command("SET", "k", "v")}. An argument is a byte[], a String, a number, a Boolean or an Enum. Completes with the raw reply (see the class comment). */
    public CompletableFuture<Object> command(Object... args) {
        return send(null, args);
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> send(Function<Object, Object> convert, Object... args) {
        byte[] bytes;
        try {
            bytes = Resp.encode(args);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        Cmd c = new Cmd(bytes, new CompletableFuture<>(), convert, null);
        submit(c);
        return (CompletableFuture<T>) c.future;
    }

    private void submit(Cmd c) {
        if (home.inEventLoop()) {
            batchClock = mLatency.now();
            accept(c);
            flush();
            reportPending();
            return;
        }
        inbox.add(c);
        if (drainScheduled.compareAndSet(false, true)) {
            try {
                home.execute(this::drain);
            } catch (RuntimeException shutDown) {
                drainScheduled.set(false);
                failInbox(new RedisException("the event loop is shut down", false));
            }
        }
    }

    /** On the loop: takes what the threads have put in the inbox and writes it together. */
    private void drain() {
        for (;;) {
            batchClock = mLatency.now();   // one clock read for everything taken from the inbox (reading it per command is the dear part of timing)
            Cmd c;
            while ((c = inbox.poll()) != null) {
                accept(c);
            }
            flush();
            reportPending();
            drainScheduled.set(false);
            if (inbox.isEmpty() || !drainScheduled.compareAndSet(false, true)) {
                return;
            }
        }
    }

    /** Tells the pending gauge how many commands are waiting since it was last told: one atomic add per batch. */
    private void reportPending() {
        int n = pending.size();
        if (n != pendingReported) {
            mPending.add(n - pendingReported);
            pendingReported = n;
        }
    }

    private void failInbox(RedisException e) {
        for (Cmd c; (c = inbox.poll()) != null; ) {
            reject(c, e);
        }
    }

    private void accept(Cmd c) {
        if (state != State.ONLINE) {
            reject(c, new RedisException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            return;
        }
        int n = 1 + (c.chain == null ? 0 : c.chain.length);
        if (pending.size() + n > config.maxPending()) {
            reject(c, new RedisException("too many commands waiting for a reply (maxPending " + config.maxPending() + ")", true));
            return;
        }
        c.start = batchClock;
        pending.add(c);
        if (c.chain != null) {
            for (Cmd f : c.chain) {
                pending.add(f);
            }
        }
        batch.add(c.bytes);
    }

    private void reject(Cmd c, RedisException e) {
        fail(c, e);
        int n = 1;
        if (c.chain != null) {
            for (Cmd f : c.chain) {
                fail(f, e);
                n++;
            }
        }
        mFailed.add(n);
    }

    private static void fail(Cmd c, RedisException e) {
        if (c.future != null) {
            c.future.completeExceptionally(e);
        }
        if (c.internal != null) {
            c.internal.accept(null, e);
        }
    }

    private static final int GATHER_LIMIT = 1 << 20;
    private static final int BIG = 64 * 1024;

    /** Writes the commands accepted since the last flush: small ones in one buffer (one system call), big ones as they are. */
    private void flush() {
        int n = batch.size();
        if (n == 0) {
            return;
        }
        try {
            Context c = ctx;
            if (n == 1) {
                c.write(ByteBuffer.wrap(batch.get(0)));
            } else {
                int i = 0;
                while (i < n) {
                    int j = i;
                    int sum = 0;
                    while (j < n && batch.get(j).length < BIG && sum + batch.get(j).length <= GATHER_LIMIT) {
                        sum += batch.get(j).length;
                        j++;
                    }
                    if (j == i) {
                        c.write(ByteBuffer.wrap(batch.get(i)));
                        i++;
                    } else if (j == i + 1) {
                        c.write(ByteBuffer.wrap(batch.get(i)));
                        i = j;
                    } else {
                        byte[] one = new byte[sum];
                        int p = 0;
                        for (int k = i; k < j; k++) {
                            byte[] b = batch.get(k);
                            System.arraycopy(b, 0, one, p, b.length);
                            p += b.length;
                        }
                        c.write(ByteBuffer.wrap(one));
                        i = j;
                    }
                }
            }
        } catch (Exception e) {
            fail(new RedisException("write failed: " + e, true, e));
        } finally {
            batch.clear();
        }
    }

    // ---- connecting ----

    private void onLoop(Runnable r) {
        if (home.inEventLoop()) {
            r.run();
        } else {
            try {
                home.execute(r);
            } catch (RuntimeException shutDown) {
                // the loop is gone; nothing more can happen to this client
            }
        }
    }

    private void startConnecting() {
        if (state == State.CLOSED) {
            return;
        }
        state = State.CONNECTING;
        online = false;
        int g = ++gen;
        parser = new RespParser(config.maxBulkBytes(), config.maxElements());
        acc = new Accumulator();
        // resolving the name can block: not on the loop
        Thread.startVirtualThread(() -> {
            InetSocketAddress addr;
            try {
                addr = new InetSocketAddress(config.host(), config.port());
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(config.host());
                }
            } catch (Exception e) {
                onLoop(() -> attemptFailed(g, new RedisException("cannot resolve " + config.host() + ": " + e, true, e)));
                return;
            }
            Stage stage = new RedisStage(g);
            Pipeline pipeline = config.sslContext() == null ? Pipeline.of(stage)
                    : Pipeline.of(TlsStage.client(config.sslContext(), config.host(), config.port()), stage);
            Options options = Options.defaults().withKeepAlive(true);
            if (config.connectTimeout() != null) {
                options = options.withConnectTimeout(config.connectTimeout());
            }
            Client.connect(home, addr, options, pipeline).whenComplete((conn, err) -> {
                if (err != null) {
                    onLoop(() -> attemptFailed(g, new RedisException("cannot connect to " + config.host() + ":" + config.port() + ": " + err, true, err)));
                }
            });
        });
    }

    private void attemptFailed(int g, Throwable err) {
        if (g == gen && state == State.CONNECTING) {
            connectionEnded(err);
        }
    }

    private final class RedisStage implements Stage {
        private final int g;

        RedisStage(int g) {
            this.g = g;
        }

        @Override
        public void onActive(Context c) {
            if (g != gen || state != State.CONNECTING) {
                c.connection().close();
                return;
            }
            ctx = c;
            if (config.connectTimeout() != null) {
                connectTimer = home.schedule(() -> {
                    if (g == gen && state == State.CONNECTING) {
                        fail(new RedisException("no login within " + config.connectTimeout().toMillis() + " ms", true));
                    }
                }, config.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
            }
            handshake();
        }

        @Override
        public void onRead(Context c, Object msg) {
            if (g != gen) {
                return;
            }
            try {
                onData((ByteBuffer) msg);
            } catch (RedisException e) {
                fail(e);
            } catch (RuntimeException e) {
                LOG.log(Logger.Level.WARNING, "Redis reply handling failed", e);
                fail(new RedisException("internal error: " + e, false, e));
            }
        }

        @Override
        public void onClose(Context c, Throwable cause) {
            if (g == gen) {
                connectionEnded(cause != null ? cause : new java.io.IOException("the connection was closed"));
            }
            c.fireClose(cause);
        }
    }

    private Throwable lossCause;

    /** Closes the connection because of {@code cause}; {@link #connectionEnded} does the rest. */
    private void fail(Throwable cause) {
        if (lossCause == null) {
            lossCause = cause;
        }
        Context c = ctx;
        if (c != null) {
            c.connection().close();
        } else {
            connectionEnded(cause);
        }
    }

    private void connectionEnded(Throwable cause) {
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        Throwable why = lossCause != null ? lossCause : cause;
        lossCause = null;
        ctx = null;
        online = false;
        boolean wasOnline = state == State.ONLINE;
        acc.release();
        failPending(why instanceof RedisException r ? r : new RedisException("the connection was lost: " + why, true, why));
        if (state == State.CLOSED) {
            closed.complete(null);
            return;
        }
        if (!everOnline) {   // the first attempt: the caller decides whether to try again
            state = State.CLOSED;
            ready.completeExceptionally(why instanceof RedisException ? why : new RedisException("cannot connect to " + config.host() + ":" + config.port() + ": " + why, true, why));
            giveUp(why, false);
            return;
        }
        if (wasOnline) {
            mLost.increment();
            safely(() -> listener.connectionLost(why));
        }
        boolean retriable = !(why instanceof RedisException r) || r.retriable();
        if (!retriable || config.reconnect() == null) {
            giveUp(why, true);
            return;
        }
        int max = config.reconnect().maxAttempts();
        attempt++;
        if (max > 0 && attempt > max) {
            giveUp(new RedisException("gave up reconnecting after " + max + " attempts", false, why), true);
            return;
        }
        Duration delay = config.reconnect().delayFor(attempt);
        state = State.OFFLINE;
        mReconnects.increment();
        safely(() -> listener.reconnecting(attempt, delay));
        reconnectTimer = home.schedule(this::startConnecting, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void giveUp(Throwable cause, boolean notify) {
        state = State.CLOSED;
        failInbox(new RedisException("the client is closed", false));
        if (notify) {
            safely(() -> listener.gaveUp(cause));
        }
        closed.complete(null);   // after the listener: whoever waits for the close has then been told why
    }

    private void failPending(RedisException e) {
        for (Cmd c; (c = pending.poll()) != null; ) {
            fail(c, e);
            if (c.future != null) {
                mFailed.increment();
            }
        }
        reportPending();
        batch.clear();
    }

    private void safely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "Redis listener failed", t);
        }
    }

    // ---- login ----

    private void internal(BiConsumer<Object, RedisException> done, Object... args) {
        internal(done, 0, args);
    }

    private void internal(BiConsumer<Object, RedisException> done, int confirmations, Object... args) {
        Cmd c = new Cmd(Resp.encode(args), null, null, done);
        c.confirmations = confirmations;
        pending.add(c);
        batch.add(c.bytes);
        flush();
    }

    private void handshake() {
        if (config.protocol() == 3) {
            List<Object> hello = new ArrayList<>(List.of("HELLO", 3));
            if (config.password() != null) {
                hello.addAll(List.of("AUTH", config.username() != null ? config.username() : "default", config.password()));
            }
            if (config.clientName() != null) {
                hello.addAll(List.of("SETNAME", config.clientName()));
            }
            internal((v, err) -> {
                if (err == null) {
                    protocol = 3;
                    serverInfo = v instanceof Map<?, ?> m ? readable(m) : Map.of();
                    selectDatabase();
                } else if (err.fromServer() && (err.getMessage().startsWith("ERR unknown command") || err.getMessage().startsWith("NOPROTO")
                        || err.getMessage().contains("unknown command 'HELLO'") || err.getMessage().startsWith("ERR Unknown subcommand"))) {
                    resp2Login();   // before Redis 6
                } else {
                    loginFailed(err);
                }
            }, hello.toArray());
        } else {
            resp2Login();
        }
    }

    /** HELLO's answer with the byte[] values as text. */
    private static Map<Object, Object> readable(Map<?, ?> m) {
        Map<Object, Object> out = new LinkedHashMap<>();
        m.forEach((k, v) -> out.put(k, v instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : v));
        return java.util.Collections.unmodifiableMap(out);
    }

    private void resp2Login() {
        protocol = 2;
        if (config.password() != null) {
            internal((v, err) -> {
                if (err != null) {
                    loginFailed(err);
                } else {
                    setName();
                }
            }, config.username() != null ? new Object[] {"AUTH", config.username(), config.password()} : new Object[] {"AUTH", config.password()});
        } else {
            setName();
        }
    }

    private void setName() {
        if (config.clientName() != null) {
            internal((v, err) -> {
                if (err != null) {
                    loginFailed(err);
                } else {
                    selectDatabase();
                }
            }, "CLIENT", "SETNAME", config.clientName());
        } else {
            selectDatabase();
        }
    }

    private void selectDatabase() {
        if (config.database() != 0) {
            internal((v, err) -> {
                if (err != null) {
                    loginFailed(err);
                } else {
                    wentOnline();
                }
            }, "SELECT", config.database());
        } else {
            wentOnline();
        }
    }

    private void loginFailed(RedisException err) {
        fail(new RedisException(err.code(), "login failed: " + err.getMessage(), false, err.fromServer(), err));
    }

    private void wentOnline() {
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        boolean again = everOnline;
        everOnline = true;
        attempt = 0;
        state = State.ONLINE;
        online = true;
        // the subscriptions go back before anything else the user asks for
        if (!channels.isEmpty()) {
            internal((v, err) -> logResubscribe(err), channels.size(), concat("SUBSCRIBE", channels.keySet()));
        }
        if (!patterns.isEmpty()) {
            internal((v, err) -> logResubscribe(err), patterns.size(), concat("PSUBSCRIBE", patterns.keySet()));
        }
        if (!again) {
            ready.complete(this);
        }
        safely(() -> listener.connected(again));
        // commands that were put in the inbox while the client was connecting
        drainScheduled.set(false);
        if (!inbox.isEmpty()) {
            drain();
        }
    }

    private void logResubscribe(RedisException err) {
        if (err != null) {
            LOG.log(Logger.Level.WARNING, "restoring the subscriptions failed: " + err.getMessage());
        }
    }

    private static Object[] concat(String command, Iterable<String> rest) {
        List<Object> l = new ArrayList<>();
        l.add(command);
        rest.forEach(l::add);
        return l.toArray();
    }

    // ---- replies ----

    private void onData(ByteBuffer data) {
        replyClock = mLatency.now();   // one clock read for all the replies in this read
        ByteBuffer in;
        if (acc.remaining() == 0) {
            in = data;
        } else {
            acc.append(data);
            in = acc.buffer();
        }
        for (;;) {
            Object v = parser.next(in);
            if (v == RespParser.INCOMPLETE) {
                break;
            }
            if (state == State.CLOSED || ctx == null) {
                return;
            }
            dispatch(v);
        }
        reportPending();
        if (in == data && data.hasRemaining()) {
            acc.append(data);
        }
    }

    private void dispatch(Object v) {
        if (v instanceof RespParser.Push p) {
            onPush(p);
            return;
        }
        Cmd c = pending.poll();
        if (c == null) {
            throw new RedisException("protocol error: a reply with no command waiting: " + (v instanceof RedisError e ? e.message() : String.valueOf(v)), false);
        }
        if (v instanceof RedisError e) {
            fail(c, RedisException.of(e));
            if (c.future != null) {
                mError.increment();
                mLatency.endAt(c.start, replyClock);
            }
            return;
        }
        if (c.confirmations > 0) {
            // a SUBSCRIBE answered with an ordinary reply instead of pushes (RESP2 style): count it as all confirmations
            c.confirmations = 0;
        }
        Object value = v;
        if (c.convert != null) {
            try {
                value = c.convert.apply(v);
            } catch (RuntimeException ex) {
                fail(c, new RedisException("cannot read the reply: " + ex, false, ex));
                mError.increment();
                return;
            }
        }
        if (c.future != null) {
            mOk.increment();
            mLatency.endAt(c.start, replyClock);
            c.future.complete(value);
        }
        if (c.internal != null) {
            c.internal.accept(v, null);
        }
    }

    private static String text(Object o) {
        return o instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : o instanceof String s ? s : String.valueOf(o);
    }

    private void onPush(RespParser.Push p) {
        List<Object> items = p.items();
        if (items.isEmpty()) {
            return;
        }
        String kind = text(items.get(0));
        switch (kind) {
            case "message" -> {
                if (items.size() == 3) {
                    deliver(text(items.get(1)), null, items.get(2), channels.get(text(items.get(1))));
                }
            }
            case "pmessage" -> {
                if (items.size() == 4) {
                    deliver(text(items.get(2)), text(items.get(1)), items.get(3), patterns.get(text(items.get(1))));
                }
            }
            case "subscribe", "psubscribe", "unsubscribe", "punsubscribe" -> {
                Cmd head = pending.peek();
                if (head != null && head.confirmations > 0) {
                    if (--head.confirmations == 0) {
                        pending.poll();
                        Object count = items.size() > 2 ? items.get(2) : null;
                        if (head.future != null) {
                            head.future.complete(count);
                        }
                        if (head.internal != null) {
                            head.internal.accept(count, null);
                        }
                    }
                }
            }
            default -> safely(() -> listener.push(items));
        }
    }

    private void deliver(String channel, String pattern, Object payload, List<MessageHandler> handlers) {
        if (handlers == null || !(payload instanceof byte[] bytes)) {
            return;
        }
        for (MessageHandler h : handlers.toArray(new MessageHandler[0])) {
            try {
                h.onMessage(channel, pattern, bytes);
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "Redis message handler failed", t);
            }
        }
    }

    // ---- pub/sub ----

    private CompletableFuture<Void> subscription(String command, Map<String, List<MessageHandler>> map, String name, MessageHandler handler, boolean add) {
        if (protocol != 3 && everOnline) {
            return CompletableFuture.failedFuture(new RedisException("pub/sub on this connection needs RESP3 (Redis 6 or later)", false));
        }
        CompletableFuture<Void> out = new CompletableFuture<>();
        Runnable work = () -> {
            if (state != State.ONLINE) {
                out.completeExceptionally(new RedisException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
                return;
            }
            if (add) {
                List<MessageHandler> l = map.get(name);
                if (l != null) {
                    l.add(handler);   // already subscribed on the server
                    out.complete(null);
                    return;
                }
                map.put(name, new ArrayList<>(List.of(handler)));   // before the answer: messages may come first
            } else if (map.remove(name) == null) {
                out.complete(null);
                return;
            }
            Cmd c = new Cmd(Resp.encode(new Object[] {command, name}), null, null, (v, err) -> {
                if (err != null) {
                    if (add) {
                        map.remove(name);
                    }
                    out.completeExceptionally(err);
                } else {
                    out.complete(null);
                }
            });
            c.confirmations = 1;
            pending.add(c);
            batch.add(c.bytes);
            flush();
        };
        onLoop(work);
        return out;
    }

    /** Subscribes to a channel; completes when the server has confirmed. Subscribing again to the same channel adds a handler without asking the server. Restored after a reconnect. */
    public CompletableFuture<Void> subscribe(String channel, MessageHandler handler) {
        return subscription("SUBSCRIBE", channels, channel, handler, true);
    }

    /** Subscribes to a pattern ({@code news.*}); the handler gets the pattern and the channel. */
    public CompletableFuture<Void> psubscribe(String pattern, MessageHandler handler) {
        return subscription("PSUBSCRIBE", patterns, pattern, handler, true);
    }

    /** Removes every handler of the channel and unsubscribes on the server. */
    public CompletableFuture<Void> unsubscribe(String channel) {
        return subscription("UNSUBSCRIBE", channels, channel, null, false);
    }

    public CompletableFuture<Void> punsubscribe(String pattern) {
        return subscription("PUNSUBSCRIBE", patterns, pattern, null, false);
    }

    // ---- state ----

    public boolean isConnected() {
        return online;
    }

    /** The protocol in use: 3 (RESP3) or 2. Known after the connection is up. */
    public int protocol() {
        return protocol;
    }

    /** What the server told in {@code HELLO} (server, version, proto, mode, role...); empty with RESP2. */
    public Map<Object, Object> serverInfo() {
        return serverInfo;
    }

    /** False when more than the high-water mark of data is queued for the socket: a producer of many commands should pause. See {@link #whenWritable()}. */
    public boolean isWritable() {
        Context c = ctx;
        return c == null || c.connection().isWritable();
    }

    /** Completes at once if the connection can take more, otherwise when its queue has drained; fails if the client is not connected. */
    public CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            Context c = ctx;
            if (state != State.ONLINE || c == null) {
                f.completeExceptionally(new RedisException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            } else {
                c.connection().whenWritable().whenComplete((v, t) -> {
                    if (t == null) {
                        f.complete(null);
                    } else {
                        f.completeExceptionally(new RedisException("the connection was lost", true, t));
                    }
                });
            }
        });
        return f;
    }

    /** Commands sent and waiting for a reply. */
    public CompletableFuture<Integer> pendingCommands() {
        CompletableFuture<Integer> f = new CompletableFuture<>();
        onLoop(() -> f.complete(pending.size()));
        return f;
    }

    public CompletableFuture<Void> closeFuture() {
        return closed;
    }

    /** Closes the connection; commands waiting for a reply fail. The client does not reconnect afterwards. */
    public CompletableFuture<Void> disconnect() {
        onLoop(() -> {
            if (state == State.CLOSED) {
                return;
            }
            State was = state;
            state = State.CLOSED;
            online = false;
            if (reconnectTimer != null) {
                reconnectTimer.cancel();
            }
            if (connectTimer != null) {
                connectTimer.cancel();
            }
            RedisException why = new RedisException("the client was closed", false);
            failPending(why);
            failInbox(why);
            if (!ready.isDone()) {
                ready.completeExceptionally(why);
            }
            Context c = ctx;
            if (c != null && was != State.OFFLINE) {
                try {
                    c.closeAfterFlush();
                } catch (Exception e) {
                    c.connection().close();
                }
            } else {
                closed.complete(null);
            }
        });
        return closed;
    }

    @Override
    public void close() {
        disconnect();
    }

    // ---- transactions and scripts ----

    /**
     * Runs the commands as one transaction ({@code MULTI}, the commands, {@code EXEC}), written together so that no other command of this connection comes between
     * them. Completes with the list of the replies (an error of one command is a {@link RedisError} in the list), or fails if the server refuses the transaction
     * (a command with a syntax error: EXECABORT); completes with null if a {@code WATCH}ed key changed and the server did not run it. To make it conditional on keys not changing, {@code WATCH} them on a {@link #duplicate()} and run the transaction there.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Object>> transaction(Object[]... commands) {
        byte[][] encoded = new byte[commands.length + 2][];
        try {
            encoded[0] = Resp.encode(new Object[] {"MULTI"});
            for (int i = 0; i < commands.length; i++) {
                encoded[i + 1] = Resp.encode(commands[i]);
            }
            encoded[commands.length + 1] = Resp.encode(new Object[] {"EXEC"});
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        int total = 0;
        for (byte[] b : encoded) {
            total += b.length;
        }
        byte[] all = new byte[total];
        int p = 0;
        for (byte[] b : encoded) {
            System.arraycopy(b, 0, all, p, b.length);
            p += b.length;
        }
        CompletableFuture<Object> result = new CompletableFuture<>();
        Cmd[] rest = new Cmd[commands.length + 1];
        for (int i = 0; i < commands.length; i++) {
            rest[i] = new Cmd(new byte[0], null, null, (v, err) -> { });   // each is answered +QUEUED (or an error, which EXEC then reports)
        }
        rest[commands.length] = new Cmd(new byte[0], result, null, null);
        Cmd first = new Cmd(all, null, null, (v, err) -> {
            if (err != null) {
                result.completeExceptionally(err);
            }
        });
        first.chain = rest;
        submit(first);
        return result.thenApply(v -> (List<Object>) v);
    }

    /** Runs a script by its SHA-1 and falls back to sending the source when the server does not have it ({@code NOSCRIPT}); the server keeps it afterwards. */
    public CompletableFuture<Object> eval(Script script, List<String> keys, List<Object> args) {
        List<Object> a = new ArrayList<>();
        a.add("EVALSHA");
        a.add(script.sha1());
        a.add(keys.size());
        a.addAll(keys);
        a.addAll(args);
        CompletableFuture<Object> out = new CompletableFuture<>();
        command(a.toArray()).whenComplete((v, err) -> {
            if (err == null) {
                out.complete(v);
            } else if (err instanceof RedisException e && "NOSCRIPT".equals(e.code())) {
                a.set(0, "EVAL");
                a.set(1, script.source());
                command(a.toArray()).whenComplete((v2, err2) -> {
                    if (err2 == null) {
                        out.complete(v2);
                    } else {
                        out.completeExceptionally(err2);
                    }
                });
            } else {
                out.completeExceptionally(err);
            }
        });
        return out;
    }

    // ---- conversions ----

    static String str(Object v) {
        return v == null ? null : v instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : String.valueOf(v);
    }

    private static long num(Object v) {
        return v instanceof Long l ? l : v instanceof Double d ? d.longValue() : Long.parseLong(str(v));
    }

    private static final Function<Object, Object> STRING = RedisClient::str;
    private static final Function<Object, Object> LONG = v -> v == null ? null : num(v);
    private static final Function<Object, Object> BOOL = v -> v instanceof Boolean b ? b : num(v) != 0;

    @SuppressWarnings("unchecked")
    private static List<String> strings(Object v) {
        if (v == null) {
            return List.of();
        }
        List<Object> l = (List<Object>) v;
        List<String> out = new ArrayList<>(l.size());
        for (Object o : l) {
            out.add(str(o));
        }
        return out;
    }

    /** A map reply (RESP3) or a flat list of pairs (RESP2) as an ordered map of strings. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object v) {
        Map<String, String> out = new LinkedHashMap<>();
        if (v instanceof Map<?, ?> m) {
            m.forEach((k, val) -> out.put(str(k), str(val)));
        } else if (v instanceof List<?> l) {
            for (int i = 0; i + 1 < l.size(); i += 2) {
                out.put(str(l.get(i)), str(l.get(i + 1)));
            }
        }
        return out;
    }

    // ---- commands: connection and keys ----

    public CompletableFuture<String> ping() {
        return send(STRING, "PING");
    }

    public CompletableFuture<String> echo(String message) {
        return send(STRING, "ECHO", message);
    }

    /** The number of keys deleted. */
    public CompletableFuture<Long> del(String... keys) {
        return send(LONG, prepend("DEL", keys));
    }

    public CompletableFuture<Long> unlink(String... keys) {
        return send(LONG, prepend("UNLINK", keys));
    }

    /** How many of the keys exist (a key named twice counts twice). */
    public CompletableFuture<Long> exists(String... keys) {
        return send(LONG, prepend("EXISTS", keys));
    }

    public CompletableFuture<Boolean> expire(String key, long seconds) {
        return send(BOOL, "EXPIRE", key, seconds);
    }

    public CompletableFuture<Boolean> pexpire(String key, long millis) {
        return send(BOOL, "PEXPIRE", key, millis);
    }

    /** Seconds to live: -1 if the key has no expiry, -2 if it does not exist. */
    public CompletableFuture<Long> ttl(String key) {
        return send(LONG, "TTL", key);
    }

    public CompletableFuture<Long> pttl(String key) {
        return send(LONG, "PTTL", key);
    }

    public CompletableFuture<Boolean> persist(String key) {
        return send(BOOL, "PERSIST", key);
    }

    /** none, string, list, set, zset, hash, stream. */
    public CompletableFuture<String> type(String key) {
        return send(STRING, "TYPE", key);
    }

    public CompletableFuture<Void> rename(String key, String newKey) {
        return send(v -> null, "RENAME", key, newKey);
    }

    /** One page of keys; start with cursor {@code "0"}, go on with the cursor of the result until it is {@code "0"} again. {@code match} and {@code count} may be null. */
    public CompletableFuture<ScanResult> scan(String cursor, String match, Long count) {
        List<Object> a = new ArrayList<>(List.of("SCAN", cursor));
        if (match != null) {
            a.addAll(List.of("MATCH", match));
        }
        if (count != null) {
            a.addAll(List.of("COUNT", count));
        }
        return send(v -> {
            List<?> l = (List<?>) v;
            return new ScanResult(str(l.get(0)), strings(l.get(1)));
        }, a.toArray());
    }

    public CompletableFuture<Long> dbSize() {
        return send(LONG, "DBSIZE");
    }

    public CompletableFuture<Void> flushDb() {
        return send(v -> null, "FLUSHDB");
    }

    /** The {@code INFO} text, optionally of one section. */
    public CompletableFuture<String> info(String section) {
        return section == null ? send(STRING, "INFO") : send(STRING, "INFO", section);
    }

    private static Object[] prepend(String command, Object[] rest) {
        Object[] a = new Object[rest.length + 1];
        a[0] = command;
        System.arraycopy(rest, 0, a, 1, rest.length);
        return a;
    }

    // ---- strings ----

    /** The value as UTF-8 text, or null if the key does not exist. */
    public CompletableFuture<String> get(String key) {
        return send(STRING, "GET", key);
    }

    /** The value as it is stored, or null. */
    public CompletableFuture<byte[]> getBytes(String key) {
        return send(null, "GET", key);
    }

    public CompletableFuture<Void> set(String key, String value) {
        return send(v -> null, "SET", key, value);
    }

    public CompletableFuture<Void> setBytes(String key, byte[] value) {
        return send(v -> null, "SET", key, value);
    }

    /** SET with options. Completes with true if the value was set, false if NX or XX said no; with {@link SetArgs#andGet()} use {@link #setGet} for the old value. */
    public CompletableFuture<Boolean> set(String key, String value, SetArgs args) {
        List<Object> a = new ArrayList<>(List.of("SET", key, value));
        a.addAll(args.toArgs());
        return send(v -> v != null, a.toArray());
    }

    /** SET ... GET: completes with the old value (null if there was none). */
    public CompletableFuture<String> setGet(String key, String value, SetArgs args) {
        List<Object> a = new ArrayList<>(List.of("SET", key, value));
        a.addAll(args.andGet().toArgs());
        return send(STRING, a.toArray());
    }

    public CompletableFuture<String> getDel(String key) {
        return send(STRING, "GETDEL", key);
    }

    /** The values, null for a key that does not exist, in the order of the keys. */
    public CompletableFuture<List<String>> mget(String... keys) {
        return send(RedisClient::stringsKeepingNulls, prepend("MGET", keys));
    }

    private static List<String> stringsKeepingNulls(Object v) {
        List<?> l = (List<?>) v;
        List<String> out = new ArrayList<>(l.size());
        for (Object o : l) {
            out.add(str(o));
        }
        return out;
    }

    /** {@code mset("a", "1", "b", "2")}: pairs of key and value. */
    public CompletableFuture<Void> mset(String... keysAndValues) {
        if (keysAndValues.length == 0 || keysAndValues.length % 2 != 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("mset needs pairs of key and value"));
        }
        return send(v -> null, prepend("MSET", keysAndValues));
    }

    public CompletableFuture<Long> incr(String key) {
        return send(LONG, "INCR", key);
    }

    public CompletableFuture<Long> incrBy(String key, long by) {
        return send(LONG, "INCRBY", key, by);
    }

    public CompletableFuture<Double> incrByFloat(String key, double by) {
        return send(v -> Double.parseDouble(str(v)), "INCRBYFLOAT", key, by);
    }

    public CompletableFuture<Long> decr(String key) {
        return send(LONG, "DECR", key);
    }

    public CompletableFuture<Long> decrBy(String key, long by) {
        return send(LONG, "DECRBY", key, by);
    }

    /** The length of the string after the append. */
    public CompletableFuture<Long> append(String key, String value) {
        return send(LONG, "APPEND", key, value);
    }

    public CompletableFuture<Long> strlen(String key) {
        return send(LONG, "STRLEN", key);
    }

    // ---- hashes ----

    public CompletableFuture<String> hget(String key, String field) {
        return send(STRING, "HGET", key, field);
    }

    /** Sets one field; completes with the number of fields that are new (1 or 0). */
    public CompletableFuture<Long> hset(String key, String field, String value) {
        return send(LONG, "HSET", key, field, value);
    }

    public CompletableFuture<Long> hset(String key, Map<String, String> fields) {
        if (fields.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("no fields"));
        }
        List<Object> a = new ArrayList<>(List.of("HSET", key));
        fields.forEach((f, v) -> {
            a.add(f);
            a.add(v);
        });
        return send(LONG, a.toArray());
    }

    public CompletableFuture<List<String>> hmget(String key, String... fields) {
        Object[] a = new Object[fields.length + 2];
        a[0] = "HMGET";
        a[1] = key;
        System.arraycopy(fields, 0, a, 2, fields.length);
        return send(RedisClient::stringsKeepingNulls, a);
    }

    public CompletableFuture<Map<String, String>> hgetAll(String key) {
        return send(RedisClient::stringMap, "HGETALL", key);
    }

    public CompletableFuture<Long> hdel(String key, String... fields) {
        Object[] a = new Object[fields.length + 2];
        a[0] = "HDEL";
        a[1] = key;
        System.arraycopy(fields, 0, a, 2, fields.length);
        return send(LONG, a);
    }

    public CompletableFuture<Boolean> hexists(String key, String field) {
        return send(BOOL, "HEXISTS", key, field);
    }

    public CompletableFuture<Long> hincrBy(String key, String field, long by) {
        return send(LONG, "HINCRBY", key, field, by);
    }

    public CompletableFuture<Long> hlen(String key) {
        return send(LONG, "HLEN", key);
    }

    public CompletableFuture<List<String>> hkeys(String key) {
        return send(RedisClient::strings, "HKEYS", key);
    }

    public CompletableFuture<List<String>> hvals(String key) {
        return send(RedisClient::strings, "HVALS", key);
    }

    // ---- lists ----

    /** Pushes to the head; completes with the length of the list. */
    public CompletableFuture<Long> lpush(String key, String... values) {
        return send(LONG, prepend2("LPUSH", key, values));
    }

    public CompletableFuture<Long> rpush(String key, String... values) {
        return send(LONG, prepend2("RPUSH", key, values));
    }

    public CompletableFuture<String> lpop(String key) {
        return send(STRING, "LPOP", key);
    }

    public CompletableFuture<String> rpop(String key) {
        return send(STRING, "RPOP", key);
    }

    public CompletableFuture<Long> llen(String key) {
        return send(LONG, "LLEN", key);
    }

    /** The elements from {@code start} to {@code stop}, both included; negative counts from the end ({@code lrange(k, 0, -1)} is all of it). */
    public CompletableFuture<List<String>> lrange(String key, long start, long stop) {
        return send(RedisClient::strings, "LRANGE", key, start, stop);
    }

    public CompletableFuture<String> lindex(String key, long index) {
        return send(STRING, "LINDEX", key, index);
    }

    public CompletableFuture<Void> ltrim(String key, long start, long stop) {
        return send(v -> null, "LTRIM", key, start, stop);
    }

    private static Object[] prepend2(String command, String key, Object[] rest) {
        Object[] a = new Object[rest.length + 2];
        a[0] = command;
        a[1] = key;
        System.arraycopy(rest, 0, a, 2, rest.length);
        return a;
    }

    // ---- sets ----

    /** The number of members that were new. */
    public CompletableFuture<Long> sadd(String key, String... members) {
        return send(LONG, prepend2("SADD", key, members));
    }

    public CompletableFuture<Long> srem(String key, String... members) {
        return send(LONG, prepend2("SREM", key, members));
    }

    public CompletableFuture<List<String>> smembers(String key) {
        return send(RedisClient::strings, "SMEMBERS", key);
    }

    public CompletableFuture<Boolean> sismember(String key, String member) {
        return send(BOOL, "SISMEMBER", key, member);
    }

    public CompletableFuture<Long> scard(String key) {
        return send(LONG, "SCARD", key);
    }

    public CompletableFuture<String> spop(String key) {
        return send(STRING, "SPOP", key);
    }

    // ---- sorted sets ----

    /** Adds or updates one member; completes with the number of members that are new (1 or 0). */
    public CompletableFuture<Long> zadd(String key, double score, String member) {
        return send(LONG, "ZADD", key, score, member);
    }

    public CompletableFuture<Long> zrem(String key, String... members) {
        return send(LONG, prepend2("ZREM", key, members));
    }

    /** The score, or null if the member is not there. */
    public CompletableFuture<Double> zscore(String key, String member) {
        return send(v -> v == null ? null : v instanceof Double d ? d : Double.parseDouble(str(v)), "ZSCORE", key, member);
    }

    public CompletableFuture<Long> zcard(String key) {
        return send(LONG, "ZCARD", key);
    }

    public CompletableFuture<Long> zrank(String key, String member) {
        return send(LONG, "ZRANK", key, member);
    }

    public CompletableFuture<Double> zincrBy(String key, double by, String member) {
        return send(v -> v instanceof Double d ? d : Double.parseDouble(str(v)), "ZINCRBY", key, by, member);
    }

    /** The members by rank (lowest score first), {@code start} and {@code stop} included, negative from the end. */
    public CompletableFuture<List<String>> zrange(String key, long start, long stop) {
        return send(RedisClient::strings, "ZRANGE", key, start, stop);
    }

    /** As {@link #zrange} with the scores, in order. */
    public CompletableFuture<Map<String, Double>> zrangeWithScores(String key, long start, long stop) {
        return send(v -> {
            Map<String, Double> out = new LinkedHashMap<>();
            List<?> l = (List<?>) v;
            for (int i = 0; i < l.size(); i++) {
                Object e = l.get(i);
                if (e instanceof List<?> pair) {   // RESP3: a list of [member, score]
                    out.put(str(pair.get(0)), pair.get(1) instanceof Double d ? d : Double.parseDouble(str(pair.get(1))));
                } else {                           // RESP2: member, score, member, score...
                    Object s = l.get(++i);
                    out.put(str(e), s instanceof Double d ? d : Double.parseDouble(str(s)));
                }
            }
            return out;
        }, "ZRANGE", key, start, stop, "WITHSCORES");
    }

    // ---- publishing ----

    /** Publishes a message; completes with the number of subscribers that got it. */
    public CompletableFuture<Long> publish(String channel, String message) {
        return send(LONG, "PUBLISH", channel, message);
    }

    public CompletableFuture<Long> publish(String channel, byte[] message) {
        return send(LONG, "PUBLISH", channel, message);
    }

    @Override
    public String toString() {
        return "RedisClient[" + config.host() + ":" + config.port() + ", " + state + "]";
    }
}
