package io.hydrangea.mongodb;

import io.hydrangea.Client;
import io.hydrangea.EventLoop;
import io.hydrangea.Latency;
import io.hydrangea.Metrics;
import io.hydrangea.Options;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;

import io.hydrangea.log.Logger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * One connection to a MongoDB server: OP_MSG in both directions, the handshake ({@code hello}) and the SCRAM login, commands from any thread written together and
 * matched to their replies by request id. The server runs a connection's commands one after the other, so this is one lane; {@link MongoClient} drives several.
 * A connection that is lost stays closed: its commands fail with a retriable {@link MongoException} and the client opens another.
 */
final class MongoConnection {

    private static final Logger LOG = Logger.get(MongoConnection.class);
    private static final int OP_MSG = 2013;
    private static final AtomicInteger REQUEST_IDS = new AtomicInteger(1);
    static final String DRIVER_VERSION = "1.0";

    private enum State { CONNECTING, ONLINE, CLOSED }

    /** A command written or about to be, waiting for its reply. */
    static final class Cmd {
        final int requestId;
        final byte[] bytes;
        final CompletableFuture<Map<String, Object>> future;        // null for the connection's own commands
        final BiConsumer<Map<String, Object>, MongoException> internal;
        long start;
        Scheduled timer;

        Cmd(int requestId, byte[] bytes, CompletableFuture<Map<String, Object>> future, BiConsumer<Map<String, Object>, MongoException> internal) {
            this.requestId = requestId;
            this.bytes = bytes;
            this.future = future;
            this.internal = internal;
        }
    }

    private final MongoConfig config;
    private final String address;                                   // host:port of this connection
    private final boolean probe;                                    // hello only, then close: for discovery
    private final EventLoop loop;
    private final CompletableFuture<MongoConnection> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final ConcurrentLinkedQueue<Cmd> inbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private volatile Context ctx;
    private volatile State state = State.CONNECTING;
    private volatile Map<String, Object> hello = Map.of();

    // loop thread only
    private final Map<Integer, Cmd> pending = new LinkedHashMap<>();
    private final List<byte[]> batch = new ArrayList<>();
    private Accumulator acc = new Accumulator();
    private Scheduled connectTimer;
    private Throwable lossCause;
    private int pendingReported;
    private final Metrics.Count mOk, mError, mFailed, mPending, mLost;
    private final Latency mLatency;

    MongoConnection(MongoConfig config, String address, boolean probe, EventLoop loop, Metrics m) {
        this.config = config;
        this.address = address;
        this.probe = probe;
        this.loop = loop;
        mOk = m.counter("mongo.commands", null, "outcome", "ok");
        mError = m.counter("mongo.commands", null, "outcome", "error");
        mFailed = m.counter("mongo.commands", null, "outcome", "failed");
        mPending = m.gauge("mongo.commands.pending", null);
        mLost = m.counter("mongo.connections.lost", null);
        mLatency = m.latency("mongo.command.duration");
    }

    /** Connects to {@code address} ({@code host:port}), shakes hands, logs in. The first failure is final: there is no retry inside a connection. */
    static CompletableFuture<MongoConnection> open(MongoConfig config, String address, EventLoop loop, Metrics metrics) {
        MongoConnection c = new MongoConnection(config, address, false, loop, metrics);
        c.onLoop(c::startConnecting);
        return c.ready;
    }

    /** Connects, asks {@code hello} and closes: the answer says what the server is (standalone, mongos, which replica set, who is its primary). */
    static CompletableFuture<Map<String, Object>> probe(MongoConfig config, String address, EventLoop loop, Metrics metrics) {
        MongoConnection c = new MongoConnection(config, address, true, loop, metrics);
        c.onLoop(c::startConnecting);
        return c.ready.thenApply(MongoConnection::hello);
    }

    String address() {
        return address;
    }

    EventLoop loop() {
        return loop;
    }

    boolean isOnline() {
        return state == State.ONLINE;
    }

    CompletableFuture<Void> closeFuture() {
        return closed;
    }

    /** The server's {@code hello} answer: {@code maxBsonObjectSize}, {@code maxWriteBatchSize}, {@code maxWireVersion}, ... */
    Map<String, Object> hello() {
        return hello;
    }

    // ---- commands ----

    /** Sends the command to {@code db}; the reply when {@code ok} is 1, else a {@link MongoException}. */
    CompletableFuture<Map<String, Object>> command(String db, Map<String, Object> command) {
        int id = REQUEST_IDS.getAndIncrement();
        byte[] bytes;
        try {
            bytes = encode(id, db, command);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(MongoException.local("cannot encode the command: " + e.getMessage(), false, e));
        }
        if (bytes.length > config.maxMessageBytes()) {
            return CompletableFuture.failedFuture(MongoException.local("the command is " + bytes.length + " bytes, more than maxMessageBytes " + config.maxMessageBytes(), false, null));
        }
        Cmd c = new Cmd(id, bytes, new CompletableFuture<>(), null);
        submit(c);
        return c.future;
    }

    /** The OP_MSG for a command: header, flags 0, one body section with {@code $db} added. */
    static byte[] encode(int requestId, String db, Map<String, Object> command) {
        Map<String, Object> body = new LinkedHashMap<>(command);
        body.put("$db", db);
        Bson.Writer w = new Bson.Writer(256);
        w.int32(0);                                                // length, below
        w.int32(requestId);
        w.int32(0);
        w.int32(OP_MSG);
        w.int32(0);                                                // flags
        w.byte1(0);                                                // section kind 0: a body
        w.document(body, 0);
        byte[] out = w.toBytes();
        out[0] = (byte) out.length;
        out[1] = (byte) (out.length >> 8);
        out[2] = (byte) (out.length >> 16);
        out[3] = (byte) (out.length >> 24);
        return out;
    }

    private void submit(Cmd c) {
        if (loop.inEventLoop()) {
            accept(c, mLatency.now());
            flush();
            reportPending();
            return;
        }
        inbox.add(c);
        if (drainScheduled.compareAndSet(false, true)) {
            try {
                loop.execute(this::drain);
            } catch (RuntimeException shutDown) {
                drainScheduled.set(false);
                failInbox(MongoException.local("the event loop is shut down", false, null));
            }
        }
    }

    private void drain() {
        for (;;) {
            long now = mLatency.now();
            Cmd c;
            while ((c = inbox.poll()) != null) {
                accept(c, now);
            }
            flush();
            reportPending();
            drainScheduled.set(false);
            if (inbox.isEmpty() || !drainScheduled.compareAndSet(false, true)) {
                return;
            }
        }
    }

    private void accept(Cmd c, long now) {
        if (state != State.ONLINE) {
            reject(c, state == State.CLOSED ? MongoException.lost("the connection to " + address + " is closed", lossCause) : MongoException.local("not connected yet", true, null));
            return;
        }
        if (pending.size() >= config.maxPending()) {
            reject(c, MongoException.local("too many commands waiting for a reply (maxPending " + config.maxPending() + ")", true, null));
            return;
        }
        c.start = now;
        pending.put(c.requestId, c);
        batch.add(c.bytes);
        if (config.commandTimeout() != null && c.future != null) {
            c.timer = loop.schedule(() -> timedOut(c), config.commandTimeout().toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private void timedOut(Cmd c) {
        if (pending.remove(c.requestId) == c) {
            mFailed.increment();
            c.future.completeExceptionally(MongoException.timeout("no reply within " + config.commandTimeout().toMillis() + " ms (the server may still be running the command)"));
            reportPending();
        }
    }

    private void reject(Cmd c, MongoException e) {
        mFailed.increment();
        fail(c, e);
    }

    private static void fail(Cmd c, MongoException e) {
        if (c.timer != null) {
            c.timer.cancel();
        }
        if (c.future != null) {
            c.future.completeExceptionally(e);
        }
        if (c.internal != null) {
            c.internal.accept(null, e);
        }
    }

    private void failInbox(MongoException e) {
        for (Cmd c; (c = inbox.poll()) != null; ) {
            reject(c, e);
        }
    }

    private void reportPending() {
        int n = pending.size();
        if (n != pendingReported) {
            mPending.add(n - pendingReported);
            pendingReported = n;
        }
    }

    private static final int GATHER_LIMIT = 1 << 20;

    /** Writes the commands accepted since the last flush, small ones gathered into one buffer. */
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
                    int j = i, sum = 0;
                    while (j < n && sum + batch.get(j).length <= GATHER_LIMIT) {
                        sum += batch.get(j).length;
                        j++;
                    }
                    if (j == i) {
                        c.write(ByteBuffer.wrap(batch.get(i++)));
                        continue;
                    }
                    if (j == i + 1) {
                        c.write(ByteBuffer.wrap(batch.get(i)));
                    } else {
                        byte[] one = new byte[sum];
                        int p = 0;
                        for (int k = i; k < j; k++) {
                            System.arraycopy(batch.get(k), 0, one, p, batch.get(k).length);
                            p += batch.get(k).length;
                        }
                        c.write(ByteBuffer.wrap(one));
                    }
                    i = j;
                }
            }
        } catch (Exception e) {
            failConnection(MongoException.local("write failed: " + e, true, e));
        } finally {
            batch.clear();
        }
    }

    // ---- connecting ----

    private void onLoop(Runnable r) {
        if (loop.inEventLoop()) {
            r.run();
        } else {
            try {
                loop.execute(r);
            } catch (RuntimeException shutDown) {
                ready.completeExceptionally(MongoException.local("the event loop is shut down", false, null));
            }
        }
    }

    private void startConnecting() {
        Thread.startVirtualThread(() -> {                          // resolving the name can block: not on the loop
            InetSocketAddress addr;
            String host = MongoConfig.hostOf(address);
            int port = MongoConfig.portOf(address);
            try {
                addr = new InetSocketAddress(host, port);
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(host);
                }
            } catch (Exception e) {
                onLoop(() -> connectionEnded(MongoException.lost("cannot resolve " + host + ": " + e, e)));
                return;
            }
            Stage stage = new MongoStage();
            Pipeline pipeline = config.sslContext() == null ? Pipeline.of(stage) : Pipeline.of(TlsStage.client(config.sslContext(), host, port), stage);
            Options options = Options.defaults().withKeepAlive(true).withConnectTimeout(config.connectTimeout());
            Client.connect(loop, addr, options, pipeline).whenComplete((conn, err) -> {
                if (err != null) {
                    onLoop(() -> {
                        if (state == State.CONNECTING) {
                            connectionEnded(MongoException.lost("cannot connect to " + address + ": " + err, err));
                        }
                    });
                }
            });
        });
    }

    private final class MongoStage implements Stage {
        @Override
        public void onActive(Context c) {
            if (state != State.CONNECTING) {
                c.connection().close();
                return;
            }
            ctx = c;
            connectTimer = loop.schedule(() -> {
                if (state == State.CONNECTING) {
                    failConnection(MongoException.lost("no login within " + config.connectTimeout().toMillis() + " ms", null));
                }
            }, config.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
            handshake();
        }

        @Override
        public void onRead(Context c, Object msg) {
            try {
                onData((ByteBuffer) msg);
            } catch (MongoException e) {
                failConnection(e);
            } catch (RuntimeException e) {
                failConnection(MongoException.local("bad reply: " + e.getMessage(), true, e));
            }
        }

        @Override
        public void onClose(Context c, Throwable cause) {
            connectionEnded(cause != null ? cause : new java.io.IOException("the connection was closed"));
            c.fireClose(cause);
        }
    }

    /** Closes the connection because of {@code cause}; {@link #connectionEnded} does the rest. */
    private void failConnection(Throwable cause) {
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
        if (state == State.CLOSED) {
            return;
        }
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        Throwable why = lossCause != null ? lossCause : cause;
        boolean wasOnline = state == State.ONLINE;
        state = State.CLOSED;
        ctx = null;
        acc.release();
        MongoException e = why instanceof MongoException m ? m : MongoException.lost("the connection to " + address + " was lost: " + why, why);
        for (Cmd c : pending.values()) {
            fail(c, e);
            if (c.future != null) {
                mFailed.increment();
            }
        }
        pending.clear();
        batch.clear();
        reportPending();
        failInbox(MongoException.lost("the connection to " + address + " is closed", why));
        if (wasOnline) {
            if (!probe) {
                mLost.increment();
            }
        } else {
            ready.completeExceptionally(e);
        }
        closed.complete(null);
    }

    /** Closes; commands waiting for a reply fail. */
    void close() {
        onLoop(() -> {
            if (state != State.CLOSED) {
                lossCause = MongoException.lost("the connection to " + address + " was closed by the client", null);   // closed because the primary moved: the commands on it may go again
                Context c = ctx;
                if (c != null) {
                    c.connection().close();
                } else {
                    connectionEnded(lossCause);
                }
            }
        });
    }

    // ---- reading ----

    private void onData(ByteBuffer data) {
        acc.append(data);
        ByteBuffer in = acc.buffer();
        for (;;) {
            if (in.remaining() < 4) {
                return;
            }
            int length = in.order(ByteOrder.LITTLE_ENDIAN).getInt(in.position());
            if (length < 16 || length > config.maxMessageBytes()) {
                throw MongoException.local("the server sent a message of " + length + " bytes (allowed: 16.." + config.maxMessageBytes() + ")", true, null);
            }
            if (in.remaining() < length) {
                return;
            }
            int end = in.position() + length;
            ByteBuffer msg = in.slice(in.position(), length).order(ByteOrder.LITTLE_ENDIAN);
            in.position(end);
            message(msg);
        }
    }

    private void message(ByteBuffer msg) {
        msg.getInt();                                              // length
        msg.getInt();                                              // requestID
        int responseTo = msg.getInt();
        int opCode = msg.getInt();
        if (opCode != OP_MSG) {
            throw MongoException.local("unexpected opcode " + opCode + " from the server", true, null);
        }
        int flags = msg.getInt();
        int bodyEnd = (flags & 1) != 0 ? msg.limit() - 4 : msg.limit();        // checksumPresent: 4 bytes of CRC-32C at the end, not checked
        Map<String, Object> body = null;
        while (msg.position() < bodyEnd) {
            int kind = msg.get() & 0xFF;
            if (kind == 0) {
                if (body != null) {
                    throw MongoException.local("two body sections in one message", true, null);
                }
                body = Bson.decode(msg);
            } else if (kind == 1) {                                // a document sequence: not used in replies, skipped if present
                int size = msg.getInt(msg.position());
                if (size < 5 || msg.position() + size > bodyEnd) {
                    throw MongoException.local("bad section length", true, null);
                }
                msg.position(msg.position() + size);
            } else {
                throw MongoException.local("unknown section kind " + kind, true, null);
            }
        }
        if (body == null) {
            throw MongoException.local("a reply without a body", true, null);
        }
        Cmd c = pending.remove(responseTo);
        if (c == null) {
            return;                                                // timed out, or a reply to nothing: dropped
        }
        if (c.timer != null) {
            c.timer.cancel();
        }
        reportPending();
        Object ok = body.get("ok");
        boolean fine = ok instanceof Number n ? n.doubleValue() == 1.0 : Boolean.TRUE.equals(ok);
        if (c.future != null) {
            mLatency.end(c.start);
            if (fine) {
                mOk.increment();
                c.future.complete(body);
            } else {
                mError.increment();
                c.future.completeExceptionally(MongoException.of(body));
            }
        } else if (c.internal != null) {
            c.internal.accept(fine ? body : null, fine ? null : MongoException.of(body));
        }
    }

    // ---- handshake and login ----

    private void internal(String db, Map<String, Object> command, BiConsumer<Map<String, Object>, MongoException> done) {
        int id = REQUEST_IDS.getAndIncrement();
        Cmd c = new Cmd(id, encode(id, db, command), null, done);
        pending.put(id, c);
        batch.add(c.bytes);
        flush();
    }

    private void handshake() {
        Map<String, Object> client = Bson.doc(
                "driver", Bson.doc("name", "hydrangea-mongodb", "version", DRIVER_VERSION),
                "os", Bson.doc("type", System.getProperty("os.name", "unknown").toLowerCase(java.util.Locale.ROOT).contains("win") ? "Windows" : System.getProperty("os.name", "unknown")),
                "platform", "Java " + Runtime.version().feature());
        if (config.appName() != null) {
            client = Bson.doc("application", Bson.doc("name", config.appName()), "driver", client.get("driver"), "os", client.get("os"), "platform", client.get("platform"));
        }
        Map<String, Object> hello = Bson.doc("hello", 1, "helloOk", true, "client", client);
        if (config.username() != null) {
            hello.put("saslSupportedMechs", config.authSource() + "." + config.username());
        }
        internal("admin", hello, (reply, err) -> {
            if (err != null && err.fromServer() && (err.code() == 59 || err.getMessage().contains("no such command"))) {
                Map<String, Object> legacy = new LinkedHashMap<>(hello);     // before 4.4 (wire version 9): isMaster
                legacy.remove("hello");
                legacy.remove("helloOk");
                Map<String, Object> isMaster = Bson.doc("isMaster", 1);
                isMaster.putAll(legacy);
                internal("admin", isMaster, this::helloDone);
            } else {
                helloDone(reply, err);
            }
        });
    }

    private void helloDone(Map<String, Object> reply, MongoException err) {
        if (err != null) {
            failConnection(MongoException.local("handshake failed: " + err.getMessage(), true, err));
            return;
        }
        hello = reply;
        if (probe) {
            if (connectTimer != null) {
                connectTimer.cancel();
            }
            state = State.ONLINE;
            ready.complete(this);
            close();
            return;
        }
        if (config.username() == null) {
            wentOnline();
            return;
        }
        String mechanism = "SCRAM-SHA-256";
        if (reply.get("saslSupportedMechs") instanceof List<?> mechs) {
            if (!mechs.contains("SCRAM-SHA-256") && mechs.contains("SCRAM-SHA-1")) {
                mechanism = "SCRAM-SHA-1";
            } else if (!mechs.contains("SCRAM-SHA-256")) {
                failConnection(MongoException.local("the user " + config.username() + " on " + config.authSource() + " offers none of SCRAM-SHA-256 and SCRAM-SHA-1 (" + mechs + ")", false, null));
                return;
            }
        }
        Scram scram = Scram.of(mechanism, config.username(), config.password());
        Map<String, Object> start = Bson.doc("saslStart", 1, "mechanism", mechanism, "payload", scram.clientFirst().getBytes(StandardCharsets.UTF_8),
                "autoAuthorize", 1, "options", Bson.doc("skipEmptyExchange", true));
        internal(config.authSource(), start, (r1, e1) -> {
            if (e1 != null) {
                loginFailed(e1);
                return;
            }
            try {
                String clientFinal = scram.clientFinal(payload(r1));
                Map<String, Object> cont = Bson.doc("saslContinue", 1, "conversationId", r1.get("conversationId"), "payload", clientFinal.getBytes(StandardCharsets.UTF_8));
                internal(config.authSource(), cont, (r2, e2) -> {
                    if (e2 != null) {
                        loginFailed(e2);
                        return;
                    }
                    try {
                        scram.verifyServerFinal(payload(r2));
                    } catch (RuntimeException bad) {
                        failConnection(MongoException.local(bad.getMessage(), false, bad));
                        return;
                    }
                    if (Boolean.TRUE.equals(r2.get("done"))) {
                        wentOnline();
                        return;
                    }
                    Map<String, Object> last = Bson.doc("saslContinue", 1, "conversationId", r1.get("conversationId"), "payload", new byte[0]);
                    internal(config.authSource(), last, (r3, e3) -> {
                        if (e3 != null) {
                            loginFailed(e3);
                        } else {
                            wentOnline();
                        }
                    });
                });
            } catch (RuntimeException bad) {
                failConnection(MongoException.local(bad.getMessage(), false, bad));
            }
        });
    }

    private static String payload(Map<String, Object> reply) {
        Object p = reply.get("payload");
        if (p instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        if (p instanceof String s) {
            return s;
        }
        throw new IllegalStateException("a SASL reply without a payload");
    }

    private void loginFailed(MongoException err) {
        failConnection(new MongoException(err.code(), err.codeName(), "login failed: " + err.getMessage(), err.fromServer(), false, false, null, err.reply(), err));
    }

    private void wentOnline() {
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        state = State.ONLINE;
        ready.complete(this);
        drainScheduled.set(false);
        if (!inbox.isEmpty()) {
            drain();
        }
    }
}
