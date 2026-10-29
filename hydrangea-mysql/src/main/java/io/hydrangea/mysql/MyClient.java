package io.hydrangea.mysql;

import io.hydrangea.Backoff;
import io.hydrangea.Client;
import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;

import java.lang.System.Logger;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A MySQL connection that many threads can use at once. Calls from any thread are queued, written together and answered in order, so a burst costs few system calls
 * and no call waits for the one before it to come back (pipelining). The connection belongs to one event loop and nothing here blocks.
 * <pre>{@code
 * MyClient db = MyClient.connect(group, MyConfig.of("localhost", 3306, "app", "appdb").withPassword("secret")).join();
 * MyResult r = db.query("SELECT id, name FROM users WHERE id = ?", 42).join();
 * String name = r.first().getString("name");
 * }</pre>
 * <ul>
 *   <li><b>Without parameters</b> a call goes as a text query ({@code COM_QUERY}): one round trip, rows in text. <b>With parameters</b> ({@code ?}) it goes through a server-side
 *       prepared statement and the binary protocol ({@code COM_STMT_EXECUTE}); a statement is prepared on first use and kept per connection
 *       ({@link MyConfig#statementCacheSize()}). {@link #queryPrepared} forces the prepared path for a call without parameters, {@link #queryText} the text path.</li>
 *   <li><b>Pipelining</b>: the protocol has no equivalent of PostgreSQL's per-query {@code Sync}, but the server does read commands one after the other, and the answers come in order,
 *       so commands are written back to back. The exception is the first use of a statement: its {@code COM_STMT_PREPARE} must be answered (it gives the statement id) before the
 *       {@code EXECUTE} can be written, and calls made after it wait for that answer so that order is kept.</li>
 *   <li><b>Errors are per call</b>: a failing statement answers with an error packet and the ones queued behind it run normally.</li>
 *   <li>Transactions are {@code START TRANSACTION} and {@code COMMIT}/{@code ROLLBACK}; use {@link MyPool#transaction} so that the calls of one transaction share a connection.
 *       <b>DDL</b> commits implicitly in MySQL.</li>
 *   <li><b>Big results</b>: a result is held whole, so it has a limit ({@link MyConfig#withResultLimits}); {@link #stream} reads through a server-side cursor with memory for one
 *       piece. <b>Batches</b>: {@link #executeBatch} runs a statement for many parameter sets with their EXECUTEs written back to back (not atomic: see there).</li>
 *   <li><b>Time limits</b> ({@link MyConfig#withQueryTimeout}, {@link #query(java.time.Duration, String, Object...)}): a call that runs out fails with a
 *       {@link MyException#isTimeout() timeout} and the server is asked to {@code KILL QUERY} it on a connection of its own.</li>
 *   <li><b>Reconnecting</b> ({@link MyConfig#withReconnect}, off by default): when the connection is lost the client waits as the {@link io.hydrangea.Backoff} says and connects
 *       again, telling the {@link MyListener}. Calls that were on the lost connection fail (they may have run); calls made while it is down fail at once and are retriable. The new
 *       session starts with an empty statement cache and none of the old session's state ({@code SET}, temporary tables, an open transaction: the first call after a loss inside a
 *       transaction fails once, saying so). A {@link MyPool} replaces lost connections itself and turns this off for them.</li>
 *   <li><b>Against a hostile server or a man in the middle</b> (without TLS nothing on the wire is authenticated): the client never sends a file the server asks for
 *       ({@code LOCAL INFILE}), never sends the password to a {@code mysql_clear_password} switch, refuses to send it encrypted with a public key the server hands over unless
 *       {@link MyConfig#withAllowPublicKeyRetrieval} says the network is trusted, sends nothing in the clear when TLS was asked for and the server does not offer it, and refuses
 *       bytes smuggled before the TLS handshake. With TLS the server's certificate is verified against the host name. See {@code MyAttackTest}.</li>
 *   <li><b>Proxies</b>: ProxySQL reads one command at a time and drops a client that pipelines; {@link MyConfig#withMaxInFlight} {@code (1)} makes the client wait for each answer.
 *       MariaDB speaks the same protocol and works as MySQL does.</li>
 * </ul>
 */
public final class MyClient implements AutoCloseable {

    private static final Logger LOG = System.getLogger(MyClient.class.getName());

    // capability flags
    private static final int CLIENT_LONG_PASSWORD = 0x1, CLIENT_LONG_FLAG = 0x4, CLIENT_CONNECT_WITH_DB = 0x8, CLIENT_PROTOCOL_41 = 0x200, CLIENT_SSL = 0x800,
            CLIENT_TRANSACTIONS = 0x2000, CLIENT_SECURE_CONNECTION = 0x8000, CLIENT_MULTI_RESULTS = 0x20000, CLIENT_PS_MULTI_RESULTS = 0x40000,
            CLIENT_PLUGIN_AUTH = 0x80000, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x200000, CLIENT_DEPRECATE_EOF = 0x1000000;
    private static final int SERVER_STATUS_IN_TRANS = 1, SERVER_MORE_RESULTS_EXISTS = 8, SERVER_STATUS_CURSOR_EXISTS = 0x40, SERVER_STATUS_LAST_ROW_SENT = 0x80;
    private static final int COM_QUIT = 0x01, COM_QUERY = 0x03, COM_PING = 0x0E, COM_STMT_PREPARE = 0x16, COM_STMT_EXECUTE = 0x17, COM_STMT_CLOSE = 0x19, COM_STMT_FETCH = 0x1C;
    private static final int CURSOR_TYPE_READ_ONLY = 1;

    private enum State { CONNECTING, AUTH, ONLINE, OFFLINE, CLOSED }      // OFFLINE: lost, waiting for the next reconnect attempt

    private enum Kind { TEXT, PREPARED, MARK, END, STREAM, FETCH, BATCH }

    /** A prepared statement of this connection. */
    static final class Stmt {
        final int id;
        final int paramCount;

        Stmt(int id, int paramCount) {
            this.id = id;
            this.paramCount = paramCount;
        }
    }

    /** A call. */
    private static final class Op {
        final Kind kind;
        final String sql;
        final Object[] params;
        final CompletableFuture<MyResult> future;
        List<String> statements;    // a MARK: what starts the transaction
        long timeoutNanos;
        Scheduled timer;
        boolean abandoned;       // it timed out: the caller has been told; its answer, when it comes, is read and dropped
        boolean killSent;
        boolean internal;        // the START TRANSACTION a transaction sends ahead of its first call: its answer is not for anyone
        long start;
        // set once written
        Stmt stmt;
        boolean preparing;       // COM_STMT_PREPARE is out, the EXECUTE is not
        boolean oneShot;         // no cache: close the statement after the execute
        // the answer being read
        Phase phase = Phase.HEAD;
        int expected;            // packets still to come in the phase
        int prepParams;
        int prepCols;
        List<MyColumn> cols;
        MyRow.Meta meta;
        List<MyRow> rows;
        MyResult first, last;
        long rowCount, byteCount;   // of the result being read, against the limits
        boolean overflow;           // over the limits: the rows are read and dropped, the call fails at the end
        // streams
        int fetchSize;
        MyRowStream stream;         // a STREAM once its cursor is open, a FETCH: whose
        CompletableFuture<MyRowStream> streamReady;
        CompletableFuture<List<MyRow>> fetchDone;
        // batches
        List<Object[]> sets;
        long[] batchResults;
        int batchIdx;
        int batchWritten;
        MyException batchError;
        CompletableFuture<long[]> batchDone;

        Op(Kind kind, String sql, Object[] params, CompletableFuture<MyResult> future) {
            this.kind = kind;
            this.sql = sql;
            this.params = params;
            this.future = future;
        }

        boolean binaryRows() {
            return kind == Kind.PREPARED || kind == Kind.STREAM || kind == Kind.FETCH || kind == Kind.BATCH;
        }

        /** The future the caller holds, whatever the kind. */
        void fail(MyException e) {
            switch (kind) {
                case STREAM -> streamReady.completeExceptionally(e);
                case FETCH -> fetchDone.completeExceptionally(e);
                case BATCH -> batchDone.completeExceptionally(e);
                default -> future.completeExceptionally(e);
            }
        }
    }

    private enum Phase { HEAD, PREP_PARAMS, PREP_COLS, COLS, ROWS }

    private final MyConfig config;
    private final MyListener listener;
    private final CompletableFuture<MyClient> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private EventLoopGroup group;
    private EventLoop home;

    // any thread
    private final ConcurrentLinkedQueue<Op> inbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private final AtomicInteger inflight = new AtomicInteger();
    private volatile Context ctx;
    private volatile boolean online;
    private volatile boolean inTransaction;
    private volatile long threadId;
    private volatile String serverVersion = "";

    // loop thread only
    private State state = State.CONNECTING;
    private Scheduled connectTimer;
    private boolean closing;
    private boolean loggedIn;
    private int gen;                          // the number of the connection attempt in progress: what an attempt that is over reports is ignored
    private int reconnectAttempt;             // consecutive failures since the connection was lost
    private Scheduled reconnectTimer;
    private boolean lostInTx;                 // the connection was lost inside a transaction: the first call of the new session must not run as if the transaction were still there
    private Throwable lossCause;
    private io.hydrangea.Connection rawConnection;
    private final ArrayDeque<Op> pending = new ArrayDeque<>();       // written, answer not complete
    private final ArrayDeque<Op> held = new ArrayDeque<>();          // waiting for a COM_STMT_PREPARE answer: not written
    private boolean prepareOut;                                      // a COM_STMT_PREPARE is out
    private MyRowStream gate;                                        // a stream owns the connection: other calls wait in `deferred`
    private final ArrayDeque<Op> deferred = new ArrayDeque<>();
    private Buf out = new Buf(512);
    private Accumulator acc = new Accumulator();
    private LinkedHashMap<String, Stmt> cache;
    private boolean beginArmed;
    private List<String> beginSql = List.of("START TRANSACTION");
    // login
    private int seq;
    private byte[] scramble;
    private String plugin;
    private int serverCaps;
    private boolean secure;                                          // TLS: the password may go in the clear
    private boolean awaitingKey;                                     // we asked for the server's RSA key
    private io.hydrangea.Metrics.Count mOk, mError, mFailed, mPending, mTimeout, mKills, mLost, mReconnects;
    private io.hydrangea.Latency mLatency;
    private int pendingReported;
    private long batchClock, replyClock;

    private MyClient(MyConfig config, MyListener listener) {
        this.config = config;
        this.listener = listener != null ? listener : new MyListener() { };
        int size = config.statementCacheSize();
        if (size > 0) {
            this.cache = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Stmt> eldest) {
                    if (size() > size) {
                        closeStatement(eldest.getValue());
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    /** Connects, logs in and completes when the server is ready for calls. A first attempt that fails is not retried. */
    public static CompletableFuture<MyClient> connect(EventLoopGroup group, MyConfig config) {
        return connect(group, config, null);
    }

    public static CompletableFuture<MyClient> connect(EventLoopGroup group, MyConfig config, MyListener listener) {
        MyClient c = new MyClient(config, listener);
        c.group = group;
        c.home = group.next();
        io.hydrangea.Metrics m = group.metrics();
        c.mOk = m.counter("my.queries", null, "outcome", "ok");
        c.mError = m.counter("my.queries", null, "outcome", "error");       // the server answered with an error
        c.mFailed = m.counter("my.queries", null, "outcome", "failed");     // no answer: connection lost, client closed, too many waiting
        c.mPending = m.gauge("my.queries.pending", null);
        c.mTimeout = m.counter("my.queries", null, "outcome", "timeout");   // given up on: the time limit ran out
        c.mKills = m.counter("my.kills", null);                              // KILL QUERY statements sent
        c.mLost = m.counter("my.connections.lost", null);
        c.mReconnects = m.counter("my.reconnects", null);
        c.mLatency = m.latency("my.query.duration");
        c.onLoop(c::startConnecting);
        return c.ready;
    }

    // ---- API ----

    /**
     * Runs one statement. Without parameters it is a text query; with parameters ({@code ?}) a prepared statement. A parameter is null, a String, a number, a Boolean,
     * a byte[], a UUID, a BigDecimal, an Enum (its name), or a {@code java.time} LocalDate, LocalTime, LocalDateTime, Instant (as UTC) or OffsetDateTime (as UTC).
     */
    public CompletableFuture<MyResult> query(String sql, Object... params) {
        return submit(params.length == 0 ? Kind.TEXT : Kind.PREPARED, sql, params);
    }

    /**
     * As {@link #query} with a time limit (counted from the call, so it includes the time the call waits in the queue). When it runs out the call fails with a
     * {@link MyException#isTimeout() timeout} and, if it is the statement the server is running, a {@code KILL QUERY} goes out on a connection of its own. With calls
     * pipelined the kill is sent only while the timed-out call is the first one waiting for an answer, so that it does not hit another call's statement; a call that
     * timed out while queued behind others is read and dropped when its answer comes, and is killed if it is running by then. The kill hits whatever the connection is running at that
     * moment: it may be another call's statement if the timed-out one has just finished (that call fails with error 1317, retriable by the caller).
     */
    public CompletableFuture<MyResult> query(java.time.Duration timeout, String sql, Object... params) {
        Op op = new Op(params.length == 0 ? Kind.TEXT : Kind.PREPARED, sql, params.length == 0 ? params : snapshot(params), new CompletableFuture<>());
        op.timeoutNanos = timeout.toNanos();
        return submitOp(op);
    }

    /**
     * Reads a query in pieces: a server-side cursor ({@code COM_STMT_EXECUTE} with {@code CURSOR_TYPE_READ_ONLY}, then {@code COM_STMT_FETCH} for each piece), so a result
     * of any size is read with memory for about one piece. The future completes when the cursor is open (the columns are known); {@link MyRowStream#next()} gives the next
     * {@code fetchSize} rows, an empty list at the end. The connection belongs to the stream until it ends or is closed: calls made meanwhile wait, in their order. The
     * result limits of {@link MyConfig#withResultLimits} do not apply. A statement the server cannot open a cursor for (a CALL, a statement without rows) is read whole and
     * given as the stream's only piece. The time limit, if any, covers the opening of the cursor; the fetches have none.
     */
    public CompletableFuture<MyRowStream> stream(int fetchSize, String sql, Object... params) {
        if (fetchSize < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("fetchSize"));
        }
        Op op = new Op(Kind.STREAM, sql, snapshot(params), null);
        op.fetchSize = fetchSize;
        op.streamReady = new CompletableFuture<>();
        submitOp(op);
        return op.streamReady;
    }

    /**
     * Runs one statement for many parameter sets: prepared once, then every {@code COM_STMT_EXECUTE} is written back to back and the answers are read in order, so a batch
     * of a thousand inserts costs one round trip. The result is the rows affected by each set. <b>Not atomic</b>: MySQL runs every set whatever happened to the ones
     * before it; when some fail the future fails with the first error and its position in the message, and the other sets have still run. Run the batch inside
     * {@link MyPool#transaction} for all or nothing (the failure rolls the transaction back).
     */
    public CompletableFuture<long[]> executeBatch(String sql, List<Object[]> parameterSets) {
        if (parameterSets.isEmpty()) {
            return CompletableFuture.completedFuture(new long[0]);
        }
        List<Object[]> sets = new ArrayList<>(parameterSets.size());
        for (Object[] set : parameterSets) {
            sets.add(snapshot(set));
        }
        Op op = new Op(Kind.BATCH, sql, NO_PARAMS, null);
        op.sets = sets;
        op.batchResults = new long[sets.size()];
        op.batchDone = new CompletableFuture<>();
        submitOp(op);
        return op.batchDone;
    }

    /** Loop thread: the next piece of a stream. */
    void fetch(MyRowStream stream, CompletableFuture<List<MyRow>> done) {
        Op op = new Op(Kind.FETCH, null, NO_PARAMS, null);
        op.stream = stream;
        op.fetchDone = done;
        op.stmt = stream.stmt;
        op.fetchSize = stream.fetchSize;
        submitOp(op);
    }

    /** Loop thread: the stream is over (its last row was read, it failed, or the caller is done with it): free the statement and the connection. */
    void endStream(MyRowStream stream) {
        if (gate != stream) {
            return;
        }
        gate = null;
        if (stream.open) {
            inflight.decrementAndGet();      // an open stream counts as one call in flight (the pool then prefers another connection)
        }
        stream.done();
        if (state == State.ONLINE && stream.stmt != null) {
            closeStatement(stream.stmt);
            flush();
        }
        releaseDeferred();
    }

    /** As {@link #query} but always as a text query: parameters are not allowed. */
    public CompletableFuture<MyResult> queryText(String sql) {
        return submit(Kind.TEXT, sql, NO_PARAMS);
    }

    /** As {@link #query} but always through a prepared statement, even without parameters (binary rows). */
    public CompletableFuture<MyResult> queryPrepared(String sql, Object... params) {
        return submit(Kind.PREPARED, sql, params);
    }

    /** {@code COM_PING}. */
    public CompletableFuture<Void> ping() {
        return submit(Kind.TEXT, null, NO_PARAMS).thenApply(r -> null);
    }

    /** The next call starts a transaction: it carries the {@code START TRANSACTION} ({@code sql} is the whole statement), written ahead of it without a round trip of its own. */
    CompletableFuture<Void> armBegin(List<String> statements) {
        Op op = new Op(Kind.MARK, null, NO_PARAMS, new CompletableFuture<>());
        op.statements = statements;
        return submitOp(op).thenApply(r -> null);
    }

    /** {@code COMMIT} or {@code ROLLBACK}; nothing if no call came after {@link #armBegin} (nothing was begun). */
    CompletableFuture<MyResult> endTransaction(boolean commit) {
        return submit(Kind.END, commit ? "COMMIT" : "ROLLBACK", NO_PARAMS);
    }

    /** This connection behind the {@link io.hydrangea.sql.SqlClient} interface shared with the PostgreSQL client. Everything MySQL-only stays on this class. */
    public io.hydrangea.sql.SqlClient asSql() {
        return new MySql.Client(this);
    }

    /** True once logged in and until closed or lost. */
    public boolean isOpen() {
        return online;
    }

    /** True while the server says the session is inside a transaction (from the status flags of the last answer). */
    public boolean inTransaction() {
        return inTransaction;
    }

    /** Calls sent or queued whose answers have not come back. */
    public int pendingCalls() {
        return inflight.get();
    }

    /** True when called on the event loop this connection belongs to: a call made here is written at once, with no hand-off to another thread. */
    public boolean onOwnLoop() {
        return home.inEventLoop();
    }

    public long threadId() {
        return threadId;
    }

    public String serverVersion() {
        return serverVersion;
    }

    /** Sends {@code COM_QUIT} and closes the connection once it is written; calls still waiting fail. */
    public CompletableFuture<Void> disconnect() {
        onLoop(() -> {
            if (state == State.CLOSED) {
                return;
            }
            closing = true;
            if (reconnectTimer != null) {
                reconnectTimer.cancel();
                reconnectTimer = null;
            }
            Context c = ctx;
            if (state == State.OFFLINE) {
                state = State.CLOSED;
                closed.complete(null);
                return;
            }
            if (state == State.ONLINE && c != null) {
                state = State.CLOSED;
                online = false;
                try {
                    Buf b = new Buf(8);
                    int at = b.begin();
                    b.b(COM_QUIT);
                    b.end(at, 0);
                    c.write(ByteBuffer.wrap(b.a, 0, b.n));
                    c.closeAfterFlush();
                } catch (Exception e) {
                    c.connection().close();
                }
            } else if (c != null) {
                c.connection().close();
            } else {
                state = State.CLOSED;
                online = false;
                if (connectTimer != null) {
                    connectTimer.cancel();
                }
                if (rawConnection != null) {
                    rawConnection.close();
                }
                ready.completeExceptionally(new MyException("the client was closed", false));
                closed.complete(null);
            }
        });
        return closed;
    }

    @Override
    public void close() {
        disconnect();
    }

    // ---- sending ----

    private static final Object[] NO_PARAMS = new Object[0];

    private CompletableFuture<MyResult> submit(Kind kind, String sql, Object[] params) {
        Op op = new Op(kind, sql, kind == Kind.PREPARED ? snapshot(params) : params, new CompletableFuture<>());
        if (kind == Kind.TEXT && sql != null && params.length > 0) {
            op.future.completeExceptionally(new IllegalArgumentException("a text query takes no parameters"));
            return op.future;
        }
        return submitOp(op);
    }

    private CompletableFuture<MyResult> submitOp(Op op) {
        if (op.timeoutNanos == 0 && config.queryTimeout() != null && (op.kind == Kind.TEXT || op.kind == Kind.PREPARED || op.kind == Kind.STREAM) && op.sql != null) {
            op.timeoutNanos = config.queryTimeout().toNanos();
        }
        if (inflight.get() >= config.maxPending()) {
            op.fail(new MyException("too many calls wait for an answer (maxPending " + config.maxPending() + ")", true));
            return op.future;
        }
        inflight.incrementAndGet();
        if (home.inEventLoop() && inbox.isEmpty()) {     // not past what other threads queued before: a transaction relies on the order
            batchClock = mLatency.now();
            accept(op);
            flush();
            reportPending();
            return op.future;
        }
        inbox.add(op);
        if (drainScheduled.compareAndSet(false, true)) {
            try {
                home.execute(this::drain);
            } catch (RuntimeException shutDown) {
                drainScheduled.set(false);
                failInbox(false);
            }
        }
        return op.future;
    }

    /** byte[] parameters are copied now, so that the caller may reuse the array at once (the call is written later, on the loop). */
    private static Object[] snapshot(Object[] params) {
        Object[] copy = params;
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof byte[] b) {
                if (copy == params) {
                    copy = params.clone();
                }
                copy[i] = b.clone();
            }
        }
        return copy;
    }

    private void drain() {
        for (;;) {
            batchClock = mLatency.now();
            Op op;
            while ((op = inbox.poll()) != null) {
                accept(op);
            }
            flush();
            reportPending();
            drainScheduled.set(false);
            if (inbox.isEmpty() || !drainScheduled.compareAndSet(false, true)) {
                return;
            }
        }
    }

    private void reportPending() {
        int n = pending.size() + held.size();
        if (n != pendingReported) {
            mPending.add(n - pendingReported);
            pendingReported = n;
        }
    }

    private void failInbox(boolean retriable) {
        MyException e = new MyException(retriable ? "the client was closed" : "the event loop is shut down", retriable);
        for (Op op; (op = inbox.poll()) != null; ) {
            reject(op, e);
        }
    }

    private void reject(Op op, MyException e) {
        if (op.abandoned) {
            return;                       // it timed out earlier: the caller has been told and it was counted then
        }
        inflight.decrementAndGet();
        mFailed.increment();
        if (op.kind == Kind.FETCH) {
            op.stream.failed(e);
            endStream(op.stream);
        }
        op.fail(e);
    }

    private void accept(Op op) {
        if (state != State.ONLINE) {
            reject(op, new MyException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            return;
        }
        if (lostInTx && !op.internal && (op.kind == Kind.TEXT || op.kind == Kind.PREPARED || op.kind == Kind.STREAM || op.kind == Kind.BATCH)) {
            lostInTx = false;             // the connection came back, but the transaction this client was in did not: do not run on as if it had
            reject(op, new MyException("the connection was lost inside a transaction and has been re-established: the transaction is gone, start it again", false));
            return;
        }
        if (op.timeoutNanos > 0) {
            startTimer(op);
        }
        if (gate != null && op.stream != gate) {      // a stream owns the connection until it ends
            deferred.add(op);
            return;
        }
        if (prepareOut || !held.isEmpty() || full()) {   // order: nothing is written past a PREPARE whose answer is due; nor past the in-flight limit
            held.add(op);
            return;
        }
        write(op);
    }

    /** True when as many commands as {@link MyConfig#maxInFlight()} allows are out. */
    private boolean full() {
        int max = config.maxInFlight();
        return max > 0 && pending.size() >= max;
    }

    private void startTimer(Op op) {
        op.timer = home.schedule(() -> onTimeout(op), op.timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void onTimeout(Op op) {
        if (op.future.isDone() || op.abandoned) {
            return;
        }
        op.abandoned = true;
        inflight.decrementAndGet();
        mTimeout.increment();
        op.fail(MyException.timeout("the call did not finish within " + TimeUnit.NANOSECONDS.toMillis(op.timeoutNanos) + " ms"));
        if (held.remove(op) || deferred.remove(op)) {
            return;                       // never written: nothing to kill, nothing to read
        }
        if (pending.peek() == op) {
            sendKill(op);
        }
    }

    /** Asks the server, on a connection of its own, to stop what this connection is running. Best effort. */
    private void sendKill(Op op) {
        if (op.killSent || threadId == 0) {
            return;
        }
        op.killSent = true;
        mKills.increment();
        long id = threadId;
        MyConfig kc = config.withQueryTimeout(null).withStatementCacheSize(0).withConnectTimeout(java.time.Duration.ofSeconds(5)).withReconnect(null);
        connect(group, kc).whenComplete((c, err) -> {
            if (err != null) {
                LOG.log(Logger.Level.DEBUG, "KILL QUERY connection failed: " + err);
                return;
            }
            c.queryText("KILL QUERY " + id).whenComplete((r, e) -> c.close());
        });
    }

    /** Writes the call, or parks it behind a PREPARE. */
    private void write(Op op) {
        if (op.abandoned) {
            return;
        }
        switch (op.kind) {
            case MARK -> {
                beginArmed = true;
                beginSql = op.statements;
                inflight.decrementAndGet();
                op.future.complete(null);
                return;
            }
            case END -> {
                if (beginArmed) {                // no call came after the mark: nothing was begun, nothing to end
                    beginArmed = false;
                    inflight.decrementAndGet();
                    op.future.complete(null);
                    return;
                }
            }
            default -> { }
        }
        op.start = batchClock;
        if (beginArmed && op.kind != Kind.END) {
            beginArmed = false;
            for (String statement : beginSql) {
                if (full()) {                 // one at a time: the rest of the start, then the call, follow as the answers come
                    beginSql = beginSql.subList(beginSql.indexOf(statement), beginSql.size());
                    beginArmed = true;
                    held.addFirst(op);
                    return;
                }
                Op begin = new Op(Kind.TEXT, statement, NO_PARAMS, new CompletableFuture<>());
                begin.internal = true;
                begin.start = batchClock;
                inflight.incrementAndGet();
                command(COM_QUERY, begin.sql);
                pending.add(begin);
            }
            beginSql = List.of("START TRANSACTION");
            if (full()) {
                held.addFirst(op);
                return;
            }
        }
        switch (op.kind) {
            case TEXT, END -> {
                if (op.sql == null) {
                    commandNoArg(COM_PING);
                } else {
                    command(COM_QUERY, op.sql);
                }
                pending.add(op);
            }
            case PREPARED, BATCH -> {
                Stmt st = cache != null ? cache.get(op.sql) : null;
                if (st != null) {
                    op.stmt = st;
                    try {
                        writeExecute(op);
                    } catch (MyException e) {
                        finish(op, null, e);
                        return;
                    }
                    pending.add(op);
                } else {
                    op.preparing = true;
                    op.oneShot = cache == null;
                    command(COM_STMT_PREPARE, op.sql);
                    pending.add(op);
                    prepareOut = true;
                }
            }
            case STREAM -> {                 // always its own statement: the cursor lives on it, and closing the statement closes the cursor
                op.stream = new MyRowStream(this, home, op.fetchSize);
                gate = op.stream;            // the connection is the stream's from now on: what comes after waits (also what is still in `held`)
                op.preparing = true;
                op.oneShot = true;
                command(COM_STMT_PREPARE, op.sql);
                pending.add(op);
                prepareOut = true;
            }
            case FETCH -> {
                int at = out.begin();
                out.b(COM_STMT_FETCH).i32(op.stmt.id).i32(op.fetchSize);
                out.end(at, 0);
                op.meta = op.stream.meta;
                op.rows = new ArrayList<>();
                op.phase = Phase.ROWS;
                pending.add(op);
            }
            default -> throw new IllegalStateException();
        }
    }

    private void command(int cmd, String sql) {
        int at = out.begin();
        out.b(cmd);
        byte[] b = sql.getBytes(StandardCharsets.UTF_8);
        if (b.length > config.maxPacketBytes()) {
            throw new MyException("the statement is " + b.length + " bytes, over maxPacketBytes", false);
        }
        out.raw(b);
        out.end(at, 0);
    }

    private void commandNoArg(int cmd) {
        int at = out.begin();
        out.b(cmd);
        out.end(at, 0);
    }

    private void closeStatement(Stmt st) {
        int at = out.begin();
        out.b(COM_STMT_CLOSE).i32(st.id);
        out.end(at, 0);
    }

    private void writeExecute(Op op) {
        int mark = out.n;
        try {
            if (op.kind == Kind.BATCH) {
                for (int i = 0; i < op.sets.size(); i++) {
                    Object[] set = op.sets.get(i);
                    if (set.length != op.stmt.paramCount) {
                        throw new MyException("set " + i + ": the statement has " + op.stmt.paramCount + " parameters, " + set.length + " given", false);
                    }
                }
                int n = config.maxInFlight() > 0 ? 1 : op.sets.size();      // with a limit, one set at a time: the next goes out as each answer comes
                for (int i = 0; i < n; i++) {
                    encodeExecute(op, op.sets.get(i));
                }
                op.batchWritten = n;
                return;
            }
            Object[] p = op.params;
            if (p.length != op.stmt.paramCount) {
                throw new MyException("the statement has " + op.stmt.paramCount + " parameters, " + p.length + " given", false);
            }
            encodeExecute(op, p);
        } catch (RuntimeException e) {
            out.n = mark;                 // nothing of a call that cannot be encoded goes out
            throw e instanceof MyException ? e : new MyException("cannot encode the parameters: " + e, false, e);
        }
    }

    private void encodeExecute(Op op, Object[] p) {
        int at = out.begin();
        out.b(COM_STMT_EXECUTE).i32(op.stmt.id).b(op.kind == Kind.STREAM ? CURSOR_TYPE_READ_ONLY : 0).i32(1);
        if (p.length > 0) {
            byte[] nulls = new byte[(p.length + 7) / 8];
            for (int i = 0; i < p.length; i++) {
                if (p[i] == null) {
                    nulls[i >> 3] |= (byte) (1 << (i & 7));
                }
            }
            out.raw(nulls).b(1);
            Buf values = new Buf(64);
            for (int i = 0; i < p.length; i++) {
                encodeParam(out, values, p[i]);
            }
            out.raw(values.a, 0, values.n);
        }
        out.end(at, 0);
    }

    /** The type of a parameter goes into {@code types} (2 bytes), its value into {@code values}. */
    private static void encodeParam(Buf types, Buf values, Object v) {
        if (v == null) {
            types.b(MyRow.T_NULL).b(0);
        } else if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            types.b(MyRow.T_LONGLONG).b(0);
            values.i64(((Number) v).longValue());
        } else if (v instanceof Boolean b) {
            types.b(MyRow.T_TINY).b(0);
            values.b(b ? 1 : 0);
        } else if (v instanceof Double || v instanceof Float) {
            types.b(MyRow.T_DOUBLE).b(0);
            values.i64(Double.doubleToLongBits(((Number) v).doubleValue()));
        } else if (v instanceof String s) {
            types.b(MyRow.T_VAR_STRING).b(0);
            values.lenencString(s);
        } else if (v instanceof byte[] b) {
            types.b(MyRow.T_BLOB).b(0);
            values.lenencBytes(b);
        } else if (v instanceof BigDecimal d) {
            types.b(MyRow.T_NEWDECIMAL).b(0);
            values.lenencString(d.toPlainString());
        } else if (v instanceof LocalDate d) {
            types.b(MyRow.T_DATE).b(0);
            values.b(4).i16(d.getYear()).b(d.getMonthValue()).b(d.getDayOfMonth());
        } else if (v instanceof LocalDateTime t) {
            types.b(MyRow.T_DATETIME).b(0);
            dateTime(values, t);
        } else if (v instanceof Instant i) {
            types.b(MyRow.T_DATETIME).b(0);
            dateTime(values, LocalDateTime.ofInstant(i, ZoneOffset.UTC));
        } else if (v instanceof OffsetDateTime t) {
            types.b(MyRow.T_DATETIME).b(0);
            dateTime(values, t.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
        } else if (v instanceof LocalTime t) {
            types.b(MyRow.T_TIME).b(0);
            int micros = t.getNano() / 1000;
            values.b(micros == 0 ? 8 : 12).b(0).i32(0).b(t.getHour()).b(t.getMinute()).b(t.getSecond());
            if (micros != 0) {
                values.i32(micros);
            }
        } else if (v instanceof UUID u) {
            types.b(MyRow.T_VAR_STRING).b(0);
            values.lenencString(u.toString());
        } else if (v instanceof Enum<?> e) {
            types.b(MyRow.T_VAR_STRING).b(0);
            values.lenencString(e.name());
        } else {
            throw new MyException("unsupported parameter type " + v.getClass().getName(), false);
        }
    }

    private static void dateTime(Buf values, LocalDateTime t) {
        int micros = t.getNano() / 1000;
        values.b(micros == 0 ? 7 : 11).i16(t.getYear()).b(t.getMonthValue()).b(t.getDayOfMonth()).b(t.getHour()).b(t.getMinute()).b(t.getSecond());
        if (micros != 0) {
            values.i32(micros);
        }
    }

    private void flush() {
        if (out.n == 0) {
            return;
        }
        Buf b = out;
        out = new Buf(Math.max(512, b.n));
        Context c = ctx;
        if (c == null) {
            return;
        }
        try {
            c.write(ByteBuffer.wrap(b.a, 0, b.n));
        } catch (Exception e) {
            fail(new MyException("write failed: " + e, false, e));
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
        final int g = ++gen;
        Thread.startVirtualThread(() -> {
            InetSocketAddress addr;
            try {
                addr = new InetSocketAddress(config.host(), config.port());
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(config.host());
                }
            } catch (Exception e) {
                onLoop(() -> connectFailed(g, new MyException("cannot resolve " + config.host() + ": " + e, true, e)));
                return;
            }
            Options options = Options.defaults().withKeepAlive(true).withTcpNoDelay(true);
            if (config.connectTimeout() != null) {
                options = options.withConnectTimeout(config.connectTimeout());
            }
            Pipeline pipeline = config.sslContext() == null ? Pipeline.of(new MyStage(g))
                    : Pipeline.of(new MySslStage(g), io.hydrangea.tls.TlsStage.client(config.sslContext(), config.host(), config.port()), new MyStage(g));
            Client.connect(home, addr, options, pipeline).whenComplete((conn, err) -> {
                if (err != null) {
                    onLoop(() -> connectFailed(g, new MyException("cannot connect to " + config.host() + ":" + config.port() + ": " + err, true, err)));
                } else {
                    onLoop(() -> {
                        if (g == gen) {
                            rawConnection = conn;
                            startLoginTimer();
                        } else {
                            conn.close();
                        }
                    });
                    conn.closeFuture().thenRun(() -> onLoop(() -> connectFailed(g, new MyException("the connection closed before login", true))));
                }
            });
        });
    }

    private void startLoginTimer() {
        if (connectTimer != null || config.connectTimeout() == null) {
            return;
        }
        connectTimer = home.schedule(() -> {
            if (state == State.CONNECTING || state == State.AUTH) {
                fail(new MyException("no login within " + config.connectTimeout().toMillis() + " ms", true));
            }
        }, config.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }

    private void connectFailed(int g, MyException e) {
        if (g != gen || state != State.CONNECTING) {
            return;
        }
        if (connectTimer != null) {
            connectTimer.cancel();
            connectTimer = null;
        }
        if (lossCause instanceof MyException why) {
            e = why;
        }
        lossCause = null;
        if (reconnects()) {
            attemptFailed(e);             // an attempt to get an established connection back
            return;
        }
        state = State.CLOSED;
        ready.completeExceptionally(e);
        closed.complete(null);
    }

    // ---- reconnecting ----

    private boolean reconnects() {
        return loggedIn && !closing && config.reconnect() != null;
    }

    /** The connection was lost, or an attempt to get it back failed: wait as the back-off says and try again, or give up. */
    private void scheduleReconnect(Throwable why) {
        Backoff b = config.reconnect();
        reconnectAttempt++;
        if (b.maxAttempts() > 0 && reconnectAttempt > b.maxAttempts()) {
            state = State.CLOSED;
            closed.complete(null);
            safely(() -> listener.onGaveUp(why));
            return;
        }
        state = State.OFFLINE;
        java.time.Duration delay = b.delayFor(reconnectAttempt);
        int attempt = reconnectAttempt;
        safely(() -> listener.onReconnecting(attempt, delay));
        reconnectTimer = home.schedule(this::startReconnect, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void attemptFailed(Throwable why) {
        ctx = null;
        rawConnection = null;
        scheduleReconnect(why);
    }

    /** Loop thread: the wait is over. A fresh session: nothing of the old one is left that could be mistaken for the new one's. */
    private void startReconnect() {
        reconnectTimer = null;
        if (state != State.OFFLINE) {
            return;
        }
        state = State.CONNECTING;
        lossCause = null;
        acc = new Accumulator();
        ctx = null;
        rawConnection = null;
        earlyHandshake = null;
        scramble = null;
        plugin = null;
        secure = false;
        awaitingKey = false;
        beginArmed = false;
        beginSql = List.of("START TRANSACTION");
        gate = null;
        prepareOut = false;
        inTransaction = false;
        if (cache != null) {
            cache.clear();             // the statements lived in the old session
        }
        out = new Buf(512);
        startConnecting();
    }

    private void safely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "listener failed", t);
        }
    }

    /**
     * The first step of MySQL's TLS, next to the socket and below the {@code TlsStage}: the server speaks first, in the clear. This stage reads that greeting, checks that the server
     * offers TLS, answers with the short {@code SSLRequest} packet, and only then lets the {@code TlsStage} start its handshake. The greeting waits in {@code earlyHandshake} for the
     * login that follows inside TLS. The greeting must be alone in what the server sent: bytes after it would be plain text taken for part of the TLS session
     * (the attack of CVE-2021-23222 against PostgreSQL's equivalent), so extra bytes close the connection.
     */
    private final class MySslStage implements Stage {
        private final int g;
        private final Accumulator early = new Accumulator();
        private boolean passing;

        MySslStage(int g) {
            this.g = g;
        }

        @Override
        public void onActive(Context c) {
            if (g != gen || state != State.CONNECTING) {
                c.connection().close();
                return;
            }
            ctx = c;                             // the greeting comes first: the TlsStage is activated later, by onRead
        }

        @Override
        public void onRead(Context c, Object msg) throws Exception {
            if (passing) {
                c.fireRead(msg);
                return;
            }
            if (g != gen) {
                return;
            }
            try {
                greeting(c, (ByteBuffer) msg);
            } catch (MyException e) {
                fail(e);
            }
        }

        private void greeting(Context c, ByteBuffer msg) throws Exception {
            early.append(msg);
            ByteBuffer in = early.buffer();
            if (in.remaining() < 4) {
                return;
            }
            int len = (in.get(0) & 0xFF) | (in.get(1) & 0xFF) << 8 | (in.get(2) & 0xFF) << 16;
            if (len > 65536) {
                throw new MyException("protocol error: the greeting is " + len + " bytes", false);
            }
            if (in.remaining() < 4 + len) {
                return;
            }
            if (in.remaining() != 4 + len) {
                throw new MyException("protocol error: data after the greeting, before TLS", false);
            }
            byte[] greeting = new byte[len];
            in.get(4, greeting, 0, len);
            early.release();
            int serverCaps = serverCapabilities(greeting);
            if ((serverCaps & CLIENT_SSL) == 0) {
                fail(new MyException("the server does not support TLS", false));
                return;
            }
            Buf b = new Buf(48);
            int at = b.begin();
            b.i32(clientCapabilities() | CLIENT_SSL).i32(0x01000000).b(45).zeros(23);
            b.end(at, 1);
            c.write(ByteBuffer.wrap(b.a, 0, b.n));
            earlyHandshake = greeting;
            passing = true;
            c.fireActive();                      // now the TlsStage starts its handshake
        }

        @Override
        public void onClose(Context c, Throwable cause) {
            if (!passing && g == gen) {
                connectionEnded(cause != null ? cause : new java.io.IOException("the connection was closed"));
            }
            c.fireClose(cause);
        }
    }

    private byte[] earlyHandshake;

    private int clientCapabilities() {
        int client = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS
                | CLIENT_PS_MULTI_RESULTS | CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_DEPRECATE_EOF;
        if (config.database() != null) {
            client |= CLIENT_CONNECT_WITH_DB;
        }
        return client;
    }

    /** The capability flags in the server's greeting. */
    private static int serverCapabilities(byte[] p) {
        int at = 1;
        while (p[at] != 0) {
            at++;
        }
        at += 1 + 4 + 8 + 1;                   // NUL, thread id, scramble part 1, filler
        int caps = (int) le(p, at, 2);
        at += 2 + 1 + 2;                       // lower flags, charset, status
        return caps | (int) le(p, at, 2) << 16;
    }

    private final class MyStage implements Stage {
        private final int g;

        MyStage(int g) {
            this.g = g;
        }

        @Override
        public void onActive(Context c) {
            if (g != gen || state != State.CONNECTING) {
                c.connection().close();
                return;
            }
            ctx = c;
            state = State.AUTH;
            startLoginTimer();
            byte[] early = earlyHandshake;
            if (early != null) {                 // TLS: MySslStage read the server's greeting in the clear, the TLS handshake is done, now the login goes on inside it
                earlyHandshake = null;
                try {
                    secure = true;
                    handshake(early);
                } catch (MyException e) {
                    fail(e);
                }
            }
        }

        @Override
        public void onRead(Context c, Object msg) {
            if (g != gen) {
                return;
            }
            try {
                onData((ByteBuffer) msg);
            } catch (MyException e) {
                fail(e);
            } catch (RuntimeException e) {
                LOG.log(Logger.Level.WARNING, "MySQL packet handling failed", e);
                fail(new MyException("internal error: " + e, false, e));
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

    private void fail(Throwable cause) {
        if (lossCause == null) {
            lossCause = cause;
        }
        Context c = ctx;
        if (c != null) {
            c.connection().close();
        } else if (rawConnection != null) {
            rawConnection.close();
        } else {
            connectionEnded(cause);
        }
    }

    private void connectionEnded(Throwable cause) {
        if ((state == State.CLOSED && closed.isDone()) || state == State.OFFLINE) {
            return;
        }
        if (connectTimer != null) {
            connectTimer.cancel();
            connectTimer = null;
        }
        Throwable why = lossCause != null ? lossCause : cause;
        lossCause = null;
        ctx = null;
        online = false;
        State was = state;
        acc.release();
        MyException e = why instanceof MyException p ? p : new MyException("the connection was lost: " + why, false, why);
        for (Op op; (op = pending.poll()) != null; ) {
            failOp(op, e);
        }
        for (Op op; (op = held.poll()) != null; ) {
            failOp(op, new MyException(e.getMessage(), true, e.getCause()));     // never written: safe to run again
        }
        for (Op op; (op = deferred.poll()) != null; ) {
            failOp(op, new MyException(e.getMessage(), true, e.getCause()));
        }
        if (gate != null) {
            if (gate.open) {
                inflight.decrementAndGet();      // an open stream counts as a call in flight; one that had not opened yet was failed with its op above
            }
            gate.failed(e);
            gate = null;
        }
        failInbox(true);
        prepareOut = false;
        reportPending();
        if (was == State.ONLINE || (was == State.CLOSED && loggedIn)) {       // an established connection ended (CLOSED: by our own close, which set the state first)
            if (!closing) {
                mLost.increment();
                lostInTx = inTransaction || beginArmed;
            }
            safely(() -> listener.onClose(closing ? null : why));
        }
        if (reconnects() && was != State.CLOSED) {
            rawConnection = null;
            if (was == State.ONLINE) {
                reconnectAttempt = 0;
            }
            scheduleReconnect(why);                // a lost connection, or a failed login of an attempt: wait, then try again
            return;
        }
        state = State.CLOSED;
        if (!ready.isDone()) {
            ready.completeExceptionally(e);
        }
        closed.complete(null);
    }

    private void failOp(Op op, MyException e) {
        if (op.internal) {
            inflight.decrementAndGet();
            return;
        }
        reject(op, e);
    }

    // ---- reading ----

    private void onData(ByteBuffer data) {
        replyClock = mLatency.now();
        ByteBuffer in;
        if (acc.remaining() == 0) {
            in = data;
        } else {
            acc.append(data);
            in = acc.buffer();
        }
        for (;;) {
            int avail = in.remaining();
            if (avail < 4) {
                break;
            }
            int start = in.position();
            int q = start;
            int total = 0;
            boolean complete = false;
            int lastSeq = 0;
            for (;;) {
                if (start + avail - q < 4) {
                    break;
                }
                int len = (in.get(q) & 0xFF) | (in.get(q + 1) & 0xFF) << 8 | (in.get(q + 2) & 0xFF) << 16;
                lastSeq = in.get(q + 3) & 0xFF;
                total += len;
                if (total > config.maxPacketBytes()) {
                    throw new MyException("protocol error: a packet of " + total + " bytes is over maxPacketBytes", false);
                }
                if (start + avail - q < 4 + len) {
                    break;
                }
                q += 4 + len;
                if (len < 0xFFFFFF) {
                    complete = true;
                    break;
                }
            }
            if (!complete) {
                break;
            }
            byte[] payload = new byte[total];
            int w = 0;
            int r = start;
            while (r < q) {
                int len = (in.get(r) & 0xFF) | (in.get(r + 1) & 0xFF) << 8 | (in.get(r + 2) & 0xFF) << 16;
                in.get(r + 4, payload, w, len);
                w += len;
                r += 4 + len;
            }
            in.position(q);
            try {
                handle(payload, lastSeq);
            } catch (IndexOutOfBoundsException | NegativeArraySizeException | IllegalArgumentException | ArithmeticException | java.nio.BufferUnderflowException e) {
                // a count that runs past the packet, a negative length, a string that is not there: the server is sending rubbish, which is a protocol error, not our bug
                throw new MyException("protocol error: malformed packet", false, e);
            }
            if (state == State.CLOSED || ctx == null) {
                return;
            }
        }
        if (in == data && data.hasRemaining()) {
            acc.append(data);
        }
        reportPending();
    }

    private void handle(byte[] p, int packetSeq) {
        if (state == State.AUTH) {
            login(p, packetSeq);
            return;
        }
        if (state != State.ONLINE) {
            return;
        }
        Op op = pending.peek();
        if (op == null) {
            throw new MyException("protocol error: a packet arrived with no call waiting (0x" + Integer.toHexString(p.length > 0 ? p[0] & 0xFF : 0) + ")", false);
        }
        switch (op.phase) {
            case HEAD -> head(op, p);
            case PREP_PARAMS -> {
                if (--op.expected == 0) {
                    op.phase = op.prepCols > 0 ? Phase.PREP_COLS : Phase.HEAD;
                    op.expected = op.prepCols;
                    if (op.prepCols == 0) {
                        preparedDone(op);
                    }
                }
            }
            case PREP_COLS -> {
                if (--op.expected == 0) {
                    preparedDone(op);
                }
            }
            case COLS -> {
                op.cols.add(column(p));
                if (--op.expected == 0) {
                    op.meta = new MyRow.Meta(op.cols);
                    op.rows = new ArrayList<>();
                    op.phase = Phase.ROWS;
                }
            }
            case ROWS -> rowOrEnd(op, p);
        }
    }

    private void head(Op op, byte[] p) {
        int h = p[0] & 0xFF;
        if (h == 0xFF) {
            error(op, p);
            return;
        }
        if (op.preparing) {
            if (h != 0x00) {
                throw new MyException("protocol error: unexpected answer to COM_STMT_PREPARE", false);
            }
            int id = (int) le(p, 1, 4);
            op.prepCols = (int) le(p, 5, 2);
            op.prepParams = (int) le(p, 7, 2);
            op.stmt = new Stmt(id, op.prepParams);
            if (op.prepParams > 0) {
                op.phase = Phase.PREP_PARAMS;
                op.expected = op.prepParams;
            } else if (op.prepCols > 0) {
                op.phase = Phase.PREP_COLS;
                op.expected = op.prepCols;
            } else {
                preparedDone(op);
            }
            return;
        }
        if (h == 0x00) {          // OK
            int[] at = {1};
            long affected = lenenc(p, at);
            long insertId = lenenc(p, at);
            int status = (int) le(p, at[0], 2);
            int warnings = (int) le(p, at[0] + 2, 2);
            resultEnd(op, new MyResult(List.of(), List.of(), affected, insertId, warnings, status), status);
            return;
        }
        if (h == 0xFB) {
            throw new MyException("protocol error: the server asked for a local file (LOCAL INFILE is not supported)", false);
        }
        int[] at = {0};
        long n = lenenc(p, at);
        if (n <= 0 || n > 4096) {
            throw new MyException("protocol error: " + n + " columns", false);
        }
        op.cols = new ArrayList<>((int) n);
        op.expected = (int) n;
        op.phase = Phase.COLS;
    }

    private void rowOrEnd(Op op, byte[] p) {
        int h = p[0] & 0xFF;
        if (h == 0xFE && p.length < 0xFFFFFF) {            // the OK that ends the rows (CLIENT_DEPRECATE_EOF)
            int[] at = {1};
            long affected = lenenc(p, at);
            long insertId = lenenc(p, at);
            int status = (int) le(p, at[0], 2);
            int warnings = (int) le(p, at[0] + 2, 2);
            resultEnd(op, new MyResult(op.meta == null ? List.of() : List.of(op.meta.columns), op.rows, affected, insertId, warnings, status), status);
            return;
        }
        if (h == 0xFF) {
            error(op, p);
            return;
        }
        if (op.kind != Kind.FETCH && op.kind != Kind.STREAM) {
            op.rowCount++;
            op.byteCount += p.length;
            if (op.overflow || op.rowCount > config.maxResultRows() || op.byteCount > config.maxResultBytes()) {
                if (!op.overflow) {
                    op.overflow = true;
                    op.rows = null;             // what was kept is dropped; the rest is read and dropped as it comes
                }
                return;
            }
        }
        op.rows.add(op.binaryRows() ? MyRow.binary(op.meta, p) : MyRow.text(op.meta, p));
    }

    /** One result of the answer is complete; another follows if the server says so. */
    private void resultEnd(Op op, MyResult r, int status) {
        inTransaction = (status & SERVER_STATUS_IN_TRANS) != 0;
        if (op.kind == Kind.FETCH) {
            pending.poll();
            fetchDone(op, r.rows(), (status & SERVER_STATUS_LAST_ROW_SENT) != 0);
            return;
        }
        if (op.kind == Kind.STREAM) {
            pending.poll();
            streamOpened(op, r, status);
            return;
        }
        if (op.first == null) {
            op.first = r;
        } else {
            op.last.setNext(r);
        }
        op.last = r;
        op.phase = Phase.HEAD;
        op.cols = null;
        op.meta = null;
        op.rows = null;
        if ((status & SERVER_MORE_RESULTS_EXISTS) != 0) {
            return;
        }
        if (op.kind == Kind.BATCH) {
            op.batchResults[op.batchIdx++] = op.first.affectedRows();
            op.first = null;
            op.last = null;
            if (op.batchIdx < op.sets.size()) {
                nextBatchSet(op);
                return;                   // the next set's answer follows
            }
        }
        pending.poll();
        if (op.oneShot && op.stmt != null) {
            closeStatement(op.stmt);
            flush();
        }
        if (op.kind == Kind.BATCH) {
            finish(op, null, op.batchError);
            return;
        }
        if (op.overflow) {
            finish(op, null, new MyException("the result is larger than the limit of " + config.maxResultRows() + " rows / " + config.maxResultBytes()
                    + " bytes (MyConfig.withResultLimits): read it with stream()", false));
            return;
        }
        finish(op, op.first, null);
    }

    /** Under an in-flight limit the sets of a batch go one at a time: the next one, once an answer is in. */
    private void nextBatchSet(Op op) {
        if (op.batchWritten < op.sets.size()) {
            encodeExecute(op, op.sets.get(op.batchWritten++));
            flush();
        }
    }

    /** The EXECUTE of a stream answered: with a cursor (no rows, the status says so), or, when the server could not open one, with the rows themselves. */
    private void streamOpened(Op op, MyResult r, int status) {
        if (op.abandoned) {              // it timed out meanwhile: nothing to hand over
            closeStatement(op.stmt);
            flush();
            if (gate == op.stream) {
                gate = null;
            }
            finish(op, null, null);
            releaseDeferred();
            return;
        }
        boolean cursor = (status & SERVER_STATUS_CURSOR_EXISTS) != 0;
        MyRowStream stream = op.stream;
        stream.opened(op.stmt, r.columns(), op.meta != null ? op.meta : new MyRow.Meta(r.columns()));
        if (!cursor) {
            stream.whole(r.rows());      // the whole result came at once: the stream's one and only piece
        }
        op.cols = null;
        op.meta = null;
        op.rows = null;
        if (op.timer != null) {
            op.timer.cancel();
        }
        // inflight stays raised while the stream is open: endStream lowers it
        mLatency.endAt(op.start, replyClock);
        mOk.increment();
        op.streamReady.complete(stream);
    }

    private void fetchDone(Op op, List<MyRow> rows, boolean last) {
        inflight.decrementAndGet();
        if (last) {
            op.stream.last();
        }
        op.fetchDone.complete(rows);
        afterAnswer();
    }

    /** After a stream: the calls that waited behind it, in their order. */
    private void releaseDeferred() {
        if (deferred.isEmpty()) {
            return;
        }
        List<Op> waiting = new ArrayList<>(deferred);
        deferred.clear();
        batchClock = mLatency.now();
        for (Op o : waiting) {
            accept(o);                   // may close the gate again (another stream): the rest wait again, still in order
        }
        flush();
    }

    private void error(Op op, byte[] p) {
        int code = (int) le(p, 1, 2);
        int msgAt = 3;
        String state = null;
        if (p.length > 3 && p[3] == '#') {
            state = new String(p, 4, 5, StandardCharsets.ISO_8859_1);
            msgAt = 9;
        }
        String msg = new String(p, msgAt, p.length - msgAt, StandardCharsets.UTF_8);
        MyException e = new MyException(msg + " (error " + code + (state != null ? ", SQLSTATE " + state : "") + ")", code, state,
                code == 1213 || code == 1205 || code == 1243, null);
        if (op.kind == Kind.BATCH && !op.preparing) {           // this set failed; the others run anyway, their answers follow
            if (op.batchError == null) {
                op.batchError = new MyException("set " + op.batchIdx + ": " + e.getMessage(), e.errorCode(), e.sqlState(), e.retriable(), null);
            }
            op.batchResults[op.batchIdx++] = -1;
            op.first = null;
            op.last = null;
            op.phase = Phase.HEAD;
            op.cols = null;
            op.meta = null;
            op.rows = null;
            if (op.batchIdx < op.sets.size()) {
                nextBatchSet(op);
                return;
            }
            pending.poll();
            if (op.oneShot && op.stmt != null) {
                closeStatement(op.stmt);
                flush();
            }
            finish(op, null, op.batchError);
            return;
        }
        pending.poll();
        if (op.preparing) {
            op.preparing = false;
            prepareOut = false;
            finish(op, null, e);
            releaseHeld();
            return;
        }
        if (op.kind == Kind.FETCH) {
            inflight.decrementAndGet();
            mError.increment();
            op.stream.failed(e);
            op.fetchDone.completeExceptionally(e);
            endStream(op.stream);
            return;
        }
        if (code == 1243 && op.stmt != null && cache != null) {      // the server no longer knows the statement
            cache.remove(op.sql);
        }
        if (op.oneShot && op.stmt != null) {
            closeStatement(op.stmt);
            flush();
        }
        finish(op, null, e);
    }

    /** The answer to COM_STMT_PREPARE is complete: remember the statement and send the EXECUTE. */
    private void preparedDone(Op op) {
        op.preparing = false;
        op.phase = Phase.HEAD;
        if (cache != null && !op.oneShot) {
            cache.put(op.sql, op.stmt);
        }
        if (op.abandoned) {              // it timed out while its statement was being prepared: no execute
            pending.poll();
            prepareOut = false;
            if (op.oneShot) {
                closeStatement(op.stmt);
            }
            releaseHeld();
            return;
        }
        // the op stays first in `pending`: its answer is next
        try {
            writeExecute(op);
        } catch (MyException e) {
            pending.poll();
            prepareOut = false;
            if (op.oneShot) {
                closeStatement(op.stmt);
            }
            finish(op, null, e);
            releaseHeld();
            return;
        }
        prepareOut = false;
        releaseHeld();
        flush();
    }

    private void releaseHeld() {
        batchClock = mLatency.now();
        while (!prepareOut && !full()) {
            Op next = held.poll();
            if (next == null) {
                break;
            }
            if (gate != null && next.stream != gate) {       // a stream was written ahead of it: it waits for the stream to end
                deferred.add(next);
                continue;
            }
            try {
                write(next);
            } catch (MyException e) {
                finish(next, null, e);
            }
        }
        flush();
    }

    private void finish(Op op, MyResult r, MyException e) {
        if (op.timer != null) {
            op.timer.cancel();
        }
        if (op.abandoned) {
            afterAnswer();
            return;                       // the caller has been told and it was counted then
        }
        if (op.internal) {
            inflight.decrementAndGet();
            afterAnswer();
            return;                       // an error of the START TRANSACTION shows up in the first call's answer as well
        }
        inflight.decrementAndGet();
        mLatency.endAt(op.start, replyClock);
        if (e != null) {
            if (e.errorCode() != 0) {
                mError.increment();
            } else {
                mFailed.increment();
            }
            op.fail(e);
        } else {
            mOk.increment();
            if (op.kind == Kind.BATCH) {
                op.batchDone.complete(op.batchResults);
            } else {
                op.future.complete(r);
            }
        }
        if (op.kind == Kind.STREAM && gate == op.stream && !op.stream.open) {     // the stream failed before its cursor opened: the connection is free again
            gate = null;
            releaseDeferred();
        }
        afterAnswer();
    }

    /** After an answer: a call that timed out while queued and is now running is killed; under an in-flight limit, the next held call goes out. */
    private void afterAnswer() {
        killNextIfDue();
        if (config.maxInFlight() > 0 && !held.isEmpty()) {
            releaseHeld();
        }
    }

    /** After an answer: if the call now first in line has timed out already (it was queued behind others), it is running now: kill it. */
    private void killNextIfDue() {
        Op next = pending.peek();
        if (next != null && next.abandoned && !next.killSent) {
            sendKill(next);
        }
    }

    private static MyColumn column(byte[] p) {
        int[] at = {0};
        String[] parts = new String[6];     // catalog, schema, table, org_table, name, org_name
        for (int i = 0; i < 6; i++) {
            int len = (int) lenenc(p, at);
            parts[i] = new String(p, at[0], len, StandardCharsets.UTF_8);
            at[0] += len;
        }
        lenenc(p, at);                       // length of the fixed fields: 0x0c
        int charset = (int) le(p, at[0], 2);
        int type = p[at[0] + 6] & 0xFF;
        int flags = (int) le(p, at[0] + 7, 2);
        int decimals = p[at[0] + 9] & 0xFF;
        return new MyColumn(parts[4], parts[2], type, flags, charset, decimals);
    }

    private static long le(byte[] d, int p, int len) {
        long v = 0;
        for (int i = len - 1; i >= 0; i--) {
            v = v << 8 | (d[p + i] & 0xFF);
        }
        return v;
    }

    /** A length-encoded integer at {@code at[0]}; moves the position past it. */
    private static long lenenc(byte[] d, int[] at) {
        int p = at[0];
        int b = d[p] & 0xFF;
        if (b < 251) {
            at[0] = p + 1;
            return b;
        }
        switch (b) {
            case 0xFC -> {
                at[0] = p + 3;
                return le(d, p + 1, 2);
            }
            case 0xFD -> {
                at[0] = p + 4;
                return le(d, p + 1, 3);
            }
            case 0xFE -> {
                at[0] = p + 9;
                return le(d, p + 1, 8);
            }
            default -> {
                at[0] = p + 1;
                return -1;           // 0xFB: NULL
            }
        }
    }

    // ---- login ----

    private void login(byte[] p, int packetSeq) {
        int h = p[0] & 0xFF;
        if (h == 0xFF) {
            int code = (int) le(p, 1, 2);
            int msgAt = p.length > 3 && p[3] == '#' ? 9 : 3;
            String msg = new String(p, msgAt, p.length - msgAt, StandardCharsets.UTF_8);
            throw new MyException("login failed: " + msg + " (error " + code + ")", code, msgAt == 9 ? new String(p, 4, 5, StandardCharsets.ISO_8859_1) : null, false, null);
        }
        if (scramble == null) {
            handshake(p);
            return;
        }
        seq = packetSeq;
        if (h == 0x00) {                 // OK: logged in
            loggedIn();
            return;
        }
        if (h == 0xFE) {                 // AuthSwitchRequest: the server wants another plugin
            int end = 1;
            while (p[end] != 0) {
                end++;
            }
            plugin = new String(p, 1, end - 1, StandardCharsets.UTF_8);
            int dataEnd = p.length;
            if (dataEnd > end + 1 && p[dataEnd - 1] == 0) {
                dataEnd--;
            }
            scramble = java.util.Arrays.copyOfRange(p, end + 1, dataEnd);
            sendAuth(Auth.response(plugin, config.password(), scramble));
            return;
        }
        if (h == 0x01) {                 // AuthMoreData (caching_sha2_password)
            if (awaitingKey) {
                awaitingKey = false;
                String pem = new String(p, 1, p.length - 1, StandardCharsets.UTF_8);
                sendAuth(Auth.rsaEncrypt(config.password() == null ? "" : config.password(), scramble, pem));
                return;
            }
            int code = p.length > 1 ? p[1] & 0xFF : 0;
            if (code == 3) {
                return;                   // fast authentication succeeded: an OK follows
            }
            if (code == 4) {              // full authentication: the password itself, which must not travel in the clear
                if (secure) {
                    byte[] pw = (config.password() == null ? "" : config.password()).getBytes(StandardCharsets.UTF_8);
                    byte[] withNul = java.util.Arrays.copyOf(pw, pw.length + 1);
                    sendAuth(withNul);
                } else {
                    if (!config.allowPublicKeyRetrieval()) {
                        throw new MyException("the server asks for the full caching_sha2_password authentication on a connection without TLS: refused, the password would go"
                                + " encrypted with a public key the server hands over, which a man in the middle could replace with his own (use TLS, or"
                                + " MyConfig.withAllowPublicKeyRetrieval(true) when the network is trusted)", false);
                    }
                    awaitingKey = true;
                    sendAuth(new byte[]{2});     // ask for the server's public key
                }
                return;
            }
        }
        throw new MyException("protocol error: unexpected packet 0x" + Integer.toHexString(h) + " during login", false);
    }

    private void handshake(byte[] p) {
        if ((p[0] & 0xFF) != 10) {
            throw new MyException("protocol error: unsupported handshake protocol version " + (p[0] & 0xFF), false);
        }
        int at = 1;
        int end = at;
        while (p[end] != 0) {
            end++;
        }
        serverVersion = new String(p, at, end - at, StandardCharsets.UTF_8);
        at = end + 1;
        threadId = le(p, at, 4);
        at += 4;
        byte[] part1 = java.util.Arrays.copyOfRange(p, at, at + 8);
        at += 8 + 1;                           // filler
        int caps = (int) le(p, at, 2);
        at += 2;
        at += 1 + 2;                           // charset, status
        caps |= (int) le(p, at, 2) << 16;
        at += 2;
        int authLen = p[at] & 0xFF;
        at += 1 + 10;                          // reserved
        int part2Len = Math.max(13, authLen - 8);
        byte[] part2 = java.util.Arrays.copyOfRange(p, at, at + part2Len);
        at += part2Len;
        // the scramble is 20 bytes: 8 + 12 (the 13th is a NUL)
        scramble = new byte[part1.length + part2Len - 1];
        System.arraycopy(part1, 0, scramble, 0, part1.length);
        System.arraycopy(part2, 0, scramble, part1.length, part2Len - 1);
        int e2 = at;
        while (e2 < p.length && p[e2] != 0) {
            e2++;
        }
        plugin = at < p.length ? new String(p, at, e2 - at, StandardCharsets.UTF_8) : Auth.NATIVE;
        serverCaps = caps;
        int need = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF | CLIENT_SECURE_CONNECTION;
        if ((caps & need) != need) {
            throw new MyException("the server " + serverVersion + " lacks capabilities this client needs (protocol 4.1, plugin auth, DEPRECATE_EOF)", false);
        }
        int client = clientCapabilities();
        if (secure) {
            client |= CLIENT_SSL;
        }
        Buf b = new Buf(128);
        int pkt = b.begin();
        b.i32(client).i32(0x01000000).b(45).zeros(23);
        b.cstr(config.user());
        b.lenencBytes(Auth.response(plugin, config.password(), scramble));
        if (config.database() != null) {
            b.cstr(config.database());
        }
        b.cstr(plugin);
        seq = secure ? 2 : 1;                 // the SSLRequest of MySslStage was packet 1
        b.end(pkt, seq);
        write(b);
    }

    private void sendAuth(byte[] data) {
        Buf b = new Buf(data.length + 8);
        int pkt = b.begin();
        b.raw(data);
        b.end(pkt, seq + 1);
        write(b);
    }

    private void write(Buf b) {
        try {
            ctx.write(ByteBuffer.wrap(b.a, 0, b.n));
        } catch (Exception e) {
            fail(new MyException("write failed: " + e, true, e));
        }
    }

    private void loggedIn() {
        if (connectTimer != null) {
            connectTimer.cancel();
            connectTimer = null;
        }
        state = State.ONLINE;
        online = true;
        scramble = null;
        boolean again = loggedIn;               // a login after the first: this is the connection coming back
        loggedIn = true;
        reconnectAttempt = 0;
        if (again) {
            mReconnects.increment();
            safely(listener::onReconnected);
        }
        ready.complete(this);
    }

    @Override
    public String toString() {
        return "MyClient{" + config.host() + ":" + config.port() + ", " + state + "}";
    }
}
