package io.hydrangea.postgres;

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
import io.hydrangea.tls.TlsStage;

import java.lang.System.Logger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A PostgreSQL connection that many threads can use at once. Queries from any thread are queued, written together and answered in order,
 * so a burst costs few system calls and no query waits for the one before it to come back (pipelining). Like {@code RedisClient}, the
 * connection belongs to one event loop and nothing here blocks.
 * <pre>{@code
 * PgClient db = PgClient.connect(group, PgConfig.of("localhost", 5432, "app", "appdb").withPassword("secret")).join();
 * PgResult r = db.query("SELECT id, name FROM users WHERE id = $1", 42).join();
 * String name = r.first().getString("name");
 * }</pre>
 * <ul>
 *   <li><b>Queries</b> use the extended protocol with {@code $1, $2...} parameters ({@link #query}); a statement is prepared on first use and
 *       kept (per connection, {@link PgConfig#statementCacheSize()}), so later calls skip parsing. {@link #execute} uses the simple protocol:
 *       several statements in one string, no parameters.</li>
 *   <li><b>Errors are per query.</b> Every query ends with its own {@code Sync}, so a failing query does not spoil the ones queued behind it.
 *       One consequence: if the first use of a statement fails to parse, queries already queued with the same SQL fail too (state 26000)
 *       and the next call prepares it again.</li>
 *   <li><b>Binary transfer</b> ({@link PgConfig#withBinaryTransfer}, on): parameters and results go in binary where the server has described the statement and the binary form
 *       changes nothing a caller can see; the first run of a statement is text. A list, array or {@code byte[]} parameter is copied when the query is made, so the caller may reuse it
 *       at once.</li>
 *   <li><b>Big results</b>: a result is held whole, so it has a limit ({@link PgConfig#withResultLimits}); {@link #stream} reads in pieces with memory for about two of them.
 *       <b>Batches</b>: {@link #executeBatch} runs a statement for many parameter sets with one {@code Sync} (all or nothing).
 *       <b>Arrays</b>: a {@code List} or Java array is a parameter, an array column is a {@code List} ({@link PgRow#getList(int)}).</li>
 *   <li><b>Time limits</b> ({@link PgConfig#withQueryTimeout}, or per call: {@link #query(java.time.Duration, String, Object...)}): a query that runs out fails with a
 *       {@link PgException#isTimeout() timeout} and the server is asked to cancel it with a {@code CancelRequest}. With queries pipelined the cancel is sent only while the timed-out
 *       query is the one the server is running (see the method for what that means for the others).</li>
 *   <li><b>A table that changes under a cached statement</b> ({@code cached plan must not change result type}, SQLSTATE 0A000): the client drops the statement and, outside a
 *       transaction, runs the query again once (after the queries already queued; in a transaction the error is reported, since the transaction is aborted by it).</li>
 *   <li><b>Transactions</b> need the connection for themselves: other threads' queries would run inside yours. Use {@link PgPool#transaction}, or
 *       a {@link #duplicate()} of your own.</li>
 *   <li><b>One connection</b> runs one query at a time on the server, so for parallelism use a {@link PgPool}.</li>
 *   <li><b>TLS</b> ({@link PgConfig#withTls}) is negotiated the PostgreSQL way: an {@code SSLRequest}, then the handshake. A server that answers {@code N} is an error, not a fall-back
 *       to plain text; bytes after the {@code S} are refused (CVE-2021-23222). <b>Against a hostile server or a man in the middle</b> on a connection without TLS: SCRAM proves the
 *       password without sending it and checks the server's signature, so a fake server is found out; a request for the password in clear text is refused unless
 *       {@link PgConfig#withAllowClearTextPassword} says the network is trusted (MD5 is answered: it is a hash, weak but not the password). See {@code PgAttackTest}.</li>
 *   <li><b>COPY</b>: {@link #copyIn(String, Iterable)} and {@link #copyOut(String, java.util.function.Consumer)} (text, CSV and binary data pass through as bytes).
 *       A {@code COPY ... FROM STDIN} owns the connection until it ends (other queries wait); a {@code COPY} sent through {@link #query} or
 *       {@link #execute} fails.</li>
 *   <li><b>Reconnecting</b> ({@link PgConfig#withReconnect}, off by default; a {@link PgPool} replaces its connections itself and turns it off for them): after an established
 *       connection is lost the client waits as the {@link io.hydrangea.Backoff} says and connects again, telling the {@link PgListener}. Queries that were on the lost connection fail
 *       (they may have run); queries sent meanwhile fail at once and retriably. The new connection is a new session: the statement cache starts empty and channels given to {@link #listen}
 *       are listened to again, but {@code SET} values and temporary tables are gone, and so is a transaction: if the connection was lost inside one, the first query of the new session
 *       fails once with an error that says so rather than run as if the transaction were still open.</li>
 *   <li><b>Poolers</b>: through PgBouncer in transaction mode (tested with 1.26) everything here works, the statement cache included, as long as PgBouncer knows prepared statements
 *       ({@code max_prepared_statements}, from 1.21). With an older one a named statement lives on one server connection only: use {@code withStatementCacheSize(0)}. {@code LISTEN},
 *       {@code SET} and the like need a direct connection or session pooling.</li>
 *   <li><b>Not supported (yet):</b> replication, Kerberos/GSS, several hosts with failover, composite and range types (they come back as text).</li>
 *   <li><b>Login</b>: SCRAM-SHA-256 (no channel binding), MD5 and cleartext passwords.</li>
 * </ul>
 */
public final class PgClient implements AutoCloseable {

    private static final Logger LOG = System.getLogger(PgClient.class.getName());

    private enum State { CONNECTING, AUTH, ONLINE, OFFLINE, CLOSED }      // OFFLINE: lost, waiting for the next reconnect attempt

    private static final class Stmt {
        final String name;
        PgColumn[] cols;        // from the Describe of the first use; null until its answer has come
        int[] paramOids;        // the types of the parameters as the server prepared them (its ParameterDescription); null until its answer has come
        boolean formatsDone;
        byte[] resultFormats;   // per column 1 = ask for binary; null = all text
        boolean[] binaryCols;   // the same as flags, for the rows

        Stmt(String name) {
            this.name = name;
        }
    }

    /** Statement cache key: the SQL and the parameter types it was prepared with. */
    private static final class StmtKey {
        final String sql;
        final int[] oids;
        final int hash;

        StmtKey(String sql, int[] oids) {
            this.sql = sql;
            this.oids = oids;
            this.hash = sql.hashCode() * 31 + Arrays.hashCode(oids);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof StmtKey k && k.hash == hash && k.sql.equals(sql) && Arrays.equals(k.oids, oids);
        }
    }

    /** One query sent and waiting for its ReadyForQuery. */
    private static final int MARK = 1, END = 2, COPY_IN = 3, COPY_OUT = 4, STREAM = 5, BATCH = 6;

    private static final class Op {
        final boolean simple;
        final String sql;
        final int[] oids;
        final byte[][] values;
        Object[] params;                                // the parameters as Java values, for the binary form of a statement the server has already described
        List<Object[]> setParams;                       // BATCH: the same for each set
        boolean[] formats;                              // the columns of this query's rows that arrive in binary (null = none)
        final CompletableFuture<Object> future = new CompletableFuture<>();
        long start;                     // the clock when the loop accepted it (0 = not timed)
        PgColumn[] cols;
        List<PgRow> rows;
        String tag;
        List<PgResult> results;         // simple protocol: one per statement
        PgException error;
        Stmt stmt;                      // extended protocol with a cache
        StmtKey key;
        boolean newStmt;                // this query prepares the statement
        boolean parsed;                 // ParseComplete seen
        int control;                    // 0 = a query; MARK = arm the next query with BEGIN; END = commit/rollback unless nothing began
        int skipComplete;               // CommandCompletes of the BEGIN this query carries, not to be reported
        int skipParse;                  // the same for its ParseComplete
        java.util.function.Consumer<byte[]> sink;       // COPY TO STDOUT: where the data goes (loop thread)
        Throwable sinkError;
        CompletableFuture<PgCopyIn> copyReady;          // COPY FROM STDIN: completes when the server asks for data
        boolean copyEnded;                              // CopyDone or CopyFail sent
        long timeoutNanos;                              // 0 = none
        io.hydrangea.Scheduled timer;
        boolean abandoned;                              // timed out: the caller has its answer; the server's is read and dropped
        boolean cancelSent;
        boolean strayCancel;                            // the error is a cancel that was meant for another query
        boolean carriedBegin;                           // this query carried the BEGIN of a transaction
        boolean internal;                               // the client's own (the LISTENs it repeats after a reconnect)
        int retries;                                    // a stale cached plan is retried once
        int rowCount;                                   // rows and bytes of the result being collected, against the limits of the config
        long rowBytes;
        boolean overflow;
        int fetchSize;                                  // STREAM: rows per piece
        CompletableFuture<PgRowStream> streamReady;
        java.util.ArrayDeque<List<PgRow>> ready;        // STREAM: pieces the server has sent that the caller has not taken (at most one)
        CompletableFuture<List<PgRow>> waiter;          // STREAM: the caller's wait for the next piece
        boolean requested;                              // STREAM: an Execute is out and its piece has not come
        boolean streamDone;                             // STREAM: CommandComplete seen
        boolean syncSent;
        boolean streamClosed;
        long streamRows;
        List<byte[][]> sets;                            // BATCH: the parameter sets
        long[] counts;                                  // BATCH: the row count of each item answered so far
        int nCounts;

        Op(boolean simple, String sql, int[] oids, byte[][] values) {
            this.simple = simple;
            this.sql = sql;
            this.oids = oids;
            this.values = values;
        }
    }

    private final PgConfig config;
    private final PgListener listener;
    private final CompletableFuture<PgClient> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private EventLoopGroup group;
    private EventLoop home;

    // any thread
    private final ConcurrentLinkedQueue<Op> inbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private final AtomicInteger inflight = new AtomicInteger();
    private volatile Context ctx;
    private volatile boolean online;
    private volatile char txStatus = 'I';
    private volatile int backendPid;
    private int backendSecret;                 // loop thread
    private volatile InetSocketAddress remoteAddr;
    private long lastCancelNanos;              // loop thread: when the last CancelRequest went out (0 = none)
    private final Map<String, String> serverParams = new ConcurrentHashMap<>();

    // loop thread only
    private State state = State.CONNECTING;
    private Scheduled connectTimer;
    private boolean closing;
    private boolean loggedIn;
    private int gen;                          // the number of the connection attempt in progress: what an attempt that is over reports is ignored
    private int reconnectAttempt;             // consecutive failures since the connection was lost
    private Scheduled reconnectTimer;
    private boolean lostInTx;                 // the connection was lost inside a transaction: the transaction is gone, and the first query of the new session must not run as if it were not
    private final java.util.Set<String> listening = ConcurrentHashMap.newKeySet();      // channels given to listen(), listened to again after a reconnect
    private io.hydrangea.Metrics.Count mReconnects;
    private Op gate;                   // a COPY FROM STDIN owns the connection: other queries wait in `deferred`
    private final ArrayDeque<Op> deferred = new ArrayDeque<>();
    private String beginSql = "BEGIN";   // what the armed BEGIN is
    private boolean beginArmed;         // the next query starts a transaction: it carries the BEGIN
    private Throwable lossCause;
    private io.hydrangea.Connection rawConnection;       // set once the TCP connection is up, before the pipeline is active
    private final ArrayDeque<Op> pending = new ArrayDeque<>();
    private Buf out = new Buf(512);
    private Accumulator acc = new Accumulator();
    private Scram scram;
    private long stmtSeq;
    private final List<String> toClose = new ArrayList<>();
    private LinkedHashMap<StmtKey, Stmt> cache;
    private long batchClock;
    private long replyClock;
    private int pendingReported;
    private io.hydrangea.Metrics.Count mOk;
    private io.hydrangea.Metrics.Count mError;
    private io.hydrangea.Metrics.Count mFailed;
    private io.hydrangea.Metrics.Count mPending;
    private io.hydrangea.Metrics.Count mLost;
    private io.hydrangea.Latency mLatency;
    private io.hydrangea.Metrics.Count mTimeout;
    private io.hydrangea.Metrics.Count mCancels;
    private io.hydrangea.Metrics.Count mReplanned;
    private io.hydrangea.Metrics.Count mBinaryParams;
    private io.hydrangea.Metrics.Count mBinaryColumns;

    private PgClient(PgConfig config, PgListener listener) {
        this.config = config;
        this.listener = listener != null ? listener : new PgListener() { };
        int size = config.statementCacheSize();
        if (size > 0) {
            this.cache = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<StmtKey, Stmt> eldest) {
                    if (size() > size) {
                        toClose.add(eldest.getValue().name);
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    /** Connects, logs in and completes when the server is ready for queries. A first attempt that fails is not retried. */
    public static CompletableFuture<PgClient> connect(EventLoopGroup group, PgConfig config) {
        return connect(group, config, null);
    }

    public static CompletableFuture<PgClient> connect(EventLoopGroup group, PgConfig config, PgListener listener) {
        PgClient c = new PgClient(config, listener);
        c.group = group;
        c.home = group.next();
        io.hydrangea.Metrics m = group.metrics();
        c.mOk = m.counter("pg.queries", null, "outcome", "ok");
        c.mError = m.counter("pg.queries", null, "outcome", "error");       // the server answered with an error
        c.mFailed = m.counter("pg.queries", null, "outcome", "failed");     // no answer: connection lost, client closed, too many waiting
        c.mPending = m.gauge("pg.queries.pending", null);
        c.mLost = m.counter("pg.connections.lost", null);
        c.mReconnects = m.counter("pg.reconnects", null);
        c.mTimeout = m.counter("pg.queries", null, "outcome", "timeout");   // given up on: the time limit ran out
        c.mCancels = m.counter("pg.cancels", null);                          // CancelRequests sent
        c.mBinaryParams = m.counter("pg.binary.parameters", null);                // parameters sent in binary
        c.mBinaryColumns = m.counter("pg.binary.columns", null);                   // result columns asked for in binary, once for each execution
        c.mReplanned = m.counter("pg.statements.replanned", null);           // cached statements dropped because the table changed under them
        c.mLatency = m.latency("pg.query.duration");
        c.onLoop(c::startConnecting);
        return c.ready;
    }

    /** A new connection with the same settings: for a transaction of your own, or {@code LISTEN}. */
    public CompletableFuture<PgClient> duplicate() {
        return connect(group, config, listener);
    }

    // ---- API ----

    /**
     * Runs one statement with parameters ({@code $1, $2...}). A parameter is null, a String, a number, a Boolean, a byte[], a UUID, a BigDecimal,
     * an Enum (sent as its name) or a {@code java.time} LocalDate, LocalTime, LocalDateTime, OffsetDateTime or Instant. A String is sent without a type,
     * so the server takes the type the SQL asks for ({@code WHERE id = $1} with "42" works); other types are sent with theirs.
     */
    public CompletableFuture<PgResult> query(String sql, Object... params) {
        return query(config.queryTimeout(), sql, params);
    }

    /**
     * As {@link #query(String, Object...)} with its own time limit ({@code null} = none), counted from the moment the client takes the query up, so time spent waiting behind
     * other queries on the connection counts. When it runs out the future fails at once with a {@link PgException#isTimeout() timeout}, and
     * <ul>
     *   <li>a query that was still waiting to be sent does not run;</li>
     *   <li>a query that was sent <b>may already have run, or may still run</b>: the client asks the server to cancel it (a {@code CancelRequest} on a connection of its own) only
     *       while it is the one the server is working on, so a cancel does not hit a query of another caller. A query waiting behind slower ones cannot be stopped before its turn,
     *       and is cancelled when it gets there (if it did not finish by then). A cancel can still reach the next query when the timed-out one finishes at the same moment; that
     *       query then fails with a {@link PgException#retriable() retriable} error saying so.</li>
     * </ul>
     */
    public CompletableFuture<PgResult> query(java.time.Duration timeout, String sql, Object... params) {
        if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        if (sql == null || sql.indexOf('\0') >= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters"));
        }
        int n = params == null ? 0 : params.length;
        if (n > 32767) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("at most 32767 parameters"));
        }
        int[] oids = new int[n];
        byte[][] values = new byte[n][];
        Object[] kept = new Object[n];
        try {
            encodeParams(params, kept, oids, values);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        Op op = new Op(false, sql, oids, values);
        op.params = kept;
        op.timeoutNanos = timeout == null ? 0 : timeout.toNanos();
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<PgResult> f = (CompletableFuture<PgResult>) (CompletableFuture<?>) op.future;
        return f;
    }

    /** Runs a string of one or more statements (simple protocol, no parameters) and returns one result per statement. */
    public CompletableFuture<List<PgResult>> execute(String sql) {
        return execute(config.queryTimeout(), sql);
    }

    /** As {@link #execute(String)} with its own time limit; see {@link #query(java.time.Duration, String, Object...)}. */
    public CompletableFuture<List<PgResult>> execute(java.time.Duration timeout, String sql) {
        if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        if (sql == null || sql.indexOf('\0') >= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters"));
        }
        Op op = new Op(true, sql, null, null);
        op.timeoutNanos = timeout == null ? 0 : timeout.toNanos();
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PgResult>> f = (CompletableFuture<List<PgResult>>) (CompletableFuture<?>) op.future;
        return f;
    }

    /**
     * For {@link PgPool#transaction}: the next query sent on this connection starts a transaction, by carrying the {@code BEGIN} in the same
     * batch (no extra round trip). If that {@code BEGIN} fails the rest of the batch is skipped by the server, so the query does not run outside
     * a transaction. Completes when the loop has taken note.
     */
    CompletableFuture<Void> armBegin() {
        return armBegin("BEGIN");
    }

    /** As {@link #armBegin()} with the statement that begins the transaction ({@code BEGIN ISOLATION LEVEL SERIALIZABLE}...). */
    CompletableFuture<Void> armBegin(String beginSql) {
        Op op = new Op(true, beginSql, null, null);
        op.control = MARK;
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> f = (CompletableFuture<Void>) (CompletableFuture<?>) op.future;
        return f;
    }

    /** Ends the transaction {@link #armBegin} began: {@code COMMIT} or {@code ROLLBACK}, or nothing at all if no query was sent since, so no {@code BEGIN} went out. */
    CompletableFuture<List<PgResult>> endTransaction(boolean commit) {
        Op op = new Op(true, commit ? "COMMIT" : "ROLLBACK", null, null);
        op.control = END;
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PgResult>> f = (CompletableFuture<List<PgResult>>) (CompletableFuture<?>) op.future;
        return f;
    }


    // ---- LISTEN ----

    /**
     * {@code LISTEN} on a channel, and remember it: with {@link PgConfig#reconnect()} the client listens again after a reconnect, which {@code execute("LISTEN x")} would not
     * survive (the new session has no listeners). Notifications go to {@link PgListener#onNotification}; those sent while the connection was down are lost.
     */
    public CompletableFuture<Void> listen(String channel) {
        try {
            return execute("LISTEN " + quoteIdentifier(channel)).thenAccept(r -> listening.add(channel));
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> unlisten(String channel) {
        try {
            return execute("UNLISTEN " + quoteIdentifier(channel)).thenAccept(r -> listening.remove(channel));
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static String quoteIdentifier(String name) {
        if (name == null || name.isEmpty() || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("a channel name must be non-empty and without NUL characters");
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    // ---- reading in pieces, and batches ----

    /**
     * Runs a query and returns its rows in pieces of {@code fetchSize} (a cursor over the extended protocol): memory stays bounded however large the result, unlike
     * {@link #query} whose result is held whole (and refused past {@link PgConfig#maxResultRows()}). Completes when the columns are known. See {@link PgRowStream}: the
     * connection belongs to the stream until it ends, and a time limit does not apply to it.
     * <pre>{@code
     * db.stream(1000, "SELECT id, payload FROM events WHERE day = $1", day).thenCompose(rows -> rows.forEach(row -> handle(row)));
     * }</pre>
     */
    public CompletableFuture<PgRowStream> stream(int fetchSize, String sql, Object... params) {
        if (fetchSize < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("fetchSize must be at least 1"));
        }
        if (sql == null || sql.indexOf('\0') >= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters"));
        }
        int n = params == null ? 0 : params.length;
        int[] oids = new int[n];
        byte[][] values = new byte[n][];
        Object[] kept = new Object[n];
        try {
            encodeParams(params, kept, oids, values);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        Op op = new Op(false, sql, oids, values);
        op.params = kept;
        op.control = STREAM;
        op.fetchSize = fetchSize;
        op.streamReady = new CompletableFuture<>();
        op.ready = new java.util.ArrayDeque<>(2);
        submit(op);
        return op.streamReady;
    }

    /**
     * Runs one statement once for each set of parameters and completes with the row count of each, in a single round trip and with a single {@code Sync}: the items run in
     * one implicit transaction, so <b>either all take effect or none</b> (inside a transaction of your own, a failing item aborts it). A failure says which item
     * failed ({@code batch item 3: ...}). Rows a statement returns are dropped. For very many items send several batches, a few thousand each.
     */
    public CompletableFuture<long[]> executeBatch(String sql, List<Object[]> parameterSets) {
        return executeBatch(config.queryTimeout(), sql, parameterSets);
    }

    /** As {@link #executeBatch(String, List)} with its own time limit for the whole batch; see {@link #query(java.time.Duration, String, Object...)}. */
    public CompletableFuture<long[]> executeBatch(java.time.Duration timeout, String sql, List<Object[]> parameterSets) {
        if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("timeout must be positive"));
        }
        if (sql == null || sql.indexOf('\0') >= 0 || parameterSets == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters, parameterSets non-null"));
        }
        if (parameterSets.isEmpty()) {
            return CompletableFuture.completedFuture(new long[0]);
        }
        int arity = parameterSets.get(0) == null ? 0 : parameterSets.get(0).length;
        if (arity > 32767) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("at most 32767 parameters"));
        }
        int[] oids = new int[arity];
        List<byte[][]> sets = new ArrayList<>(parameterSets.size());
        List<Object[]> keptSets = new ArrayList<>(parameterSets.size());
        try {
            for (Object[] params : parameterSets) {
                int n = params == null ? 0 : params.length;
                if (n != arity) {
                    throw new IllegalArgumentException("every set needs " + arity + " parameters, one has " + n);
                }
                byte[][] values = new byte[n][];
                Object[] kept = new Object[n];
                encodeParams(params, kept, sets.isEmpty() ? oids : new int[n], values);   // the statement is prepared with the types of the first set
                sets.add(values);
                keptSets.add(kept);
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        Op op = new Op(false, sql, oids, null);
        op.control = BATCH;
        op.setParams = keptSets;
        op.sets = sets;
        op.counts = new long[sets.size()];
        op.timeoutNanos = timeout == null ? 0 : timeout.toNanos();
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<long[]> f = (CompletableFuture<long[]>) (CompletableFuture<?>) op.future;
        return f;
    }

    /**
     * Prepares the parameters of a query on the caller's thread: a copy of each that the caller could change ({@code kept}), its type ({@code oids}) and its text. The text of a list
     * or array is the dear part and is not built here ({@link PgTypes#LAZY}): once the server has described the statement most of them go in binary and never need it.
     */
    private static void encodeParams(Object[] params, Object[] kept, int[] oids, byte[][] values) {
        for (int i = 0; i < values.length; i++) {
            Object p = params[i];
            kept[i] = PgTypes.snapshot(p);
            if (PgTypes.isArrayValue(p)) {
                oids[i] = PgTypes.arrayOidOf(kept[i]);
                values[i] = PgTypes.LAZY;
            } else {
                PgTypes.Param e = PgTypes.encode(p);
                oids[i] = e.oid();
                values[i] = e.text();
            }
        }
    }

    /** Builds the text of the parameters that were left for later, in place; every Bind that sends text calls it first. */
    private static byte[][] materialize(byte[][] values, Object[] params) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == PgTypes.LAZY) {
                values[i] = PgTypes.encode(params[i]).text();
            }
        }
        return values;
    }

    /** Loop thread (through {@link PgRowStream}). */
    void streamNext(Object o, CompletableFuture<List<PgRow>> f) {
        Op op = (Op) o;
        onLoop(() -> {
            if (op.waiter != null) {
                f.completeExceptionally(new IllegalStateException("the previous piece has not arrived yet"));
            } else if (!op.ready.isEmpty()) {
                f.complete(op.ready.poll());
            } else if (op.error != null) {
                f.completeExceptionally(op.error);
            } else if (op.streamDone || op.streamClosed) {
                f.complete(List.of());
            } else {
                op.waiter = f;
            }
            requestMore(op);             // the caller has taken a piece: the server can be working on the next while it is read
        });
    }

    CompletableFuture<Void> streamClose(Object o) {
        Op op = (Op) o;
        onLoop(() -> {
            if (op.streamClosed) {
                return;
            }
            op.streamClosed = true;
            op.ready.clear();
            if (!op.streamDone && state == State.ONLINE && gate == op) {
                sendSyncIfNeeded(op);      // a Sync destroys the portal; what the server already sent is read and dropped
            }
            CompletableFuture<List<PgRow>> w = op.waiter;
            if (w != null) {
                op.waiter = null;
                w.complete(List.of());
            }
        });
        return op.future.handle((v, e) -> null);
    }

    /** Loop thread: asks the server for the next piece unless one is on its way, the stream is over, or the caller has not taken the one that came. */
    private void requestMore(Op op) {
        if (op.streamDone || op.requested || op.streamClosed || op.error != null || op.syncSent || !op.ready.isEmpty() || gate != op || ctx == null
                || state != State.ONLINE) {
            return;
        }
        Buf b = new Buf(48);
        Wire.execute(b, "", op.fetchSize);
        Wire.flush(b);
        op.requested = true;
        send(b.toByteArray());
    }

    private void sendSyncIfNeeded(Op op) {
        if (op.syncSent || ctx == null || state != State.ONLINE) {
            return;
        }
        op.syncSent = true;
        Buf b = new Buf(8);
        Wire.sync(b);
        send(b.toByteArray());
    }

    /** A piece is complete: the portal was suspended ({@code complete} false) or the query ended. */
    private void streamBatchDone(Op op, boolean complete) {
        List<PgRow> rows = op.rows;
        op.rows = null;
        op.requested = false;
        if (op.streamClosed) {
            if (complete) {
                op.streamDone = true;
            }
            return;
        }
        if (rows != null && !rows.isEmpty()) {
            op.streamRows += rows.size();
            op.ready.add(rows);
        }
        if (complete) {
            op.streamDone = true;
            sendSyncIfNeeded(op);
        }
        CompletableFuture<List<PgRow>> w = op.waiter;
        if (w != null) {
            if (!op.ready.isEmpty()) {
                op.waiter = null;
                w.complete(op.ready.poll());
            } else if (op.streamDone) {
                op.waiter = null;
                w.complete(List.of());
            }
        }
        requestMore(op);
    }

    /** The columns are known: the caller of {@link #stream} can have its stream. */
    private void streamReadyCheck(Op op) {
        if (op.streamReady != null && !op.streamReady.isDone()) {
            PgColumn[] cols = op.cols != null ? op.cols : op.stmt != null ? op.stmt.cols : null;
            if (cols != null) {
                op.streamReady.complete(new PgRowStream(this, op, cols));
            }
        }
    }

    /** The server answered a stream with an error: it skips everything up to a Sync, so send one. */
    private void streamError(Op op) {
        op.requested = false;
        sendSyncIfNeeded(op);
        if (op.streamReady != null && !op.streamReady.isDone()) {
            op.streamReady.completeExceptionally(op.error);
        }
        CompletableFuture<List<PgRow>> w = op.waiter;
        if (w != null && op.ready.isEmpty()) {
            op.waiter = null;
            w.completeExceptionally(op.error);
        }
    }

    private static void addCount(Op op, long n) {
        if (op.nCounts < op.counts.length) {
            op.counts[op.nCounts++] = n;
        }
    }

    // ---- COPY ----

    /**
     * Starts {@code COPY ... FROM STDIN} (for example {@code COPY t (a, b) FROM STDIN WITH (FORMAT csv)}); completes when the server is ready for the data. See
     * {@link PgCopyIn}: the connection belongs to the copy until it ends.
     */
    public CompletableFuture<PgCopyIn> copyIn(String sql) {
        if (sql == null || sql.indexOf('\0') >= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters"));
        }
        Op op = new Op(true, sql, null, null);
        op.control = COPY_IN;
        op.copyReady = new CompletableFuture<>();
        submit(op);
        return op.copyReady;
    }

    /** {@code COPY ... FROM STDIN} with the data given in pieces; completes with the number of rows. The {@code chunks} are read on the event loop thread: they must not block. */
    public CompletableFuture<Long> copyIn(String sql, Iterable<byte[]> chunks) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        copyIn(sql).whenComplete((h, err) -> {
            if (err != null) {
                result.completeExceptionally(err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err);
                return;
            }
            pump(h, chunks.iterator(), result);
        });
        return result;
    }

    /** {@code COPY ... FROM STDIN} with all the data in memory. */
    public CompletableFuture<Long> copyIn(String sql, byte[] data) {
        return copyIn(sql, java.util.List.of(data));
    }

    private static void pump(PgCopyIn h, java.util.Iterator<byte[]> it, CompletableFuture<Long> result) {
        try {
            while (it.hasNext()) {
                CompletableFuture<Void> w = h.write(it.next());
                if (!w.isDone()) {
                    w.whenComplete((v, e) -> {
                        if (e != null) {
                            h.finish().whenComplete((n, e2) -> result.completeExceptionally(e2 != null ? e2 : e));
                        } else {
                            pump(h, it, result);
                        }
                    });
                    return;
                }
                if (w.isCompletedExceptionally()) {      // the server refused the data already: finishing gives its error
                    h.finish().whenComplete((n, e2) -> result.completeExceptionally(e2 != null ? e2 : new PgException("the copy was refused", false)));
                    return;
                }
            }
        } catch (RuntimeException e) {
            h.abort("the data source failed: " + e).whenComplete((n, e2) -> result.completeExceptionally(e));
            return;
        }
        h.finish().whenComplete((n, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
            } else {
                result.complete(n);
            }
        });
    }

    /**
     * Runs {@code COPY ... TO STDOUT} and hands each piece the server sends (usually one row in the text and CSV formats) to {@code sink}, on the event loop thread: it must
     * not block. There is no flow control on the way in: the server sends as fast as the network allows. Completes with the number of rows.
     */
    public CompletableFuture<Long> copyOut(String sql, java.util.function.Consumer<byte[]> sink) {
        if (sql == null || sql.indexOf('\0') >= 0 || sink == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("sql must be non-null and without NUL characters, sink non-null"));
        }
        Op op = new Op(true, sql, null, null);
        op.control = COPY_OUT;
        op.sink = sink;
        submit(op);
        @SuppressWarnings("unchecked")
        CompletableFuture<Long> f = (CompletableFuture<Long>) (CompletableFuture<?>) op.future;
        return f;
    }

    /** {@code COPY ... TO STDOUT} collected in memory. */
    public CompletableFuture<byte[]> copyOut(String sql) {
        java.io.ByteArrayOutputStream all = new java.io.ByteArrayOutputStream();
        return copyOut(sql, all::writeBytes).thenApply(n -> all.toByteArray());
    }

    /** Loop thread (called through {@link PgCopyIn}). */
    void copyWrite(Object o, byte[] chunk, CompletableFuture<Void> f) {
        Op op = (Op) o;
        onLoop(() -> {
            Context c = ctx;
            if (c == null || state != State.ONLINE || gate != op || op.copyEnded) {
                f.completeExceptionally(new PgException("the COPY is not open", false));
                return;
            }
            if (op.error != null) {
                f.completeExceptionally(op.error);
                return;
            }
            Buf b = new Buf(chunk.length + 5);
            Wire.copyData(b, chunk);
            try {
                c.write(ByteBuffer.wrap(b.a, 0, b.n));
            } catch (Exception e) {
                PgException ex = new PgException("write failed: " + e, false, e);
                fail(ex);
                f.completeExceptionally(ex);
                return;
            }
            c.connection().whenWritable().whenComplete((v, e) -> {
                if (e == null) {
                    f.complete(null);
                } else {
                    f.completeExceptionally(new PgException("the connection was lost: " + e, false, e));
                }
            });
        });
    }

    /** Loop thread (through {@link PgCopyIn}): CopyDone, or CopyFail with {@code failure} as the reason. Once. */
    void copyEnd(Object o, String failure) {
        Op op = (Op) o;
        onLoop(() -> {
            Context c = ctx;
            if (c == null || state != State.ONLINE || gate != op || op.copyEnded) {
                return;               // the result future already says how it ended
            }
            op.copyEnded = true;
            Buf b = new Buf(32);
            if (failure == null) {
                Wire.copyDone(b);
            } else {
                Wire.copyFail(b, failure);
            }
            try {
                c.write(ByteBuffer.wrap(b.a, 0, b.n));
            } catch (Exception e) {
                fail(new PgException("write failed: " + e, false, e));
            }
        });
    }

    /** Queries sent or queued and not yet answered (any thread; what {@link PgPool} balances on). */
    /**
     * This connection behind the {@link io.hydrangea.sql.SqlClient} interface shared with the MySQL client: {@code ?} placeholders (turned into {@code $n};
     * {@code ??} for a literal {@code ?}), the common result and row types. Everything PostgreSQL-only stays on this class.
     */
    public io.hydrangea.sql.SqlClient asSql() {
        return new PgSql.Client(this);
    }

    /** True when called on the event loop this connection belongs to: a query made here is written at once, with no hand-off to another thread. */
    public boolean onOwnLoop() {
        return home.inEventLoop();
    }

    public int inflight() {
        return inflight.get();
    }

    /** True from login until the connection ends. */
    public boolean isOpen() {
        return online;
    }

    /** The server's transaction state as of its last answer: 'I' idle, 'T' in a transaction, 'E' in a failed transaction. */
    public char transactionStatus() {
        return txStatus;
    }

    public int backendPid() {
        return backendPid;
    }

    /** A server setting reported at login or when it changed ({@code server_version}, {@code TimeZone}...), or null. */
    public String serverParameter(String name) {
        return serverParams.get(name);
    }

    /** Closes the connection; queries waiting for an answer fail. */
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
            State was = state;
            PgException why = new PgException("the client was closed", false);
            failPending(why);
            failInbox(true);
            if (!ready.isDone()) {
                ready.completeExceptionally(why);
            }
            Context c = ctx;
            if (c != null && was == State.ONLINE) {
                state = State.CLOSED;
                online = false;
                try {
                    Buf b = new Buf(8);
                    Wire.terminate(b);
                    c.write(ByteBuffer.wrap(b.toByteArray()));
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

    private void submit(Op op) {
        inflight.incrementAndGet();
        if (home.inEventLoop() && inbox.isEmpty()) {     // not past what other threads queued before: a transaction relies on the order
            batchClock = mLatency.now();
            accept(op);
            flush();
            reportPending();
            return;
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
    }

    /** On the loop: takes what the threads have put in the inbox and writes it together. */
    private void drain() {
        for (;;) {
            batchClock = mLatency.now();   // one clock read for the whole batch
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
        int n = pending.size();
        if (n != pendingReported) {
            mPending.add(n - pendingReported);
            pendingReported = n;
        }
    }

    private void failInbox(boolean retriable) {
        PgException e = new PgException(retriable ? "the client was closed" : "the event loop is shut down", retriable);
        for (Op op; (op = inbox.poll()) != null; ) {
            reject(op, e);
        }
    }

    private void reject(Op op, PgException e) {
        if (op.abandoned) {
            return;                       // it timed out earlier: the caller has been told and it was counted then
        }
        cancelTimer(op);
        inflight.decrementAndGet();
        mFailed.increment();
        if (op.copyReady != null) {
            op.copyReady.completeExceptionally(e);
        }
        if (op.control == STREAM) {
            op.error = e;
            if (op.streamReady != null) {
                op.streamReady.completeExceptionally(e);
            }
            if (op.waiter != null) {
                CompletableFuture<List<PgRow>> w = op.waiter;
                op.waiter = null;
                w.completeExceptionally(e);
            }
        }
        op.future.completeExceptionally(e);
    }

    private void accept(Op op) {
        if (state != State.ONLINE) {
            reject(op, new PgException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            return;
        }
        if (lostInTx && op.control == 0 && !op.internal) {       // the connection came back, but the transaction this client was in did not: do not run on as if it had
            lostInTx = false;
            reject(op, new PgException("the connection was lost inside a transaction and has been re-established: the transaction is gone, start it again", false));
            return;
        }
        startTimer(op);
        if (gate != null) {                      // a COPY FROM STDIN owns the connection until it ends
            deferred.add(op);
            return;
        }
        if (op.control == MARK) {
            beginArmed = true;
            beginSql = op.sql;
            inflight.decrementAndGet();
            op.future.complete(null);
            return;
        }
        if (op.control == END && beginArmed) {       // no query came after the mark: nothing was begun, nothing to end
            beginArmed = false;
            inflight.decrementAndGet();
            op.future.complete(List.of());
            return;
        }
        if (pending.size() + deferred.size() >= config.maxPending()) {
            reject(op, new PgException("too many queries waiting for an answer (maxPending " + config.maxPending() + ")", true));
            return;
        }
        op.start = batchClock;
        boolean copy = op.control == COPY_IN || op.control == COPY_OUT || op.control == STREAM;
        if (copy && beginArmed) {                 // a COPY cannot carry the BEGIN: send it as a query of its own just before
            beginArmed = false;
            Op begin = new Op(true, beginSql, null, null);
            begin.carriedBegin = true;
            inflight.incrementAndGet();
            begin.start = batchClock;
            begin.results = new ArrayList<>(1);
            Wire.query(out, beginSql);
            pending.add(begin);
        }
        boolean lead = beginArmed && (op.control == 0 || op.control == BATCH);
        if (lead) {
            beginArmed = false;
            op.skipComplete = 1;
            op.carriedBegin = true;
        }
        try {
            if (op.control == STREAM || op.control == BATCH) {
                encodeStreamOrBatch(op, lead);
            } else if (op.simple) {
                Wire.query(out, lead ? beginSql + ";" + op.sql : op.sql);
                op.results = new ArrayList<>(1);
            } else if (cache == null) {
                if (lead) {
                    leadBegin(op);
                }
                Wire.parse(out, "", op.sql, op.oids);
                Wire.bind(out, "", "", materialize(op.values, op.params));
                Wire.describe(out, 'P', "");
                Wire.execute(out, "");
                Wire.sync(out);
            } else {
                StmtKey key = new StmtKey(op.sql, op.oids);
                Stmt st = cache.get(key);
                boolean fresh = st == null;
                if (fresh) {
                    st = new Stmt("S" + (++stmtSeq));
                    cache.put(key, st);      // may push the oldest statement out: its name lands in toClose
                }
                if (lead) {
                    leadBegin(op);
                }
                for (String name : toClose) {
                    Wire.close(out, 'S', name);
                }
                toClose.clear();
                if (fresh) {
                    Wire.parse(out, st.name, op.sql, op.oids);
                    Wire.describe(out, 'S', st.name);
                }
                bind(op, st, op.values, op.params, true);
                Wire.execute(out, "");
                Wire.sync(out);
                op.stmt = st;
                op.key = key;
                op.newStmt = fresh;
            }
        } catch (RuntimeException e) {    // a NUL in the SQL, say: nothing of this query was kept in the buffer we care about
            reject(op, new PgException("cannot encode the query: " + e.getMessage(), false, e));
            fail(new PgException("cannot encode the query: " + e.getMessage(), false, e));
            return;
        }
        pending.add(op);
        if (op.control == COPY_IN || op.control == STREAM) {
            gate = op;
        }
    }

    /**
     * Writes the Bind of {@code st}. Once the server has described the statement, parameters whose Java values can be sent in binary for the type the server gave them are
     * (a bytea goes as its bytes, not hex; an {@code int[]} as an array of 4-byte integers...), and the results are asked for in binary for the columns where that changes nothing
     * the caller can see. The first use of a statement has neither answer yet and is all text.
     */
    private void bind(Op op, Stmt st, byte[][] values, Object[] params, boolean forRows) {
        byte[] resultFormats = null;
        if (config.binaryTransfer()) {
            if (st.cols != null && !st.formatsDone) {
                byte[] f = new byte[st.cols.length];
                boolean any = false;
                for (int i = 0; i < f.length; i++) {
                    if (PgTypes.binaryResultOk(st.cols[i].typeOid())) {
                        f[i] = 1;
                        any = true;
                    }
                }
                if (any) {
                    st.resultFormats = f;
                    boolean[] flags = new boolean[f.length];
                    for (int i = 0; i < f.length; i++) {
                        flags[i] = f[i] == 1;
                    }
                    st.binaryCols = flags;
                }
                st.formatsDone = true;
            }
            if (forRows) {
                resultFormats = st.resultFormats;
                op.formats = st.binaryCols;
                if (resultFormats != null) {
                    int n = 0;
                    for (byte f : resultFormats) {
                        n += f;
                    }
                    mBinaryColumns.add(n);
                }
            }
        }
        boolean[] binary = null;
        if (config.binaryTransfer() && st.paramOids != null && params != null && params.length == st.paramOids.length && values.length == params.length) {
            byte[][] sent = null;
            for (int i = 0; i < params.length; i++) {
                byte[] b = PgTypes.encodeBinary(params[i], st.paramOids[i]);
                if (b != null) {
                    if (sent == null) {
                        sent = values.clone();
                        binary = new boolean[values.length];
                    }
                    sent[i] = b;
                    binary[i] = true;
                }
            }
            if (sent != null) {
                values = sent;
                int n = 0;
                for (boolean x : binary) {
                    if (x) {
                        n++;
                    }
                }
                mBinaryParams.add(n);
            }
        }
        if (params != null) {
            materialize(values, params);        // the parameters that did not go in binary need their text after all
        }
        Wire.bind(out, "", st.name, values, binary, resultFormats);
    }

    /** A stream: Parse/Bind/Execute(fetchSize) and a Flush, no Sync (that ends the portal); a batch: one Bind and Execute per parameter set and a single Sync. */
    private void encodeStreamOrBatch(Op op, boolean lead) {
        boolean stream = op.control == STREAM;
        if (cache == null) {
            if (lead) {
                leadBegin(op);
            }
            Wire.parse(out, "", op.sql, op.oids);
            if (stream) {
                Wire.bind(out, "", "", materialize(op.values, op.params));
                Wire.describe(out, 'P', "");
                Wire.execute(out, "", op.fetchSize);
                Wire.flush(out);
                op.requested = true;
            } else {
                for (int i = 0; i < op.sets.size(); i++) {
                    Wire.bind(out, "", "", materialize(op.sets.get(i), op.setParams.get(i)));
                    Wire.execute(out, "");
                }
                Wire.sync(out);
            }
            return;
        }
        StmtKey key = new StmtKey(op.sql, op.oids);
        Stmt st = cache.get(key);
        boolean fresh = st == null;
        if (fresh) {
            st = new Stmt("S" + (++stmtSeq));
            cache.put(key, st);
        }
        if (lead) {
            leadBegin(op);
        }
        for (String name : toClose) {
            Wire.close(out, 'S', name);
        }
        toClose.clear();
        if (fresh) {
            Wire.parse(out, st.name, op.sql, op.oids);
            Wire.describe(out, 'S', st.name);
        }
        if (stream) {
            bind(op, st, op.values, op.params, true);
            Wire.execute(out, "", op.fetchSize);
            Wire.flush(out);
            op.requested = true;
        } else {
            for (int i = 0; i < op.sets.size(); i++) {
                bind(op, st, op.sets.get(i), op.setParams == null ? null : op.setParams.get(i), false);
                Wire.execute(out, "");
            }
            Wire.sync(out);
        }
        op.stmt = st;
        op.key = key;
        op.newStmt = fresh;
    }

    // ---- time limits and cancel ----

    private void startTimer(Op op) {
        if (op.timeoutNanos > 0 && op.timer == null && (op.control == 0 || op.control == BATCH)) {      // not a COPY or a stream, which take as long as their data
            op.timer = home.schedule(() -> onTimeout(op), op.timeoutNanos, TimeUnit.NANOSECONDS);
        }
    }

    private static void cancelTimer(Op op) {
        io.hydrangea.Scheduled t = op.timer;
        if (t != null) {
            op.timer = null;
            t.cancel();
        }
    }

    /** Loop thread: the time limit of {@code op} ran out. */
    private void onTimeout(Op op) {
        op.timer = null;
        if (op.future.isDone() || op.abandoned || state != State.ONLINE) {
            return;
        }
        PgException e = PgException.timeout("the query did not finish within " + op.timeoutNanos / 1_000_000 + " ms");
        if (deferred.remove(op)) {                 // never sent: it will not run
            inflight.decrementAndGet();
            mTimeout.increment();
            op.future.completeExceptionally(e);
            return;
        }
        if (!pending.contains(op)) {
            return;
        }
        inflight.decrementAndGet();
        mTimeout.increment();
        op.abandoned = true;                       // sent: the caller gets its answer now, the server's is read and dropped
        mLatency.endAt(op.start, mLatency.now());
        op.future.completeExceptionally(e);
        if (pending.peek() == op) {                // the one the server is working on: stop it
            sendCancel(op);
        }
    }

    /** After a query was answered: if the next one in line already timed out, the server is about to work on it, so that is the moment to cancel it. */
    private void cancelNextIfAbandoned() {
        Op next = pending.peek();
        if (next != null && next.abandoned && !next.cancelSent) {
            sendCancel(next);
        }
    }

    /**
     * Asks the server to cancel what this connection's backend is running: a short-lived connection of its own sends the pid and secret key of
     * {@code BackendKeyData}. Best effort: nothing says whether it hit anything.
     */
    private void sendCancel(Op op) {
        if (op.cancelSent) {
            return;
        }
        op.cancelSent = true;
        InetSocketAddress addr = remoteAddr;
        int pid = backendPid;
        int secret = backendSecret;
        if (addr == null || pid == 0) {
            return;
        }
        lastCancelNanos = System.nanoTime();
        mCancels.increment();
        Options options = Options.defaults().withIdleTimeout(java.time.Duration.ofSeconds(5));
        if (config.connectTimeout() != null) {
            options = options.withConnectTimeout(config.connectTimeout());
        }
        Pipeline pipeline = config.sslContext() == null ? Pipeline.of(new CancelStage(pid, secret, x -> { }))
                : Pipeline.of(new SslRequestStage(e -> { }, c -> { }),
                        TlsStage.client(config.sslContext(), config.host(), config.port()), new CancelStage(pid, secret, x -> { }));
        try {
            Client.connect(home, addr, options, pipeline).whenComplete((c, err) -> {
                if (err != null) {
                    LOG.log(Logger.Level.DEBUG, "cancel request to " + addr + " failed: " + err);
                }
            });
        } catch (RuntimeException e) {
            LOG.log(Logger.Level.DEBUG, "cancel request failed: " + e);
        }
    }

    private static boolean isStalePlan(PgException e) {
        return "0A000".equals(e.sqlState()) && e.getMessage().contains("cached plan must not change result type");
    }

    /** After a COPY FROM STDIN: the queries that waited behind it, in their order. */
    private void releaseDeferred() {
        if (deferred.isEmpty()) {
            return;
        }
        List<Op> waiting = new ArrayList<>(deferred);
        deferred.clear();
        batchClock = mLatency.now();
        for (Op o : waiting) {
            accept(o);                            // may close the gate again (another COPY): the rest wait again, still in order
        }
        flush();
    }

    /** The BEGIN that opens a transaction, ahead of the query's own messages and before its Sync. */
    private void leadBegin(Op op) {
        Wire.parse(out, "", beginSql, NO_OIDS);
        Wire.bind(out, "", "", NO_VALUES);
        Wire.execute(out, "");
        op.skipParse = 1;
    }

    private static final int[] NO_OIDS = new int[0];
    private static final byte[][] NO_VALUES = new byte[0][];

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
            fail(new PgException("write failed: " + e, false, e));
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
        // resolving the name can block: not on the loop
        Thread.startVirtualThread(() -> {
            InetSocketAddress addr;
            try {
                addr = new InetSocketAddress(config.host(), config.port());
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(config.host());
                }
            } catch (Exception e) {
                onLoop(() -> connectFailed(g, new PgException("cannot resolve " + config.host() + ": " + e, true, e)));
                return;
            }
            remoteAddr = addr;
            Options options = Options.defaults().withKeepAlive(true).withTcpNoDelay(true);
            if (config.connectTimeout() != null) {
                options = options.withConnectTimeout(config.connectTimeout());
            }
            Pipeline pipeline = config.sslContext() == null ? Pipeline.of(new PgStage(g))
                    : Pipeline.of(new SslRequestStage(e -> onLoop(() -> connectFailed(g, e)),
                            cause -> onLoop(() -> connectFailed(g, new PgException("the connection closed before the TLS handshake finished"
                                    + (cause != null ? ": " + cause : ""), true, cause)))),
                            TlsStage.client(config.sslContext(), config.host(), config.port()), new PgStage(g));
            Client.connect(home, addr, options, pipeline).whenComplete((conn, err) -> {
                if (err != null) {
                    onLoop(() -> connectFailed(g, new PgException("cannot connect to " + config.host() + ":" + config.port() + ": " + err, true, err)));
                } else {
                    onLoop(() -> {
                        if (g == gen) {
                            rawConnection = conn;
                            startLoginTimer();
                        } else {
                            conn.close();
                        }
                    });
                    // a close before the stage is active (TLS refused, no login answer) is not reported to the stage
                    conn.closeFuture().thenRun(() -> onLoop(() -> connectFailed(g, new PgException("the connection closed before login", true))));
                }
            });
        });
    }

    /** The limit for everything between the TCP connection and the end of the login (the TLS request, its handshake, the authentication). */
    private void startLoginTimer() {
        if (connectTimer != null || config.connectTimeout() == null) {
            return;
        }
        connectTimer = home.schedule(() -> {
            if (state == State.CONNECTING || state == State.AUTH) {
                fail(new PgException("no login within " + config.connectTimeout().toMillis() + " ms", true));
            }
        }, config.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }

    private void connectFailed(int g, PgException e) {
        if (g != gen || state != State.CONNECTING) {
            return;
        }
        if (connectTimer != null) {
            connectTimer.cancel();
            connectTimer = null;
        }
        if (lossCause instanceof PgException why) {
            e = why;                      // the reason we closed the connection (a timeout), not the generic "closed before login"
        }
        lossCause = null;
        if (loggedIn && !closing && config.reconnect() != null) {
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
        Duration delay = b.delayFor(reconnectAttempt);
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
        scram = null;
        ctx = null;
        rawConnection = null;
        beginArmed = false;
        beginSql = "BEGIN";
        gate = null;
        txStatus = 'I';
        if (cache != null) {
            cache.clear();             // the statements lived in the old session
        }
        toClose.clear();
        out = new Buf(512);
        startConnecting();
    }

    private final class PgStage implements Stage {
        private final int g;

        PgStage(int g) {
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
            try {
                c.write(ByteBuffer.wrap(Wire.startup(config.user(), config.database(), config.applicationName())));
            } catch (Exception e) {
                fail(new PgException("write failed: " + e, true, e));
            }
        }

        @Override
        public void onRead(Context c, Object msg) {
            if (g != gen) {
                return;
            }
            try {
                onData((ByteBuffer) msg);
            } catch (PgException e) {
                fail(e);
            } catch (RuntimeException e) {
                LOG.log(Logger.Level.WARNING, "PostgreSQL message handling failed", e);
                fail(new PgException("internal error: " + e, false, e));
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

    /** Closes the connection because of {@code cause}; {@link #connectionEnded} does the rest. */
    private void fail(Throwable cause) {
        if (lossCause == null) {
            lossCause = cause;
        }
        Context c = ctx;
        if (c != null) {
            c.connection().close();
        } else if (rawConnection != null) {
            rawConnection.close();           // not active yet (waiting for the TLS answer): its close reaches connectFailed
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
        PgException e = why instanceof PgException p ? p : new PgException("the connection was lost: " + why, false, why);
        failPending(e);
        failInbox(true);
        if (was == State.ONLINE || (was == State.CLOSED && loggedIn)) {       // an established connection ended (CLOSED: by our own close, which set the state first)
            if (!closing) {
                mLost.increment();
                lostInTx = txStatus != 'I' || beginArmed;
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
            ready.completeExceptionally(why instanceof PgException p ? p : new PgException("cannot connect to " + config.host() + ":" + config.port() + ": " + why, true, why));
        }
        closed.complete(null);
    }

    private void failPending(PgException e) {
        gate = null;
        for (Op op; (op = pending.poll()) != null; ) {
            reject(op, e);
        }
        for (Op op; (op = deferred.poll()) != null; ) {
            reject(op, e);
        }
        reportPending();
    }

    private void safely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "listener failed", t);
        }
    }

    // ---- messages ----

    private void onData(ByteBuffer data) {
        replyClock = mLatency.now();   // one clock read for all the answers in this read
        ByteBuffer in;
        if (acc.remaining() == 0) {
            in = data;
        } else {
            acc.append(data);
            in = acc.buffer();
        }
        for (;;) {
            if (in.remaining() < 5) {
                break;
            }
            int p = in.position();
            byte type = in.get(p);
            int len = in.getInt(p + 1);
            if (len < 4 || len - 4 > config.maxMessageBytes()) {
                throw new PgException("protocol error: message length " + len, false);
            }
            if (in.remaining() < 1 + len) {
                break;
            }
            ByteBuffer payload = in.slice(p + 5, len - 4);
            in.position(p + 1 + len);
            try {
                handle(type, payload);
            } catch (java.nio.BufferUnderflowException | IndexOutOfBoundsException | NegativeArraySizeException | IllegalArgumentException | ArithmeticException e) {
                // a body shorter than its own counts say, a negative length, a number out of range: the server is sending rubbish, which is a protocol error, not our bug
                throw new PgException("protocol error: malformed '" + printable(type) + "' message", false, e);
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

    private static String printable(byte type) {
        return type >= 32 && type < 127 ? String.valueOf((char) type) : "0x" + Integer.toHexString(type & 0xff);
    }

    private void handle(byte type, ByteBuffer m) {
        switch (type) {
            case 'R' -> onAuth(m);
            case 'S' -> {
                String name = cstr(m);
                serverParams.put(name, cstr(m));
            }
            case 'K' -> {
                backendPid = m.getInt();
                backendSecret = m.getInt();
            }
            case 'Z' -> onReady(m.get());
            case 'N' -> {
                Map<Character, String> f = fields(m);
                safely(() -> listener.onNotice(f));
            }
            case 'A' -> {
                int pid = m.getInt();
                String channel = cstr(m);
                String payload = cstr(m);
                safely(() -> listener.onNotification(pid, channel, payload));
            }
            case 'v' -> { }
            case 'E' -> onError(fields(m));
            default -> {
                if (state != State.ONLINE) {
                    throw new PgException("protocol error: message '" + (char) type + "' during login", false);
                }
                onResultMessage(type, m);
            }
        }
    }

    private void onAuth(ByteBuffer m) {
        if (state != State.AUTH) {
            throw new PgException("protocol error: authentication message while " + state, false);
        }
        int code = m.getInt();
        try {
            switch (code) {
                case 0 -> { }
                case 3 -> {                   // AuthenticationCleartextPassword
                    if (config.sslContext() == null && !config.allowClearTextPassword()) {
                        throw new PgException("the server asks for the password in clear text on a connection without TLS: refused, a man in the middle could read it"
                                + " (use TLS, or PgConfig.withAllowClearTextPassword(true) when the network is trusted)", false);
                    }
                    send(Wire.password(Wire.cstring(requirePassword())));
                }
                case 5 -> {
                    byte[] salt = new byte[4];
                    m.get(salt);
                    MessageDigest md5 = MessageDigest.getInstance("MD5"); // codeql[java/weak-cryptographic-algorithm, java/potentially-weak-cryptographic-algorithm] AuthenticationMD5Password is defined as MD5 by the PostgreSQL protocol; SCRAM-SHA-256 is used whenever the server offers it
                    md5.update(requirePassword().getBytes(StandardCharsets.UTF_8));
                    md5.update(config.user().getBytes(StandardCharsets.UTF_8));
                    String inner = hex(md5.digest());
                    md5.update(inner.getBytes(StandardCharsets.US_ASCII));
                    md5.update(salt);
                    send(Wire.password(Wire.cstring("md5" + hex(md5.digest()))));
                }
                case 10 -> {
                    boolean scramOffered = false;
                    for (String mech; !(mech = cstr(m)).isEmpty(); ) {
                        scramOffered |= mech.equals("SCRAM-SHA-256");
                    }
                    if (!scramOffered) {
                        throw new PgException("the server offers no SASL mechanism we support (we have SCRAM-SHA-256)", false);
                    }
                    scram = Scram.forPostgres(requirePassword());
                    byte[] first = scram.clientFirst().getBytes(StandardCharsets.UTF_8);
                    Buf b = new Buf(64 + first.length);
                    b.cstr("SCRAM-SHA-256").i32(first.length).raw(first);
                    send(Wire.password(b.toByteArray()));
                }
                case 11 -> send(Wire.password(scram.clientFinal(rest(m)).getBytes(StandardCharsets.UTF_8)));
                case 12 -> scram.verifyServerFinal(rest(m));
                default -> throw new PgException("unsupported authentication method " + code
                        + (code == 2 ? " (Kerberos)" : code == 7 || code == 9 ? " (GSSAPI/SSPI)" : ""), false);
            }
        } catch (java.security.GeneralSecurityException | IllegalStateException e) {
            throw new PgException("login failed: " + e.getMessage(), false, e);
        }
    }

    private String requirePassword() {
        if (config.password() == null) {
            throw new PgException("the server asks for a password and none is configured", false);
        }
        return config.password();
    }

    private void send(byte[] bytes) {
        try {
            ctx.write(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            throw new PgException("write failed: " + e, false, e);
        }
    }

    private void onReady(byte status) {
        txStatus = (char) status;
        if (state == State.AUTH) {
            state = State.ONLINE;
            online = true;
            boolean again = loggedIn;               // a login after the first: this is the connection coming back
            loggedIn = true;
            scram = null;
            reconnectAttempt = 0;
            if (connectTimer != null) {
                connectTimer.cancel();
                connectTimer = null;
            }
            if (again) {
                mReconnects.increment();
                for (String channel : listening) {  // what this client listened to before, listened to again
                    Op listenOp = new Op(true, "LISTEN " + quoteIdentifier(channel), null, null);
                    listenOp.internal = true;
                    inflight.incrementAndGet();
                    accept(listenOp);
                }
                flush();
                safely(listener::onReconnected);
            } else {
                ready.complete(this);
            }
            return;
        }
        Op op = pending.poll();
        if (op == null) {
            throw new PgException("protocol error: ReadyForQuery with no query waiting", false);
        }
        finish(op);
    }

    private void onError(Map<Character, String> f) {
        PgException e = new PgException(f);
        if (state != State.ONLINE) {
            throw e;       // a refused login
        }
        Op op = pending.peek();
        if (op == null) {
            if ("FATAL".equals(f.get('S')) || "PANIC".equals(f.get('S'))) {
                throw e;       // the server ending the session (a shutdown, pg_terminate_backend, idle_session_timeout...): that is the reason the connection is lost
            }
            throw new PgException("protocol error: an error with no query waiting: " + e.getMessage(), false);
        }
        if (!op.abandoned && "57014".equals(e.sqlState()) && lastCancelNanos != 0 && System.nanoTime() - lastCancelNanos < 5_000_000_000L
                && String.valueOf(f.get('M')).contains("user request")) {
            // a cancel we sent for a query that timed out reached this one instead (the other finished first): this one did not complete either
            e = PgException.retriableServerError(f, "the statement was cancelled by the time limit of another query on this connection and can be sent again ("
                    + f.get('M') + ")");
            op.strayCancel = true;
        }
        if (op.control == BATCH && op.error == null) {
            e = PgException.batchFailure(e, op.nCounts);          // the items before it were answered; this is the one that failed
        }
        if (op.error == null) {
            op.error = e;
        }
        if (op.control == STREAM) {
            streamError(op);
        }
        if (op.newStmt && !op.parsed && cache != null && cache.get(op.key) == op.stmt) {
            cache.remove(op.key);    // never prepared: the next call tries again
        }
    }

    private void onResultMessage(byte type, ByteBuffer m) {
        Op op = pending.peek();
        if (op == null) {
            throw new PgException("protocol error: message '" + (char) type + "' with no query waiting", false);
        }
        switch (type) {
            case '1' -> {
                if (op.skipParse > 0) {
                    op.skipParse--;         // the ParseComplete of the BEGIN this query carries
                } else {
                    op.parsed = true;
                }
            }
            case '2' -> {
                if (op.control == STREAM) {
                    streamReadyCheck(op);
                }
            }
            case 's' -> {
                if (op.control == STREAM) {
                    streamBatchDone(op, false);
                }
            }
            case 't' -> {
                if (op.stmt != null && op.newStmt && op.stmt.paramOids == null) {       // the answer to the Describe of the statement this query prepared
                    int n = m.getShort() & 0xffff;
                    int[] oids = new int[n];
                    for (int i = 0; i < n; i++) {
                        oids[i] = m.getInt();
                    }
                    op.stmt.paramOids = oids;
                }
            }
            case '3', 'c' -> { }
            case 'd' -> {
                if (op.control == COPY_OUT && op.sink != null && op.sinkError == null) {
                    byte[] chunk = new byte[m.remaining()];
                    m.get(chunk);
                    try {
                        op.sink.accept(chunk);
                    } catch (Throwable t) {
                        op.sinkError = t;       // the rest is read and dropped so that the protocol stays in step
                    }
                }
            }
            case 'n' -> {
                op.cols = PgResult.NO_COLUMNS;
                if (op.stmt != null && op.stmt.cols == null) {
                    op.stmt.cols = PgResult.NO_COLUMNS;
                }
                streamReadyCheck(op);
            }
            case 'T' -> {
                PgColumn[] cols = rowDescription(m);
                op.cols = cols;
                if (op.stmt != null && op.stmt.cols == null) {
                    op.stmt.cols = cols;
                }
                if (op.simple) {
                    op.rows = new ArrayList<>();
                }
                streamReadyCheck(op);
            }
            case 'D' -> {
                if (op.control == BATCH || op.overflow || op.streamClosed || op.abandoned) {
                    break;                 // nobody will look at it
                }
                PgColumn[] cols = op.cols != null ? op.cols : op.stmt != null ? op.stmt.cols : null;
                if (cols == null) {
                    throw new PgException("protocol error: a row without a description", false);
                }
                if (op.control == 0 && (++op.rowCount > config.maxResultRows() || (op.rowBytes += m.limit()) > config.maxResultBytes())) {
                    op.overflow = true;    // the rest of this result is read and dropped; the query fails, the connection does not
                    op.rows = null;
                    if (op.error == null) {
                        op.error = new PgException("the result is larger than the limit (" + config.maxResultRows() + " rows, " + config.maxResultBytes()
                                + " bytes: PgConfig.withResultLimits); read it in pieces with stream()", false);
                    }
                    break;
                }
                int n = m.getShort() & 0xffff;
                byte[][] values = new byte[n][];
                for (int i = 0; i < n; i++) {
                    int len = m.getInt();
                    if (len >= 0) {
                        if (len > m.remaining()) {
                            throw new PgException("protocol error: a column longer than its row", false);
                        }
                        byte[] v = new byte[len];
                        m.get(v);
                        values[i] = v;
                    }
                }
                if (op.rows == null) {
                    op.rows = new ArrayList<>();
                }
                op.rows.add(new PgRow(cols, values, op.formats));
            }
            case 'C' -> {
                String tag = cstr(m);
                if (op.skipComplete > 0) {      // the BEGIN this query carries
                    op.skipComplete--;
                    op.cols = null;
                    op.rows = null;
                } else if (op.control == STREAM) {
                    op.tag = tag;
                    streamBatchDone(op, true);
                } else if (op.control == BATCH) {
                    addCount(op, PgResult.countOf(tag));
                } else if (op.simple) {
                    op.results.add(result(op, tag));
                    op.cols = null;
                    op.rows = null;
                } else {
                    op.tag = tag;
                }
            }
            case 'I' -> {
                if (op.control == BATCH) {
                    addCount(op, 0);
                } else if (op.simple) {
                    op.results.add(new PgResult(PgResult.NO_COLUMNS, List.of(), ""));
                } else {
                    op.tag = "";
                }
            }
            case 'G' -> {
                if (op.control == COPY_IN) {
                    op.copyReady.complete(new PgCopyIn(this, op, copyResult(op)));
                    break;
                }
                // COPY ... FROM STDIN through query()/execute(): nothing to send, so abort it; the server answers with an error
                if (op.error == null) {
                    op.error = new PgException("COPY is not supported by this client", false);
                }
                Buf b = new Buf(32);
                Wire.copyFail(b, "COPY is not supported by this client");
                send(b.toByteArray());
            }
            case 'H' -> {
                if (op.control != COPY_OUT && op.error == null) {
                    op.error = new PgException("COPY is not supported by this client", false);
                }
            }
            case 'W' -> {
                if (op.error == null) {
                    op.error = new PgException("COPY is not supported by this client", false);
                }
            }
            default -> throw new PgException("protocol error: unexpected message '" + (char) type + "'", false);
        }
    }

    private static PgResult result(Op op, String tag) {
        PgColumn[] cols = op.cols != null ? op.cols : op.stmt != null && op.stmt.cols != null ? op.stmt.cols : PgResult.NO_COLUMNS;
        return new PgResult(cols, op.rows != null ? op.rows : List.of(), tag);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Long> copyResult(Op op) {
        return (CompletableFuture<Long>) (CompletableFuture<?>) op.future;
    }

    private void retryEnd(Op op) {
        op.retries++;
        resetForRetry(op);
        accept(op);
        flush();
        reportPending();
    }

    /**
     * Whether a query that failed without effect may be sent again, which puts it behind the queries already queued: only when that cannot move it across the boundary of a
     * transaction. Not in a transaction; not while a BEGIN is waiting for its query or a COPY or a stream owns the connection; and not if any query queued behind it begins or ends a
     * transaction (it would run inside somebody else's, or outside its own). A query that carried a BEGIN itself needs an empty queue behind it: the rest of its transaction is there.
     */
    private boolean canRetry(Op op) {
        if (txStatus != 'I' || beginArmed || gate != null || !deferred.isEmpty()) {
            return false;
        }
        for (Op o : pending) {
            if (op.carriedBegin || o.carriedBegin || o.control == END || o.control == COPY_IN || o.control == STREAM) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sends a query again after an attempt that failed without effect. If it carried the BEGIN of a transaction the BEGIN goes again: it failed too when the error was
     * a cancel that hit the BEGIN itself (the connection is then outside a transaction), and a retry without it would run the statement outside the transaction.
     */
    private void retry(Op op) {
        op.retries++;
        boolean begin = op.carriedBegin;
        resetForRetry(op);
        if (begin) {
            beginArmed = true;
        }
        accept(op);
        flush();
        reportPending();
    }

    /** What a query had collected from a failed attempt, dropped before it is sent again. */
    private static void resetForRetry(Op op) {
        op.error = null;
        op.cols = null;
        op.rows = null;
        op.tag = null;
        op.results = null;
        op.stmt = null;
        op.key = null;
        op.newStmt = false;
        op.parsed = false;
        op.skipComplete = 0;
        op.skipParse = 0;
        op.carriedBegin = false;
        op.formats = null;
        op.nCounts = 0;
        op.rowCount = 0;
        op.rowBytes = 0;
        op.overflow = false;
    }

    private void finish(Op op) {
        if (op.abandoned) {                   // timed out earlier: already answered and counted
            cancelNextIfAbandoned();
            return;
        }
        if (op.error != null && op.retries == 0 && op.control == 0 && isStalePlan(op.error) && cache != null && op.key != null && op.stmt != null) {
            // the table changed under a cached statement ("cached plan must not change result type"): drop the statement, and run the query again if that is safe
            if (cache.get(op.key) == op.stmt) {      // the first of a burst drops the statement; the others find it gone already
                cache.remove(op.key);
                toClose.add(op.stmt.name);
                mReplanned.increment();
            }
            if (!op.newStmt && canRetry(op)) {     // autocommit: the statement failed while being planned and did nothing, so it can run again (after the queries already queued)
                retry(op);        // its time limit keeps running
                return;
            }
        }
        if (op.strayCancel && op.error != null && op.control == END && op.retries < 5) {
            // the COMMIT or ROLLBACK that ends a transaction was hit by a cancel meant for another query: the transaction is still open and the connection must not go back to
            // the pool in that state, so send it again (the transaction is the caller's alone, so there is nothing to be reordered with)
            op.strayCancel = false;
            retryEnd(op);
            return;
        }
        if (op.strayCancel && op.error != null && op.retries == 0 && (op.control == 0 || op.control == BATCH) && canRetry(op)) {
            // the cancel of another query hit this one: it was stopped before it finished and, outside a transaction, left no trace, so run it again (it is not cancelled twice)
            op.strayCancel = false;
            retry(op);
            return;
        }
        cancelTimer(op);
        inflight.decrementAndGet();
        mLatency.endAt(op.start, replyClock);
        if (op.error == null && op.sinkError != null) {
            op.error = new PgException("the copy sink failed: " + op.sinkError, false, op.sinkError);
        }
        if (op.control == COPY_IN && !op.copyReady.isDone()) {      // no CopyInResponse came: an error, or a statement that is not a COPY
            op.copyReady.completeExceptionally(op.error != null ? op.error : new PgException("the statement did not start a COPY FROM STDIN", false));
            if (op.error == null) {
                op.error = new PgException("the statement did not start a COPY FROM STDIN", false);
            }
        }
        if (op.control == STREAM) {
            if (op.streamReady != null && !op.streamReady.isDone()) {
                op.streamReady.completeExceptionally(op.error != null ? op.error : new PgException("the stream ended before its columns were known", false));
            }
            CompletableFuture<List<PgRow>> w = op.waiter;
            if (w != null) {
                op.waiter = null;
                if (op.error != null) {
                    w.completeExceptionally(op.error);
                } else {
                    w.complete(List.of());
                }
            }
            op.streamDone = true;
        }
        if (op.error != null) {
            mError.increment();
            op.future.completeExceptionally(op.error);
        } else if (op.control == STREAM) {
            mOk.increment();
            op.future.complete(op.streamRows);
        } else if (op.control == BATCH) {
            mOk.increment();
            op.future.complete(java.util.Arrays.copyOf(op.counts, op.nCounts));
        } else if (op.control == COPY_IN || op.control == COPY_OUT) {
            mOk.increment();
            op.future.complete(op.results.isEmpty() ? -1L : op.results.get(0).updateCount());
        } else {
            mOk.increment();
            op.future.complete(op.simple ? op.results : result(op, op.tag != null ? op.tag : ""));
        }
        if (gate == op) {
            gate = null;
            releaseDeferred();
        }
        cancelNextIfAbandoned();
    }

    // ---- decoding helpers ----

    private static PgColumn[] rowDescription(ByteBuffer m) {
        int n = m.getShort() & 0xffff;
        PgColumn[] cols = new PgColumn[n];
        for (int i = 0; i < n; i++) {
            String name = cstr(m);
            m.getInt();     // table OID
            m.getShort();   // attribute number
            int type = m.getInt();
            m.getShort();   // type size
            m.getInt();     // type modifier
            m.getShort();   // format
            cols[i] = new PgColumn(name, type);
        }
        return cols;
    }

    private static Map<Character, String> fields(ByteBuffer m) {
        Map<Character, String> f = new LinkedHashMap<>();
        while (m.hasRemaining()) {
            byte code = m.get();
            if (code == 0) {
                break;
            }
            f.put((char) code, cstr(m));
        }
        return Collections.unmodifiableMap(f);
    }

    private static String cstr(ByteBuffer m) {
        int start = m.position();
        int i = start;
        int limit = m.limit();
        while (i < limit && m.get(i) != 0) {
            i++;
        }
        if (i >= limit) {
            throw new PgException("protocol error: unterminated string", false);
        }
        byte[] b = new byte[i - start];
        m.get(b);
        m.get();    // the NUL
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String rest(ByteBuffer m) {
        byte[] b = new byte[m.remaining()];
        m.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String hex(byte[] b) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] out = new char[b.length * 2];
        for (int i = 0; i < b.length; i++) {
            out[2 * i] = digits[(b[i] >> 4) & 0xf];
            out[2 * i + 1] = digits[b[i] & 0xf];
        }
        return new String(out);
    }
}
