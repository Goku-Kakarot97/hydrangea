package io.hydrangea.mongodb;

import io.hydrangea.EventLoopGroup;

import io.hydrangea.log.Logger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A MongoDB client: {@link MongoConfig#connections} connections to the primary, shared by every thread, each pipelining the commands given to it and all of them
 * used in turn. Every call returns a {@link CompletableFuture} completed on an event loop (continuations must not block). Documents are {@code Map<String, Object>}
 * ({@link Bson#doc} builds one) or records ({@link Bson.Name} renames a component, {@code @Bson.Name("_id") ObjectId id}).
 * <pre>{@code
 * MongoClient mongo = MongoClient.connect(group, MongoConfig.parse("mongodb://app:pw@h1,h2,h3/shop?replicaSet=rs0")).join();
 * MongoCollection products = mongo.collection("products");
 * products.insertOne(Bson.doc("name", "lamp", "price", 20)).join();
 * List<Map<String, Object>> cheap = products.find(Bson.doc("price", Bson.doc("$lt", 30))).join();
 * }</pre>
 * <ul>
 *   <li><b>Replica sets</b>: the seeds are asked {@code hello}; a member answers with the set's hosts and its primary, and the connections go to the primary. When a
 *       command fails because the primary stepped down or its connection was lost, the client asks the members again, waits up to
 *       {@link MongoConfig#serverSelectionTimeout} for a primary, and sends <em>reads</em> once more; <em>writes</em> too when {@link MongoConfig#retryWrites} is on
 *       (they carry a transaction number, so the server runs each exactly once). A standalone server or a mongos is used as it is.</li>
 *   <li><b>Transactions</b> ({@link #transaction}): run on a replica set or mongos in a {@link MongoSession}; a transient failure runs the whole transaction again,
 *       an unknown commit outcome sends the commit again.</li>
 *   <li><b>Errors</b>: the server's {@code ok: 0} becomes a {@link MongoException} with its {@code code}/{@code codeName}; a write command with failed writes too
 *       ({@link MongoException#writeErrors}, after the other writes of an unordered batch have run). A command that was in flight when a connection was lost fails
 *       with {@link MongoException#connectionLost} unless it was retried as above.</li>
 *   <li><b>Metrics</b> on the group: {@code mongo.commands{outcome=ok|error|failed}}, {@code mongo.command.duration}, {@code mongo.commands.pending},
 *       {@code mongo.connections.lost}, {@code mongo.primary.changes}.</li>
 * </ul>
 * Not covered: reads from secondaries (read preferences), change streams, GridFS, client-side encryption, {@code mongodb+srv}.
 */
public final class MongoClient implements AutoCloseable {

    private static final Logger LOG = Logger.get(MongoClient.class);
    private static final Duration TRANSACTION_RETRY_LIMIT = Duration.ofSeconds(120);

    /** What a command is, for retrying: a read is sent again after a failover; a write only with a transaction number; a commit by its own rule; nothing else. */
    enum Kind { READ, WRITE, COMMIT, OTHER }

    private final EventLoopGroup group;
    private final MongoConfig config;
    private final Slot[] slots;
    private final AtomicInteger next = new AtomicInteger();
    private final ConcurrentLinkedQueue<MongoSession> sessionPool = new ConcurrentLinkedQueue<>();
    private final io.hydrangea.Metrics.Count mPrimaryChanges;
    private volatile boolean closed;

    // topology
    private final Object topologyLock = new Object();
    private volatile String primary;                               // host:port, or null while unknown
    private volatile String lastPrimary;                           // the one before, to count changes
    private volatile Set<String> known;                            // the seeds and every member heard of
    private volatile boolean replicaSetMode;                       // a setName was seen
    private volatile boolean mongosMode;
    private volatile boolean sessionsSupported;                    // hello.logicalSessionTimeoutMinutes
    private CompletableFuture<String> selecting;                   // the discovery under way (topologyLock)

    /** One connection's place: the connection being opened or open, replaced when it is lost. */
    private static final class Slot {
        volatile CompletableFuture<MongoConnection> conn;
        volatile long lastFailure;
    }

    private MongoClient(EventLoopGroup group, MongoConfig config) {
        this.group = group;
        this.config = config;
        this.slots = new Slot[config.connections()];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new Slot();
        }
        this.known = new LinkedHashSet<>(config.hosts());
        this.mPrimaryChanges = group.metrics().counter("mongo.primary.changes", null);
    }

    /** Finds the primary (or the server), opens the connections and logs in on each; the future fails if that cannot be done within {@link MongoConfig#serverSelectionTimeout}. */
    public static CompletableFuture<MongoClient> connect(EventLoopGroup group, MongoConfig config) {
        MongoClient c = new MongoClient(group, config);
        return c.selectPrimary().thenCompose(p -> {
            List<CompletableFuture<MongoConnection>> all = new ArrayList<>();
            for (Slot s : c.slots) {
                all.add(c.reopen(s));
            }
            return CompletableFuture.allOf(all.toArray(new CompletableFuture[0]));
        }).handle((v, err) -> {
            if (err != null) {
                c.close();
                throw err instanceof CompletionException ce ? ce : new CompletionException(err);
            }
            return c;
        });
    }

    public MongoConfig config() {
        return config;
    }

    /** The primary (or the server) in use, {@code host:port}; null while none is known. */
    public String primary() {
        return primary;
    }

    /** True once a replica set has been seen: transactions and retryable writes are available. */
    public boolean isReplicaSet() {
        return replicaSetMode || mongosMode;
    }

    /** A collection of the configured database. */
    public MongoCollection collection(String name) {
        return new MongoCollection(this, config.database(), name, null);
    }

    public MongoCollection collection(String database, String name) {
        return new MongoCollection(this, database, name, null);
    }

    /** An explicit session, for a transaction driven by hand or causal consistency; close it when done. */
    public MongoSession startSession() {
        return new MongoSession(this);
    }

    // ---- commands ----

    /** Any command on {@code db}: {@code command("admin", Bson.doc("serverStatus", 1))}. The reply when {@code ok} is 1, else a {@link MongoException}. */
    public CompletableFuture<Map<String, Object>> command(String db, Map<String, Object> command) {
        return execute(db, command, Kind.OTHER, null);
    }

    /** {@code ping}: a round trip. */
    public CompletableFuture<Void> ping() {
        return run("admin", Bson.doc("ping", 1), Kind.READ).thenApply(r -> null);
    }

    /** The server version ({@code buildInfo.version}). */
    public CompletableFuture<String> serverVersion() {
        return run("admin", Bson.doc("buildInfo", 1), Kind.READ).thenApply(r -> String.valueOf(r.get("version")));
    }

    /** The primary's {@code hello} answer: {@code maxBsonObjectSize}, {@code maxWriteBatchSize}, {@code maxWireVersion}, {@code setName}, {@code hosts}, ... */
    public CompletableFuture<Map<String, Object>> hello() {
        return connection().thenApply(MongoConnection::hello);
    }

    public CompletableFuture<List<String>> listDatabaseNames() {
        return run("admin", Bson.doc("listDatabases", 1, "nameOnly", true), Kind.READ).thenApply(r -> names(r.get("databases")));
    }

    /** The collections of the configured database. */
    public CompletableFuture<List<String>> listCollectionNames() {
        return listCollectionNames(config.database());
    }

    public CompletableFuture<List<String>> listCollectionNames(String db) {
        return cursor(db, Bson.doc("listCollections", 1, "nameOnly", true), 0, null).thenCompose(MongoCursor::toList).thenApply(docs -> {
            List<String> out = new ArrayList<>(docs.size());
            for (Map<String, Object> d : docs) {
                out.add(String.valueOf(d.get("name")));
            }
            return out;
        });
    }

    public CompletableFuture<Void> dropDatabase(String db) {
        return execute(db, Bson.doc("dropDatabase", 1), Kind.OTHER, null).thenApply(r -> null);
    }

    private static List<String> names(Object list) {
        List<String> out = new ArrayList<>();
        if (list instanceof List<?> l) {
            for (Object o : l) {
                if (o instanceof Map<?, ?> m && m.get("name") != null) {
                    out.add(String.valueOf(m.get("name")));
                }
            }
        }
        return out;
    }

    // ---- transactions ----

    /**
     * Runs {@code body} in a transaction: a session is started, the body's commands (made through {@code session.collection(...)}) are one atomic unit, and the
     * commit happens when the body's future completes. A failure labelled {@code TransientTransactionError} (a write conflict, a stepped-down primary) runs the
     * whole body again; a commit whose outcome is unknown is sent again; up to two minutes in all. Any other failure aborts and is reported.
     * <pre>{@code
     * mongo.transaction(s -> s.collection("accounts").updateOne(Bson.doc("_id", "a"), Bson.doc("$inc", Bson.doc("balance", -10)))
     *         .thenCompose(r -> s.collection("accounts").updateOne(Bson.doc("_id", "b"), Bson.doc("$inc", Bson.doc("balance", 10)))))
     *      .join();
     * }</pre>
     * The body must do all its work through the session and complete its future only when every command has been answered; it may run more than once.
     */
    public <T> CompletableFuture<T> transaction(Function<MongoSession, CompletableFuture<T>> body) {
        MongoSession session = startSession();
        CompletableFuture<T> result = new CompletableFuture<>();
        transactionAttempt(session, body, System.nanoTime() + TRANSACTION_RETRY_LIMIT.toNanos(), result);
        return result.whenComplete((v, e) -> session.close());
    }

    private <T> void transactionAttempt(MongoSession session, Function<MongoSession, CompletableFuture<T>> body, long deadline, CompletableFuture<T> result) {
        CompletableFuture<T> bodyResult;
        try {
            session.startTransaction();
            bodyResult = body.apply(session);
            if (bodyResult == null) {
                throw new IllegalStateException("the transaction body returned null");
            }
        } catch (Throwable t) {
            session.abortTransaction().whenComplete((v, e) -> result.completeExceptionally(t));
            return;
        }
        bodyResult.whenComplete((value, err) -> {
            if (err != null) {
                MongoException m = unwrap(err);
                session.abortTransaction().whenComplete((v, e) -> {
                    if (m != null && m.hasLabel("TransientTransactionError") && System.nanoTime() < deadline) {
                        transactionAttempt(session, body, deadline, result);
                    } else {
                        result.completeExceptionally(unwrapAny(err));
                    }
                });
                return;
            }
            commitWithRetry(session, deadline).whenComplete((v, cerr) -> {
                if (cerr == null) {
                    result.complete(value);
                    return;
                }
                MongoException m = unwrap(cerr);
                if (m != null && m.hasLabel("TransientTransactionError") && System.nanoTime() < deadline) {
                    transactionAttempt(session, body, deadline, result);
                } else {
                    result.completeExceptionally(unwrapAny(cerr));
                }
            });
        });
    }

    private CompletableFuture<Void> commitWithRetry(MongoSession session, long deadline) {
        return session.commitTransaction().handle((v, err) -> {
            if (err == null) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            MongoException m = unwrap(err);
            if (m != null && m.hasLabel("UnknownTransactionCommitResult") && System.nanoTime() < deadline) {
                return session.retryCommit();
            }
            throw err instanceof CompletionException ce ? ce : new CompletionException(err);
        }).thenCompose(f -> f);
    }

    private static MongoException unwrap(Throwable t) {
        Throwable c = t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
        return c instanceof MongoException m ? m : null;
    }

    private static Throwable unwrapAny(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }

    // ---- running commands: sessions and retries ----

    /** Runs a command for a collection or session: adds the session fields (an implicit session when none is given) and retries as the kind allows. */
    CompletableFuture<Map<String, Object>> execute(String db, Map<String, Object> command, Kind kind, MongoSession session) {
        if (!sessionsSupported) {
            return run(db, command, kind == Kind.WRITE ? Kind.OTHER : kind);      // no session: a write cannot be retried safely
        }
        boolean retryableWrite = kind == Kind.WRITE && config.retryWrites() && isReplicaSet();
        if (session != null) {
            Map<String, Object> decorated;
            try {
                decorated = session.decorate(command, retryableWrite);
            } catch (IllegalStateException e) {
                return CompletableFuture.failedFuture(e);
            }
            return run(db, decorated, session.inTransaction() ? Kind.OTHER : kind);
        }
        MongoSession implicit = acquireSession();
        return run(db, implicit.decorate(command, retryableWrite), kind).whenComplete((r, e) -> releaseSession(implicit));
    }

    /** Runs a command that answers with a cursor and wraps it; the cursor keeps its session (needed for {@code getMore}) until it is done. */
    CompletableFuture<MongoCursor> cursor(String db, Map<String, Object> command, int batchSize, MongoSession session) {
        if (!sessionsSupported) {
            return runOn(db, command, Kind.READ).thenApply(cr -> new MongoCursor(cr.conn, db, cr.reply, batchSize, null, null));
        }
        MongoSession s = session != null ? session : acquireSession();
        Map<String, Object> decorated;
        try {
            decorated = s.decorate(command, false);
        } catch (IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }
        return runOn(db, decorated, s.inTransaction() ? Kind.OTHER : Kind.READ).handle((cr, err) -> {
            if (err != null) {
                if (session == null) {
                    releaseSession(s);
                }
                throw err instanceof CompletionException ce ? ce : new CompletionException(err);
            }
            return new MongoCursor(cr.conn, db, cr.reply, batchSize, s, session == null ? () -> releaseSession(s) : null);
        });
    }

    MongoSession acquireSession() {
        MongoSession s = sessionPool.poll();
        return s != null ? s : new MongoSession(this);
    }

    void releaseSession(MongoSession s) {
        if (!s.isClosed() && !s.inTransaction() && sessionPool.size() < 64) {
            sessionPool.add(s);
        }
    }

    /** A reply and the connection it came on. */
    record Answer(MongoConnection conn, Map<String, Object> reply) {
    }

    /** Runs the command as given (session fields already there), retrying as the kind allows. */
    CompletableFuture<Map<String, Object>> run(String db, Map<String, Object> command, Kind kind) {
        return runOn(db, command, kind).thenApply(Answer::reply);
    }

    /** Every command is a CLIENT span named after the command and its collection ({@code find products}), child of the calling thread's current span. */
    private CompletableFuture<Answer> runOn(String db, Map<String, Object> command, Kind kind) {
        CompletableFuture<Answer> out = new CompletableFuture<>();
        String op = command.isEmpty() ? "command" : command.keySet().iterator().next();
        Object target = command.isEmpty() ? null : command.get(op);
        String collection = target instanceof String s ? s : null;
        io.hydrangea.trace.Span span = io.hydrangea.trace.Tracing.start(collection == null ? op : op + " " + collection, io.hydrangea.trace.Span.Kind.CLIENT);
        if (span.isRecording()) {
            span.attribute("db.system.name", "mongodb").attribute("db.namespace", db).attribute("db.operation.name", op)
                    .attribute("server.address", String.join(",", config.hosts()));
            if (collection != null) {
                span.attribute("db.collection.name", collection);
            }
            out.whenComplete((r, err) -> {
                if (err != null) {
                    span.error(err instanceof CompletionException && err.getCause() != null ? err.getCause() : err);
                }
                span.end();
            });
        }
        attempt(db, command, kind, false, out);
        return out;
    }

    private void attempt(String db, Map<String, Object> command, Kind kind, boolean retried, CompletableFuture<Answer> out) {
        connection().whenComplete((c, err) -> {
            if (err != null) {
                out.completeExceptionally(unwrapAny(err));           // no primary within the timeout: nothing to retry on
                return;
            }
            c.command(db, command).whenComplete((reply, cerr) -> {
                if (cerr == null) {
                    out.complete(new Answer(c, reply));
                    return;
                }
                MongoException m = unwrap(cerr);
                if (m == null) {
                    out.completeExceptionally(unwrapAny(cerr));
                    return;
                }
                boolean failover = m.connectionLost() || m.notPrimary();
                if (failover) {
                    primaryLost(c.address(), m);
                }
                boolean retryable = failover || m.hasLabel("RetryableWriteError");
                boolean mayRetry = !retried && retryable && (kind == Kind.READ || kind == Kind.COMMIT || (kind == Kind.WRITE && command.containsKey("txnNumber")));
                if (mayRetry) {
                    attempt(db, command, kind, true, out);
                } else {
                    out.completeExceptionally(m);
                }
            });
        });
    }

    // ---- topology ----

    /** The primary's address, found now if unknown; fails after {@link MongoConfig#serverSelectionTimeout}. */
    CompletableFuture<String> selectPrimary() {
        String p = primary;
        if (p != null) {
            return CompletableFuture.completedFuture(p);
        }
        synchronized (topologyLock) {
            if (primary != null) {
                return CompletableFuture.completedFuture(primary);
            }
            if (selecting == null) {
                CompletableFuture<String> f = new CompletableFuture<>();
                selecting = f;
                discover(System.nanoTime() + config.serverSelectionTimeout().toNanos(), f, null);
                f.whenComplete((v, e) -> {
                    synchronized (topologyLock) {
                        selecting = null;
                    }
                });
            }
            return selecting;
        }
    }

    /** One round of {@code hello} to every known host; another after half a second if no primary answered, until the deadline. */
    private void discover(long deadline, CompletableFuture<String> result, Throwable lastError) {
        if (closed) {
            result.completeExceptionally(MongoException.local("the client is closed", false, null));
            return;
        }
        List<String> hosts = new ArrayList<>(known);
        List<CompletableFuture<Map<String, Object>>> probes = new ArrayList<>(hosts.size());
        for (String h : hosts) {
            probes.add(MongoConnection.probe(config, h, group.next(), group.metrics()));
        }
        CompletableFuture.allOf(probes.stream().map(p -> p.handle((r, e) -> null)).toArray(CompletableFuture[]::new)).thenRun(() -> {
            String found = null;
            Map<String, Object> foundHello = null;
            Throwable firstError = lastError;
            Set<String> learned = new LinkedHashSet<>(known);
            for (int i = 0; i < hosts.size(); i++) {
                CompletableFuture<Map<String, Object>> p = probes.get(i);
                if (p.isCompletedExceptionally()) {
                    try {
                        p.join();
                    } catch (CompletionException e) {
                        if (firstError == null) {
                            firstError = e.getCause();
                        }
                    }
                    continue;
                }
                Map<String, Object> hello = p.join();
                String setName = hello.get("setName") instanceof String s ? s : null;
                if (setName != null) {
                    if (config.replicaSet() != null && !config.replicaSet().equals(setName)) {
                        LOG.warn("{} belongs to replica set {}, not {}: ignored", hosts.get(i), setName, config.replicaSet());
                        continue;
                    }
                    replicaSetMode = true;
                    for (String key : List.of("hosts", "passives")) {
                        if (hello.get(key) instanceof List<?> l) {
                            for (Object o : l) {
                                learned.add(MongoConfig.normalize(String.valueOf(o)));
                            }
                        }
                    }
                    if (hello.get("primary") instanceof String pr) {
                        learned.add(MongoConfig.normalize(pr));
                    }
                    if (Boolean.TRUE.equals(hello.get("isWritablePrimary")) || Boolean.TRUE.equals(hello.get("ismaster"))) {
                        found = hosts.get(i);
                        foundHello = hello;
                    }
                } else if ("isdbgrid".equals(hello.get("msg"))) {
                    mongosMode = true;
                    if (found == null) {
                        found = hosts.get(i);
                        foundHello = hello;
                    }
                } else if (found == null) {                        // a standalone server
                    found = hosts.get(i);
                    foundHello = hello;
                }
            }
            known = learned;
            if (found != null) {
                sessionsSupported = foundHello.get("logicalSessionTimeoutMinutes") != null;
                String before = lastPrimary;
                primary = found;
                lastPrimary = found;
                if (before != null && !before.equals(found)) {
                    mPrimaryChanges.increment();
                }
                LOG.debug("MongoDB primary: {}", found);
                result.complete(found);
                return;
            }
            if (System.nanoTime() >= deadline) {
                String why = firstError != null ? firstError.getMessage() : "no member is a primary";
                result.completeExceptionally(MongoException.lost("no primary found among " + learned + " within " + config.serverSelectionTimeout().toMillis() + " ms: " + why, firstError));
                return;
            }
            Throwable err = firstError;
            try {
                group.next().schedule(() -> discover(deadline, result, err), 500, TimeUnit.MILLISECONDS);
            } catch (RuntimeException shutDown) {
                result.completeExceptionally(MongoException.local("the event loop is shut down", false, shutDown));
            }
        });
    }

    /** {@code address} is not the primary any more (its connection was lost, or it said so): forget it, drop the connections to it, find the new one on the next command. */
    private void primaryLost(String address, MongoException why) {
        synchronized (topologyLock) {
            if (!address.equals(primary)) {
                return;
            }
            primary = null;
            LOG.info("MongoDB primary {} lost ({}): looking for the new one", address, why.getMessage());
        }
        for (Slot s : slots) {
            CompletableFuture<MongoConnection> f = s.conn;
            if (f != null && f.isDone() && !f.isCompletedExceptionally() && f.join().address().equals(address)) {
                f.join().close();
            }
        }
    }

    // ---- connections ----

    /** The next connection to the primary in turn, opening one in place of a lost one; waits for a primary to be found when none is known. */
    CompletableFuture<MongoConnection> connection() {
        if (closed) {
            return CompletableFuture.failedFuture(MongoException.local("the client is closed", false, null));
        }
        String p = primary;
        if (p == null) {
            return selectPrimary().thenCompose(found -> connection());
        }
        int n = slots.length;
        int first = Math.floorMod(next.getAndIncrement(), n);
        for (int i = 0; i < n; i++) {
            CompletableFuture<MongoConnection> f = slots[(first + i) % n].conn;
            if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
                MongoConnection c = f.join();
                if (c.isOnline() && c.address().equals(p)) {
                    return f;
                }
            }
        }
        return reopen(slots[first]);
    }

    private CompletableFuture<MongoConnection> reopen(Slot s) {
        synchronized (s) {
            CompletableFuture<MongoConnection> f = s.conn;
            String p = primary;
            if (p == null) {
                return selectPrimary().thenCompose(found -> reopen(s));
            }
            if (f != null && !f.isDone()) {
                return f;                                          // being opened: queue behind it
            }
            if (f != null && !f.isCompletedExceptionally() && f.join().isOnline() && f.join().address().equals(p)) {
                return f;
            }
            long now = System.nanoTime();
            if (f != null && f.isCompletedExceptionally() && now - s.lastFailure < 1_000_000_000L) {
                return f;                                          // the last attempt failed less than a second ago: do not hammer the server
            }
            CompletableFuture<MongoConnection> opening = MongoConnection.open(config, p, group.next(), group.metrics());
            s.conn = opening;
            opening.whenComplete((c, err) -> {
                if (err != null) {
                    s.lastFailure = System.nanoTime();
                    MongoException m = unwrap(err);
                    if (m != null && m.connectionLost()) {
                        primaryLost(p, m);                         // the primary is gone: the members are asked again next time
                    }
                } else if (closed) {
                    c.close();
                }
            });
            return opening;
        }
    }

    /** Closes every connection; commands under way fail. */
    @Override
    public void close() {
        closed = true;
        for (Slot s : slots) {
            CompletableFuture<MongoConnection> f = s.conn;
            if (f != null) {
                f.thenAccept(MongoConnection::close);
            }
        }
    }
}
