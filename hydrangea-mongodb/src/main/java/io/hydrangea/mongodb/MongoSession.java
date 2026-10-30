package io.hydrangea.mongodb;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A logical session: the unit MongoDB uses for causal consistency, retryable writes and multi-document transactions. Every command made through the session (or
 * through a {@link #collection} of it) carries its id. Most code never needs one: the client keeps implicit sessions for retryable writes, and
 * {@link MongoClient#transaction} runs a transaction in one for you. {@link MongoClient#startSession} gives an explicit one, to drive a transaction by hand:
 * <pre>{@code
 * MongoSession s = mongo.startSession();
 * s.startTransaction();
 * s.collection("accounts").updateOne(Bson.doc("_id", "a"), Bson.doc("$inc", Bson.doc("balance", -10)))
 *   .thenCompose(r -> s.collection("accounts").updateOne(Bson.doc("_id", "b"), Bson.doc("$inc", Bson.doc("balance", 10))))
 *   .thenCompose(r -> s.commitTransaction())
 *   .whenComplete((v, err) -> { if (err != null) s.abortTransaction(); s.close(); });
 * }</pre>
 * Transactions need a replica set or a sharded cluster. Inside one, commands are not retried on their own; a failure with the {@code TransientTransactionError}
 * label means the whole transaction may be run again (what {@link MongoClient#transaction} does).
 */
public final class MongoSession implements AutoCloseable {

    private final MongoClient client;
    final Map<String, Object> lsid;
    private long txnNumber;
    private boolean inTransaction;
    private boolean firstInTransaction;
    private boolean closed;

    MongoSession(MongoClient client) {
        this.client = client;
        this.lsid = Map.of("id", UUID.randomUUID());
    }

    /** A collection of the configured database, whose operations run in this session. */
    public MongoCollection collection(String name) {
        return new MongoCollection(client, client.config().database(), name, this);
    }

    public MongoCollection collection(String database, String name) {
        return new MongoCollection(client, database, name, this);
    }

    /** Any command in this session. */
    public CompletableFuture<Map<String, Object>> command(String db, Map<String, Object> command) {
        return client.execute(db, command, MongoClient.Kind.OTHER, this);
    }

    /** Starts a transaction: the commands that follow, until {@link #commitTransaction} or {@link #abortTransaction}, are one atomic unit. */
    public synchronized void startTransaction() {
        if (closed) {
            throw new IllegalStateException("the session is closed");
        }
        if (inTransaction) {
            throw new IllegalStateException("a transaction is already running");
        }
        txnNumber++;
        inTransaction = true;
        firstInTransaction = true;
    }

    public synchronized boolean inTransaction() {
        return inTransaction;
    }

    /** Commits. A failure with the {@code UnknownTransactionCommitResult} label means the outcome is unknown and the commit may be sent again. */
    public CompletableFuture<Void> commitTransaction() {
        Map<String, Object> cmd;
        synchronized (this) {
            if (!inTransaction) {
                return CompletableFuture.failedFuture(new IllegalStateException("no transaction is running"));
            }
            if (firstInTransaction) {                              // nothing was sent: nothing to commit on the server
                inTransaction = false;
                return CompletableFuture.completedFuture(null);
            }
            cmd = transactionCommand("commitTransaction");
        }
        return client.run("admin", cmd, MongoClient.Kind.COMMIT).handle((r, err) -> {
            synchronized (this) {
                inTransaction = false;
            }
            if (err != null) {
                throw err instanceof RuntimeException re ? re : new java.util.concurrent.CompletionException(err);
            }
            return null;
        });
    }

    /** Commits again after an unknown outcome: the server answers the same whether the first commit went through or not. */
    CompletableFuture<Void> retryCommit() {
        Map<String, Object> cmd;
        synchronized (this) {
            cmd = transactionCommand("commitTransaction");
            cmd.put("writeConcern", Map.of("w", "majority"));
        }
        return client.run("admin", cmd, MongoClient.Kind.COMMIT).thenApply(r -> null);
    }

    /** Aborts; errors are swallowed (the transaction may already be gone). */
    public CompletableFuture<Void> abortTransaction() {
        Map<String, Object> cmd;
        synchronized (this) {
            if (!inTransaction) {
                return CompletableFuture.completedFuture(null);
            }
            inTransaction = false;
            if (firstInTransaction) {
                return CompletableFuture.completedFuture(null);
            }
            cmd = transactionCommand("abortTransaction");
        }
        return client.run("admin", cmd, MongoClient.Kind.OTHER).handle((r, err) -> null);
    }

    private Map<String, Object> transactionCommand(String name) {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put(name, 1);
        cmd.put("lsid", lsid);
        cmd.put("txnNumber", txnNumber);
        cmd.put("autocommit", false);
        return cmd;
    }

    /**
     * The command with this session's fields: {@code lsid}; in a transaction {@code txnNumber}, {@code autocommit: false} and {@code startTransaction: true} on the
     * first command; for a retryable write outside one, a fresh {@code txnNumber}.
     */
    synchronized Map<String, Object> decorate(Map<String, Object> command, boolean retryableWrite) {
        if (closed) {
            throw new IllegalStateException("the session is closed");
        }
        Map<String, Object> out = new LinkedHashMap<>(command);
        out.put("lsid", lsid);
        if (inTransaction) {
            out.put("txnNumber", txnNumber);
            out.put("autocommit", false);
            if (firstInTransaction) {
                out.put("startTransaction", true);
                firstInTransaction = false;
            }
        } else if (retryableWrite) {
            out.put("txnNumber", ++txnNumber);
        }
        return out;
    }

    /** Ends the session on the server ({@code endSessions}, best effort); a transaction still running is aborted first. */
    @Override
    public void close() {
        boolean abort;
        synchronized (this) {
            if (closed) {
                return;
            }
            abort = inTransaction;
        }
        (abort ? abortTransaction() : CompletableFuture.completedFuture(null)).whenComplete((v, e) -> {
            synchronized (this) {
                closed = true;
            }
            client.run("admin", Bson.doc("endSessions", List.of(lsid)), MongoClient.Kind.OTHER).whenComplete((r, err) -> { });
        });
    }

    synchronized boolean isClosed() {
        return closed;
    }
}
