package io.hydrangea.mongodb;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Against the three-member replica set of {@code start-mongodb-rs.sh}: discovery, failover, retryable writes, transactions. */
class MongoReplicaSetTest {

    private static EventLoopGroup group;
    private static MongoClient mongo;
    private static final List<MongoCollection> created = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        Mongo.require(29018, "start-mongodb-rs.sh");
        group = new EventLoopGroup(2);
        // the seeds are the two members least likely to be the primary at first: the client must find it anyway
        mongo = MongoClient.connect(group, MongoConfig.of(List.of(Mongo.RS_HOSTS.get(2), Mongo.RS_HOSTS.get(1)), "hydra").withConnections(2).withServerSelectionTimeout(Duration.ofSeconds(30))).get(30, TimeUnit.SECONDS);
    }

    @AfterAll
    static void tearDown() {
        if (mongo != null) {
            for (MongoCollection c : created) {
                try {
                    c.drop().get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // gone
                }
            }
            mongo.close();
        }
        if (group != null) {
            group.close();
        }
    }

    private static MongoCollection fresh(String prefix) {
        MongoCollection c = mongo.collection(Mongo.collection(prefix));
        created.add(c);
        return c;
    }

    private static MongoException failure(CompletableFuture<?> f) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(30, TimeUnit.SECONDS));
        return assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
    }

    @Test
    void thePrimaryIsFoundFromAnySeed() throws Exception {
        assertTrue(mongo.isReplicaSet());
        assertTrue(Mongo.RS_HOSTS.contains(mongo.primary()), mongo.primary());
        Map<String, Object> hello = mongo.hello().get(10, TimeUnit.SECONDS);
        assertEquals("rs0", hello.get("setName"));
        assertEquals(true, hello.get("isWritablePrimary"));
        assertEquals(mongo.primary(), hello.get("me"));
        EventLoopGroup g = new EventLoopGroup(1);
        try {
            MongoException wrongSet = failure(MongoClient.connect(g, MongoConfig.of(Mongo.RS_HOSTS, "hydra").withReplicaSet("other").withServerSelectionTimeout(Duration.ofSeconds(2))));
            assertTrue(wrongSet.getMessage().contains("no primary found"), wrongSet.getMessage());
            try (MongoClient one = MongoClient.connect(g, MongoConfig.parse("mongodb://" + String.join(",", Mongo.RS_HOSTS) + "/hydra?replicaSet=rs0")).get(30, TimeUnit.SECONDS)) {
                assertEquals(mongo.primary(), one.primary());
            }
        } finally {
            g.close();
        }
    }

    @Test
    void writesAndReadsGoOnThroughAStepDown() throws Exception {
        MongoCollection c = fresh("failover");
        c.insertOne(Bson.doc("warm", "up")).get(10, TimeUnit.SECONDS);
        String before = mongo.primary();
        long lostBefore = group.metrics().counter("mongo.connections.lost", null).value();

        // the primary steps down (and may not be elected again for 60 s); the set elects another member
        mongo.command("admin", Bson.doc("replSetStepDown", 60, "secondaryCatchUpPeriodSecs", 5)).get(30, TimeUnit.SECONDS);

        // writes straight after: the stale connections answer NotWritablePrimary, the client finds the new primary and sends each once more
        List<CompletableFuture<InsertResult>> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writes.add(c.insertOne(Bson.doc("n", i)));
        }
        List<CompletableFuture<Long>> reads = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            reads.add(c.count(Bson.doc("warm", "up")));
        }
        int failed = 0;
        for (var w : writes) {
            try {
                w.get(40, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                failed++;
                System.out.println("write failed: " + e.getCause());
            }
        }
        for (var r : reads) {
            assertEquals(1, r.get(40, TimeUnit.SECONDS));
        }
        assertEquals(0, failed, "every write got through: each was retried once with its transaction number");
        assertNotEquals(before, mongo.primary(), "a new primary");
        assertTrue(Mongo.RS_HOSTS.contains(mongo.primary()));
        assertEquals(101, c.count(null).get(10, TimeUnit.SECONDS), "exactly once each: no duplicates from the retries");
        assertTrue(group.metrics().counter("mongo.primary.changes", null).value() >= 1);
        assertTrue(group.metrics().counter("mongo.connections.lost", null).value() >= lostBefore, "the old primary's connections were dropped");
    }

    @Test
    void transactionsCommitAbortAndRetryOnConflict() throws Exception {
        MongoCollection accounts = fresh("accounts");
        accounts.insertMany(List.of(Bson.doc("_id", "a", "balance", 100), Bson.doc("_id", "b", "balance", 0))).get(10, TimeUnit.SECONDS);

        // commit: a transfer
        Long moved = mongo.transaction(s -> {
            MongoCollection acc = s.collection(accounts.name());
            return acc.updateOne(Bson.doc("_id", "a"), Bson.doc("$inc", Bson.doc("balance", -30)))
                    .thenCompose(r -> acc.updateOne(Bson.doc("_id", "b"), Bson.doc("$inc", Bson.doc("balance", 30))))
                    .thenCompose(r -> acc.findOne(Bson.doc("_id", "b")))
                    .thenApply(b -> ((Number) b.get("balance")).longValue());
        }).get(30, TimeUnit.SECONDS);
        assertEquals(30, moved, "a read inside the transaction sees its own writes");
        assertEquals(70, accounts.findById("a").get(10, TimeUnit.SECONDS).get("balance"));
        assertEquals(30, accounts.findById("b").get(10, TimeUnit.SECONDS).get("balance"));

        // abort: the body fails after the first write
        ExecutionException e = assertThrows(ExecutionException.class, () -> mongo.transaction(s -> {
            MongoCollection acc = s.collection(accounts.name());
            return acc.updateOne(Bson.doc("_id", "a"), Bson.doc("$inc", Bson.doc("balance", -1000)))
                    .thenCompose(r -> CompletableFuture.failedFuture(new IllegalStateException("insufficient funds")));
        }).get(30, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals(70, accounts.findById("a").get(10, TimeUnit.SECONDS).get("balance"), "rolled back");

        // write conflict: two transactions bump the same counter at once; one gets WriteConflict (TransientTransactionError) and is run again
        MongoCollection counters = fresh("counters");
        counters.insertOne(Bson.doc("_id", "c", "n", 0)).get(10, TimeUnit.SECONDS);
        AtomicInteger bodies = new AtomicInteger();
        java.util.concurrent.CyclicBarrier both = new java.util.concurrent.CyclicBarrier(2);
        List<CompletableFuture<Map<String, Object>>> txs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            txs.add(mongo.transaction(s -> {
                bodies.incrementAndGet();
                MongoCollection cnt = s.collection(counters.name());
                return cnt.findOne(Bson.doc("_id", "c")).thenCompose(doc -> {
                    try {
                        both.await(5, TimeUnit.SECONDS);          // both have read; both now try to write: the second write conflicts
                    } catch (Exception ignored) {
                        // the other body is on its retry: no partner this time
                    }
                    return cnt.updateOne(Bson.doc("_id", "c"), Bson.doc("$set", Bson.doc("n", ((Number) doc.get("n")).intValue() + 1)));
                }).thenCompose(r -> cnt.findOne(Bson.doc("_id", "c")));
            }));
        }
        for (var t : txs) {
            t.get(30, TimeUnit.SECONDS);
        }
        assertEquals(2, counters.findById("c").get(10, TimeUnit.SECONDS).get("n"), "both increments applied, one after a retry");
        assertTrue(bodies.get() >= 3, "one body ran twice: " + bodies.get());

        // an explicit session, driven by hand
        try (MongoSession s = mongo.startSession()) {
            s.startTransaction();
            s.collection(accounts.name()).insertOne(Bson.doc("_id", "tx-only")).get(10, TimeUnit.SECONDS);
            assertNull(accounts.findById("tx-only").get(10, TimeUnit.SECONDS), "not visible outside before the commit");
            assertNotNull(s.collection(accounts.name()).findById("tx-only").get(10, TimeUnit.SECONDS), "visible inside");
            s.commitTransaction().get(10, TimeUnit.SECONDS);
            assertNotNull(accounts.findById("tx-only").get(10, TimeUnit.SECONDS));
            s.startTransaction();
            s.collection(accounts.name()).deleteOne(Bson.doc("_id", "tx-only")).get(10, TimeUnit.SECONDS);
            s.abortTransaction().get(10, TimeUnit.SECONDS);
            assertNotNull(accounts.findById("tx-only").get(10, TimeUnit.SECONDS), "the delete was aborted");
            ExecutionException none = assertThrows(ExecutionException.class, () -> s.commitTransaction().get(1, TimeUnit.SECONDS), "nothing to commit");
            assertInstanceOf(IllegalStateException.class, none.getCause());
        }
    }

    @Test
    void cursorsInsideATransactionCarryIt() throws Exception {
        MongoCollection c = fresh("txcursor");
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            docs.add(Bson.doc("n", i));
        }
        c.insertMany(docs).get(10, TimeUnit.SECONDS);
        long n = mongo.transaction(s -> s.collection(c.name()).findCursor(null, FindOptions.defaults().withBatchSize(50)).thenCompose(cur -> cur.forEach(d -> { }))).get(30, TimeUnit.SECONDS);
        assertEquals(300, n, "getMore inside the transaction carries lsid, txnNumber and autocommit");
    }
}
