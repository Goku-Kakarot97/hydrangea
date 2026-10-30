package io.hydrangea.mongodb;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Against a real MongoDB ({@code start-mongodb.sh}). */
class MongoClientTest {

    private static EventLoopGroup group;
    private static MongoClient mongo;
    private static final List<MongoCollection> created = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        Mongo.require();
        group = new EventLoopGroup(2);
        mongo = MongoClient.connect(group, Mongo.config().withConnections(2)).get(15, TimeUnit.SECONDS);
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
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        return assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
    }

    @Test
    void serverInfo() throws Exception {
        mongo.ping().get(10, TimeUnit.SECONDS);
        assertTrue(mongo.serverVersion().get(10, TimeUnit.SECONDS).matches("\\d+\\.\\d+.*"));
        Map<String, Object> hello = mongo.hello().get(10, TimeUnit.SECONDS);
        assertTrue(((Number) hello.get("maxWireVersion")).intValue() >= 6, hello.toString());
        assertTrue(((Number) hello.get("maxBsonObjectSize")).intValue() >= 16 * 1024 * 1024);
        assertTrue(mongo.listDatabaseNames().get(10, TimeUnit.SECONDS).contains("admin"));
    }

    @Test
    void crud() throws Exception {
        MongoCollection c = fresh("crud");
        InsertResult ins = c.insertOne(Bson.doc("name", "lamp", "price", 20)).get(10, TimeUnit.SECONDS);
        assertInstanceOf(ObjectId.class, ins.id(), "an _id was made up");
        Map<String, Object> d = c.findById(ins.id()).get(10, TimeUnit.SECONDS);
        assertEquals("lamp", d.get("name"));
        assertEquals(20, d.get("price"));
        assertEquals(ins.id(), d.get("_id"));
        assertNull(c.findById("nope").get(10, TimeUnit.SECONDS));

        assertEquals("given", c.insertOne(Bson.doc("_id", "given", "n", 1)).get(10, TimeUnit.SECONDS).id());
        MongoException dup = failure(c.insertOne(Bson.doc("_id", "given", "n", 2)));
        assertTrue(dup.isDuplicateKey(), dup.getMessage());
        assertEquals(11000, dup.code());
        assertEquals(1, dup.writeErrors().size());

        UpdateResult u = c.updateOne(Bson.doc("name", "lamp"), Bson.doc("$set", Bson.doc("price", 25), "$inc", Bson.doc("views", 1))).get(10, TimeUnit.SECONDS);
        assertEquals(1, u.matched());
        assertEquals(1, u.modified());
        assertNull(u.upsertedId());
        assertEquals(25, c.findById(ins.id()).get(10, TimeUnit.SECONDS).get("price"));
        assertEquals(0, c.updateOne(Bson.doc("name", "lamp"), Bson.doc("$set", Bson.doc("price", 25))).get(10, TimeUnit.SECONDS).modified(), "a no-op update matches without modifying");
        UpdateResult up = c.updateOne(Bson.doc("name", "chair"), Bson.doc("$set", Bson.doc("price", 40)), true).get(10, TimeUnit.SECONDS);
        assertTrue(up.upserted());
        assertEquals(0, up.matched());
        assertEquals(2, c.updateMany(Bson.doc("price", Bson.doc("$gte", 25)), Bson.doc("$set", Bson.doc("dear", true))).get(10, TimeUnit.SECONDS).modified());
        UpdateResult rep = c.replaceOne(Bson.doc("name", "chair"), Bson.doc("name", "chair", "price", 41)).get(10, TimeUnit.SECONDS);
        assertEquals(1, rep.modified());
        assertNull(c.findOne(Bson.doc("name", "chair")).get(10, TimeUnit.SECONDS).get("dear"), "replaced whole");

        assertEquals(3, c.count(null).get(10, TimeUnit.SECONDS));
        assertEquals(1, c.count(Bson.doc("price", Bson.doc("$gt", 30))).get(10, TimeUnit.SECONDS));
        assertEquals(1, c.deleteOne(Bson.doc("_id", "given")).get(10, TimeUnit.SECONDS));
        assertEquals(0, c.deleteOne(Bson.doc("_id", "given")).get(10, TimeUnit.SECONDS));
        assertEquals(2, c.deleteMany(Bson.doc()).get(10, TimeUnit.SECONDS));
        assertEquals(0, c.count(null).get(10, TimeUnit.SECONDS));
    }

    @Test
    void findWithOptionsAndCursors() throws Exception {
        MongoCollection c = fresh("find");
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            docs.add(Bson.doc("n", i, "color", i % 3 == 0 ? "red" : "blue", "tags", List.of("t" + (i % 5))));
        }
        assertEquals(250, c.insertMany(docs).get(10, TimeUnit.SECONDS).count());

        List<Map<String, Object>> page = c.find(Bson.doc("color", "red"), FindOptions.defaults().withSort(Bson.doc("n", -1)).withSkip(2).withLimit(5).withProjection(Bson.doc("n", 1, "_id", 0))).get(10, TimeUnit.SECONDS);
        assertEquals(5, page.size());
        assertEquals(Map.of("n", 243), page.get(0));
        assertEquals(Map.of("n", 231), page.get(4));

        assertEquals(250, c.find(null).get(10, TimeUnit.SECONDS).size(), "more than one batch of 101, gathered");
        MongoCursor cursor = c.findCursor(Bson.doc(), FindOptions.defaults().withBatchSize(100).withSort(Bson.doc("n", 1))).get(10, TimeUnit.SECONDS);
        assertEquals(100, cursor.next().get(10, TimeUnit.SECONDS).size());
        assertTrue(cursor.id() != 0);
        assertEquals(100, cursor.next().get(10, TimeUnit.SECONDS).size());
        List<Map<String, Object>> last = cursor.next().get(10, TimeUnit.SECONDS);
        assertEquals(50, last.size());
        assertEquals(249, last.get(49).get("n"));
        assertEquals(0, cursor.next().get(10, TimeUnit.SECONDS).size());
        assertEquals(0, cursor.id());

        long[] sum = {0};
        assertEquals(250, c.findCursor(null, FindOptions.defaults().withBatchSize(64)).get(10, TimeUnit.SECONDS).forEach(d -> sum[0] += (Integer) d.get("n")).get(10, TimeUnit.SECONDS));
        assertEquals(249L * 250 / 2, sum[0]);

        MongoCursor early = c.findCursor(null, FindOptions.defaults().withBatchSize(10)).get(10, TimeUnit.SECONDS);
        assertEquals(10, early.next().get(10, TimeUnit.SECONDS).size());
        early.close().get(10, TimeUnit.SECONDS);
        assertEquals(0, early.next().get(10, TimeUnit.SECONDS).size());

        assertEquals(List.of("blue", "red"), c.distinct("color", null).get(10, TimeUnit.SECONDS));
        assertEquals(5, c.distinct("tags", Bson.doc("color", "blue")).get(10, TimeUnit.SECONDS).size());
        assertNull(c.findOne(Bson.doc("n", 9999)).get(10, TimeUnit.SECONDS));

        MongoException bad = failure(c.find(Bson.doc("n", Bson.doc("$nosuchop", 1))));
        assertEquals(2, bad.code(), bad.getMessage());
        assertEquals("BadValue", bad.codeName());
        assertTrue(bad.fromServer());
    }

    @Test
    void aggregateAndFindAndModify() throws Exception {
        MongoCollection c = fresh("agg");
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            docs.add(Bson.doc("cat", i % 3 == 0 ? "a" : "b", "v", i));
        }
        c.insertMany(docs).get(10, TimeUnit.SECONDS);
        List<Map<String, Object>> groups = c.aggregate(List.of(
                Bson.doc("$group", Bson.doc("_id", "$cat", "total", Bson.doc("$sum", "$v"), "n", Bson.doc("$sum", 1))),
                Bson.doc("$sort", Bson.doc("_id", 1)))).get(10, TimeUnit.SECONDS);
        assertEquals(2, groups.size());
        assertEquals("a", groups.get(0).get("_id"));
        assertEquals(10, groups.get(0).get("n"));
        assertEquals(135, groups.get(0).get("total"));
        assertEquals(30, c.aggregateCursor(List.of(Bson.doc("$match", Bson.doc())), 7).get(10, TimeUnit.SECONDS).toList().get(10, TimeUnit.SECONDS).size());

        Map<String, Object> before = c.findOneAndUpdate(Bson.doc("v", 5), Bson.doc("$inc", Bson.doc("v", 100))).get(10, TimeUnit.SECONDS);
        assertEquals(5, before.get("v"));
        Map<String, Object> after = c.findOneAndUpdate(Bson.doc("v", 105), Bson.doc("$inc", Bson.doc("v", 1)), true, false, null).get(10, TimeUnit.SECONDS);
        assertEquals(106, after.get("v"));
        assertNull(c.findOneAndUpdate(Bson.doc("v", -1), Bson.doc("$set", Bson.doc("x", 1))).get(10, TimeUnit.SECONDS));
        Map<String, Object> upserted = c.findOneAndUpdate(Bson.doc("v", -1), Bson.doc("$set", Bson.doc("x", 1)), true, true, null).get(10, TimeUnit.SECONDS);
        assertEquals(-1, upserted.get("v"));
        Map<String, Object> gone = c.findOneAndDelete(Bson.doc("v", -1)).get(10, TimeUnit.SECONDS);
        assertEquals(1, gone.get("x"));
        assertEquals(30, c.count(null).get(10, TimeUnit.SECONDS));
    }

    enum Status { ACTIVE, GONE }

    record Product(@Bson.Name("_id") ObjectId id, String name, double price, Status status, List<String> tags, Instant since) {
    }

    @Test
    void recordsInAndOut() throws Exception {
        MongoCollection c = fresh("records");
        Product lamp = new Product(null, "lamp", 20.5, Status.ACTIVE, List.of("led"), Instant.ofEpochMilli(1700000000000L));
        InsertResult ins = c.insertOne(lamp).get(10, TimeUnit.SECONDS);
        ObjectId id = assertInstanceOf(ObjectId.class, ins.id(), "a null _id component gets a generated id");
        Product back = c.findById(id, Product.class).get(10, TimeUnit.SECONDS);
        assertEquals(id, back.id());
        assertEquals("lamp", back.name());
        assertEquals(20.5, back.price());
        assertEquals(Status.ACTIVE, back.status());
        assertEquals(List.of("led"), back.tags());
        assertEquals(lamp.since(), back.since());
        assertNull(c.findById(ObjectId.generate(), Product.class).get(10, TimeUnit.SECONDS));

        List<Product> many = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            many.add(new Product(ObjectId.generate(), "chair " + i, 10 + i, Status.GONE, List.of(), null));
        }
        c.insertMany(many).get(10, TimeUnit.SECONDS);
        List<Product> chairs = c.find(Bson.doc("status", "GONE"), FindOptions.defaults().withSort(Bson.doc("price", 1)), Product.class).get(10, TimeUnit.SECONDS);
        assertEquals(20, chairs.size());
        assertEquals("chair 0", chairs.get(0).name());
        assertNull(chairs.get(0).since());
        assertEquals(1, c.replaceOne(Bson.doc("_id", id), new Product(id, "desk lamp", 22, Status.ACTIVE, List.of("led", "desk"), lamp.since())).get(10, TimeUnit.SECONDS).modified());
        assertEquals("desk lamp", c.findOne(Bson.doc("_id", id), Product.class).get(10, TimeUnit.SECONDS).name());
        double[] sum = {0};
        assertEquals(21, c.findCursor(null, FindOptions.defaults().withBatchSize(8)).get(10, TimeUnit.SECONDS).forEach(Product.class, p -> sum[0] += p.price()).get(10, TimeUnit.SECONDS));
        assertEquals(22 + 20 * 10 + 190, sum[0], 1e-9);
    }

    @Test
    void indexes() throws Exception {
        MongoCollection c = fresh("idx");
        assertEquals("email_1", c.createIndex(Bson.doc("email", 1), Map.of("unique", true)).get(10, TimeUnit.SECONDS));
        assertEquals("a_1_b_-1", c.createIndex(Bson.doc("a", 1, "b", -1)).get(10, TimeUnit.SECONDS));
        assertEquals("byName", c.createIndex(Bson.doc("name", "text"), Map.of("name", "byName")).get(10, TimeUnit.SECONDS));
        assertEquals("email_1", c.createIndex(Bson.doc("email", 1), Map.of("unique", true)).get(10, TimeUnit.SECONDS), "creating it again is fine");
        List<Map<String, Object>> indexes = c.listIndexes().get(10, TimeUnit.SECONDS);
        assertEquals(4, indexes.size(), indexes.toString());
        c.insertOne(Bson.doc("email", "a@x")).get(10, TimeUnit.SECONDS);
        assertTrue(failure(c.insertOne(Bson.doc("email", "a@x"))).isDuplicateKey());
        c.dropIndex("a_1_b_-1").get(10, TimeUnit.SECONDS);
        assertEquals(3, c.listIndexes().get(10, TimeUnit.SECONDS).size());
        assertTrue(mongo.listCollectionNames().get(10, TimeUnit.SECONDS).contains(c.name()));
        assertTrue(c.drop().get(10, TimeUnit.SECONDS));
        assertFalse(c.drop().get(10, TimeUnit.SECONDS), "dropping a collection that is not there");
        assertFalse(mongo.listCollectionNames().get(10, TimeUnit.SECONDS).contains(c.name()));
    }

    @Test
    void unorderedInsertReportsEveryFailure() throws Exception {
        MongoCollection c = fresh("unordered");
        c.insertOne(Bson.doc("_id", 1)).get(10, TimeUnit.SECONDS);
        MongoException e = failure(c.insertMany(List.of(Bson.doc("_id", 1), Bson.doc("_id", 2), Bson.doc("_id", 1), Bson.doc("_id", 3)), false));
        assertEquals(2, e.writeErrors().size(), e.getMessage());
        assertEquals(0, e.writeErrors().get(0).get("index"));
        assertEquals(2, e.writeErrors().get(1).get("index"));
        assertEquals(3, c.count(null).get(10, TimeUnit.SECONDS), "the other writes ran");
        MongoException ordered = failure(c.insertMany(List.of(Bson.doc("_id", 4), Bson.doc("_id", 1), Bson.doc("_id", 5)), true));
        assertEquals(1, ordered.writeErrors().size());
        assertEquals(4, c.count(null).get(10, TimeUnit.SECONDS), "ordered: stopped at the failure");
    }

    @Test
    void manyConcurrentCommandsPipelineOverTheConnections() throws Exception {
        MongoCollection c = fresh("conc");
        List<CompletableFuture<InsertResult>> fs = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            fs.add(c.insertOne(Bson.doc("n", i)));
        }
        for (var f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        List<CompletableFuture<Long>> counts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            counts.add(c.count(Bson.doc("n", Bson.doc("$lt", i))));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(i, counts.get(i).get(30, TimeUnit.SECONDS), "replies matched to their commands by id");
        }
        assertEquals(500, c.count(null).get(10, TimeUnit.SECONDS));
    }

    @Test
    void rawCommandsAndTimeouts() throws Exception {
        Map<String, Object> status = mongo.command("admin", Bson.doc("serverStatus", 1)).get(10, TimeUnit.SECONDS);
        assertNotNull(status.get("uptime"));
        MongoException unknown = failure(mongo.command("admin", Bson.doc("noSuchCommand", 1)));
        assertEquals(59, unknown.code(), unknown.getMessage());
        assertEquals("CommandNotFound", unknown.codeName());
        // the server stops a slow query at maxTimeMS
        MongoCollection c = fresh("slow");
        c.insertOne(Bson.doc("n", 1)).get(10, TimeUnit.SECONDS);
        MongoException slow = failure(c.find(Bson.doc("$where", "sleep(2000) || true"), FindOptions.defaults().withMaxTime(Duration.ofMillis(300))));
        assertEquals(50, slow.code(), slow.getMessage());
        assertTrue(slow.isTimeout());
    }

    @Test
    void loginFailuresAndScramSha1() throws Exception {
        EventLoopGroup g = new EventLoopGroup(1);
        try {
            ExecutionException e = assertThrows(ExecutionException.class, () -> MongoClient.connect(g, Mongo.config().withCredentials(Mongo.USER, "wrong")).get(10, TimeUnit.SECONDS));
            MongoException m = assertInstanceOf(MongoException.class, e.getCause());
            assertEquals(18, m.code(), m.getMessage());                      // AuthenticationFailed
            assertTrue(m.getMessage().contains("login failed"), m.getMessage());
            assertFalse(m.getMessage().contains("wrong"), "the password is not in the message");

            e = assertThrows(ExecutionException.class, () -> MongoClient.connect(g, Mongo.config().withCredentials("nobody", "x")).get(10, TimeUnit.SECONDS));
            assertEquals(18, assertInstanceOf(MongoException.class, e.getCause()).code());

            // the "legacy" user of start-mongodb.sh has SCRAM-SHA-1 credentials only: the handshake says so and the client follows
            try (MongoClient legacy = MongoClient.connect(g, Mongo.config().withCredentials("legacy", "legacy")).get(10, TimeUnit.SECONDS)) {
                assertEquals(0, legacy.collection(Mongo.collection("legacy")).count(null).get(10, TimeUnit.SECONDS));
            } catch (ExecutionException noUser) {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "no legacy user: " + noUser.getCause());
            }
            // a URI with everything in it
            MongoConfig parsed = MongoConfig.parse("mongodb://hy%40dra:p%3Aw@localhost:29017/shop?authSource=admin&appName=t&connectTimeoutMS=2000&maxPoolSize=3");
            assertEquals("hy@dra", parsed.username());
            assertEquals("p:w", parsed.password());
            assertEquals("shop", parsed.database());
            assertEquals("admin", parsed.authSource());
            assertEquals("t", parsed.appName());
            assertEquals(Duration.ofSeconds(2), parsed.connectTimeout());
            assertEquals(3, parsed.connections());
            assertFalse(parsed.toString().contains("p:w"), parsed.toString());
            MongoConfig seeds = MongoConfig.parse("mongodb://h1,h2:27018,[::1]:27019/?replicaSet=rs0&retryWrites=false&serverSelectionTimeoutMS=5000");
            assertEquals(List.of("h1:27017", "h2:27018", "[::1]:27019"), seeds.hosts());
            assertEquals("rs0", seeds.replicaSet());
            assertEquals("test", seeds.database());
            assertFalse(seeds.retryWrites());
            assertEquals(Duration.ofSeconds(5), seeds.serverSelectionTimeout());
            assertNull(seeds.username());
            assertThrows(IllegalArgumentException.class, () -> MongoConfig.parse("mongodb+srv://x/y"));
            // a standalone server: no transactions
            MongoException noTx = failure(mongo.transaction(sx -> sx.collection("x").insertOne(Bson.doc("a", 1))));
            assertTrue(noTx.fromServer(), noTx.getMessage());
            assertFalse(mongo.isReplicaSet());
            assertEquals(Mongo.HOST + ":" + Mongo.PORT, mongo.primary());
        } finally {
            g.close();
        }
    }
}
