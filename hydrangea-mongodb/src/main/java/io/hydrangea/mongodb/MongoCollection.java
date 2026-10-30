package io.hydrangea.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The operations on one collection. Filters, updates, projections and pipelines are documents written the way the shell writes them
 * ({@code Bson.doc("price", Bson.doc("$gt", 10))}, {@code Bson.doc("$set", Bson.doc("sold", true))}). A document to insert may be a {@code Map} or a record; a document
 * read is a {@code Map}, or a record with the {@code Class} overloads.
 */
public final class MongoCollection {

    private final MongoClient client;
    private final String db;
    private final String name;
    private final MongoSession session;                            // null: an implicit session per operation

    MongoCollection(MongoClient client, String db, String name, MongoSession session) {
        if (name == null || name.isEmpty() || name.contains("$") || name.contains("\0")) {
            throw new IllegalArgumentException("a collection name without $ or NUL is required");
        }
        this.client = client;
        this.db = db;
        this.name = name;
        this.session = session;
    }

    public String database() {
        return db;
    }

    public String name() {
        return name;
    }

    /** {@code db.collection}. */
    public String namespace() {
        return db + "." + name;
    }

    private CompletableFuture<Map<String, Object>> read(Map<String, Object> command) {
        return client.execute(db, command, MongoClient.Kind.READ, session);
    }

    private CompletableFuture<Map<String, Object>> write(Map<String, Object> command) {
        return client.execute(db, command, MongoClient.Kind.WRITE, session);
    }

    private CompletableFuture<Map<String, Object>> other(Map<String, Object> command) {
        return client.execute(db, command, MongoClient.Kind.OTHER, session);
    }

    // ---- insert ----

    /** Inserts the document (a {@code Map} or a record); an {@code _id} is made up when it has none. */
    public CompletableFuture<InsertResult> insertOne(Object document) {
        return insertMany(List.of(document), true);
    }

    /** Inserts the documents in order, stopping at the first failure; the ids of all of them (the ones that failed included: look at the exception's {@code writeErrors}). */
    public CompletableFuture<InsertResult> insertMany(List<?> documents) {
        return insertMany(documents, true);
    }

    /** {@code ordered} false: every document is tried, the failed ones are reported together. */
    public CompletableFuture<InsertResult> insertMany(List<?> documents, boolean ordered) {
        if (documents.isEmpty()) {
            return CompletableFuture.completedFuture(new InsertResult(List.of()));
        }
        List<Map<String, Object>> docs = new ArrayList<>(documents.size());
        List<Object> ids = new ArrayList<>(documents.size());
        try {
            for (Object d : documents) {
                Map<String, Object> m = withId(d);
                docs.add(m);
                ids.add(m.get("_id"));
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        InsertResult result = new InsertResult(ids);
        return write(Bson.doc("insert", name, "documents", docs, "ordered", ordered)).thenApply(r -> {
            checkWrites(r);
            return result;
        });
    }

    /** The document as a map with {@code _id} first, generated if absent. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> withId(Object document) {
        Map<String, Object> m = document instanceof Map<?, ?> given ? (Map<String, Object>) given : document instanceof Record r ? Bson.fromRecord(r) : null;
        if (m == null) {
            throw new IllegalArgumentException("a document is a Map or a record, not " + (document == null ? "null" : document.getClass().getName()));
        }
        if (m.get("_id") != null) {
            return m;
        }
        Map<String, Object> out = new LinkedHashMap<>(m.size() * 2 + 2);
        out.put("_id", ObjectId.generate());
        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!e.getKey().equals("_id")) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private static void checkWrites(Map<String, Object> reply) {
        if (reply.get("writeErrors") instanceof List<?> l && !l.isEmpty() || reply.get("writeConcernError") != null) {
            throw MongoException.writes(reply);
        }
    }

    // ---- find ----

    /** Every document matching {@code filter} ({@code Bson.doc()} or null for all), in one list. For many documents use {@link #findCursor} or a limit. */
    public CompletableFuture<List<Map<String, Object>>> find(Map<String, Object> filter) {
        return find(filter, FindOptions.defaults());
    }

    public CompletableFuture<List<Map<String, Object>>> find(Map<String, Object> filter, FindOptions options) {
        return findCursor(filter, options).thenCompose(MongoCursor::toList);
    }

    /** The matching documents as records. */
    public <T> CompletableFuture<List<T>> find(Map<String, Object> filter, Class<T> type) {
        return find(filter, FindOptions.defaults(), type);
    }

    public <T> CompletableFuture<List<T>> find(Map<String, Object> filter, FindOptions options, Class<T> type) {
        return findCursor(filter, options).thenCompose(c -> c.toList(type));
    }

    /** The first matching document, or null. */
    public CompletableFuture<Map<String, Object>> findOne(Map<String, Object> filter) {
        return findOne(filter, FindOptions.defaults());
    }

    public CompletableFuture<Map<String, Object>> findOne(Map<String, Object> filter, FindOptions options) {
        return find(filter, options.withLimit(1).withBatchSize(0)).thenApply(l -> l.isEmpty() ? null : l.get(0));
    }

    public <T> CompletableFuture<T> findOne(Map<String, Object> filter, Class<T> type) {
        return findOne(filter).thenApply(d -> d == null ? null : Bson.toRecord(type, d));
    }

    /** The document with this {@code _id}, or null. */
    public CompletableFuture<Map<String, Object>> findById(Object id) {
        return findOne(Bson.doc("_id", id));
    }

    public <T> CompletableFuture<T> findById(Object id, Class<T> type) {
        return findOne(Bson.doc("_id", id), type);
    }

    /** A cursor over the matching documents, batch by batch ({@link FindOptions#batchSize}). */
    public CompletableFuture<MongoCursor> findCursor(Map<String, Object> filter, FindOptions options) {
        Map<String, Object> find = Bson.doc("find", name, "filter", filter == null ? Map.of() : filter);
        options.applyTo(find);
        if (options.limit() == 1) {
            find.put("singleBatch", true);
        }
        return client.cursor(db, find, options.batchSize(), session);
    }

    // ---- count, distinct, aggregate ----

    /** The number of documents matching {@code filter} (null for all). */
    public CompletableFuture<Long> count(Map<String, Object> filter) {
        Map<String, Object> count = Bson.doc("count", name);
        if (filter != null && !filter.isEmpty()) {
            count.put("query", filter);
        }
        return read(count).thenApply(r -> r.get("n") instanceof Number n ? n.longValue() : 0L);
    }

    /** The distinct values of {@code field} among the documents matching {@code filter} (null for all). */
    public CompletableFuture<List<Object>> distinct(String field, Map<String, Object> filter) {
        Map<String, Object> distinct = Bson.doc("distinct", name, "key", field);
        if (filter != null && !filter.isEmpty()) {
            distinct.put("query", filter);
        }
        return read(distinct).thenApply(r -> r.get("values") instanceof List<?> l ? new ArrayList<Object>(l) : List.of());
    }

    /** Runs the pipeline and gathers every result. */
    public CompletableFuture<List<Map<String, Object>>> aggregate(List<?> pipeline) {
        return aggregateCursor(pipeline, 0).thenCompose(MongoCursor::toList);
    }

    /** Runs the pipeline, results batch by batch (0 for the server's default batch size). */
    public CompletableFuture<MongoCursor> aggregateCursor(List<?> pipeline, int batchSize) {
        Map<String, Object> cursor = batchSize > 0 ? Bson.doc("batchSize", batchSize) : Bson.doc();
        return client.cursor(db, Bson.doc("aggregate", name, "pipeline", pipeline, "cursor", cursor), batchSize, session);
    }

    // ---- update, delete ----

    /** Applies {@code update} (operators: {@code $set}, {@code $inc}, ... or a pipeline) to the first document matching {@code filter}. */
    public CompletableFuture<UpdateResult> updateOne(Map<String, Object> filter, Object update) {
        return update(filter, update, false, false);
    }

    /** With {@code upsert}: inserts when nothing matches ({@link UpdateResult#upsertedId}). */
    public CompletableFuture<UpdateResult> updateOne(Map<String, Object> filter, Object update, boolean upsert) {
        return update(filter, update, false, upsert);
    }

    public CompletableFuture<UpdateResult> updateMany(Map<String, Object> filter, Object update) {
        return update(filter, update, true, false);
    }

    public CompletableFuture<UpdateResult> updateMany(Map<String, Object> filter, Object update, boolean upsert) {
        return update(filter, update, true, upsert);
    }

    /** Replaces the first matching document with {@code replacement} (a whole document without operators; a {@code Map} or a record). */
    public CompletableFuture<UpdateResult> replaceOne(Map<String, Object> filter, Object replacement) {
        return replaceOne(filter, replacement, false);
    }

    public CompletableFuture<UpdateResult> replaceOne(Map<String, Object> filter, Object replacement, boolean upsert) {
        Object doc = replacement instanceof Record r ? Bson.fromRecord(r) : replacement;
        return update(filter, doc, false, upsert);
    }

    private CompletableFuture<UpdateResult> update(Map<String, Object> filter, Object update, boolean multi, boolean upsert) {
        Map<String, Object> u = Bson.doc("q", filter == null ? Map.of() : filter, "u", update, "multi", multi, "upsert", upsert);
        return write(Bson.doc("update", name, "updates", List.of(u))).thenApply(r -> {
            checkWrites(r);
            Object upsertedId = null;
            if (r.get("upserted") instanceof List<?> l && !l.isEmpty() && l.get(0) instanceof Map<?, ?> m) {
                upsertedId = m.get("_id");
            }
            return new UpdateResult(number(r.get("n")) - (upsertedId != null ? 1 : 0), number(r.get("nModified")), upsertedId);   // n counts the upserted document too
        });
    }

    /** Deletes the first matching document; how many were deleted (0 or 1). */
    public CompletableFuture<Long> deleteOne(Map<String, Object> filter) {
        return delete(filter, 1);
    }

    /** Deletes every matching document (an empty filter: all of them); how many. */
    public CompletableFuture<Long> deleteMany(Map<String, Object> filter) {
        return delete(filter, 0);
    }

    private CompletableFuture<Long> delete(Map<String, Object> filter, int limit) {
        Map<String, Object> d = Bson.doc("q", filter == null ? Map.of() : filter, "limit", limit);
        return write(Bson.doc("delete", name, "deletes", List.of(d))).thenApply(r -> {
            checkWrites(r);
            return number(r.get("n"));
        });
    }

    private static long number(Object v) {
        return v instanceof Number n ? n.longValue() : 0L;
    }

    // ---- find and modify ----

    /** Applies {@code update} to the first matching document and gives it back as it was before, or null when nothing matched. */
    public CompletableFuture<Map<String, Object>> findOneAndUpdate(Map<String, Object> filter, Object update) {
        return findOneAndUpdate(filter, update, false, false, null);
    }

    /** With {@code returnNew} (the document after the update), {@code upsert}, and a {@code sort} to pick which document when several match. */
    public CompletableFuture<Map<String, Object>> findOneAndUpdate(Map<String, Object> filter, Object update, boolean returnNew, boolean upsert, Map<String, Object> sort) {
        Map<String, Object> cmd = Bson.doc("findAndModify", name, "query", filter == null ? Map.of() : filter, "update", update, "new", returnNew, "upsert", upsert);
        if (sort != null) {
            cmd.put("sort", sort);
        }
        return write(cmd).thenApply(MongoCollection::value);
    }

    /** Deletes the first matching document and gives it back, or null. */
    public CompletableFuture<Map<String, Object>> findOneAndDelete(Map<String, Object> filter) {
        return write(Bson.doc("findAndModify", name, "query", filter == null ? Map.of() : filter, "remove", true)).thenApply(MongoCollection::value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> value(Map<String, Object> reply) {
        return reply.get("value") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
    }

    // ---- indexes, collection ----

    /** Creates an index on {@code keys} ({@code Bson.doc("email", 1)}, {@code Bson.doc("a", 1, "b", -1)}); the index name. Creating one that exists is fine. */
    public CompletableFuture<String> createIndex(Map<String, Object> keys) {
        return createIndex(keys, Map.of());
    }

    /** With options: {@code unique}, {@code sparse}, {@code name}, {@code expireAfterSeconds}, {@code partialFilterExpression}, ... */
    public CompletableFuture<String> createIndex(Map<String, Object> keys, Map<String, Object> options) {
        Map<String, Object> index = new LinkedHashMap<>();
        index.put("key", keys);
        String indexName = options.get("name") instanceof String s ? s : defaultIndexName(keys);
        index.put("name", indexName);
        options.forEach((k, v) -> {
            if (!k.equals("name") && !k.equals("key")) {
                index.put(k, v);
            }
        });
        return other(Bson.doc("createIndexes", name, "indexes", List.of(index))).thenApply(r -> indexName);
    }

    static String defaultIndexName(Map<String, Object> keys) {
        StringBuilder sb = new StringBuilder();
        keys.forEach((k, v) -> sb.append(sb.length() > 0 ? "_" : "").append(k).append('_').append(v instanceof Number n ? (n.doubleValue() == (long) n.doubleValue() ? Long.toString(n.longValue()) : n.toString()) : v));
        return sb.toString();
    }

    public CompletableFuture<Void> dropIndex(String indexName) {
        return other(Bson.doc("dropIndexes", name, "index", indexName)).thenApply(r -> null);
    }

    /** The indexes ({@code name}, {@code key}, {@code unique}, ...). */
    public CompletableFuture<List<Map<String, Object>>> listIndexes() {
        return client.cursor(db, Bson.doc("listIndexes", name), 0, session).thenCompose(MongoCursor::toList);
    }

    /** Drops the collection; false when there was none. */
    public CompletableFuture<Boolean> drop() {
        return other(Bson.doc("drop", name)).handle((r, err) -> {
            if (err != null) {
                Throwable t = err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err;
                if (t instanceof MongoException m && m.code() == 26) {
                    return false;                                  // NamespaceNotFound
                }
                throw t instanceof RuntimeException re ? re : new java.util.concurrent.CompletionException(t);
            }
            return r.containsKey("ns") || r.containsKey("nIndexesWas");    // newer servers answer ok for a missing collection, without these
        });
    }

    /** A {@code maxTimeMS} for the next find: convenience for {@code FindOptions.defaults().withMaxTime(d)}. */
    public static FindOptions within(Duration d) {
        return FindOptions.defaults().withMaxTime(d);
    }
}
