package io.hydrangea.elasticsearch;

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

import static org.junit.jupiter.api.Assertions.*;

/** Against a real Elasticsearch ({@code start-elasticsearch.sh}). */
class EsClientTest {

    private static EventLoopGroup group;
    private static EsClient es;
    private static final List<String> created = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        Es.require();
        group = new EventLoopGroup(2);
        es = EsClient.connect(group, Es.config()).get(10, TimeUnit.SECONDS);
    }

    @AfterAll
    static void tearDown() {
        if (es != null) {
            for (String i : created) {
                try {
                    es.deleteIndex(i).get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // gone already
                }
            }
            es.close();
        }
        if (group != null) {
            group.close();
        }
    }

    private static String newIndex(String prefix) throws Exception {
        String name = Es.index(prefix);
        es.createIndex(name, Json.obj("settings", Json.obj("number_of_shards", 1, "number_of_replicas", 0))).get(10, TimeUnit.SECONDS);
        created.add(name);
        return name;
    }

    private static EsException failure(CompletableFuture<?> f) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertInstanceOf(EsException.class, e.getCause(), String.valueOf(e.getCause()));
        return (EsException) e.getCause();
    }

    @Test
    void infoAndHealth() throws Exception {
        Map<String, Object> info = es.info().get(10, TimeUnit.SECONDS);
        assertNotNull(Json.at(info, "version", "number"), info.toString());
        Map<String, Object> health = es.health().get(10, TimeUnit.SECONDS);
        assertTrue(List.of("green", "yellow").contains(Json.str(health.get("status"))), health.toString());
    }

    @Test
    void indexLifecycle() throws Exception {
        String name = Es.index("life");
        assertFalse(es.indexExists(name).get(10, TimeUnit.SECONDS));
        es.createIndex(name, Json.obj("mappings", Json.obj("properties", Json.obj("price", Json.obj("type", "integer"))))).get(10, TimeUnit.SECONDS);
        assertTrue(es.indexExists(name).get(10, TimeUnit.SECONDS));
        EsException again = failure(es.createIndex(name, null));
        assertEquals(400, again.status());
        assertEquals("resource_already_exists_exception", again.type(), again.getMessage());
        es.deleteIndex(name).get(10, TimeUnit.SECONDS);
        assertFalse(es.indexExists(name).get(10, TimeUnit.SECONDS));
        EsException gone = failure(es.deleteIndex(name));
        assertTrue(gone.isNotFound());
        assertEquals("index_not_found_exception", gone.type());
    }

    @Test
    void documentCrud() throws Exception {
        String idx = newIndex("crud");
        EsWriteResponse w = es.index(idx, "1", Json.obj("name", "lamp", "price", 20)).get(10, TimeUnit.SECONDS);
        assertTrue(w.created());
        assertEquals("1", w.id());
        assertEquals(1, w.version());

        EsDocument d = es.get(idx, "1").get(10, TimeUnit.SECONDS);
        assertTrue(d.found());
        assertEquals("lamp", d.field("name"));
        assertEquals(20L, d.field("price"));
        assertEquals(w.seqNo(), d.seqNo());

        EsDocument missing = es.get(idx, "nope").get(10, TimeUnit.SECONDS);
        assertFalse(missing.found());
        assertEquals(Map.of(), missing.source());
        assertFalse(es.exists(idx, "nope").get(10, TimeUnit.SECONDS));
        assertTrue(es.exists(idx, "1").get(10, TimeUnit.SECONDS));

        EsWriteResponse u = es.update(idx, "1", Json.obj("price", 25)).get(10, TimeUnit.SECONDS);
        assertEquals("updated", u.result());
        assertEquals(2, u.version());
        assertEquals(25L, es.get(idx, "1").get(10, TimeUnit.SECONDS).field("price"));
        assertEquals("lamp", es.get(idx, "1").get(10, TimeUnit.SECONDS).field("name"), "a partial update keeps the other fields");
        assertEquals("noop", es.update(idx, "1", Json.obj("price", 25)).get(10, TimeUnit.SECONDS).result());

        EsException notThere = failure(es.update(idx, "nope", Json.obj("price", 1)));
        assertTrue(notThere.isNotFound());
        assertEquals("document_missing_exception", notThere.type());
        EsWriteResponse upserted = es.updateWith(idx, "nope", Json.obj("doc", Json.obj("price", 1), "doc_as_upsert", true), null).get(10, TimeUnit.SECONDS);
        assertTrue(upserted.created());

        EsWriteResponse generated = es.index(idx, null, Json.obj("name", "no id")).get(10, TimeUnit.SECONDS);
        assertNotNull(generated.id());
        assertTrue(generated.created());

        EsException conflict = failure(es.create(idx, "1", Json.obj("name", "again")));
        assertTrue(conflict.isConflict());
        assertEquals("version_conflict_engine_exception", conflict.type());

        assertTrue(es.delete(idx, "1").get(10, TimeUnit.SECONDS).deleted());
        assertEquals("not_found", es.delete(idx, "1").get(10, TimeUnit.SECONDS).result());
        assertFalse(es.exists(idx, "1").get(10, TimeUnit.SECONDS));
    }

    @Test
    void optimisticConcurrency() throws Exception {
        String idx = newIndex("occ");
        es.index(idx, "1", Json.obj("n", 1)).get(10, TimeUnit.SECONDS);
        EsDocument d = es.get(idx, "1").get(10, TimeUnit.SECONDS);
        Map<String, String> ifUnchanged = Map.of("if_seq_no", Long.toString(d.seqNo()), "if_primary_term", Long.toString(d.primaryTerm()));
        es.index(idx, "1", Json.obj("n", 2), ifUnchanged).get(10, TimeUnit.SECONDS);
        EsException stale = failure(es.index(idx, "1", Json.obj("n", 3), ifUnchanged));
        assertTrue(stale.isConflict());
        assertEquals(2L, es.get(idx, "1").get(10, TimeUnit.SECONDS).field("n"));
    }

    @Test
    void idsAndIndexNamesAreEncoded() throws Exception {
        String idx = newIndex("enc");
        for (String id : List.of("a/b", "x?y=1&z=2", "sp ace", "ü-ñ-日本", "../../_cluster/settings", "_search", "100%", "#frag")) {
            assertTrue(es.index(idx, id, Json.obj("id", id), Map.of("refresh", "true")).get(10, TimeUnit.SECONDS).created(), id);
            EsDocument d = es.get(idx, id).get(10, TimeUnit.SECONDS);
            assertTrue(d.found(), id);
            assertEquals(id, d.id());
            assertEquals(id, d.field("id"));
        }
        assertEquals(8, es.count(idx, null).get(10, TimeUnit.SECONDS));
        EsException bad = failure(es.createIndex("Upper Case", null));
        assertEquals(400, bad.status());
        assertEquals("invalid_index_name_exception", bad.type(), bad.getMessage());
    }

    @Test
    void searchSortAggregationsAndSearchAfter() throws Exception {
        String idx = newIndex("search");
        List<BulkOp> ops = new ArrayList<>();
        String[] colors = {"red", "green", "blue"};
        for (int i = 0; i < 30; i++) {
            ops.add(BulkOp.index(idx, Integer.toString(i), Json.obj("n", i, "color", colors[i % 3], "text", "item number " + i + (i % 10 == 0 ? " special" : ""))));
        }
        EsBulkResponse b = es.bulk(ops, Map.of("refresh", "true")).get(10, TimeUnit.SECONDS);
        assertFalse(b.errors(), b.failures().toString());
        assertEquals(30, b.items().size());

        EsSearchResponse r = es.search(idx, Json.obj("query", Json.obj("match", Json.obj("text", "special")))).get(10, TimeUnit.SECONDS);
        assertEquals(3, r.total());
        assertEquals("eq", r.totalRelation());
        assertEquals(3, r.hits().size());
        assertFalse(Double.isNaN(r.hits().get(0).score()));
        assertEquals(idx, r.hits().get(0).index());

        EsSearchResponse sorted = es.search(idx, Json.obj("size", 10, "sort", Json.arr(Json.obj("n", "desc")), "query", Json.obj("match_all", Json.obj()))).get(10, TimeUnit.SECONDS);
        assertEquals(30, sorted.total());
        assertEquals(29L, sorted.hits().get(0).field("n"));
        assertEquals(List.of(20L), sorted.lastSort());
        EsSearchResponse page2 = es.search(idx, Json.obj("size", 10, "sort", Json.arr(Json.obj("n", "desc")), "search_after", sorted.lastSort())).get(10, TimeUnit.SECONDS);
        assertEquals(19L, page2.hits().get(0).field("n"));
        assertEquals(10L, page2.hits().get(9).field("n"));

        EsSearchResponse agg = es.search(idx, Json.obj("size", 0, "aggs", Json.obj("by_color", Json.obj("terms", Json.obj("field", "color.keyword")),
                "avg_n", Json.obj("avg", Json.obj("field", "n"))))).get(10, TimeUnit.SECONDS);
        assertEquals(0, agg.hits().size());
        assertEquals(3, agg.buckets("by_color").size());
        assertEquals(10L, Json.at(agg.buckets("by_color").get(0), "doc_count"));
        assertEquals(14.5, Json.dbl(agg.aggregation("avg_n").get("value")));

        assertEquals(10, es.count(idx, Json.obj("query", Json.obj("term", Json.obj("color.keyword", "red")))).get(10, TimeUnit.SECONDS));
        assertEquals(30, es.count(null, Json.obj("query", Json.obj("term", Json.obj("_index", idx)))).get(10, TimeUnit.SECONDS), "a count over every index");

        EsException badQuery = failure(es.search(idx, Json.obj("query", Json.obj("no_such_query", Json.obj()))));
        assertEquals(400, badQuery.status());
        assertEquals("parsing_exception", badQuery.type(), badQuery.getMessage());

        assertEquals(10, es.deleteByQuery(idx, Json.obj("query", Json.obj("term", Json.obj("color.keyword", "red")))).get(10, TimeUnit.SECONDS));
        es.refresh(idx).get(10, TimeUnit.SECONDS);
        assertEquals(20, es.count(idx, null).get(10, TimeUnit.SECONDS));
    }

    @Test
    void bulkReportsEachItem() throws Exception {
        String idx = newIndex("bulk");
        es.index(idx, "exists", Json.obj("v", 1)).get(10, TimeUnit.SECONDS);
        EsBulkResponse b = es.bulk(List.of(
                BulkOp.index(idx, "a", Json.obj("v", 1)),
                BulkOp.create(idx, "exists", Json.obj("v", 2)),                 // 409
                BulkOp.update(idx, "missing", Json.obj("v", 3)),                 // 404
                BulkOp.delete(idx, "exists"),
                BulkOp.updateWith(idx, "up", Json.obj("doc", Json.obj("v", 4), "doc_as_upsert", true)),
                BulkOp.index(idx, null, Json.obj("v", 5))), Map.of("refresh", "true")).get(10, TimeUnit.SECONDS);
        assertTrue(b.errors());
        assertEquals(6, b.items().size());
        List<EsBulkResponse.Item> items = b.items();
        assertEquals("created", items.get(0).result());
        assertEquals(201, items.get(0).status());
        assertEquals("index", items.get(0).action());
        assertTrue(items.get(1).failed());
        assertEquals(409, items.get(1).status());
        assertEquals("version_conflict_engine_exception", items.get(1).errorType());
        assertEquals(409, items.get(1).toException().status());
        assertTrue(items.get(2).failed());
        assertEquals(404, items.get(2).status());
        assertEquals("deleted", items.get(3).result());
        assertEquals("created", items.get(4).result());
        assertNotNull(items.get(5).id());
        assertEquals(2, b.failures().size());
        assertEquals(3, es.count(idx, null).get(10, TimeUnit.SECONDS));
    }

    @Test
    void scrollWalksEveryHit() throws Exception {
        String idx = newIndex("scroll");
        List<BulkOp> ops = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            ops.add(BulkOp.index(idx, Integer.toString(i), Json.obj("n", i)));
        }
        assertFalse(es.bulk(ops, Map.of("refresh", "true")).get(10, TimeUnit.SECONDS).errors());

        EsScroll s = es.scroll(idx, Json.obj("size", 100, "sort", Json.arr(Json.obj("n", "asc"))), Duration.ofMinutes(1));
        List<EsHit> p1 = s.next().get(10, TimeUnit.SECONDS);
        assertEquals(100, p1.size());
        assertEquals(250, s.total());
        assertEquals(0L, p1.get(0).field("n"));
        assertEquals(100, s.next().get(10, TimeUnit.SECONDS).size());
        List<EsHit> p3 = s.next().get(10, TimeUnit.SECONDS);
        assertEquals(50, p3.size());
        assertEquals(249L, p3.get(49).field("n"));
        assertEquals(0, s.next().get(10, TimeUnit.SECONDS).size());
        assertEquals(0, s.next().get(10, TimeUnit.SECONDS).size(), "the end stays the end");

        long[] sum = {0};
        long n = es.scroll(idx, Json.obj("size", 64, "query", Json.obj("range", Json.obj("n", Json.obj("lt", 200)))), Duration.ofMinutes(1))
                .forEach(h -> sum[0] += Json.lng(h.field("n"))).get(10, TimeUnit.SECONDS);
        assertEquals(200, n);
        assertEquals(199L * 200 / 2, sum[0]);

        EsScroll early = es.scroll(idx, Json.obj("size", 10), Duration.ofMinutes(1));
        assertEquals(10, early.next().get(10, TimeUnit.SECONDS).size());
        early.close().get(10, TimeUnit.SECONDS);
        assertEquals(0, early.next().get(10, TimeUnit.SECONDS).size());
    }

    @Test
    void rawRequestsReachEverythingElse() throws Exception {
        String idx = newIndex("raw");
        es.request("PUT", "/" + idx + "/_alias/" + idx + "-alias").get(10, TimeUnit.SECONDS);
        EsResponse aliases = es.request("GET", "/" + idx + "/_alias").get(10, TimeUnit.SECONDS);
        assertEquals(200, aliases.status());
        assertNotNull(aliases.at(idx, "aliases", idx + "-alias"));
        EsResponse cat = es.request("GET", "/_cat/indices/" + idx, Map.of("format", "json", "h", "index,health"), null).get(10, TimeUnit.SECONDS);
        assertEquals(idx, Json.at(cat.json(), 0, "index"));
        EsResponse health = es.request("GET", "/_cat/health").get(10, TimeUnit.SECONDS);
        assertTrue(health.isJson(), "the client asks for JSON, so even _cat answers JSON");
        assertNotNull(Json.at(health.json(), 0, "status"), String.valueOf(health.json()));
        EsException nothing = failure(es.request("GET", "/_no_such_endpoint"));
        assertTrue(nothing.isNotFound() || nothing.status() == 400, nothing.getMessage());
    }

    enum Category { LAMP, CHAIR }

    record Product(String name, Category category, double price, List<String> tags, @Json.Name("created_at") java.time.Instant createdAt) {
    }

    @Test
    void recordsGoInAndComeOut() throws Exception {
        String idx = newIndex("records");
        Product lamp = new Product("desk lamp", Category.LAMP, 20.5, List.of("led", "small"), java.time.Instant.parse("2026-01-02T03:04:05Z"));
        assertTrue(es.index(idx, "1", lamp).get(10, TimeUnit.SECONDS).created());
        assertEquals(lamp, es.get(idx, "1", Product.class).get(10, TimeUnit.SECONDS));
        assertNull(es.get(idx, "nope", Product.class).get(10, TimeUnit.SECONDS));
        assertEquals("2026-01-02T03:04:05Z", es.get(idx, "1").get(10, TimeUnit.SECONDS).field("created_at"), "written as ISO text under the annotated name");

        List<BulkOp> ops = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ops.add(BulkOp.index(idx, "b" + i, new Product("chair " + i, Category.CHAIR, 10 + i, List.of(), null)));
        }
        assertFalse(es.bulk(ops, Map.of("refresh", "true")).get(10, TimeUnit.SECONDS).errors());

        List<Product> chairs = es.search(idx, Json.obj("size", 50, "query", Json.obj("term", Json.obj("category.keyword", "CHAIR"))), Product.class).get(10, TimeUnit.SECONDS);
        assertEquals(20, chairs.size());
        assertEquals(Category.CHAIR, chairs.get(0).category());
        assertNull(chairs.get(0).createdAt());
        EsSearchResponse r = es.search(idx, Json.obj("query", Json.obj("match", Json.obj("name", "lamp")))).get(10, TimeUnit.SECONDS);
        assertEquals(List.of(lamp), r.sources(Product.class));
        assertEquals(lamp, r.hits().get(0).source(Product.class));

        double[] sum = {0};
        long n = es.scroll(idx, Json.obj("size", 7), Duration.ofMinutes(1)).forEach(Product.class, p -> sum[0] += p.price()).get(10, TimeUnit.SECONDS);
        assertEquals(21, n);
        assertEquals(20.5 + 20 * 10 + 190, sum[0], 1e-9);
    }

    @Test
    void compressedRequestsAndAnswersWork() throws Exception {
        String idx = newIndex("gzip");
        EventLoopGroup g = new EventLoopGroup(1);
        try (EsClient zipped = EsClient.create(g, Es.config().withRequestCompression(true).withCompression(true))) {
            List<BulkOp> ops = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                ops.add(BulkOp.index(idx, Integer.toString(i), Json.obj("n", i, "text", "the same words over and over ".repeat(5))));
            }
            assertFalse(zipped.bulk(ops, Map.of("refresh", "true")).get(10, TimeUnit.SECONDS).errors(), "a 100 KB body went gzipped");
            assertEquals(500, zipped.count(idx, null).get(10, TimeUnit.SECONDS));
            EsSearchResponse r = zipped.search(idx, Json.obj("size", 500)).get(10, TimeUnit.SECONDS);
            assertEquals(500, r.hits().size(), "a 100 KB answer came gzipped and was decoded");
            assertTrue(zipped.index(idx, "small", Json.obj("n", 1)).get(10, TimeUnit.SECONDS).created(), "a small body goes as it is");
        } finally {
            g.close();
        }
    }

    @Test
    void manyConcurrentCallsShareThePool() throws Exception {
        String idx = newIndex("conc");
        List<CompletableFuture<EsWriteResponse>> fs = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            fs.add(es.index(idx, Integer.toString(i), Json.obj("n", i)));
        }
        for (var f : fs) {
            assertTrue(f.get(30, TimeUnit.SECONDS).created());
        }
        es.refresh(idx).get(10, TimeUnit.SECONDS);
        assertEquals(200, es.count(idx, null).get(10, TimeUnit.SECONDS));
    }

    @Test
    void connectFailsFastWithWrongCredentialsOrNoCluster() throws Exception {
        // security is off on the test cluster: a wrong password is ignored there, so only the unreachable case is checked
        EventLoopGroup g = new EventLoopGroup(1);
        try {
            ExecutionException e = assertThrows(ExecutionException.class, () -> EsClient.connect(g, EsConfig.of("http://127.0.0.1:1").withConnectTimeout(Duration.ofSeconds(2))).get(10, TimeUnit.SECONDS));
            assertInstanceOf(EsException.class, e.getCause());
            assertEquals(0, ((EsException) e.getCause()).status());
        } finally {
            g.close();
        }
    }
}
