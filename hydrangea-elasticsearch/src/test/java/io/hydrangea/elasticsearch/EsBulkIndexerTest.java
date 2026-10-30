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

class EsBulkIndexerTest {

    private static EventLoopGroup group;
    private static EsClient es;
    private static final List<String> created = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        Es.require();
        group = new EventLoopGroup(1);
        es = EsClient.create(group, Es.config());
    }

    @AfterAll
    static void tearDown() {
        for (String i : created) {
            try {
                es.deleteIndex(i).get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // gone already
            }
        }
        es.close();
        group.close();
    }

    private static String newIndex(String prefix) throws Exception {
        String name = Es.index(prefix);
        es.createIndex(name, Json.obj("settings", Json.obj("number_of_shards", 1, "number_of_replicas", 0, "refresh_interval", "-1"))).get(10, TimeUnit.SECONDS);
        created.add(name);
        return name;
    }

    @Test
    void batchesBySizeAndSendsTheRestOnClose() throws Exception {
        String idx = newIndex("ix");
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(100).withFlushInterval(null));
        List<CompletableFuture<EsBulkResponse.Item>> fs = new ArrayList<>();
        for (int i = 0; i < 1234; i++) {
            fs.add(ix.add(BulkOp.index(idx, Integer.toString(i), Json.obj("n", i))));
        }
        ix.close().get(30, TimeUnit.SECONDS);
        assertEquals(0, ix.pending());
        for (int i = 0; i < fs.size(); i++) {
            EsBulkResponse.Item item = fs.get(i).get(1, TimeUnit.SECONDS);
            assertEquals("created", item.result(), "item " + i);
            assertEquals(Integer.toString(i), item.id());
        }
        es.refresh(idx).get(10, TimeUnit.SECONDS);
        assertEquals(1234, es.count(idx, null).get(10, TimeUnit.SECONDS));
        ExecutionException late = assertThrows(ExecutionException.class, () -> ix.add(BulkOp.index(idx, "x", Json.obj())).get(1, TimeUnit.SECONDS));
        assertEquals("indexer_closed", ((EsException) late.getCause()).type());
    }

    @Test
    void aPartialBatchGoesWhenTheIntervalPasses() throws Exception {
        String idx = newIndex("timer");
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(1000).withFlushInterval(Duration.ofMillis(200)));
        long t0 = System.nanoTime();
        CompletableFuture<EsBulkResponse.Item> f = ix.add(BulkOp.index(idx, "1", Json.obj("n", 1)));
        assertEquals("created", f.get(5, TimeUnit.SECONDS).result());
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 150 && ms < 3000, "sent by the timer, not at once nor never: " + ms + " ms");
        ix.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void batchesByBytes() throws Exception {
        String idx = newIndex("bytes");
        String big = "x".repeat(10_000);
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(1000).withMaxBytes(25_000).withFlushInterval(null).withMaxInFlight(2));
        List<CompletableFuture<EsBulkResponse.Item>> fs = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            fs.add(ix.add(BulkOp.index(idx, Integer.toString(i), Json.obj("big", big))));
        }
        assertEquals("created", fs.get(0).get(10, TimeUnit.SECONDS).result(), "the first two filled a request and went without a flush");
        ix.flush().get(10, TimeUnit.SECONDS);
        for (var f : fs) {
            assertEquals("created", f.get(1, TimeUnit.SECONDS).result());
        }
        es.refresh(idx).get(10, TimeUnit.SECONDS);
        assertEquals(7, es.count(idx, null).get(10, TimeUnit.SECONDS));
        ix.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void aFailedItemFailsItsFutureOnly() throws Exception {
        String idx = newIndex("fail");
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withFlushInterval(null));
        CompletableFuture<EsBulkResponse.Item> ok = ix.add(BulkOp.index(idx, "1", Json.obj("n", 1)));
        CompletableFuture<EsBulkResponse.Item> bad = ix.add(BulkOp.update(idx, "missing", Json.obj("n", 2)));
        CompletableFuture<EsBulkResponse.Item> ok2 = ix.add(BulkOp.delete(idx, "never"));
        ix.flush().get(10, TimeUnit.SECONDS);
        assertEquals("created", ok.get(1, TimeUnit.SECONDS).result());
        assertEquals("not_found", ok2.get(1, TimeUnit.SECONDS).result());
        ExecutionException e = assertThrows(ExecutionException.class, () -> bad.get(1, TimeUnit.SECONDS));
        EsException ee = assertInstanceOf(EsException.class, e.getCause());
        assertTrue(ee.isNotFound());
        assertEquals("document_missing_exception", ee.type());
        ix.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void refusesPastMaxPending() throws Exception {
        String idx = newIndex("full");
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(1000).withFlushInterval(null).withMaxPending(5));
        List<CompletableFuture<EsBulkResponse.Item>> fs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            fs.add(ix.add(BulkOp.index(idx, Integer.toString(i), Json.obj("n", i))));
        }
        ExecutionException e = assertThrows(ExecutionException.class, () -> ix.add(BulkOp.index(idx, "6", Json.obj())).get(1, TimeUnit.SECONDS));
        assertEquals("indexer_full", ((EsException) e.getCause()).type());
        assertEquals(5, ix.pending());
        ix.flush().get(10, TimeUnit.SECONDS);
        assertEquals(0, ix.pending());
        CompletableFuture<EsBulkResponse.Item> again = ix.add(BulkOp.index(idx, "6", Json.obj()));
        ix.flush().get(10, TimeUnit.SECONDS);
        assertEquals("created", again.get(1, TimeUnit.SECONDS).result(), "room again");
        ix.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void params() throws Exception {
        String idx = newIndex("params");
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withParams(Map.of("refresh", "true")));
        ix.add(BulkOp.index(idx, "1", Json.obj("n", 1)));
        ix.close().get(10, TimeUnit.SECONDS);
        assertEquals(1, es.count(idx, null).get(10, TimeUnit.SECONDS), "visible without a refresh call: the request carried refresh=true");
    }
}
