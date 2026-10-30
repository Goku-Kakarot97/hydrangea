package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.elasticsearch.BulkOp;
import io.hydrangea.elasticsearch.EsBulkIndexer;
import io.hydrangea.elasticsearch.EsClient;
import io.hydrangea.elasticsearch.EsConfig;
import io.hydrangea.elasticsearch.EsHit;
import io.hydrangea.elasticsearch.EsSearchResponse;
import io.hydrangea.elasticsearch.Json;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch: an index, documents through the bulk indexer, a search with an aggregation, a scroll over everything, and clean-up.
 * Needs an Elasticsearch on localhost:29200 with security off ({@code hydrangea-elasticsearch/src/test/resources/elasticsearch/start-elasticsearch.sh}),
 * or set {@code ES_URL} (and {@code ES_USER}/{@code ES_PASSWORD}).
 */
public final class EsExample {

    /** A document as a record: written with {@code index}/{@code BulkOp.index}, read back with {@code get(..., Product.class)}, {@code sources(Product.class)}. */
    record Product(String name, String category, double price) {
    }

    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault("ES_URL", "http://localhost:29200");
        EsConfig config = EsConfig.of(url);
        if (System.getenv("ES_USER") != null) {
            config = config.withBasicAuth(System.getenv("ES_USER"), System.getenv("ES_PASSWORD"));
        }
        try (EventLoopGroup group = new EventLoopGroup(2); EsClient es = EsClient.connect(group, config).join()) {
            System.out.println("cluster " + Json.at(es.info().join(), "version", "number"));
            String index = "hydra-example";
            if (es.indexExists(index).join()) {
                es.deleteIndex(index).join();
            }
            es.createIndex(index, Json.obj("mappings", Json.obj("properties", Json.obj(
                    "name", Json.obj("type", "text"),
                    "category", Json.obj("type", "keyword"),
                    "price", Json.obj("type", "double"))))).join();

            // bulk indexer: gathers actions into requests of 500, sends them as they fill or every second
            EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(500));
            String[] categories = {"lamp", "chair", "table"};
            for (int i = 0; i < 2000; i++) {
                ix.add(BulkOp.index(index, Integer.toString(i), new Product(categories[i % 3] + " model " + i, categories[i % 3], 10 + i % 90)));
            }
            ix.close().join();
            es.refresh(index).join();

            // one document
            es.update(index, "7", Map.of("price", 99.5)).join();
            System.out.println("doc 7: " + es.get(index, "7", Product.class).join());

            // search with an aggregation
            EsSearchResponse r = es.search(index, Json.obj(
                    "size", 3,
                    "query", Json.obj("bool", Json.obj("must", Json.obj("match", Json.obj("name", "lamp")), "filter", Json.obj("range", Json.obj("price", Json.obj("gte", 50))))),
                    "sort", List.of(Json.obj("price", "desc")),
                    "aggs", Json.obj("by_category", Json.obj("terms", Json.obj("field", "category")), "avg_price", Json.obj("avg", Json.obj("field", "price"))))).join();
            System.out.println(r.total() + " lamps at 50 or more; the dearest:");
            for (EsHit h : r.hits()) {
                System.out.println("  " + h.id() + " " + h.source(Product.class));
            }
            System.out.println("per category: " + r.buckets("by_category"));
            System.out.println("average price: " + r.aggregation("avg_price").get("value"));

            // every document, 500 at a time
            long n = es.scroll(index, Json.obj("size", 500, "_source", false), Duration.ofMinutes(1)).forEach(h -> { }).join();
            System.out.println("scrolled " + n + " documents");

            es.deleteIndex(index).join();
        }
    }
}
