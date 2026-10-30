package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.mongodb.Bson;
import io.hydrangea.mongodb.FindOptions;
import io.hydrangea.mongodb.MongoClient;
import io.hydrangea.mongodb.MongoCollection;
import io.hydrangea.mongodb.MongoConfig;
import io.hydrangea.mongodb.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MongoDB: documents as maps and as records, a unique index, updates, an aggregation, a cursor over everything, and clean-up.
 * Needs a MongoDB on localhost:29017 with user hydra/hydra ({@code hydrangea-mongodb/src/test/resources/mongodb/start-mongodb.sh}), or set {@code MONGO_URL}
 * ({@code mongodb://user:pw@host:27017/db?authSource=admin}).
 */
public final class MongoExample {

    enum Category { LAMP, CHAIR, TABLE }

    /** A document as a record: {@code _id} under its own name, generated when null. */
    record Product(@Bson.Name("_id") ObjectId id, String name, Category category, double price, List<String> tags) {
    }

    public static void main(String[] args) throws Exception {
        MongoConfig config = MongoConfig.parse(System.getenv().getOrDefault("MONGO_URL", "mongodb://hydra:hydra@localhost:29017/hydra?authSource=admin")).withConnections(2);
        try (EventLoopGroup group = new EventLoopGroup(2); MongoClient mongo = MongoClient.connect(group, config).join()) {
            System.out.println("MongoDB " + mongo.serverVersion().join());
            MongoCollection products = mongo.collection("example_products");
            products.drop().join();
            products.createIndex(Bson.doc("name", 1), Map.of("unique", true)).join();

            // records in: 2000 of them, in batches of 500
            Category[] categories = Category.values();
            for (int batch = 0; batch < 4; batch++) {
                List<Product> docs = new ArrayList<>();
                for (int i = batch * 500; i < (batch + 1) * 500; i++) {
                    docs.add(new Product(null, categories[i % 3].name().toLowerCase() + " model " + i, categories[i % 3], 10 + i % 90, List.of("tag" + i % 5)));
                }
                products.insertMany(docs).join();
            }
            System.out.println(products.count(null).join() + " products");

            // one document: an update, read back as a record
            products.updateOne(Bson.doc("name", "chair model 7"), Bson.doc("$set", Bson.doc("price", 99.5), "$push", Bson.doc("tags", "sale"))).join();
            System.out.println("chair 7: " + products.findOne(Bson.doc("name", "chair model 7"), Product.class).join());

            // a query with sort, projection and limit
            List<Map<String, Object>> dearest = products.find(Bson.doc("category", "LAMP", "price", Bson.doc("$gte", 50)),
                    FindOptions.defaults().withSort(Bson.doc("price", -1)).withLimit(3).withProjection(Bson.doc("name", 1, "price", 1, "_id", 0))).join();
            System.out.println("dearest lamps at 50 or more: " + dearest);

            // an aggregation
            List<Map<String, Object>> perCategory = products.aggregate(List.of(
                    Bson.doc("$group", Bson.doc("_id", "$category", "count", Bson.doc("$sum", 1), "avgPrice", Bson.doc("$avg", "$price"))),
                    Bson.doc("$sort", Bson.doc("_id", 1)))).join();
            System.out.println("per category: " + perCategory);

            // every document, 500 at a time, as records
            double[] total = {0};
            long n = products.findCursor(null, FindOptions.defaults().withBatchSize(500)).join().forEach(Product.class, p -> total[0] += p.price()).join();
            System.out.printf("walked %d products, total price %.1f%n", n, total[0]);

            products.drop().join();
        }
    }
}
