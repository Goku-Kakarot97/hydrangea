package io.hydrangea.kafka;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the producer and the consumer count, against a real broker (see {@code start-kafka.sh}). Skipped when none listens. */
class KafkaMetricsTest {

    private static final class Capture implements MetricsSink {
        final Map<String, DoubleSupplier> meters = new ConcurrentHashMap<>();

        private static String key(String name, String[] tags) {
            return tags.length == 0 ? name : name + "{" + String.join(",", tags) + "}";
        }

        @Override
        public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
            meters.put(key(name, tags), total);
        }

        @Override
        public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
            meters.put(key(name, tags), value);
        }

        @Override
        public Counter counter(String name, String unit, String... tags) {
            return NOOP.counter(name, unit, tags);
        }

        @Override
        public Timer timer(String name, String... tags) {
            return NOOP.timer(name, tags);
        }

        double get(String key) {
            DoubleSupplier s = meters.get(key);
            if (s == null) {
                throw new AssertionError("no meter " + key + "; have " + meters.keySet());
            }
            return s.getAsDouble();
        }
    }

    private static void eventually(BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    private static <T> T get(CompletableFuture<T> f) throws Exception {
        return f.get(30, TimeUnit.SECONDS);
    }

    @Test
    void producerCountsRecordsBytesBatchTimeAndRejections() throws Exception {
        Kafka.require(Kafka.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            KafkaClient kafka = KafkaClient.create(group, Kafka.config());
            String topic = Kafka.topic("metrics-p");
            get(kafka.createTopic(topic, 1, 1));
            try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withMaxRequestBytes(100_000))) {
                List<CompletableFuture<RecordMetadata>> sent = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    sent.add(p.send(topic, "k" + i, "value-" + i));
                }
                for (var f : sent) {
                    get(f);
                }
                eventually(() -> sink.get("kafka.producer.records{outcome,ok}") == 100, "100 acknowledged records");
                assertEquals(0, sink.get("kafka.producer.records{outcome,failed}"));
                assertTrue(sink.get("kafka.producer.bytes") > 100 * 8, "wire bytes: " + sink.get("kafka.producer.bytes"));
                assertTrue(sink.get("kafka.producer.batch.duration.count") >= 1);
                assertTrue(sink.get("kafka.producer.batch.duration.sum") > 0);

                var tooBig = p.send(topic, null, new byte[200_000]);
                assertTrue(tooBig.isCompletedExceptionally());
                assertEquals(1, sink.get("kafka.producer.rejected{reason,too_large}"));
                assertEquals(0, sink.get("kafka.producer.rejected{reason,buffer_full}"));
            }
            kafka.close();
        }
    }

    @Test
    void consumerCountsRecordsBytesFetchesAndTracksLag() throws Exception {
        Kafka.require(Kafka.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            KafkaClient kafka = KafkaClient.create(group, Kafka.config());
            String topic = Kafka.topic("metrics-c");
            get(kafka.createTopic(topic, 1, 1));
            try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
                List<CompletableFuture<RecordMetadata>> sent = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    sent.add(p.send(topic, "k", "v" + i));
                }
                for (var f : sent) {
                    get(f);
                }
            }
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            try (KafkaConsumer c = kafka.consumer(ConsumerConfig.defaults().withBuffer(500_000, 10))) {
                get(c.assign(List.of(new TopicPartition(topic, 0))));
                List<ConsumerRecord> first = new ArrayList<>();
                while (first.isEmpty()) {
                    first.addAll(get(c.poll(Duration.ofSeconds(1))));
                }
                assertEquals(10, first.size());
                eventually(() -> sink.get("kafka.consumer.fetches") >= 1, "a fetch answer");
                assertEquals(100 - first.size(), sink.get("kafka.consumer.lag"), "the broker has 100, poll has returned 10");
                int got = first.size();
                long end = System.currentTimeMillis() + 20_000;
                while (got < 100 && System.currentTimeMillis() < end) {
                    got += get(c.poll(Duration.ofSeconds(1))).size();
                }
                assertEquals(100, got);
                assertEquals(100, sink.get("kafka.consumer.records"));
                assertTrue(sink.get("kafka.consumer.bytes") > 0);
                assertEquals(0, sink.get("kafka.consumer.lag"), "everything has been read");
                assertEquals(0, sink.get("kafka.consumer.fetch.errors"));
            }
            kafka.close();
        }
    }
}
